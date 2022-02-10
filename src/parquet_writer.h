#ifndef PARQUET_WRITER_H
#define PARQUET_WRITER_H

#include <napi.h>

#include <arrow/builder.h>
#include <parquet/arrow/writer.h>

#include <vector>

inline static ArrowFieldPtr MakeField(const std::string& name, std::shared_ptr<arrow::DataType> type) {
  return std::make_shared<arrow::Field>(name, type);
}

static ArrowFieldPtr NapiObjToArrowField(const std::string& name, const Napi::Object& obj, const arrow::Type::type type) {
  switch (type) {
  case arrow::Type::type::BOOL:
    return MakeField(name, arrow::boolean());

  case arrow::Type::type::UINT8:
    return MakeField(name, arrow::uint8());

  case arrow::Type::type::INT8:
    return MakeField(name, arrow::int8());

  case arrow::Type::type::UINT16:
    return MakeField(name, arrow::uint16());

  case arrow::Type::type::INT16:
    return MakeField(name, arrow::int16());

  case arrow::Type::type::UINT32:
    return MakeField(name, arrow::uint32());

  case arrow::Type::type::INT32:
    return MakeField(name, arrow::int32());

  case arrow::Type::type::UINT64:
    return MakeField(name, arrow::uint64());

  case arrow::Type::type::INT64:
    return MakeField(name, arrow::int64());

  case arrow::Type::type::FLOAT:
    return MakeField(name, arrow::float32());

  case arrow::Type::type::DOUBLE:
    return MakeField(name, arrow::float64());

  case arrow::Type::type::STRING:
    return MakeField(name, arrow::utf8());

  case arrow::Type::type::BINARY:
    return MakeField(name, arrow::binary());

  case arrow::Type::type::FIXED_SIZE_BINARY: {
    auto width = obj.Get("width").ToNumber().Int32Value();
    return MakeField(name, arrow::fixed_size_binary(width));
  }

  case arrow::Type::type::DATE32:
    return MakeField(name, arrow::date32());

  case arrow::Type::type::TIMESTAMP: {
    auto unit = static_cast<arrow::TimeUnit::type>(obj.Get("unit").ToNumber().Int32Value());
    return MakeField(name, arrow::timestamp(unit));
  }

  case arrow::Type::type::TIME32: {
    auto unit = static_cast<arrow::TimeUnit::type>(obj.Get("unit").ToNumber().Int32Value());
    return MakeField(name, arrow::time32(unit));
  }

  case arrow::Type::type::TIME64: {
    auto unit = static_cast<arrow::TimeUnit::type>(obj.Get("unit").ToNumber().Int32Value());
    return MakeField(name, arrow::time64(unit));
  }

  default:
    // Should only happen if JS users use a number instead of the enum
    throw std::runtime_error("Invalid type id");
  }
}

inline static ArrowFieldPtr NapiObjToArrowField(const std::string& name, const Napi::Object& obj) {
  return NapiObjToArrowField(name, obj, static_cast<arrow::Type::type>(obj.Get("type").ToNumber().Int32Value()));
}

struct Column {
  std::string key;
  arrow::Type::type type;
  std::unique_ptr<arrow::ArrayBuilder> builder;
};

template <typename T>
inline static void AppendScalar(Column& column, const T& value) {
  auto result = arrow::MakeScalar(column.builder->type(), value);
  if (result.ok()) {
    column.builder->AppendScalar(*result.ValueOrDie());
  } else {
    throw std::runtime_error("Unable to make scalar from input");
  }
}

template <>
void AppendScalar<std::string>(Column& column, const std::string& value) {
  column.builder->AppendScalar(arrow::StringScalar(value));
}

template <>
void AppendScalar<std::shared_ptr<arrow::Buffer>>(Column& column, const std::shared_ptr<arrow::Buffer>& value) {
  auto type = column.builder->type();
  if (type->id() == arrow::Type::type::BINARY) {
    column.builder->AppendScalar(arrow::BinaryScalar(value));
  } else { // FIXED_SIZE_BINARY
    if (value->size() == dynamic_cast<arrow::FixedSizeBinaryType&>(*type).byte_width()) {
      column.builder->AppendScalar(arrow::FixedSizeBinaryScalar(value, type));
    } else {
      throw std::runtime_error("FixedSizeBinary buffer is the wrong size");
    }
  }
}

class ParquetWriter : public Napi::ObjectWrap<ParquetWriter> {
protected:
  std::string filepath;
  ArrowSchemaPtr schema;
  std::vector<Column> columns;
  std::shared_ptr<arrow::io::FileOutputStream> outfile;

public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func =
      DefineClass(env,
        "ParquetWriter", {
          InstanceMethod("appendRowArray",       &ParquetWriter::AppendRowArray),
          InstanceMethod("appendRowObject",      &ParquetWriter::AppendRowObject),
          InstanceMethod("open",                 &ParquetWriter::Open),
          InstanceMethod("close",                &ParquetWriter::Close),
        });

    auto constructor = new Napi::FunctionReference();
    *constructor = Napi::Persistent(func);
    env.SetInstanceData(constructor);

    exports.Set("ParquetWriter", func);
    return exports;
  }

public:
  ParquetWriter(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<ParquetWriter>(info)
  {
    auto env = info.Env();
    if (info.Length() < 2 || !info[0].IsObject() || !info[1].IsString()) {
      Napi::TypeError::New(env, "schema:Object, path:string expected").ThrowAsJavaScriptException();
      return;
    }

    filepath = info[1].ToString().Utf8Value();

    // Build schema
    auto schema = info[0].As<Napi::Object>();
    auto keys = schema.GetPropertyNames();
    arrow::FieldVector fields;
    for (uint32_t i = 0; i < keys.Length(); i++) {
      auto name = keys.Get(i).ToString().Utf8Value();
      auto fieldObj = schema.Get(name).ToObject();
      auto type = static_cast<arrow::Type::type>(fieldObj.Get("type").ToNumber().Int32Value());

      try {
        fields.push_back(NapiObjToArrowField(name, fieldObj, type));
      } catch (const std::runtime_error& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
        return;
      }

      std::unique_ptr<arrow::ArrayBuilder> builder;

      arrow::MakeBuilder(arrow::default_memory_pool(), fields.back()->type(), &builder);
      columns.push_back(Column{std::move(name), type, std::move(builder)});
    }

    this->schema = arrow::schema(fields);
  }

  void AppendRow(const Napi::Array& row) {
    if (columns.size() != row.Length()) {
      throw std::runtime_error("Number of columns does not match schema");
    }

    for (size_t i = 0; i < columns.size(); i++) {
      switch (columns[i].type) {
      case arrow::Type::type::BOOL:
        AppendScalar(columns[i], row.Get(i).ToBoolean().Value());
        break;

      case arrow::Type::type::UINT8:
        AppendScalar(columns[i], static_cast<uint8_t>(row.Get(i).ToNumber().Uint32Value()));
        break;

      case arrow::Type::type::INT8:
        AppendScalar(columns[i], static_cast<int8_t>(row.Get(i).ToNumber().Uint32Value()));
        break;

      case arrow::Type::type::UINT16:
        AppendScalar(columns[i], static_cast<uint16_t>(row.Get(i).ToNumber().Uint32Value()));
        break;

      case arrow::Type::type::INT16:
        AppendScalar(columns[i], static_cast<int16_t>(row.Get(i).ToNumber().Uint32Value()));
        break;

      case arrow::Type::type::UINT32:
        AppendScalar(columns[i], row.Get(i).ToNumber().Uint32Value());
        break;

      case arrow::Type::type::INT32:
      case arrow::Type::type::DATE32:
      case arrow::Type::type::TIME32:
        AppendScalar(columns[i], row.Get(i).ToNumber().Int32Value());
        break;

      case arrow::Type::type::UINT64: {
        auto value = row.Get(i);
        auto lossless = true;
        if (value.IsBigInt()) {
          AppendScalar(columns[i], value.As<Napi::BigInt>().Uint64Value(&lossless));
        } else {
          AppendScalar(columns[i], static_cast<uint64_t>(value.ToNumber().Int64Value()));
        }
        break;
      }

      case arrow::Type::type::INT64:
      case arrow::Type::type::TIMESTAMP:
      case arrow::Type::type::TIME64: {
        auto value = row.Get(i);
        auto lossless = true;
        if (value.IsBigInt()) {
          AppendScalar(columns[i], value.As<Napi::BigInt>().Int64Value(&lossless));
        } else {
          AppendScalar(columns[i], value.ToNumber().Int64Value());
        }
        break;
      }

      case arrow::Type::type::FLOAT:
        AppendScalar(columns[i], row.Get(i).ToNumber().FloatValue());
        break;

      case arrow::Type::type::DOUBLE:
        AppendScalar(columns[i], row.Get(i).ToNumber().DoubleValue());
        break;

      case arrow::Type::type::STRING:
        AppendScalar(columns[i], row.Get(i).ToString().Utf8Value());
        break;

      case arrow::Type::type::BINARY:
      case arrow::Type::type::FIXED_SIZE_BINARY: {
        arrow::BufferBuilder arrowBuf;
        auto napiBuf = row.Get(i).As<Napi::Buffer<uint8_t>>();
        arrowBuf.Append(napiBuf.Data(), napiBuf.Length());
        std::shared_ptr<arrow::Buffer> finalBuf;
        arrowBuf.Finish(&finalBuf);
        AppendScalar(columns[i], finalBuf);
        break;
      }

      default:
        // Should only happen if a JS user isn't using the enum
        throw std::runtime_error("Data type not supported");
      }
    }
  }

  Napi::Value AppendRowArray(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    if (info.Length() < 1 || !info[0].IsArray()) {
      Napi::TypeError::New(env, "row:Array expected").ThrowAsJavaScriptException();
      return env.Undefined();
    }

    try {
      AppendRow(info[0].As<Napi::Array>());
    } catch (const std::runtime_error& e) {
      Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
      return env.Undefined();
    }

    return env.Undefined();
  }

  Napi::Value AppendRowObject(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    if (info.Length() < 1 || !info[0].IsObject()) {
      Napi::TypeError::New(env, "row:Object expected").ThrowAsJavaScriptException();
      return env.Undefined();
    }

    // Transform object to array
    auto object = info[0].ToObject();
    auto array = Napi::Array::New(env);
    for (size_t i = 0; i < columns.size(); i++) {
      array.Set(i, object.Get(columns[i].key));
    }

    try {
      AppendRow(array);
    } catch (const std::runtime_error& e) {
      Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
      return env.Undefined();
    }

    return env.Undefined();
  }

  Napi::Value Open(const Napi::CallbackInfo& info) {
    auto env = info.Env();

    try{
      PARQUET_ASSIGN_OR_THROW(
        outfile,
        arrow::io::FileOutputStream::Open(filepath));
    } catch (const parquet::ParquetException& e) {
      Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
      return env.Undefined();
    }

    return Napi::Boolean::New(env, true);
  }

  Napi::Value Close(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    arrow::ArrayVector arrays;
    for (auto& i : columns) {
      ArrowArrayPtr out;
      i.builder->Finish(&out);
      arrays.push_back(out);
    }
    auto table = arrow::Table::Make(schema, arrays);

    try {
      PARQUET_THROW_NOT_OK(
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 3));
    } catch (const parquet::ParquetException& e) {
      Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
      return env.Undefined();
    }

    return env.Undefined();
  }

protected:
  

};

#endif // PARQUET_WRITER_H
