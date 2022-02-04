#ifndef PARQUET_WRITER_H
#define PARQUET_WRITER_H

#include <napi.h>

#include <parquet/arrow/writer.h>
#include <parquet/stream_writer.h>

#include <vector>

struct Column {
  std::string key;
  parquet::Type::type type;
};

class ParquetWriter : public Napi::ObjectWrap<ParquetWriter> {
public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func =
      DefineClass(env,
        "ParquetWriter", {
          InstanceMethod("appendRow",    &ParquetWriter::appendRow),
          InstanceMethod("open", &ParquetWriter::open)
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
    , builder()
  {
    auto env = info.Env();
    if (info.Length() < 2 || !info[0].IsObject() || !info[1].IsString()) {
      Napi::TypeError::New(env, "schema::Object, path::string expected").ThrowAsJavaScriptException();
      return;
    }

    filepath = info[1].ToString().Utf8Value();

    // Build schema
    auto schema_obj = info[0].As<Napi::Object>();
    auto keys = schema_obj.GetPropertyNames();
    parquet::schema::NodeVector fields;
    for (uint32_t i = 0; i < keys.Length(); i++) {
      auto name = keys.Get(i).ToString().Utf8Value();
      auto type_name = schema_obj.Get(name).ToObject().Get("type").ToString().Utf8Value();
      auto converted_type = convertedTypeFromString(type_name);
      auto logical_type = logicalTypeFromString(type_name);
      columns.push_back(Column{name, logical_type});
      fields.push_back(parquet::schema::PrimitiveNode::Make(
        name,
        parquet::Repetition::OPTIONAL,
        logical_type,
        converted_type));
    }
    schema = std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make("schema", parquet::Repetition::OPTIONAL, fields)
    );
  }

  Napi::Value appendRow(const Napi::CallbackInfo& info) {
    auto row = info[0].As<Napi::Object>();
    for (auto& i : columns) {
      switch (i.type) {
      case parquet::Type::INT32:
        os << row.Get(i.key).ToNumber().Int32Value();
        break;

      case parquet::Type::INT64:
        os << row.Get(i.key).ToNumber().Int64Value();
        break;

      case parquet::Type::FLOAT:
        os << row.Get(i.key).ToNumber().FloatValue();
        break;

      case parquet::Type::DOUBLE:
        os << row.Get(i.key).ToNumber().DoubleValue();
        break;

      case parquet::Type::BOOLEAN:
        os << row.Get(i.key).ToBoolean().Value();
        break;

      case parquet::Type::BYTE_ARRAY:
        os << row.Get(i.key).ToString().Utf8Value();
        break;

      default:
        os << "null";
      }
    }
    os << parquet::EndRow;

    return Napi::Boolean::New(info.Env(), true);
  }

  Napi::Value open(const Napi::CallbackInfo& info) {
    // Open file
    std::shared_ptr<arrow::io::FileOutputStream> out_file;
    PARQUET_ASSIGN_OR_THROW(
      out_file,
      arrow::io::FileOutputStream::Open(filepath)
    );

    // Setup output stream
    os = parquet::StreamWriter {parquet::ParquetFileWriter::Open(
      out_file,
      schema,
      builder.build()
    )};

    return Napi::Boolean::New(info.Env(), true);
  }

  Napi::Value close(const Napi::CallbackInfo& info) {

    return Napi::Boolean::New(info.Env(), true);
  }

protected:

  std::string filepath;
  std::shared_ptr<parquet::schema::GroupNode> schema;
  parquet::WriterProperties::Builder builder;
  parquet::StreamWriter os;
  std::vector<Column> columns;

  parquet::ConvertedType::type convertedTypeFromString(const std::string& type) {
    // Maybe it would be faster if it hashed the string and used a switch statement
    if (type == "INT32") {
      return parquet::ConvertedType::INT_32;
    } else if (type == "INT64") {
      return parquet::ConvertedType::INT_64;
    } else if (type == "TIMESTAMP_MICROS") {
      return parquet::ConvertedType::INT_64;
    } else if (type == "TIMESTAMP_MILLIS") {
      return parquet::ConvertedType::INT_64;
    } else if (type == "DATE32") {
      return parquet::ConvertedType::INT_32;
    } else if (type == "DOUBLE") {
      return parquet::ConvertedType::NONE; // Looks weird but is correct
    } else if (type == "BOOLEAN") {
      return parquet::ConvertedType::NONE; // ditto
    } else if (type == "UTF8") {
      return parquet::ConvertedType::UTF8;
    }

    throw std::invalid_argument("Unsupported parquet field type");
  }

  parquet::Type::type logicalTypeFromString(const std::string& type) {
    if (type == "INT32") {
      return parquet::Type::INT32;
    } else if (type == "INT64") {
      return parquet::Type::INT64;
    } else if (type == "TIMESTAMP_MICROS") {
      return parquet::Type::INT64;
    } else if (type == "TIMESTAMP_MILLIS") {
      return parquet::Type::INT64;
    } else if (type == "DATE32") {
      return parquet::Type::INT32;
    } else if (type == "DOUBLE") {
      return parquet::Type::DOUBLE;
    } else if (type == "BOOLEAN") {
      return parquet::Type::BOOLEAN;
    } else if (type == "UTF8") {
      return parquet::Type::BYTE_ARRAY;
    }

    throw std::invalid_argument("Unsupported parquet field type");
  }

};

#endif // PARQUET_WRITER_H