#ifndef PARQUET_WRITER_H
#define PARQUET_WRITER_H

#include <napi.h>

#include <parquet/arrow/writer.h>
#include <parquet/stream_writer.h>

#include <vector>

#include "field_type.h"

struct Column {
  std::string key;
  parquet::Type::type type;
};

class ParquetWriter : public Napi::ObjectWrap<ParquetWriter> {
protected:
  std::string filepath;
  std::shared_ptr<parquet::schema::GroupNode> schema;
  parquet::WriterProperties::Builder builder;
  parquet::StreamWriter os;
  std::vector<Column> columns;

public:
  

public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func =
      DefineClass(env,
        "ParquetWriter", {
          InstanceMethod("appendRow",       &ParquetWriter::appendRow),
          InstanceMethod("open",            &ParquetWriter::open),
          InstanceMethod("setRowGroupSize", &ParquetWriter::setRowGroupSize),
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
      Napi::TypeError::New(env, "schema:Object, path:string expected").ThrowAsJavaScriptException();
      return;
    }

    filepath = info[1].ToString().Utf8Value();

    // Build schema
    auto schema = info[0].As<Napi::Object>();
    auto keys = schema.GetPropertyNames();
    parquet::schema::NodeVector fields;
    for (uint32_t i = 0; i < keys.Length(); i++) {
      auto name = keys.Get(i).ToString().Utf8Value();
      auto fieldType = static_cast<FieldType::Type>(
        schema.Get(name).ToObject().Get("type").ToNumber().Int32Value());
      auto convertedType = ConvertedTypeFromFieldType(fieldType);
      auto logicalType = LogicalTypeFromFieldType(fieldType);
      columns.push_back(Column{name, logicalType});
      fields.push_back(parquet::schema::PrimitiveNode::Make(
        name,
        parquet::Repetition::OPTIONAL,
        logicalType,
        convertedType));
    }
    this->schema = std::static_pointer_cast<parquet::schema::GroupNode>(
      parquet::schema::GroupNode::Make("schema", parquet::Repetition::OPTIONAL, fields)
    );
  }

  Napi::Value appendRow(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    if (info.Length() < 1 || (!info[0].IsObject() && !info[0].IsArray())) {
      Napi::TypeError::New(env, "row:(Object or Array) expected").ThrowAsJavaScriptException();
      return env.Undefined();
    }

    try {
    if (info[0].IsArray()) {
      auto row = info[0].As<Napi::Array>();
      for (size_t i = 0; i < columns.size() && i < row.Length(); i++) {
        switch (columns[i].type) {
        case parquet::Type::INT32:
          os << row.Get(i).ToNumber().Int32Value();
          break;

        case parquet::Type::INT64:
          os << row.Get(i).ToNumber().Int64Value();
          break;

        case parquet::Type::FLOAT:
          os << row.Get(i).ToNumber().FloatValue();
          break;

        case parquet::Type::DOUBLE:
          os << row.Get(i).ToNumber().DoubleValue();
          break;

        case parquet::Type::BOOLEAN:
          os << row.Get(i).ToBoolean().Value();
          break;

        case parquet::Type::BYTE_ARRAY:
          os << row.Get(i).ToString().Utf8Value();
          break;

        default:
          os << "null";
        }
      }

    } else {
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
    }

    os << parquet::EndRow;

    } catch (const parquet::ParquetException& e) {
      Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
      return env.Undefined();
    }

    return env.Undefined();
  }

  Napi::Value open(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    // Open file
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    try {
      PARQUET_ASSIGN_OR_THROW(
        outfile,
        arrow::io::FileOutputStream::Open(filepath)
      );
    } catch (const parquet::ParquetException& e) {
      Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
      return env.Undefined();
    }

    // Setup output stream
    os = parquet::StreamWriter {parquet::ParquetFileWriter::Open(
      outfile,
      schema,
      builder.build()
    )};

    return Napi::Boolean::New(env, true);
  }

  Napi::Value setRowGroupSize(const Napi::CallbackInfo& info) {
    builder.max_row_group_length(info[0].ToNumber().Int64Value());
    return info.Env().Undefined();
  }

protected:
  static parquet::ConvertedType::type ConvertedTypeFromFieldType(FieldType::Type type) {
    switch (type) {
    case FieldType::Type::INT32:
      return parquet::ConvertedType::INT_32;

    case FieldType::Type::INT64:
      return parquet::ConvertedType::INT_64;

    case FieldType::Type::TIMESTAMP_MICROS:
      return parquet::ConvertedType::INT_64;

    case FieldType::Type::TIMESTAMP_MILLIS:
      return parquet::ConvertedType::INT_64;

    case FieldType::Type::DATE32:
      return parquet::ConvertedType::DATE;

    case FieldType::Type::DOUBLE:
      return parquet::ConvertedType::NONE;

    case FieldType::Type::BOOLEAN:
      return parquet::ConvertedType::NONE;

    case FieldType::Type::UTF8:
      return parquet::ConvertedType::UTF8;
    }

    // Execution only reaches here if you do something unsafe, but the compiler is complaining
    return parquet::ConvertedType::UNDEFINED;
  }

  static parquet::Type::type LogicalTypeFromFieldType(FieldType::Type type) {
    switch (type) {
    case FieldType::Type::INT32:
      return parquet::Type::INT32;

    case FieldType::Type::INT64:
      return parquet::Type::INT64;

    case FieldType::Type::TIMESTAMP_MICROS:
      return parquet::Type::INT64;

    case FieldType::Type::TIMESTAMP_MILLIS:
      return parquet::Type::INT64;

    case FieldType::Type::DATE32:
      return parquet::Type::INT32;

    case FieldType::Type::DOUBLE:
      return parquet::Type::DOUBLE;

    case FieldType::Type::BOOLEAN:
      return parquet::Type::BOOLEAN;

    case FieldType::Type::UTF8:
      return parquet::Type::BYTE_ARRAY;
    }

    // Execution only reaches here if you do something unsafe, but the compiler is complaining
    return parquet::Type::UNDEFINED;
  }

};

#endif // PARQUET_WRITER_H