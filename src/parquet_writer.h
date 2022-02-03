#ifndef PARQUET_WRITER_H
#define PARQUET_WRITER_H

#include <napi.h>

#include <parquet/arrow/writer.h>
#include <arrow/table.h>

#include <vector>

#include "column_buffer.h"

class ParquetWriter : public Napi::ObjectWrap<ParquetWriter> {
public:
  std::shared_ptr<arrow::Schema> _schema;
  std::vector<std::unique_ptr<ColumnBuffer>> _columns;

public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func =
      DefineClass(env,
        "ParquetWriter", {
          InstanceMethod("appendRow",    &ParquetWriter::appendRow),
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
    if (info.Length() < 2 || !info[0].IsString() || !info[1].IsObject()) {
      Napi::TypeError::New(env, "path::string, schema::Object expected").ThrowAsJavaScriptException();
      return;
    }

    // --- Open file ---
    

    // --- Build schema ---
    auto schema_obj = info[1].As<Napi::Object>();
    auto keys = schema_obj.GetPropertyNames();
    std::vector<std::shared_ptr<arrow::Field>> fields;

    // keys.Length() returns uint32_t, so that's all that's needed to index
    for (uint32_t i; i < keys.Length(); i++) {
      auto name = keys.Get(i).ToString().Utf8Value();
      auto type = arrowTypeFromString(schema_obj.Get(name).ToString().Utf8Value());
      fields.push_back(std::make_shared<arrow::Field>(name, type));
      try {
        _columns.push_back(ColumnBufferFactory::makeBuffer(name, type));
      } catch (const std::invalid_argument& e) {
        Napi::TypeError::New(env, e.what()).ThrowAsJavaScriptException();
      }
    }

    _schema = std::make_shared<arrow::Schema>(fields);
  }

  Napi::Value appendRow(const Napi::CallbackInfo& info) {
    auto row = info[0].As<Napi::Object>();
    for (auto& i : _columns) {
      i->push_back(row.Get(i->name));
    }

    return Napi::Boolean::New(info.Env(), true);
  }

  Napi::Value close(const Napi::CallbackInfo& info) {

    return Napi::Boolean::New(info.Env(), true);
  }

  Napi::Value isOpen(const Napi::CallbackInfo& info) {
    return Napi::Boolean::New(info.Env(), true);
  }

protected:

  std::shared_ptr<arrow::DataType> arrowTypeFromString(const std::string& type) {
    // Maybe it would be faster if it hashed the string and used a switch statement
    if (type == "INT32") {
      return arrow::int32();
    } else if (type == "INT64") {
      return arrow::int64();
    } else if (type == "TIMESTAMP_NANOS") {
      return arrow::timestamp(arrow::TimeUnit::NANO);
    } else if (type == "TIMESTAMP_MICROS") {
      return arrow::timestamp(arrow::TimeUnit::MICRO);
    } else if (type == "TIMESTAMP_MILLIS") {
      return arrow::timestamp(arrow::TimeUnit::MILLI);
    } else if (type == "TIMESTAMP_SECONDS") {
      return arrow::timestamp(arrow::TimeUnit::SECOND);
    } else if (type == "DATE32") {
      return arrow::date32();
    } else if (type == "DATE64") {
      return arrow::date64();
    } else if (type == "FLOAT") {
      return arrow::float32();
    } else if (type == "DOUBLE") {
      return arrow::float64();
    } else if (type == "BOOLEAN") {
      return arrow::boolean();
    } else if (type == "UTF8") {
      return arrow::utf8();
    }

    throw std::invalid_argument("Unsupported parquet field type");
  }

};

#endif // PARQUET_WRITER_H