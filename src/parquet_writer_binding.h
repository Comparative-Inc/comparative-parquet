#ifndef CP_PARQUET_WRITER_BINDING_H
#define CP_PARQUET_WRITER_BINDING_H

#include <napi.h>
#include "parquet_writer.h"

class ParquetWriterBinding : public Napi::ObjectWrap<ParquetWriterBinding> {
  ParquetWriter _writer;
  size_t _columnCount;

public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func =
      DefineClass(env,
        "ParquetWriter", {
          InstanceMethod("appendRowArray",       &ParquetWriterBinding::AppendRowArray),
          InstanceMethod("open",                 &ParquetWriterBinding::Open),
          InstanceMethod("close",                &ParquetWriterBinding::Close),
          InstanceMethod("setRowGroupSize",      &ParquetWriterBinding::SetRowGroupSize),
        });

    auto constructor = new Napi::FunctionReference();
    *constructor = Napi::Persistent(func);
    env.SetInstanceData(constructor);

    exports.Set("ParquetWriter", func);
    return exports;
  }

public:
  ParquetWriterBinding(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<ParquetWriterBinding>(info)
    , _writer()
    , _columnCount(0)
  {
    auto env = info.Env();
    if (info.Length() < 2 || !info[0].IsObject() || !info[1].IsString()) {
      Napi::TypeError::New(env, "schema:Object, path:string expected").ThrowAsJavaScriptException();
      return;
    }

    _writer.setFilepath(info[1].ToString().Utf8Value());

    // Build schema
    auto jsSchema = info[0].As<Napi::Object>();
    auto keys = jsSchema.GetPropertyNames();

    _columnCount = keys.Length();
    for (size_t i = 0; i < _columnCount; ++i) {
      auto name = keys.Get(i).ToString().Utf8Value();
      auto const fieldObj = jsSchema.Get(name).ToObject();
      ColumnDescriber const describer{std::move(name), fieldObj};
      try {
        _writer.addColumn(describer);
      } catch (const std::runtime_error& e) {
        Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
        return;
      }
    }
    _writer.buildSchema();
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

  Napi::Value Open(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto const potentialError = _writer.open();
    if (potentialError.has_value()) {
      Napi::Error::New(env, *potentialError).ThrowAsJavaScriptException();
      return env.Undefined();
    }
    return Napi::Boolean::New(env, true);
  }

  Napi::Value Close(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    auto const potentialError = _writer.close();
    if (potentialError.has_value()) {
      Napi::Error::New(env, *potentialError).ThrowAsJavaScriptException();
      return env.Undefined();
    }
    return env.Undefined();
  }

  Napi::Value SetRowGroupSize(const Napi::CallbackInfo& info) {
    auto env = info.Env();
    if (info.Length() < 1 || !info[0].IsNumber()) {
      Napi::TypeError::New(env, "size:number expected").ThrowAsJavaScriptException();
      return env.Undefined();
    }

    _writer.setRowGroupSize(info[0].ToNumber().Int64Value());
    return env.Undefined();
  }

protected:

  void AppendRow(const Napi::Array& row) {
    if (_columnCount != row.Length()) {
      throw std::runtime_error("Number of columns does not match schema");
    }

    InputRow const inputRow{row};
    _writer.appendRow(inputRow);
  }
  
  class ColumnDescriber : public ParquetColumnDescriber
  {
    std::string _name;
    Napi::Object const * _obj;
  public:
    ColumnDescriber(
      std::string columnName,
      Napi::Object const& obj) noexcept
      : ParquetColumnDescriber()
      , _name{std::move(columnName)}
      , _obj{&obj}
    {}

    std::string const& name() const override { return _name; }
    int arrowType() const override { return _obj->Get("type").ToNumber().Int32Value(); }
    std::int32_t width() const override { return _obj->Get("width").ToNumber().Int32Value(); }
    int timeUnit() const override { return _obj->Get("unit").ToNumber().Int32Value(); }
  };

  class InputBinary : public ParquetInputBinary
  {
    Napi::Buffer<std::uint8_t> _buffer;
  public:
    InputBinary(Napi::Buffer<std::uint8_t> buf) : _buffer{std::move(buf)} {}

    std::uint8_t const* data() const override { return _buffer.Data(); }
    size_t size() const override { return _buffer.Length(); }
  };

  class InputRow : public ParquetInputRow
  {
    Napi::Array const* _row;
  public:
    InputRow(Napi::Array const& row) : _row{&row} {}

    bool getBool(size_t columnIndex) const override { return _row->Get(columnIndex).ToBoolean().Value(); }
    std::int8_t getI8(size_t columnIndex) const override { return static_cast<std::int8_t>(_row->Get(columnIndex).ToNumber().Int32Value()); }
    std::uint8_t getU8(size_t columnIndex) const override { return static_cast<std::uint8_t>(_row->Get(columnIndex).ToNumber().Uint32Value()); }
    std::int16_t getI16(size_t columnIndex) const override { return static_cast<std::int16_t>(_row->Get(columnIndex).ToNumber().Int32Value()); }
    std::uint16_t getU16(size_t columnIndex) const override { return static_cast<std::uint16_t>(_row->Get(columnIndex).ToNumber().Uint32Value()); }
    std::int32_t getI32(size_t columnIndex) const override { return _row->Get(columnIndex).ToNumber().Int32Value(); }
    std::uint32_t getU32(size_t columnIndex) const override { return _row->Get(columnIndex).ToNumber().Uint32Value(); }
    std::int64_t getI64(size_t columnIndex) const override {
      auto value = _row->Get(columnIndex);
      auto lossless = true;
      if (value.IsBigInt()) {
        return value.As<Napi::BigInt>().Int64Value(&lossless);
      }
      return value.ToNumber().Int64Value();
    }
    std::uint64_t getU64(size_t columnIndex) const override {
      auto value = _row->Get(columnIndex);
      auto lossless = true;
      if (value.IsBigInt()) {
        return value.As<Napi::BigInt>().Uint64Value(&lossless);
      }
      return value.ToNumber().Int64Value();
    }
    float getF32(size_t columnIndex) const override { return _row->Get(columnIndex).ToNumber().FloatValue(); }
    double getF64(size_t columnIndex) const override { return _row->Get(columnIndex).ToNumber().DoubleValue(); }
    std::string getString(size_t columnIndex) const override { return _row->Get(columnIndex).ToString().Utf8Value(); }
    std::unique_ptr<ParquetInputBinary> getBinary(size_t columnIndex) const override {
      return std::make_unique<InputBinary>(_row->Get(columnIndex).As<Napi::Buffer<std::uint8_t>>());
    }
  };
};

#endif // CP_PARQUET_WRITER_BINDING_H
