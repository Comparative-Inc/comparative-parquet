#ifndef CP_PARQUET_READER_BINDING_H
#define CP_PARQUET_READER_BINDING_H

#include <napi.h>
#include "parquet_reader.h"

#define JS_ERROR(message)  do {\
    Napi::Error::New(env, message).ThrowAsJavaScriptException(); \
    return env.Null(); } while(0)

static int64_t const MIN_SAFE_INTEGER = -9007199254740991L;
static int64_t const MAX_SAFE_INTEGER =  9007199254740991L;

class ParquetReaderBinding : public Napi::ObjectWrap<ParquetReaderBinding> {
  ParquetReader _reader;

public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func =
      DefineClass(env,
        "ParquetReader", {
          InstanceMethod("getFilepath",    &ParquetReaderBinding::GetFilepath),
          InstanceMethod("getColumnNames", &ParquetReaderBinding::GetColumnNames),
          InstanceMethod("getColumnCount", &ParquetReaderBinding::GetColumnCount),
          InstanceMethod("getRowCount",    &ParquetReaderBinding::GetRowCount),
          InstanceMethod("open",           &ParquetReaderBinding::Open),
          InstanceMethod("close",          &ParquetReaderBinding::Close),
          InstanceMethod("readRow",        &ParquetReaderBinding::ReadRow),
          InstanceMethod("readRowAsArray", &ParquetReaderBinding::ReadRowAsArray),
        });

    auto constructor = new Napi::FunctionReference();
    *constructor = Napi::Persistent(func);
    env.SetInstanceData(constructor);

    exports.Set("ParquetReader", func);
    return exports;
  }

public:
  ParquetReaderBinding(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<ParquetReaderBinding>(info)
    , _reader()
  {
    Napi::Env env = info.Env();

    int length = info.Length();

    if (length <= 0 || !info[0].IsString()) {
      Napi::TypeError::New(env, "Number expected").ThrowAsJavaScriptException();
      return;
    }

    Napi::String filepath = info[0].As<Napi::String>();
    _reader.setFilepath(filepath.Utf8Value());
  }

  Napi::Value Open(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (bool const isAlreadyOpen = _reader.isOpen(); isAlreadyOpen)
      return Napi::Boolean::New(env, isAlreadyOpen);

    auto const potentialError = _reader.open();
    if (potentialError.has_value())
    {
      JS_ERROR(*potentialError);
    }
    return Napi::Boolean::New(env, true);
  }

  Napi::Value Close(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    bool const isAlreadyOpen = _reader.isOpen();
    if (!isAlreadyOpen)
      return Napi::Boolean::New(env, isAlreadyOpen);

    auto const potentialError = _reader.close();
    if (potentialError.has_value())
    {
      JS_ERROR(*potentialError);
    }

    return Napi::Boolean::New(env, false);
  }

  Napi::Value GetFilepath(const Napi::CallbackInfo& info) {
    return Napi::String::New(info.Env(), _reader.getFilepath());
  }

  Napi::Value GetColumnNames(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (!_reader.isOpen()) {
      JS_ERROR("File is not open");
    }

    size_t const columnCount = _reader.getColumnCount();
    auto results = Napi::Array::New(env, columnCount);

    for (size_t i = 0; i < columnCount; ++i) {
      results[i] = Napi::String::New(env, _reader.getColumnName(i));
    }

    return results;
  }

  Napi::Value GetColumnCount(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    return Napi::Number::New(env, _reader.getColumnCount());
  }

  Napi::Value GetRowCount(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    return Napi::Number::New(env, _reader.getRowCount());
  }

  Napi::Value ReadRow(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (!_reader.isOpen()) {
      JS_ERROR("File is not open");
    }

    if (info.Length() <= 0 || !info[0].IsNumber()) {
      Napi::TypeError::New(env, "Number expected").ThrowAsJavaScriptException();
      return env.Null();
    }

    auto rowIndex = static_cast<size_t>(info[0].As<Napi::Number>().Int64Value());
    auto results = Napi::Object::New(env);

    size_t const columnCount = _reader.getColumnCount();
    for (size_t i = 0; i < columnCount; ++i) {
      auto const& key = _reader.getColumnName(i);
      results[key] = this->ReadValue(info, i, rowIndex);
    }

    return results;
  }

  Napi::Value ReadRowAsArray(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (!_reader.isOpen()) {
      JS_ERROR("File is not open");
    }

    if (info.Length() <= 0 || !info[0].IsNumber()) {
      Napi::TypeError::New(env, "Number expected").ThrowAsJavaScriptException();
      return env.Null();
    }

    auto rowIndex = static_cast<size_t>(info[0].As<Napi::Number>().Int64Value());
    size_t const columnCount = _reader.getColumnCount();
    auto results = Napi::Array::New(env, columnCount);

    for (size_t i = 0; i < columnCount; ++i) {
      results[i] = this->ReadValue(info, i, rowIndex);
    }

    return results;
  }

  Napi::Value ReadValue(const Napi::CallbackInfo& info, size_t columnIndex, size_t rowIndex) {
    ValueReceiver receiver(info.Env());
    _reader.getValue(receiver, columnIndex, rowIndex);
    return receiver.takeValue();
  }

private:
  class ValueReceiver : public ParquetValueReceiver {
    Napi::Env   _env;
    Napi::Value _value;
  public:
    ValueReceiver(Napi::Env env)
      : _env{std::move(env)}
      , _value{_env.Null()}
    {}

    Napi::Value takeValue() {
      return std::exchange(_value, _env.Null());
    }

    void receiveBool(bool b) override {
      _value = Napi::Boolean::New(_env, b);
    }
    void receiveInt32(std::int32_t i) override {
      _value = Napi::Number::New(_env, i);
    }
    void receiveInt64(std::int64_t i) override {
      if (i <= MIN_SAFE_INTEGER || i >= MAX_SAFE_INTEGER)
        _value = Napi::BigInt::New(_env, i);
      else
        _value = Napi::Number::New(_env, i);
    }
    void receiveDouble(double d) override {
      _value = Napi::Number::New(_env, d);
    }
    void receiveString(char const * data, size_t length) override {
      _value = Napi::String::New(_env, data, length);
    }
    void receiveNull() override {
      _value = _env.Null();
    }
  };

};

#endif
