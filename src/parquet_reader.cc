#include "parquet_reader.h"

ParquetReader::ParquetReader(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<ParquetReader>(info) {
  Napi::Env env = info.Env();

  int length = info.Length();

  if (length <= 0 || !info[0].IsString()) {
    Napi::TypeError::New(env, "Number expected").ThrowAsJavaScriptException();
    return;
  }

  Napi::String filepath = info[0].As<Napi::String>();
  this->filepath_ = filepath.Utf8Value();
}

Napi::Value ParquetReader::GetValue(const Napi::CallbackInfo& info) {
  double num = this->value_;

  return Napi::Number::New(info.Env(), num);
}

Napi::Value ParquetReader::PlusOne(const Napi::CallbackInfo& info) {
  this->value_ = this->value_ + 1;

  return ParquetReader::GetValue(info);
}

Napi::Value ParquetReader::Multiply(const Napi::CallbackInfo& info) {
  Napi::Number multiple;
  if (info.Length() <= 0 || !info[0].IsNumber()) {
    multiple = Napi::Number::New(info.Env(), 1);
  } else {
    multiple = info[0].As<Napi::Number>();
  }

  Napi::Object obj = info.Env().GetInstanceData<Napi::FunctionReference>()->New(
      {Napi::Number::New(info.Env(), this->value_ * multiple.DoubleValue())});

  return obj;
}
