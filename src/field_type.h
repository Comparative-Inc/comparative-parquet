#ifndef FIELD_TYPE_H
#define FIELD_TYPE_H

#include <napi.h>

// Have to use an interim enum because some parquet types are weird
// Also it makes it clear which types are supported, and eliminates the
// need for exceptions on unsupported types.
class FieldType : public Napi::ObjectWrap<FieldType>{
public:
  enum class Type {
    INT32,
    INT64,
    TIMESTAMP_MICROS,
    TIMESTAMP_MILLIS,
    DATE32,
    DOUBLE,
    BOOLEAN,
    UTF8
  };

public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func =
      DefineClass(env,
        "FieldType", {
          InstanceValue("INT32", Napi::Number::New(env, static_cast<double>(Type::INT32))),
          InstanceValue("INT64", Napi::Number::New(env, static_cast<double>(Type::INT64))),
          InstanceValue("TIMESTAMP_MICROS", Napi::Number::New(env, static_cast<double>(Type::TIMESTAMP_MICROS))),
          InstanceValue("TIMESTAMP_MILLIS", Napi::Number::New(env, static_cast<double>(Type::TIMESTAMP_MILLIS))),
          InstanceValue("DATE32", Napi::Number::New(env, static_cast<double>(Type::DATE32))),
          InstanceValue("DOUBLE", Napi::Number::New(env, static_cast<double>(Type::DOUBLE))),
          InstanceValue("BOOLEAN", Napi::Number::New(env, static_cast<double>(Type::BOOLEAN))),
          InstanceValue("UTF8", Napi::Number::New(env, static_cast<double>(Type::UTF8)))
        });

    auto constructor = new Napi::FunctionReference();
    *constructor = Napi::Persistent(func);
    env.SetInstanceData(constructor);

    exports.Set("FieldType", func);
    return exports;
  }

public:
  FieldType(const Napi::CallbackInfo& info) : Napi::ObjectWrap<FieldType>(info) { }
};
#endif