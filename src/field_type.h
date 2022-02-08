#ifndef FIELD_TYPE_H
#define FIELD_TYPE_H

#include <napi.h>

// Have to use an interim enum because some parquet types are weird
// Also it makes it clear which types are supported, and eliminates the
// need for exceptions on unsupported types.
namespace FieldType {
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

  inline Napi::Object Init(Napi::Env env, Napi::Object exports) {
    using Napi::Object;

    Object type = Object::New(env);
    type.Set("BOOLEAN",          Napi::Number::New(env, static_cast<double>(Type::BOOLEAN))),
    type.Set("INT32",            Napi::Number::New(env, static_cast<double>(Type::INT32))),
    type.Set("INT64",            Napi::Number::New(env, static_cast<double>(Type::INT64))),
    type.Set("DOUBLE",           Napi::Number::New(env, static_cast<double>(Type::DOUBLE))),
    type.Set("DATE32",           Napi::Number::New(env, static_cast<double>(Type::DATE32))),
    type.Set("TIMESTAMP_MICROS", Napi::Number::New(env, static_cast<double>(Type::TIMESTAMP_MICROS))),
    type.Set("TIMESTAMP_MILLIS", Napi::Number::New(env, static_cast<double>(Type::TIMESTAMP_MILLIS))),
    type.Set("UTF8",             Napi::Number::New(env, static_cast<double>(Type::UTF8)));

    exports.Set("FieldType", type);

    return exports;
  }
};

#endif