#ifndef FIELD_TYPE_H
#define FIELD_TYPE_H

#include <napi.h>

#include <arrow/type_fwd.h>

// Don't use these macros outside of this file
// They assume the existense of a Napi::Env env variable
#define ENUMSET(name, enum)   Set(name, Napi::Number::New(env, static_cast<double>(enum)))
#define F_ENUMSET(name, enum) type.ENUMSET(name, arrow::Type::type::enum)
#define T_ENUMSET(name, enum) timeUnit.ENUMSET(name, arrow::TimeUnit::type::enum)

// Enums that encompass all arrow types.

// Types not supported by the parquet format or duplicated by other
// types in the parquet format have been commented out, but left
// for completeness in case that ever changes.

// See FieldToNode() in <parquet/arrow/schema.cc> for the rational
// behind these choices.
namespace Types {
  inline Napi::Object Init(Napi::Env env, Napi::Object exports) {
    using Napi::Object;

    Object type = Object::New(env);
    F_ENUMSET("BOOL",                     BOOL),
    F_ENUMSET("UINT8",                    UINT8),
    F_ENUMSET("INT8",                     INT8),
    F_ENUMSET("UINT16",                   UINT16),
    F_ENUMSET("INT16",                    INT16),
    F_ENUMSET("UINT32",                   UINT32),
    F_ENUMSET("INT32",                    INT32),
    F_ENUMSET("UINT64",                   UINT64),
    F_ENUMSET("INT64",                    INT64),
    // F_ENUMSET("HALF_FLOAT",               HALF_FLOAT),               // Not currently supported by parquet
    F_ENUMSET("FLOAT",                    FLOAT),
    F_ENUMSET("DOUBLE",                   DOUBLE),
    F_ENUMSET("STRING",                   STRING),
    // F_ENUMSET("BINARY",                   BINARY),                   // Not currently support by Napi
    // F_ENUMSET("FIXED_SIZE_BINARY",        FIXED_SIZE_BINARY),        // ...
    F_ENUMSET("DATE32",                   DATE32),
    // F_ENUMSET("DATE64",                   DATE64),                   // Equivalent to smaller version
    F_ENUMSET("TIMESTAMP",                TIMESTAMP),                 // Supports MILLI, MICRO, and NANO
    F_ENUMSET("TIME32",                   TIME32),                    // Supports MILLI
    F_ENUMSET("TIME64",                   TIME64),                    // Supports NANO and MICRO
    // F_ENUMSET("INTERVAL_MONTHS",          INTERVAL_MONTHS),          // Not currently supported by parquet
    // F_ENUMSET("INTERVAL_DAT_TIME",        INTERVAL_DAY_TIME),        // ...
    // F_ENUMSET("DECIMAL128",               DECIMAL128),               // Not currently supported by this library
    // F_ENUMSET("DECIMAL",                  DECIMAL),                  // ...
    // F_ENUMSET("DECIMAL256",               DECIMAL256),               // ...
    // F_ENUMSET("LIST",                     LIST),                     // ...
    // F_ENUMSET("STRUCT",                   STRUCT),                   // ...
    // F_ENUMSET("SPARSE_UNION",             SPARSE_UNION),             // Not currently supported by arrow
    // F_ENUMSET("DENSE_UNION",              DENSE_UNION),              // ...
    // F_ENUMSET("DICTIONARY",               DICTIONARY),               // Not currently supported by this library
    // F_ENUMSET("MAP",                      MAP),                      // ...
    // F_ENUMSET("EXTENSION",                EXTENSION),                // ...
    // F_ENUMSET("FIXED_SIZE_LIST",          FIXED_SIZE_LIST),          // Equivalent to LIST
    // F_ENUMSET("DURATION",                 DURATION),                 // Not currently supported by parquet
    // F_ENUMSET("LARGE_STRING",             LARGE_STRING),             // Equivalent to smaller version
    // F_ENUMSET("LARGE_BINARY",             LARGE_BINARY),             // ...
    // F_ENUMSET("LARGE_LIST",               LARGE_LIST),               // ...
    // F_ENUMSET("INTERVAL_MONTH_DAY_NANO",  INTERVAL_MONTH_DAY_NANO);  // Not currently supported by parquet

    exports.Set("FieldType", type);

    Object timeUnit = Object::New(env);
    // T_ENUMSET("SECOND", SECOND), // Nothing seems to actually use this
    T_ENUMSET("MILLI",  MILLI),
    T_ENUMSET("MICRO",  MICRO),
    T_ENUMSET("NANO",   NANO);

    exports.Set("TimeUnit", timeUnit);

    return exports;
  }
};

#endif