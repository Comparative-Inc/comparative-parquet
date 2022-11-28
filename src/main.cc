#include <napi.h>
#include "parquet_reader_binding.h"
#include "parquet_writer_binding.h"
#include "types_binding.h"

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  ParquetWriterBinding::Init(env, exports);
  ParquetReaderBinding::Init(env, exports);
  TypesBinding::Init(env, exports);
  return exports;
}

NODE_API_MODULE(addon, InitAll)
