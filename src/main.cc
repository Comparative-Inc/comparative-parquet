#include <napi.h>
#include "parquet_reader.h"
#include "parquet_writer.h"
#include "types.h"

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  ParquetWriter::Init(env, exports);
  ParquetReader::Init(env, exports);
  Types::Init(env, exports);
  return exports;
}

NODE_API_MODULE(addon, InitAll)
