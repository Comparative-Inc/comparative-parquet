#include <napi.h>
#include "parquet_reader.h"
#include "parquet_writer.h"

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  ParquetWriter::Init(env, exports);
  return ParquetReader::Init(env, exports);
}

NODE_API_MODULE(addon, InitAll)
