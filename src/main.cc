#include <napi.h>
#include "parquet_reader.h"

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  return ParquetReader::Init(env, exports);
}

NODE_API_MODULE(addon, InitAll)
