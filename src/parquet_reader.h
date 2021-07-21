#ifndef PARQUET_READER_H
#define PARQUET_READER_H

#include <napi.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

using std::vector;
using std::shared_ptr;
using std::unique_ptr;

typedef shared_ptr<arrow::Schema>       ArrowSchemaPtr;
typedef shared_ptr<arrow::ChunkedArray> ArrowColumnPtr;
typedef shared_ptr<arrow::Array>        ArrowArrayPtr;
typedef shared_ptr<arrow::Field>        ArrowFieldPtr;

class ParquetReader : public Napi::ObjectWrap<ParquetReader> {
public:
  std::string _filepath;
  arrow::MemoryPool* _pool;
  shared_ptr<arrow::io::RandomAccessFile> _input;
  unique_ptr<parquet::arrow::FileReader> _reader;
  vector<ArrowColumnPtr> _columns;
  vector<vector<ArrowArrayPtr>> _chunksByColumn;
  vector<ArrowFieldPtr> _fieldByColumn;
  int _columnCount;
  bool _isOpen;

public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func =
      DefineClass(env,
        "ParquetReader", {
          InstanceMethod("getFilepath",    &ParquetReader::GetFilepath),
          InstanceMethod("getColumnNames", &ParquetReader::GetColumnNames),
          InstanceMethod("open",           &ParquetReader::Open),
          InstanceMethod("close",          &ParquetReader::Close),
          InstanceMethod("readRow",        &ParquetReader::ReadRow),
        });

    auto constructor = new Napi::FunctionReference();
    *constructor = Napi::Persistent(func);
    env.SetInstanceData(constructor);

    exports.Set("ParquetReader", func);
    return exports;
  }

public:
  ParquetReader(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<ParquetReader>(info)
    , _pool(arrow::default_memory_pool())
    , _columns()
  {
    Napi::Env env = info.Env();

    int length = info.Length();

    if (length <= 0 || !info[0].IsString()) {
      Napi::TypeError::New(env, "Number expected").ThrowAsJavaScriptException();
      return;
    }

    Napi::String filepath = info[0].As<Napi::String>();
    this->_filepath = filepath.Utf8Value();
  }

  Napi::Value Open(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (_isOpen)
      return Napi::Boolean::New(env, false);

    _input = arrow::io::MemoryMappedFile::Open(
        _filepath.c_str(), arrow::io::FileMode::READ).ValueOrDie();

    /* Open file */
    auto status = parquet::arrow::OpenFile(_input, _pool, &_reader);
    if (!status.ok()) {
      Napi::Error::New(env, "Failed to open file").ThrowAsJavaScriptException();
      return Napi::Boolean::New(env, _isOpen);
    }

    _isOpen = true;

    /* Read schema & columns */
    ArrowSchemaPtr schema;
    if (!_reader->GetSchema(&schema).ok()) {
      Napi::Error::New(env, "Failed to read schema").ThrowAsJavaScriptException();
      return Napi::Boolean::New(env, _isOpen);
    }

    _columnCount = schema->num_fields();

    for (auto i = 0; i < _columnCount; i++) {
      ArrowColumnPtr column;
      if (!_reader->ReadColumn(i, &column).ok()) {
        Napi::Error::New(env, "Failed to read column").ThrowAsJavaScriptException();
        return Napi::Boolean::New(env, _isOpen);
      }
      _columns.push_back(column);
      _chunksByColumn.push_back(column->chunks());
      _fieldByColumn.push_back(schema->field(i));
    }

    return Napi::Boolean::New(env, _isOpen);
  }

  Napi::Value Close(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (!_isOpen)
      return Napi::Boolean::New(env, _isOpen);

    auto status = _input->Close();
    if (status.ok()) {
      _isOpen = false;
    } else {
      Napi::Error::New(env, "Failed to close file").ThrowAsJavaScriptException();
    }

    return Napi::Boolean::New(env, _isOpen);
  }

  Napi::Value GetFilepath(const Napi::CallbackInfo& info) {
    return Napi::String::New(info.Env(), this->_filepath);
  }

  Napi::Value GetColumnNames(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (!_isOpen) {
      Napi::Error::New(env, "File is not open").ThrowAsJavaScriptException();
      return env.Null();
    }

    auto results = Napi::Array::New(env, _columnCount);

    for (auto i = 0; i < _columnCount; i++) {
      auto field = _fieldByColumn[i];
      results[i] = Napi::String::New(env, field->name());
    }

    return results;
  }

  Napi::Value ReadRow(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    if (!_isOpen) {
      Napi::Error::New(env, "File is not open").ThrowAsJavaScriptException();
      return env.Null();
    }

    if (info.Length() <= 0 || !info[0].IsNumber()) {
      Napi::TypeError::New(env, "Number expected").ThrowAsJavaScriptException();
      return env.Null();
    }

    auto rowIndex = info[0].As<Napi::Number>().Int64Value();
    auto results = Napi::Array::New(env, _columnCount);

    for (auto i = 0; i < _columnCount; i++) {
      results[i] = this->ReadValue(info, i, rowIndex);
    }

    return results;
  }

  Napi::Value ReadValue(const Napi::CallbackInfo& info, int columnIndex, int rowIndex) {
    Napi::Env env = info.Env();

    auto absoluteIndex = 0;
    auto chunks = _chunksByColumn[columnIndex];

    for (auto chunk : chunks) {
      auto isInChunk =
           rowIndex >= absoluteIndex
        && rowIndex < (absoluteIndex + chunk->length());

      if (!isInChunk) {
        absoluteIndex += chunk->length();
        continue;
      }

      auto index = rowIndex - absoluteIndex;
      auto array = chunk->data();

      switch (_fieldByColumn[columnIndex]->type()->id()) {
        case arrow::Type::BOOL: {
          auto view = array->GetValues<bool>(1, 0);
          auto value = view[index];
          return Napi::Boolean::New(env, value);
        }
        case arrow::Type::TIMESTAMP:
        case arrow::Type::INT64: {
          auto view = array->GetValues<int64_t>(1, 0);
          auto value = view[index];
          return Napi::Number::New(env, value);
        }
        case arrow::Type::DOUBLE: {
          auto view = array->GetValues<double>(1, 0);
          auto value = view[index];
          return Napi::Number::New(env, value);
        }
        case arrow::Type::STRING: {
          const int32_t* offsets = array->GetValues<int32_t>(1, 0);
          const char*    view    = array->GetValues<char>(2, 0);

          auto start = offsets[index];
          auto end   = offsets[index + 1];
          auto data  = &view[start];
          auto length = end - start;

          return Napi::String::New(env, data, length);
        }
        default:
          return env.Null();
      }
    }

    return env.Null();
  }

};

#endif
