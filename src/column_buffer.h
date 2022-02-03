#ifndef COLUMN_DATA_H
#define COLUMN_DATA_H

#include <vector>
#include <string>

#include <napi.h>

#include <arrow/array.h>
#include <arrow/builder.h>

class ColumnBuffer {
public:
  const std::string name;

  ColumnBuffer(const std::string& name) : name(name) { }

  virtual std::shared_ptr<arrow::Array> finish() = 0;
  virtual void push_back(const Napi::Value& value) = 0;
};

template<typename T>
class ColumnBufferBasic;

// These all have to be full class specializations because we're doing funky
// things with polymorphism
template<>
class ColumnBufferBasic<int32_t> : public ColumnBuffer {
public:
  ColumnBufferBasic(const std::string& name) : ColumnBuffer(name), builder() { }

  std::shared_ptr<arrow::Array> finish() override {
    std::shared_ptr<arrow::Array> out;
    builder.Finish(&out);
    return out;
  }

  inline void push_back(const Napi::Value& value) override {
    builder.Append(value.ToNumber().Int32Value());
  }

protected:
  arrow::Int32Builder builder;
};


template<>
class ColumnBufferBasic<int64_t> : public ColumnBuffer {
public:
  ColumnBufferBasic(const std::string& name) : ColumnBuffer(name), builder() { }

  std::shared_ptr<arrow::Array> finish() override {
    std::shared_ptr<arrow::Array> out;
    builder.Finish(&out);
    return out;
  }

  inline void push_back(const Napi::Value& value) override {
    builder.Append(value.ToNumber().Int64Value());
  }

protected:
  arrow::Int64Builder builder;
};


template<>
class ColumnBufferBasic<bool> : public ColumnBuffer {
public:
  ColumnBufferBasic(const std::string& name) : ColumnBuffer(name), builder() { }

  std::shared_ptr<arrow::Array> finish() override {
    std::shared_ptr<arrow::Array> out;
    builder.Finish(&out);
    return out;
  }

  inline void push_back(const Napi::Value& value) override {
    builder.Append(value.ToBoolean().Value());
  }

protected:
  arrow::BooleanBuilder builder;
};


namespace ColumnBufferFactory {
  std::unique_ptr<ColumnBuffer> makeBuffer(const std::string& name, const std::shared_ptr<arrow::DataType>& t) {
    switch (t->id()) {
    case arrow::Type::BOOL:
      return std::make_unique<ColumnBufferBasic<bool>>(name);

    case arrow::Type::INT32:
    case arrow::Type::DATE32:
      return std::make_unique<ColumnBufferBasic<int32_t>>(name);

    case arrow::Type::INT64:
    case arrow::Type::TIMESTAMP:
      return std::make_unique<ColumnBufferBasic<int64_t>>(name);
    
    default:
      throw std::invalid_argument("Unsupported type");
    }
  }
}

#endif // COLUMN_DATA_H