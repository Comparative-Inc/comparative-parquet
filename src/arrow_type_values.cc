#include "arrow_type_values.h"
#include <arrow/type_fwd.h>

namespace cp {
namespace arrowType {
int BOOL() { return arrow::Type::type::BOOL; }
int UINT8() { return arrow::Type::type::UINT8; }
int INT8() { return arrow::Type::type::INT8; }
int UINT16() { return arrow::Type::type::UINT16; }
int INT16() { return arrow::Type::type::INT16; }
int UINT32() { return arrow::Type::type::UINT32; }
int INT32() { return arrow::Type::type::INT32; }
int UINT64() { return arrow::Type::type::UINT64; }
int INT64() { return arrow::Type::type::INT64; }
int FLOAT() { return arrow::Type::type::FLOAT; }
int DOUBLE() { return arrow::Type::type::DOUBLE; }
int STRING() { return arrow::Type::type::STRING; }
int BINARY() { return arrow::Type::type::BINARY; }
int FIXED_SIZE_BINARY() { return arrow::Type::type::FIXED_SIZE_BINARY; }
int DATE32() { return arrow::Type::type::DATE32; }
int TIMESTAMP() { return arrow::Type::type::TIMESTAMP; }
int TIME32() { return arrow::Type::type::TIME32; }
int TIME64() { return arrow::Type::type::TIME64; }
} // namespace arrowType
namespace arrowTimeUnit {
int MILLI() { return arrow::TimeUnit::type::MILLI; }
int MICRO() { return arrow::TimeUnit::type::MICRO; }
int NANO() { return arrow::TimeUnit::type::NANO; }
} // namespace arrowTimeUnit
} // namespace cp
