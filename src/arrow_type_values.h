#ifndef CP_ARROW_TYPE_VALUES_H
#define CP_ARROW_TYPE_VALUES_H

namespace cp {
namespace arrowType {
int BOOL();
int UINT8();
int INT8();
int UINT16();
int INT16();
int UINT32();
int INT32();
int UINT64();
int INT64();
int FLOAT();
int DOUBLE();
int STRING();
int BINARY();
int FIXED_SIZE_BINARY();
int DATE32();
int TIMESTAMP();
int TIME32();
int TIME64();
} // namespace arrowType
namespace arrowTimeUnit {
int MILLI();
int MICRO();
int NANO();
} // namespace arrowTimeUnit
} // namespace cp

#endif
