/*
 * writer.js
 */

const lib = require('../lib')
const type = lib.type
const timeUnit = lib.timeUnit

const schema = {
  bool: { type: type.BOOL },
  uint8: { type: type.UINT8 },
  int8: { type: type.INT8 },
  uint16: { type: type.UINT16},
  int16: { type: type.INT16},
  uint32: { type: type.UINT32 },
  int32: { type: type.INT32 },
  int64: { type: type.INT64 },
  float: { type: type.FLOAT },
  double: { type: type.DOUBLE },
  string: { type: type.STRING },
  date32: { type: type.DATE32 },
  timestamp: { 
    type: type.TIMESTAMP, 
    unit: timeUnit.MILLI,
  },
  time32: { 
    type: type.TIME32, 
    unit: timeUnit.MILLI,
  },
  time64: { 
    type: type.TIME64, 
    unit: timeUnit.NANO,
  },
}

const writer = new lib.ParquetWriter(schema, 'test-out.parquet')
console.log("pre-open")
writer.open()
console.log("post-open")
writer.appendRow([
  true,
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  1.0,
  2.0,
  'oatmeal',
  1,
  2,
  3,
  4,
])
// writer.appendRowObject({
//   field_0: 2,
//   field_1: 'As a dict',
// })
writer.close()
