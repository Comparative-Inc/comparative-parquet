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
  uint64: { type: type.UINT64 },
  int64: { type: type.INT64 },
  float: { type: type.FLOAT },
  double: { type: type.DOUBLE },
  string: { type: type.STRING },
  binary: { type: type.BINARY },
  fixed_size_binary: {
    type: type.FIXED_SIZE_BINARY,
    width: 8,
  },
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
    unit: timeUnit.MICRO,
  },
}

const writer = new lib.ParquetWriter(schema, 'test-out.parquet')
writer.setRowGroupSize(256)
writer.open()
writer.appendRow([
  true,
  1,
  2,
  3,
  4,
  5,
  6,
  1000n,
  7,
  1.0,
  2.0,
  'oatmeal',
  Buffer.from('test'),
  Buffer.from('eightchr'),
  1,
  2,
  3,
  4n,
])
writer.appendRowObject({
  bool: true,
  uint8: 1,
  int8: 2,
  string: 'oatmeal',
  uint16: 3,
  int16: 4,
  date32: 1,
  timestamp: 2,
  time32: 3,
  time64: 4n,
  uint32: 5,
  int32: 6,
  int64: 7,
  float: 1.0,
  double: 2.0,
  uint64: 1000n,
  binary: Buffer.from('test'),
  fixed_size_binary: Buffer.from('eightchr'),
})
writer.close()
