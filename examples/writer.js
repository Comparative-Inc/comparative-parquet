/*
 * writer.js
 */

const { ParquetWriter, type, timeUnit } = require('../lib')

const schema = {
  field_0: { type: type.INT32 },
  field_1: { type: type.STRING },
  field_2: { 
    type: type.TIMESTAMP,
    unit: timeUnit.MILLI,
  },
  field_3: {
    type:  type.FIXED_SIZE_BINARY,
    width: 8,
  }
}

const writer = new ParquetWriter(schema, 'example-out.parquet')
writer.open()
writer.appendRow([
  1,
  'As an array',
  2,
  Buffer.from('eightchr'),
])
writer.appendRowObject({
  field_0: 2,
  field_1: 'As a dict',
  field_2: 3,
  field_3: Buffer.from('eightchr'),
})
writer.close()
