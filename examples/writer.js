/*
 * writer.js
 */

const lib = require('../lib')
const type = lib.type
const timeUnit = lib.type

const schema = {
  field_0: { type: type.INT32 },
  field_1: { type: type.UTF8 },
  field_2: { 
    type: type.TIMESTAMP,
    unit: timeUnit.MILLI,
  },
}

const writer = new lib.ParquetWriter(schema, 'example-out.parquet')
writer.open()
writer.appendRow([
  1,
  'As an array',
  2,
])
writer.appendRowObject({
  field_0: 2,
  field_1: 'As a dict',
  field_2: 3,
})
writer.close()
