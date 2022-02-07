/*
 * writer.js
 */

const lib = require('../lib')
const type = new lib.Type()

const schema = {
  field_0: { type: type.INT32 },
  field_1: { type: type.UTF8 },
}

const writer = new lib.ParquetWriter(schema, 'example-out.parquet')
writer.open()
writer.appendRow({
  field_0: 1,
  field_1: 'Hello World!',
})
writer.close()