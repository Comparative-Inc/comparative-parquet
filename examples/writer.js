/*
 * writer.js
 */

const lib = require('../lib')

const schema = {
  field_0: { type: 'INT64' },
  field_1: { type: 'UTF8' },
}

const writer = new lib.ParquetWriter(schema, 'example-out.parquet')
writer.open()
writer.appendRow({
  field_0: 1,
  field_1: 'Hello World!',
})
writer.close()