/*
 * writer.js
 */

const fs = require('fs')
const native = require('bindings')('comparative_parquet')

const ParquetFileWriter = native.ParquetWriter

class ParquetWriter {
  filepath = null
  writer = null

  constructor(filepath, schema) {
    this.filepath = filepath

    const stat = fs.statSync(filepath)

    this.writer = new ParquetFileWriter(filepath, schema)
  }

  appendRow(row) {
    this.writer.appendRow(row)
  }
}

module.exports = ParquetWriter