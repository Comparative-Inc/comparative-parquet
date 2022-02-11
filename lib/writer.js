/*
 * writer.js
 */

const fs = require('fs')
const native = require('bindings')('comparative_parquet')

const ParquetFileWriter = native.ParquetWriter

class ParquetWriter {
  filepath = null
  schema = null
  writer = null

  constructor(schema, filepath) {
    this.filepath = filepath
    this.schema = schema
    this.writer = new ParquetFileWriter(schema, filepath)
  }

  appendRow(row) {
    this.writer.appendRowArray(row)
  }

  appendRowObject(row) {
    const rowArray = []
    for (const key in this.schema) {
      rowArray.push(row[key])
    }
    this.writer.appendRowArray(rowArray)
  }

  open() {
    this.writer.open()
  }

  close() {
    this.writer.close()
  }
}

/** Creates a writer and opens file directly */
ParquetWriter.openFile = function openFile(schema, filepath) {
  const writer = new ParquetWriter(schema, filepath)
  writer.open()
  return writer
}

module.exports = ParquetWriter