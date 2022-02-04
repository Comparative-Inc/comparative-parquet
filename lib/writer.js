/*
 * writer.js
 */

const fs = require('fs')
const native = require('bindings')('comparative_parquet')

const ParquetFileWriter = native.ParquetWriter

class ParquetWriter {
  filepath = null
  writer = null

  constructor(schema, filepath) {
    this.filepath = filepath
    this.writer = new ParquetFileWriter(schema, filepath)
  }

  appendRow(row) {
    this.writer.appendRow(row)
  }

  open() {
    this.writer.open()
  }

  setRowGroupSize(size) {

  }

  close() {

  }
}

/** Creates a writer and opens file directly */
ParquetWriter.openFile = function openFile(schema, filepath) {
  const writer = new ParquetWriter(schema, filepath)
  writer.open()
  return writer
}

module.exports = ParquetWriter