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
    this.writer.appendRowArray(row)
  }

  appendRowObject(row) {
    this.writer.appendRowObject(row)
  }

  open() {
    this.writer.open()
  }

  setRowGroupSize(size) {
    this.writer.setRowGroupSize(size)
  }

  close() {
    // This doesn't actually do anything because I can't manually close
    // the writer. It closes when the object falls out of scope
  }
}

/** Creates a writer and opens file directly */
ParquetWriter.openFile = function openFile(schema, filepath) {
  const writer = new ParquetWriter(schema, filepath)
  writer.open()
  return writer
}

module.exports = ParquetWriter