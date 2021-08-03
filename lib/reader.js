/*
 * reader.js
 */


const fs = require('fs')
const path = require('path')
const assert = require('assert')
const native = require('bindings')('comparative_parquet')

const ParquetFileReader = native.ParquetReader

class ParquetReader {
  filepath = null
  files = null
  readers = null
  rowCounts = null

  constructor(filepath) {
    this.filepath = filepath

    const stat = fs.statSync(filepath)
    if (stat.isDirectory()) {
      this.files = fs.readdirSync(filepath).map(filename => path.join(filepath, filename))
    }
    else if (stat.isFile()) {
      this.files = [filepath]
    }
    else {
      throw new Error('Unsupported node type: ' + filepath)
    }

    this.readers = this.files.map(filepath => new ParquetFileReader(filepath))
  }

  execute(fn) {
    return this.readers.map(fn)
  }

  // Replicate ParquetFileReader API

  getFilepath() {
    return this.filepath
  }

  getColumnNames() {
    return this.readers[0].getColumnNames()
  }

  getColumnCount() {
    return this.readers[0].getColumnCount()
  }

  getRowCount() {
    return this.rowCounts.reduce((acc, cur) => acc + cur)
  }

  open() {
    this.rowCounts = []
    const columnCounts = []
    const columnNames  = []
    this.execute((r, i) => {
      r.open()
      this.rowCounts[i] = r.getRowCount()
      columnCounts[i] = r.getColumnCount()
      columnNames[i] = r.getColumnNames()
    })
    for (let i = 1; i < this.files.length; i++) {
      assert.equal(columnCounts[i], columnCounts[0], 'Mismatched column count')
      assert.deepEqual(columnNames[i], columnNames[0], 'Mismatched column count')
    }
  }

  close() {
    this.execute(r => r.open())
  }

  readRow(index) {
    let readerIndex = 0
    let actualIndex = index

    while ((actualIndex - this.rowCounts[readerIndex]) >= 0) {
      actualIndex -= this.rowCounts[readerIndex]
      readerIndex += 1
    }

    return this.readers[readerIndex].readRow(actualIndex)
  }

  readRowAsArray(index) {
    let readerIndex = 0
    let actualIndex = index

    while ((actualIndex - this.rowCounts[readerIndex]) >= 0) {
      actualIndex -= this.rowCounts[readerIndex]
      readerIndex += 1
    }

    return this.readers[readerIndex].readRowAsArray(actualIndex)
  }
}


/** Creates a reader and opens file directly */
ParquetReader.openFile = function openFile(filepath) {
  const reader = new ParquetReader(filepath)
  reader.open()
  return reader
}


module.exports = ParquetReader
