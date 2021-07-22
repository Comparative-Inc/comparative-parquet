/*
 * reader.js
 */

const lib = require('../lib')

const reader = new lib.ParquetReader('/home/romgrk/github/parquet-read/input.parquet')

console.log(reader.open())
console.log(reader.getFilepath())
console.log(reader.getColumnNames())
console.log(reader.getColumnCount())
console.log(reader.getRowCount())

console.log(reader.readRow(0))

console.log(reader.readRowAsArray(0))
console.log(reader.readRowAsArray(1))
console.log(reader.readRowAsArray(2))
console.log(reader.readRowAsArray(3))

reader.close()
