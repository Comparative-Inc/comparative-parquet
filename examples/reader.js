/*
 * reader.js
 */

const lib = require('../lib')

const reader = new lib.ParquetReader('/home/romgrk/github/parquet-read/input.parquet')
reader.open()

console.log(reader)
console.log(reader.getFilepath())
console.log(reader.getColumnNames())
console.log(reader.readRow(0))
console.log(reader.readRow(1))
console.log(reader.readRow(2))
console.log(reader.readRow(3))

reader.close()
