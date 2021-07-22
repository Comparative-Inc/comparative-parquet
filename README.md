
# Parquet Node.js bindings

Uses Apache Arrow to provide native bindings for parquet files in nodejs.

```javascript

const parquet = require('comparative-parquet')
const reader = new parquet.ParquetReader('file.parquet')
reader.open()

console.log(reader)
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

```
