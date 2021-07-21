
# Parquet Node.js bindings

Uses Apache Arrow to provide native bindings for parquet files in nodejs.

```javascript

const parquet = require('comparative-parquet')
const reader = new parquet.ParquetReader('file.parquet')
reader.open()

console.log(reader)
console.log(reader.getFilepath())
console.log(reader.getColumnNames())
console.log(reader.readRow(0))
console.log(reader.readRow(1))
console.log(reader.readRow(2))
console.log(reader.readRow(3))

reader.close()

```
