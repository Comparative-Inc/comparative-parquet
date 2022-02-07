
# Parquet Node.js bindings

Uses Apache Arrow to provide native bindings for parquet files in nodejs.

```javascript
const parquet = require('comparative-parquet')
const reader = parquet.ParquetReader.openFile('file.parquet')

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

`ParquetReader.openFile` can also take as input a directory path, with files that
are all parquet files with matching schemas, and will operate on them as if they
were a single file.

```javascript
const parquet = require('comparative-parquet')
const type = new parquet.Type()

const schema = {
  field_0: { type: type.INT32 },
  field_1: { type: type.UTF8 },
}

const writer = new lib.ParquetWriter(schema, 'example-out.parquet')
writer.open()
writer.appendRowObject({
  field_0: 1,
  field_1: 'As a dict',
})
writer.appendRowArray([
  2,
  'As an array',
])
writer.close()
```

### Development

To develop this module, after running `npm install`, `node-gyp` is the build
tool used for nodejs native modules. The NPM scripts in `package.json` show
how to run the command. For development, run `npm run configure:debug` once,
then `npm run build` to rebuild the module after doing some changes.

NOTE: Before configuring & building, you may want to run `npm run compile-commands`
that will generate a `compile-commands.json` file to provide correct auto-completion
for most editors running an LSP/intellisense server.

The `bindings.gyp` file provides the build configuration for `node-gyp`. It needs
to contain all files (compilation units) that are part of the module as well as
any library or build flag required. Every time a new C++ file is added to the project,
add it there and run again the configuration and compile-commands scripts.
