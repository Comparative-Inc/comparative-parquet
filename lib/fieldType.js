/*
 * type.js
 */

const native = require('bindings')('comparative_parquet')

const FieldType = native.FieldType

// This isn't required but is present for autocompletion tools,
// so they are aware of the keys in this object. All that is really
// required is:
//
//   module.export = native.FieldType
//

module.exports = {
  BOOL:               FieldType.BOOL|0,
  UINT8:              FieldType.UINT8|0,
  INT8:               FieldType.INT8|0,
  UINT16:             FieldType.UINT16|0,
  INT16:              FieldType.INT16|0,
  UINT32:             FieldType.UINT32|0,
  INT32:              FieldType.INT32|0,
  INT64:              FieldType.INT64|0,
  FLOAT:              FieldType.FLOAT|0,
  DOUBLE:             FieldType.DOUBLE|0,
  STRING:             FieldType.STRING|0,
  DATE32:             FieldType.DATE32|0,
  TIMESTAMP:          FieldType.TIMESTAMP|0,
  TIME32:             FieldType.TIME32|0,
  TIME64:             FieldType.TIME64|0,
}