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
  BOOLEAN:          FieldType.BOOLEAN|0,
  INT32:            FieldType.INT32|0,
  INT64:            FieldType.INT64|0,
  DOUBLE:           FieldType.DOUBLE|0,
  DATE32:           FieldType.DATE32|0,
  TIMESTAMP_MICROS: FieldType.TIMESTAMP_MICROS|0,
  TIMESTAMP_MILLIS: FieldType.TIMESTAMP_MILLIS|0,
  UTF8:             FieldType.UTF8|0,
}