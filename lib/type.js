/*
 * type.js
 */

const native = require('bindings')('comparative_parquet')

const FieldType = native.FieldType

class Type {
  INT32 = null
  INT64 = null
  TIMESTAMP_MICROS = null
  TIMESTAMP_MILLIS = null
  DATE32 = null
  DOUBLE = null
  BOOLEAN = null
  UTF8 = null

  // Not sure if this is the best way to do this
  constructor(schema, filepath) {
    var tmp = new FieldType()
    this.INT32            = tmp.INT32
    this.INT64            = tmp.INT64
    this.TIMESTAMP_MICROS = tmp.TIMESTAMP_MICROS
    this.TIMESTAMP_MILLIS = tmp.TIMESTAMP_MILLIS
    this.DATE32           = tmp.DATE32
    this.DOUBLE           = tmp.DOUBLE
    this.BOOLEAN          = tmp.BOOLEAN
    this.UTF8             = tmp.UTF8
  }
}

module.exports = Type