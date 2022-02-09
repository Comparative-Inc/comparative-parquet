/*
 * timeUnit.js
 */

const native = require('bindings')('comparative_parquet')

const TimeUnit = native.TimeUnit

// This isn't required but is present for autocompletion tools,
// so they are aware of the keys in this object. All that is really
// required is:
//
//   module.export = native.TimeUnit
//

module.exports = {
    MILLI: TimeUnit.MILLI,
    MICRO: TimeUnit.MICRO,
    NANO:  TimeUnit.NANO,
  }