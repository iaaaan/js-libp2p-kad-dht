'use strict'

const utils = require('../../utils')
const each = require('async/each')

module.exports = (dht) => {
  const log = utils.logger(dht.peerInfo.id, 'rpc:put-logs')

  /**
   * Process `PutLogs` DHT messages.
   *
   * @param {PeerInfo} peer
   * @param {Message} msg
   * @param {function(Error, Message)} callback
   * @returns {undefined}
   */
  return function putLogs (peer, msg, callback) {
    const key = msg.key
    log('key: %s', key)

    const record = msg.record

    if (!record) {
      log.error('Got empty record from: %s', peer.id.toB58String())
      return callback(new Error('Empty record'))
    }

    dht._verifyRecordLocally(record, (err) => {
      if (err) {
        log.error(err.message)
        return callback(err)
      }

      record.timeReceived = new Date()

      const key = utils.bufferToKey(record.key)
      const logs = record.val

      each(
        logs,
        (log, cb) => {
          dht._appendLocal(key, log, cb)
        },
        (error) => {
          if (error) {
            return callback(error)
          }
          callback(null, msg)
        }
      )
    })
  }
}
