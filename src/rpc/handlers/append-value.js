'use strict'

const utils = require('../../utils')

module.exports = (dht) => {
  const log = utils.logger(dht.peerInfo.id, 'rpc:append-value')

  /**
   * Process `AppendValue` DHT messages.
   *
   * @param {PeerInfo} peer
   * @param {Message} msg
   * @param {function(Error, Message)} callback
   * @returns {undefined}
   */
  return function appendValue (peer, msg, callback) {
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

      dht.datastore.append(key, record.serialize(), (err) => {
        if (err) {
          return callback(err)
        }

        callback(null, msg)
      })
    })
  }
}
