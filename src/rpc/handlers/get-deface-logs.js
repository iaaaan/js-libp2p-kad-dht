'use strict'

const parallel = require('async/parallel')
const Record = require('libp2p-record').Record

const Message = require('../../message')
const utils = require('../../utils')

module.exports = (dht) => {
  const log = utils.logger(dht.peerInfo.id, 'rpc:get-value')

  /**
   * Process `GetDefaceLogs` DHT messages.
   *
   * @param {PeerInfo} peer
   * @param {Message} msg
   * @param {function(Error, Message)} callback
   * @returns {undefined}
   */
  return function getDefaceLogs (peer, msg, callback) {
    const key = msg.key

    log('key: %s', key)

    if (!key || key.length === 0) {
      return callback(new Error('Invalid key'))
    }

    const response = new Message(Message.TYPES.GET_DEFACE_LOGS, key, msg.clusterLevel)

    parallel([
      (cb) => dht._checkLocalLogs(key, cb),
      (cb) => dht._betterPeersToQuery(msg, peer, cb)
    ], (err, res) => {
      if (err) {
        return callback(err)
      }

      const logs = res[0]
      const closer = res[1]

      if (closer.length > 0) {
        log('got closer %s', closer.length)
        response.closerPeers = closer
      }

      if (!logs) {
        callback(null, response)
      } else {
        log('got logs')

        // TODO: robust parsing
        const data = logs.map(log => JSON.parse(log.value.toString()))
        const value = Buffer.from(JSON.stringify(data))
        response.record = new Record(key, value, dht.peerInfo.id)

        callback(null, response)
      }
    })
  }
}
