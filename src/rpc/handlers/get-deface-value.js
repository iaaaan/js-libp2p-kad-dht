'use strict'

const parallel = require('async/parallel')
const Record = require('libp2p-record').Record

const Message = require('../../message')
const utils = require('../../utils')

module.exports = (dht) => {
  const log = utils.logger(dht.peerInfo.id, 'rpc:get-value')

  /**
   * Process `GetDefaceValue` DHT messages.
   *
   * @param {PeerInfo} peer
   * @param {Message} msg
   * @param {function(Error, Message)} callback
   * @returns {undefined}
   */
  return function getDefaceValue (peer, msg, callback) {
    const key = msg.key

    log('key: %s', key)

    if (!key || key.length === 0) {
      return callback(new Error('Invalid key'))
    }

    const response = new Message(Message.TYPES.GET_DEFACE_VALUE, key, msg.clusterLevel)

    parallel([
      (cb) => dht._checkLocalDatastore(key, cb),
      (cb) => dht._betterPeersToQuery(msg, peer, cb)
    ], (err, res) => {
      if (err) {
        return callback(err)
      }

      const record = res[0]
      const closer = res[1]

      if (closer.length > 0) {
        log('got closer %s', closer.length)
        response.closerPeers = closer
      }

      if (!record) {
        callback(null, response)
      } else {
        log('got record')

        const peerId = peer.id.toB58String()
        dht.verifyPeer(peerId, key.toString(), (error, needsVerification) => {
          if (error || needsVerification) {
            return callback(error || `peer ${peerId} needs to verify identity`)
          }
          response.record = record
          callback(null, response)
        })

      }
    })
  }
}
