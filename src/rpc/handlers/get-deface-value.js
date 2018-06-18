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
    const date = msg.date
    const signature = msg.signature

    log('key: %s', key)

    if (!key || key.length === 0) {
      return callback(new Error('Invalid key'))
    }

    if (!date || date.length === 0) {
      return callback(new Error('Invalid date'))
    }

    if (!signature || signature.length === 0) {
      return callback(new Error('Invalid signature'))
    }

    const keyAndDate = Buffer.concat(key, date)

    peer.id.pubKey.verify(keyAndDate, signature, (error, verified) => {
      if (error) {
        return callback(new Error('Error verifying request'))
      }
      
      if (!verified) {
        return callback(new Error('Peer could not be verified'))
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

          const idString = peer.id.toB58String()
          dht.verifyPeer(peer.id, key, date, signature, (error, needsVerification) => {
            if (error || needsVerification) {
              return callback('Could not verify peer')
            }
            response.record = record
            callback(null, response)
          })

        }
      })
    })
  }
}
