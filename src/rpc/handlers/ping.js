'use strict'

const Message = require('../../message')
const utils = require('../../utils')

module.exports = (dht) => {
  const log = utils.logger(dht.peerInfo.id, 'rpc:ping')

  /**
   * Process `Ping` DHT messages.
   *
   * @param {PeerInfo} peer
   * @param {Message} msg
   * @param {function(Error, Message)} callback
   * @returns {undefined}
   */
  return function ping (peer, msg, callback) {
    log('from %s', peer.id.toB58String())

    const response = new Message(Message.TYPES.PING, null, msg.clusterLevel)
    const verification = dht.getVerification()
    if (verification) {
      response.verification = Buffer.from(JSON.stringify(verification))
    }

    return callback(null, response)
  }
}
