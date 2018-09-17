'use strict'

const KBucket = require('k-bucket')
const map = require('async/map')
const waterfall = require('async/waterfall')
const filterSeries = require('async/filterSeries')

const utils = require('./utils')

/**
 * A wrapper around `k-bucket`, to provide easy store and
 * retrival for peers.
 */
class RoutingTable {
  /**
   * @param {PeerId} self
   * @param {number} kBucketSize
   * @param {number} numberOfNodesToPing
   */
  constructor (self, kBucketSize, numberOfNodesToPing, dht) {
    this.self = self
    this._onPing = this._onPing.bind(this)
    this.dht = dht

    utils.convertPeerId(self, (err, selfKey) => {
      if (err) {
        throw err
      }

      this.kb = new KBucket({
        localNodeId: selfKey,
        numberOfNodesPerKBucket: kBucketSize,
        numberOfNodesToPing
      })

      this.kb.on('ping', this._onPing)
    })
  }

  // -- Private Methods

  /**
   * Called on the `ping` event from `k-bucket`.
   * Currently this just removes the oldest contact from
   * the list, without acutally pinging the individual peers.
   * This is the same as go does, but should probably
   * be upgraded to actually ping the individual peers.
   *
   * @param {Array<Object>} oldContacts
   * @param {Object} newContact
   * @returns {undefined}
   * @private
   */
  _onPing (oldContacts, newContact) {
    console.log('_onPing')
    const peers = oldContacts.map(contact => contact.peer)

    waterfall(
      [
        this.dht.getServerPublicKey,
        (serverPublicKey, callback) => {
          map(
            peers,
            this.dht._ping,
            (error, tokens) => {
              if (error) {
                return callback(error)
              }
              filterSeries(
                tokens,
                (token, callback) => {
                  const signedBuffer = Buffer.from(JSON.stringify({id: token.id, date: token.date}))
                  const signatureBuffer = new Buffer(signature.data)
                  serverPublicKey.verify(signedBuffer, signatureBuffer, callback)
                },
                callback
              )
            }
          )
        },
        (verifiedTokens, callback) => {
          const verifiedIds = {}
          verifiedTokens.forEach(token => {
            verifiedIds[token.id] = token.date
          })

          const tokenLessPeers = oldContacts.filter(contact => !verifiedIds[contact.peer.toB58String()])
          if (tokenLessPeers.length > 0) {
            return callback(null, tokenLessPeers[0])
          }

          const peers = oldContacts.filter(contact => verifiedIds[contact.peer.toB58String()])
            .sort((p1, p2) => verifiedIds[p2.peer.toB58String()] - verifiedIds[p1.peer.toB58String()])
        }
      ],
      (error, contact) => {
        if (error) {
          return console.log('DHT ping failed:', error)
        }

        console.log('REMOVING', contact)

        this.kb.remove(contact.id)
        this.kb.add(newContact)
      }
    )    
  }

  // -- Public Interface

  /**
   * Amount of currently stored peers.
   *
   * @type {number}
   */
  get size () {
    return this.kb.count()
  }

  /**
   * Find a specific peer by id.
   *
   * @param {PeerId} peer
   * @param {function(Error, PeerId)} callback
   * @returns {void}
   */
  find (peer, callback) {
    utils.convertPeerId(peer, (err, key) => {
      if (err) {
        return callback(err)
      }
      const closest = this.closestPeer(key)

      if (closest && closest.isEqual(peer)) {
        return callback(null, closest)
      }

      callback()
    })
  }

  /**
   * Retrieve the closest peers to the given key.
   *
   * @param {Buffer} key
   * @param {number} count
   * @returns {PeerId|undefined}
   */
  closestPeer (key, count) {
    const res = this.closestPeers(key, 1)
    if (res.length > 0) {
      return res[0]
    }
  }

  /**
   * Retrieve the `count`-closest peers to the given key.
   *
   * @param {Buffer} key
   * @param {number} count
   * @returns {Array<PeerId>}
   */
  closestPeers (key, count) {
    return this.kb.closest(key, count).map((p) => p.peer)
  }

  /**
   * Add or update the routing table with the given peer.
   *
   * @param {PeerId} peer
   * @param {function(Error)} callback
   * @returns {undefined}
   */
  add (peer, callback) {
    utils.convertPeerId(peer, (err, id) => {
      if (err) {
        return callback(err)
      }
      this.kb.add({ id: id, peer: peer })
      callback()
    })
  }

  /**
   * Remove a given peer from the table.
   *
   * @param {PeerId} peer
   * @param {function(Error)} callback
   * @returns {undefined}
   */
  remove (peer, callback) {
    utils.convertPeerId(peer, (err, id) => {
      if (err) {
        return callback(err)
      }
      this.kb.remove(id)
      callback()
    })
  }
}

module.exports = RoutingTable
