'use strict'

const PeerId = require('peer-id')
const libp2pRecord = require('libp2p-record')
const waterfall = require('async/waterfall')
const series = require('async/series')
const filterSeries = require('async/filterSeries')
const map = require('async/map')
const each = require('async/each')
const timeout = require('async/timeout')
const PeerInfo = require('peer-info')

const utils = require('./utils')
const errors = require('./errors')
const Message = require('./message')
const c = require('./constants')
const Query = require('./query')
const LimitedPeerList = require('./limited-peer-list')

const Record = libp2pRecord.Record

module.exports = (dht) => ({
  /**
   * Returns the routing tables closest peers, for the key of
   * the message.
   *
   * @param {Message} msg
   * @param {function(Error, Array<PeerInfo>)} callback
   * @returns {undefined}
   * @private
   */
  _nearestPeersToQuery (msg, callback) {
    utils.convertBuffer(msg.key, (err, key) => {
      if (err) {
        return callback(err)
      }
      let ids
      try {
        ids = dht.routingTable.closestPeers(key, dht.ncp)
      } catch (err) {
        return callback(err)
      }

      callback(null, ids.map((p) => {
        if (dht.peerBook.has(p)) {
          return dht.peerBook.get(p)
        } else {
          return dht.peerBook.put(new PeerInfo(p))
        }
      }))
    })
  },
  /**
   * Get the nearest peers to the given query, but iff closer
   * than self.
   *
   * @param {Message} msg
   * @param {PeerInfo} peer
   * @param {function(Error, Array<PeerInfo>)} callback
   * @returns {undefined}
   * @private
   */
  _betterPeersToQuery (msg, peer, callback) {
    dht._log('betterPeersToQuery')
    dht._nearestPeersToQuery(msg, (err, closer) => {
      if (err) {
        return callback(err)
      }

      const filtered = closer.filter((closer) => {
        if (dht._isSelf(closer.id)) {
          // Should bail, not sure
          dht._log.error('trying to return self as closer')
          return false
        }

        return !closer.id.isEqual(peer.id)
      })

      callback(null, filtered)
    })
  },
  /**
   * Try to fetch a given record by from the local datastore.
   * Returns the record if it is still valid, meaning
   * - it was either authored by this node, or
   * - it was receceived less than `MAX_RECORD_AGE` ago.
   *
   * @param {Buffer} key
   * @param {function(Error, Record)} callback
   * @returns {undefined}
   *
   *@private
   */
  _checkLocalValues (key, callback) {
    dht._log('checkLocalValues: %s', key)
    const dsKey = utils.bufferToKey(key)

    // 2. fetch value from ds
    dht.datastore.has(dsKey, (err, exists) => {
      if (err) {
        return callback(err)
      }
      if (!exists) {
        return callback()
      }

      dht.datastore.get(dsKey, (err, res) => {
        if (err) {
          return callback(err)
        }

        // 4. create record from the returned bytes
        let record
        try {
          record = Record.deserialize(res)
        } catch (err) {
          return callback(err)
        }

        if (!record) {
          return callback(new Error('Invalid record'))
        }

        // 5. check validity

        // 5. if: we are the author, all good
        if (record.author.isEqual(dht.peerInfo.id)) {
          return callback(null, record)
        }

        //    else: compare recvtime with maxrecordage
        if (record.timeReceived == null ||
            utils.now() - record.timeReceived > c.MAX_RECORD_AGE) {
          // 6. if: record is bad delete it and return
          return dht.datastore.delete(key, callback)
        }
        
        //    else: return good record
        return callback(null, record)
      })
    })
  },
  /**
   * Try to fetch a given record by from the local datastore.
   * Returns the record if it is still valid, meaning
   * - it was either authored by this node, or
   * - it was receceived less than `MAX_RECORD_AGE` ago.
   *
   * @param {Buffer} key
   * @param {function(Error, Record)} callback
   * @returns {undefined}
   *
   *@private
   */
  _checkLocalLogs (key, callback) {
    // TODO: not sure how different this is from _getLocalLogs
    dht._log('checkLocalLogs: %s', key)
    const dsKey = utils.bufferToKey(key)

    // 2. fetch value from ds
    dht.datastore.has(dsKey, (err, exists) => {
      if (err) {
        return callback(err)
      }
      if (!exists) {
        return callback()
      }

      dht.datastore.get(dsKey, (err, data) => {

        if (err) {
          return callback(err)
        }

        if (!(data instanceof Array)) {
          return callback('Data not logs')
        }

        if (data.length === 0) {
          return callback('Empty logs')
        }

        // TODO: this should probably be filter instead of filterSeries
        filterSeries(
          data,
          (raw, cb) => {
            let rec
            try {
              rec = Record.deserialize(raw)
            } catch (err) {
              dht._log('Error deserializing log: %s', err)
              return cb(null, false)
            }

            // TODO: dht._verifyRecordLocally ?
            return cb(null, true)
          },
          (error, filteredData) => {
            if (filteredData.length === 0) {
              return cb('All logs are invalid')
            }

            const logs = filteredData.map(raw => Record.deserialize(raw))
            callback(null, logs)
          }
        )
      })
    })
  },
  /**
   * Add the peer to the routing table and update it in the peerbook.
   *
   * @param {PeerInfo} peer
   * @param {function(Error)} callback
   * @returns {undefined}
   *
   * @private
   */
  _add (peer, callback) {
    peer = dht.peerBook.put(peer)
    dht.routingTable.add(peer.id, callback)
  },
  /**
   * Verify a record without searching the DHT.
   *
   * @param {Record} record
   * @param {function(Error)} callback
   * @returns {undefined}
   *
   * @private
   */
  _verifyRecordLocally (record, callback) {
    dht._log('verifyRecordLocally')
    series([
      (cb) => {
        if (record.signature) {
          const peer = record.author
          let info
          if (dht.peerBook.has(peer)) {
            info = dht.peerBook.get(peer)
          }

          if (!info || !info.id.pubKey) {
            return callback(new Error('Missing public key for: ' + peer.toB58String()))
          }

          record.verifySignature(info.id.pubKey, cb)
        } else {
          cb()
        }
      },
      (cb) => libp2pRecord.validator.verifyRecord(
        dht.validators,
        record,
        cb
      )
    ], callback)
  },
  /**
   * Find close peers for a given peer
   *
   * @param {Buffer} key
   * @param {PeerId} peer
   * @param {function(Error)} callback
   * @returns {void}
   *
   * @private
   */
  _closerPeersSingle (key, peer, callback) {
    dht._log('_closerPeersSingle %s from %s', key, peer.toB58String())
    dht._findPeerSingle(peer, new PeerId(key), (err, msg) => {
      if (err) {
        return callback(err)
      }

      const out = msg.closerPeers
        .filter((pInfo) => !dht._isSelf(pInfo.id))
        .map((pInfo) => dht.peerBook.put(pInfo))

      callback(null, out)
    })
  },
  /**
   * Is the given peer id the peer id?
   *
   * @param {PeerId} other
   * @returns {bool}
   *
   * @private
   */
  _isSelf (other) {
    return other && dht.peerInfo.id.id.equals(other.id)
  },
  /**
   * Ask peer `peer` if they know where the peer with id `target` is.
   *
   * @param {PeerId} peer
   * @param {PeerId} target
   * @param {function(Error)} callback
   * @returns {void}
   *
   * @private
   */
  _findPeerSingle (peer, target, callback) {
    dht._log('_findPeerSingle %s', peer.toB58String())
    const msg = new Message(Message.TYPES.FIND_NODE, target.id, 0)
    dht.network.sendRequest(peer, msg, callback)
  },
  /**
   * Store the given key/value pair at the peer `target`.
   *
   * @param {Buffer} key
   * @param {Buffer} rec - encoded record
   * @param {PeerId} target
   * @param {function(Error)} callback
   * @returns {void}
   *
   * @private
   */
  _putValueToPeer (key, rec, target, callback) {
    const msg = new Message(Message.TYPES.PUT_VALUE, key, 0)
    msg.record = rec

    dht.network.sendRequest(target, msg, (err, resp) => {
      if (err) {
        return callback(err)
      }

      if (!resp.record.value.equals(Record.deserialize(rec).value)) {
        return callback(new Error('value not put correctly'))
      }

      callback()
    })
  },
  /**
   * Store the given key/value pair at the peer `target`.
   *
   * @param {Buffer} key
   * @param {Buffer} rec - encoded record
   * @param {PeerId} target
   * @param {function(Error)} callback
   * @returns {void}
   *
   * @private
   */
  _putLogsToPeer (key, rec, target, callback) {
    const msg = new Message(Message.TYPES.PUT_LOGS, key, 0)
    msg.record = rec

    dht.network.sendRequest(target, msg, (err, resp) => {
      if (err) {
        return callback(err)
      }

      if (!resp.record.value.equals(Record.deserialize(rec).value)) {
        return callback(new Error('value not put correctly'))
      }

      callback()
    })
  },
  /**
   * Store the given key/value pair at the peer `target`.
   *
   * @param {Buffer} key
   * @param {Buffer} rec - encoded record
   * @param {PeerId} target
   * @param {function(Error)} callback
   * @returns {void}
   *
   * @private
   */
  _appendValueToPeer (key, rec, target, callback) {
    const msg = new Message(Message.TYPES.APPEND_VALUE, key, 0)
    msg.record = rec

    dht.network.sendRequest(target, msg, (err, resp) => {
      if (err) {
        return callback(err)
      }

      if (!resp.record.value.equals(Record.deserialize(rec).value)) {
        return callback(new Error('value not appended correctly'))
      }

      callback()
    })
  },
  /**
   * Store the given key/value pair locally, in the datastore.
   * @param {Buffer} key
   * @param {Buffer} rec - encoded record
   * @param {function(Error)} callback
   * @returns {void}
   *
   * @private
   */
  _putLocal (key, rec, callback) {
    dht.datastore.put(utils.bufferToKey(key), rec, callback)
  },
  /**
   * Store the given key/logs pair locally, in the datastore.
   * @param {Buffer} key
   * @param {Buffer} logsRecord
   * @param {function(Error)} callback
   * @returns {void}
   *
   * @private
   */
  _appendLocal (key, rec, callback) {
    dht.datastore.append(utils.bufferToKey(key), rec, callback)
  },
  /**
   * Get the value to the given key.
   *
   * @param {Buffer} key
   * @param {number} maxTimeout
   * @param {function(Error, Buffer)} callback
   * @returns {void}
   *
   * @private
   */
  _get (key, verification, maxTimeout, callback) {
    dht._log('_get %s', key.toString())
    waterfall([
      (cb) => {
        return dht.getMany(key, verification, 16, maxTimeout, cb)
      },
      (vals, cb) => {
        const recs = vals.map((v) => v.val)
        // const i = libp2pRecord.selection.bestRecord(dht.selectors, key, recs)
        const best = recs[0]
        dht._log('GetValue %s %s', key.toString(), best)

        if (!best) {
          return cb(new errors.NotFoundError())
        }

        // Send out correction record
        waterfall([
          (cb) => {
            utils.createPutRecord(key, best, dht.peerInfo.id, true, cb)
          },
          (fixupRec, cb) => each(vals, (v, cb) => {
            // no need to do anything
            if (v.val.equals(best)) {
              return cb()
            }

            // correct ourself
            if (dht._isSelf(v.from)) {
              return dht._putLocal(key, fixupRec, (err) => {
                if (err) {
                  dht._log.error('Failed error correcting self', err)
                }
                cb()
              })
            }

            // send correction
            dht._putValueToPeer(key, fixupRec, v.from, (err) => {
              if (err) {
                dht._log.error('Failed error correcting entry', err)
              }
              cb()
            })
          }, cb)
        ], (err) => cb(err, err ? null : best))
      }
    ], callback)
  },
  /**
   * Get logs to the given key.
   *
   * @param {Buffer} key
   * @param {number} maxTimeout
   * @param {function(Error, Buffer)} callback
   * @returns {void}
   *
   * @private
   */
  _getLogs (peerId, key, maxTimeout, callback) {
    dht._log('_getLogs %s', key.toString())
    waterfall([
      (cb) => dht.getManyLogs(key, 16, maxTimeout, cb),
      (vals, cb) => {
        // TODO: the way buffers are parsed then stringified might change their form, thus messing up signature/verification

        /*
          Log object:
          key:"5dsFZbWsNEYuuPefomVZh9v7pNR1fe"
          signature:{type: "Buffer", data: Array(256)}
        */

        const logsPerPeer = {}
        vals.forEach(record => {
          let logs
          try {
            const data = new Buffer(record.val).toString()
            logs = JSON.parse(data)
              .map(log => {
                return {
                  key: log.key,
                  signature: new Buffer(log.signature.data)
                }
              })
          } catch (e) {}
          if (record.from && logs) {
            logsPerPeer[record.from] = logs
          }
        })

        const allLogs = Object.keys(logsPerPeer)
          .map(peer => logsPerPeer[peer])
          .reduce((logs, peerLogs) => logs.concat(peerLogs), [])
          .reduce((logs, l1) => {
            if (logs.find(l2 => l1.key === l2.key && l1.signature.equals(l2.signature))) {
              return logs
            }
            return logs.concat([l1])
          }, [])

        filterSeries(
          allLogs,
          (record, cb) => {
            const signedBuffer = Buffer.from(
              JSON.stringify({
                key: record.key,
                verification: key
              })
            )

            if (peerId.pubKey) {
              // LOCAL VERIFICATION
              peerId.pubKey.verify(signedBuffer, record.signature, (error, verified) => {
                if (error) {
                  return cb(null, false)
                }
                cb(null, verified)
              })
            } else {
              // ONLINE VERIFICATION
              waterfall(
                [
                  (cb) => dht.getPublicKey(peerId, cb),
                  (publicKey, cb) => {
                    publicKey.verify(signedBuffer, record.signature, cb)
                  }
                ],
                (error, verified) => {
                  if (error) {
                    return cb(null, false)
                  }
                  cb(null, verified)
                }
              )
            }
          },
          (error, logs) => {
            if (error) {
              return cb(error)
            }

            if (!logs || logs.length === 0) {
              return cb(new errors.NotFoundError())
            }

            waterfall([
              (cb) => utils.createLogsRecord(key, logs, dht.peerInfo.id, true, cb),
              (fixupRec, cb) => {
                
                each(
                  Object.keys(logsPerPeer),
                  (peerId, cb) => {
                    const peerLogs = logsPerPeer[peerId]

                    // TODO optimize
                    if (
                      !peerLogs.find(l1 => !logs.find(l2 => l1.key === l2.key && l1.signature.equals(l2.signature))) &&
                      !logs.find(l1 => !peerLogs.find(l2 => l1.key === l2.key && l1.signature.equals(l2.signature)))
                    ) {
                      // all logs match
                      return cb()
                    }

                    // correct ourself
                    if (dht._isSelf(peerId)) {
                      return each(
                        logs,
                        (log, cb) => {
                          const rec = Buffer.from(JSON.stringify(log))
                          dht._appendLocal(key, rec, (err) => {
                            if (err) {
                              dht._log.error('Failed error correcting self', err)
                            }
                            cb()
                          })
                        },
                        (error) => {
                          cb(error)
                        }
                      )
                    }

                    // send correction
                    dht._putLogsToPeer(key, fixupRec, peerId, (err) => {
                      if (err) {
                        dht._log.error('Failed error correcting entry', err)
                      }
                      cb()
                    })



                  },
                  cb
                )

                // each(
                //   vals,
                //   (v, cb) => {

                //     // TODO optimize
                //     if (
                //       v.val.filter(l1 => !logs.some(l2 => l1.equals(l2))).length === 0 && 
                //       logs.filter(l1 => !v.val.some(l2 => l1.equals(l2))).length === 0 
                //     ) {
                //       return cb()
                //     }

                //     // correct ourself
                //     if (dht._isSelf(v.from)) {
                //       return each(
                //         logs,
                //         (log, cb) => {
                //           dht._appendLocal(key, log, (err) => {
                //             if (err) {
                //               dht._log.error('Failed error correcting self', err)
                //             }
                //             cb()
                //           })
                //         },
                //         (error) => {
                //           cb(error)
                //         }
                //       )
                //     }

                //     // send correction
                //     dht._putLogsToPeer(v.from, key, fixupRec, (err) => {
                //       if (err) {
                //         dht._log.error('Failed error correcting entry', err)
                //       }
                //       cb()
                //     })
                //   },
                //   cb
                // )
              }
            ], 
            (err) => cb(err, err ? null : logs))

            // Send out correction record
            // waterfall([

            //   (cb) => utils.createPutRecord(key, best, dht.peerInfo.id, true, cb),
            //   (fixupRec, cb) => each(vals, (v, cb) => {
            //     // no need to do anything
                // if (v.val.equals(best)) {
                //   return cb()
                // }

            //     // correct ourself
            //     if (dht._isSelf(v.from)) {
            //       return dht._putLocal(key, fixupRec, (err) => {
            //         if (err) {
            //           dht._log.error('Failed error correcting self', err)
            //         }
            //         cb()
            //       })
            //     }

            //     // send correction
                // dht._putValueToPeer(v.from, key, fixupRec, (err) => {
                //   if (err) {
                //     dht._log.error('Failed error correcting entry', err)
                //   }
                //   cb()
                // })
            //   }, cb)
            // ], (err) => cb(err, err ? null : best))


          }
        )
      }
    ], callback)
  },
  /**
   * Attempt to retrieve the value for the given key from
   * the local datastore.
   *
   * @param {Buffer} key
   * @param {function(Error, Record)} callback
   * @returns {void}
   *
   * @private
   */
  _getLocalValues (key, callback) {
    dht._log('getLocal %s', key)

    waterfall([
      (cb) => dht.datastore.get(utils.bufferToKey(key), cb),
      (raw, cb) => {
        dht._log('found %s in local datastore', key)
        let rec
        try {
          rec = Record.deserialize(raw)
        } catch (err) {
          return cb(err)
        }

        dht._verifyRecordLocally(rec, (err) => {
          if (err) {
            return cb(err)
          }

          cb(null, rec)
        })
      }
    ], callback)
  },
  /**
   * Attempt to retrieve the logs for the given key from
   * the local datastore.
   *
   * @param {Buffer} key
   * @param {function(Error, Record)} callback
   * @returns {void}
   *
   * @private
   */
  _getLocalLogs (key, callback) {
    dht._log('getLocal %s', key)

    waterfall([
      (cb) => dht.datastore.get(utils.bufferToKey(key), cb),
      (data, cb) => {
        dht._log('found %s in local datastore', key)

        if (!(data instanceof Array)) {
          return cb('Data not a log')
        }

        if (data.length === 0) {
          return cb('Empty logs')
        }

        // TODO: this should probably be filter instead of filterSeries
        filterSeries(
          data,
          (raw, cb) => {
            let rec
            try {
              rec = Record.deserialize(raw)
            } catch (err) {
              dht._log('Error deserializing log: %s', err)
              return cb(null, false)
            }

            dht._verifyRecordLocally(rec, (err) => {
              if (err) {
                dht._log('Error verifying log: %s', err)
                return cb(null, false)
              }

              cb(null, true)
            })
          },
          (error, filteredData) => {
            if (filteredData.length === 0) {
              return cb('All logs are invalid')
            }

            const logs = filteredData.map(raw => Record.deserialize(raw))
            cb(null, logs)
          }
        )        
      }
    ], callback)
  },
  /**
   * Query a particular peer for the value for the given key.
   * It will either return the value or a list of closer peers.
   *
   * Note: The peerbook is updated with new addresses found for the given peer.
   *
   * @param {PeerId} peer
   * @param {Buffer} key
   * @param {function(Error, Redcord, Array<PeerInfo>)} callback
   * @returns {void}
   *
   * @private
   */
  _getValueOrPeers (peer, key, verification, callback) {
    waterfall([
      (cb) => {
        return dht._getDefaceValueSingle(peer, key, verification, cb)
      },
      (msg, cb) => {
        const peers = msg.closerPeers
        const record = msg.record
        const error = msg.error

        if (error === 'verification error') {
          return cb(new errors.VerificationError(error))
        }

        if (record) {
          // We have a record
          return dht._verifyRecordOnline(record, (err) => {
            if (err) {
              dht._log('invalid record received, discarded')
              return cb(new errors.InvalidRecordError())
            }

            return cb(null, record, peers)
          })
        }

        if (peers.length > 0) {
          return cb(null, null, peers)
        }

        cb(new errors.NotFoundError('Not found'))
      }
    ], callback)
  },
  /**
   * Query a particular peer for the logs for the given key.
   * It will either return the logs or a list of closer peers.
   *
   * Note: The peerbook is updated with new addresses found for the given peer.
   *
   * @param {PeerId} peer
   * @param {Buffer} key
   * @param {function(Error, Redcord, Array<PeerInfo>)} callback
   * @returns {void}
   *
   * @private
   */
  _getLogsOrPeers (peer, key, callback) {
    waterfall([
      (cb) => dht._getDefaceLogsSingle(peer, key, cb),
      (msg, cb) => {
        const peers = msg.closerPeers
        const logs = msg.record
        const error = msg.error

        if (logs) {
          // We have a record
          // TODO? Really?

          // let record
          // try {
          //   record = Record.deserialize(res)
          // } catch (err) {
          //   return callback(err)
          // }

          // const logs = record.toString()

          // let logs
          // try {
          //   logs = JSON.parse(record.value.toString())
          // }

          return dht._verifyRecordOnline(logs, (err) => {
            if (err) {
              dht._log('invalid record received, discarded')
              return cb(new errors.InvalidRecordError())
            }

            return cb(null, logs, peers)
          })
        }

        if (peers.length > 0) {
          return cb(null, null, peers)
        }

        cb(new errors.NotFoundError('Not found'))
      }
    ], callback)
  },
  /**
   * Get a value via rpc call for the given parameters.
   *
   * @param {PeerId} peer
   * @param {Buffer} key
   * @param {function(Error, Message)} callback
   * @returns {void}
   *
   * @private
   */
  _getValueSingle (peer, key, callback) {
    const msg = new Message(Message.TYPES.GET_VALUE, key, 0)
    dht.network.sendRequest(peer, msg, callback)
  },
  /**
   * Get a Deface value via rpc call for the given parameters.
   *
   * @param {PeerId} peer
   * @param {Buffer} key
   * @param {function(Error, Message)} callback
   * @returns {void}
   *
   * @private
   */
  _getDefaceValueSingle (peer, key, verification, callback) {
    const msg = new Message(Message.TYPES.GET_DEFACE_VALUE, key, 0, verification)
    dht.network.sendRequest(peer, msg, callback)
  },
  /**
   * Get a Deface logs via rpc call for the given parameters.
   *
   * @param {PeerId} peer
   * @param {Buffer} key
   * @param {function(Error, Message)} callback
   * @returns {void}
   *
   * @private
   */
  _getDefaceLogsSingle (peer, key, callback) {
    const msg = new Message(Message.TYPES.GET_DEFACE_LOGS, key, 0)
    dht.network.sendRequest(peer, msg, callback)
  },
  /**
   * Verify a record, fetching missing public keys from the network.
   * Calls back with an error if the record is invalid.
   *
   * @param {Record} record
   * @param {function(Error)} callback
   * @returns {void}
   *
   * @private
   */
  _verifyRecordOnline (record, callback) {
    series([
      (cb) => {
        if (record.signature) {
          // fetch the public key
          waterfall([
            (cb) => dht.getPublicKey(record.author, cb),
            (pk, cb) => record.verifySignature(pk, cb)
          ], cb)
        } else {
          cb()
        }
      },
      (cb) => {
        libp2pRecord.validator.verifyRecord(dht.validators, record, cb)
      }
    ], callback)
  },
  /**
   * Get the public key directly from a node.
   *
   * @param {PeerId} peer
   * @param {function(Error, PublicKey)} callback
   * @returns {void}
   *
   * @private
   */
  _getPublicKeyFromNode (peer, callback) {
    const pkKey = utils.keyForPublicKey(peer)
    waterfall([
      (cb) => dht._getValueSingle(peer, pkKey, cb),
      (msg, cb) => {
        if (!msg.record || !msg.record.value) {
          return cb(new Error('Node not responding with its public key: ' + peer.toB58String()))
        }

        PeerId.createFromPubKey(msg.record.value, cb)
      },
      (recPeer, cb) => {
        // compare hashes of the pub key
        if (!recPeer.isEqual(peer)) {
          return cb(new Error('public key does not match id'))
        }

        cb(null, recPeer.pubKey)
      }
    ], callback)
  },
  /**
   * Search the dht for up to `n` providers of the given CID.
   *
   * @param {CID} key
   * @param {number} maxTimeout - How long the query should maximally run in milliseconds.
   * @param {number} n
   * @param {function(Error, Array<PeerInfo>)} callback
   * @returns {void}
   *
   * @private
   */
  _findNProviders (key, maxTimeout, n, callback) {
    let out = new LimitedPeerList(n)

    dht.providers.getProviders(key, (err, provs) => {
      if (err) {
        return callback(err)
      }

      provs.forEach((id) => {
        let info
        if (dht.peerBook.has(id)) {
          info = dht.peerBook.get(id)
        } else {
          info = dht.peerBook.put(new PeerInfo(id))
        }
        out.push(info)
      })

      // All done
      if (out.length >= n) {
        return callback(null, out.toArray())
      }

      // need more, query the network
      const query = new Query(dht, key.buffer, (peer, cb) => {
        waterfall([
          (cb) => dht._findProvidersSingle(peer, key, cb),
          (msg, cb) => {
            const provs = msg.providerPeers
            dht._log('(%s) found %s provider entries', dht.peerInfo.id.toB58String(), provs.length)

            provs.forEach((prov) => {
              out.push(dht.peerBook.put(prov))
            })

            // hooray we have all that we want
            if (out.length >= n) {
              return cb(null, {success: true})
            }

            // it looks like we want some more
            cb(null, {
              closerPeers: msg.closerPeers
            })
          }
        ], cb)
      })

      const peers = dht.routingTable.closestPeers(key.buffer, c.ALPHA)

      timeout((cb) => query.run(peers, cb), maxTimeout)((err) => {
        if (err) {
          if (err.code === 'ETIMEDOUT' && out.length > 0) {
            return callback(null, out.toArray())
          }
          return callback(err)
        }

        callback(null, out.toArray())
      })
    })
  },
  /**
   * Check for providers from a single node.
   *
   * @param {PeerId} peer
   * @param {CID} key
   * @param {function(Error, Message)} callback
   * @returns {void}
   *
   * @private
   */
  _findProvidersSingle (peer, key, callback) {
    const msg = new Message(Message.TYPES.GET_PROVIDERS, key.buffer, 0)
    dht.network.sendRequest(peer, msg, callback)
  },
  /**
   * Check verification status of a signel peer.
   *
   * @param {String} id
   * @param {String} key
   * @param {function(Error, Boolean)} callback
   * @returns {void}
   *
   * @private
   */
  _verifyPeer (id, key, callback) {
    return callback('No verifyPeer method loaded yet')
  }
})
