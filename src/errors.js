'use strict'

class InvalidRecordError extends Error {}

class NotFoundError extends Error {}

class LookupFailureError extends Error {}

class VerificationError extends Error {}

module.exports = {
  InvalidRecordError,
  NotFoundError,
  LookupFailureError,
  VerificationError
}
