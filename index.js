// @flow weak
const promiseRetry = require('promise-retry')
const FlushWritable = require('flushwritable')


function voidFunction () {}


class KinesisWritable extends FlushWritable {
  /*:: client: Object */
  /*:: logger: {debug: Function, info: Function, warn: Function} */
  /*:: queue: Object[] */
  /*:: streamName: string */
  /*:: highWaterMark: number */
  /*:: wait: number */
  /*:: _queueCheckTimer: number */
  constructor (client, streamName, {highWaterMark = 16,
                                    logger,
                                    maxRetries = 3,
                                    retryTimeout = 100,
                                    wait = 500} = {}) {
    super({objectMode: true, highWaterMark: Math.min(highWaterMark, 500)})

    if (!client) {
      throw new Error('client is required')
    }
    this.client = client

    if (!streamName) {
      throw new Error('streamName is required')
    }
    this.streamName = streamName
    this.logger = logger || {debug: () => null, info: () => null, warn: () => null}
    if (highWaterMark > 500) {
      this.logger.warn('The maximum number of records that can be added is 500, not %d', highWaterMark)
      highWaterMark = 500
    }
    this.highWaterMark = highWaterMark
    this.maxRetries = 3
    this.retryTimeout = retryTimeout
    this.wait = wait
    this._queueCheckTimer = setTimeout(() => this.writeRecords(voidFunction), this.wait)

    this.queue = []
  }

  _write (data, encoding, callback) {
    this.logger.debug('Adding to Kinesis queue', data)

    this.queue.push(data)

    if (this.queue.length >= this.highWaterMark) {
      return this.writeRecords(callback)
    }

    if (this._queueCheckTimer) {
      clearTimeout(this._queueCheckTimer)
    }
    this._queueCheckTimer = setTimeout(() => this.writeRecords(voidFunction), this.wait)

    callback()
  }

  flushWrite () {
    return new Promise((resolve, reject) => {
      this.writeRecords((err) => {
        if (err) {
          return reject(err)
        }

        if (this.queue.length) {
          return reject(new Error(`${this.queue.length} items left to push`))
        }

        resolve()
      })
    })
  }

  _flush (callback) {
    if (this._queueCheckTimer) {
      clearTimeout(this._queueCheckTimer)
    }

    promiseRetry((retry, number) => {
      return this.flushWrite()
        .catch(retry)
    }, {minTimeout: this.retryTimeout})
    .then(callback)
  }

  // eslint-disable-next-line no-unused-vars
  getPartitionKey (record) {
    return '0'
  };

  _prepRecord (record) {
    return {
      Data: JSON.stringify(record),
      PartitionKey: this.getPartitionKey(record),
    }
  };

  writeRecords (callback) {
    if (!this.queue.length) {
      callback()
      return  // Nothing to do
    }
    this.logger.debug('Writing %d records to Kinesis', this.queue.length)

    const dataToPut = this.queue.splice(0, Math.min(this.queue.length, this.highWaterMark))
    const records = dataToPut.map(this._prepRecord.bind(this))

    this.retryAWS('putRecords', {
      Records: records,
      StreamName: this.streamName,
    })
    .then((response) => {
      this.logger.info('Wrote %d records to Kinesis', records.length - response.FailedRecordCount)

      if (response.FailedRecordCount !== 0) {
        // TODO emit()
        this.logger.warn('Failed writing %d records to Kinesis', response.FailedRecordCount)

        const failedRecords = []
        response.Records.forEach((record, idx) => {
          if (record.ErrorCode) {
            this.logger.warn('Failed record with message: %s', record.ErrorMessage)
            failedRecords.push(dataToPut[idx])
          }
        })
        this.queue.unshift(...failedRecords)

        callback(new Error(`Failed to write ${failedRecords.length} records`))
      }

      callback()
    })
    .catch((err) => callback(err))
  };

  retryAWS (funcName/*: string */, params/*: Object */)/*: Promise<Object> */ {
    return promiseRetry((retry/*: function */, number/*: number */) => {
      if (number > 1) {
        this.logger.warn('AWS %s.%s attempt: %d', this.client.constructor.__super__.serviceIdentifier, funcName, number)
      }
      return this.client[funcName](params).promise()
        .catch((err) => {
          if (!err.retryable) {
            throw new Error(err)
          }

          retry(err)
        })
    }, {retries: this.maxRetries, minTimeout: this.retryTimeout})
  }
}

module.exports = KinesisWritable
