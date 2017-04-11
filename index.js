// @flow weak
const { Writable } = require('stream')
const promiseRetry = require('promise-retry')


const retryAWS = (client/*: Object */, funcName/*: string */, params/*: Object */)/*: Promise<Object> */ =>
  promiseRetry((retry/*: function */, number/*: number */) => {
    if (number > 1) {
      // log.warn('AWS %s.%s attempt: %d', client.constructor.__super__.serviceIdentifier, funcName, number);
    }
    return client[funcName](params).promise()
      .catch((err) => {
        if (!err.retryable) {
          throw new Error(err)
        }

        retry(err)
      })
  })


class KinesisWritable extends Writable {
  /*:: client: Object */
  /*:: logger: {debug: Function, info: Function, warn: Function} */
  /*:: queue: Object[] */
  /*:: streamName: string */
  /*:: highWaterMark: number */
  /*:: _queueCheckTimer: number */
  constructor (client, streamName, {highWaterMark = 16, logger} = {}) {
    super({objectMode: true, highWaterMark: Math.min(highWaterMark, 500)})

    this.client = client
    this.streamName = streamName
    this.logger = logger || {debug: () => null, info: () => null, warn: () => null}
    if (highWaterMark > 500) {
      this.logger.warn('The maximum number of records that can be added is 500, not %d', highWaterMark)
      highWaterMark = 500
    }
    this.highWaterMark = highWaterMark
    this._queueCheckTimer = setTimeout(() => this.writeRecords(() => null), 500)

    this.queue = []
  }

  // eslint-disable-next-line no-unused-vars
  getPartitionKey (record) {
    return '0'
  };

  prepRecord (record) {
    return {
      Data: JSON.stringify(record),
      PartitionKey: this.getPartitionKey(record),
    }
  };

  writeRecords (callback) {
    const queueLength = this.queue.length
    this.logger.debug('Writing %d records to Kinesis', queueLength)

    const dataToPut = this.queue.splice(0, Math.min(this.queue.length, this.highWaterMark))
    const records = dataToPut.map(this.prepRecord.bind(this))

    retryAWS(this.client, 'putRecords', {
      Records: records,
      StreamName: this.streamName,
    })
    .then((response) => {
      this.logger.info('Wrote %d records to Kinesis', records.length - response.FailedRecordCount)

      if (response.FailedRecordCount !== 0) {
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

  _write (data, encoding, callback) {
    this.logger.debug('Adding to Kinesis queue', data)

    this.queue.push(data)

    if (this.queue.length >= this.highWaterMark) {
      return this.writeRecords(callback)
    }

    if (this._queueCheckTimer) {
      clearTimeout(this._queueCheckTimer)
    }
    this._queueCheckTimer = setTimeout(() => this.writeRecords(() => null), 500)

    callback()
  }
};

module.exports = KinesisWritable
