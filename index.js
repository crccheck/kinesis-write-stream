// @flow weak
const { Writable } = require('stream')
const promiseRetry = require('promise-retry')


const retryAWS = (client/*: Object */, funcName/*: string */, params/*: Object */)/*: Promise<Object> */ =>
  promiseRetry((retry/*: function */, number/*: number */) => {
    if (number > 1) {
      // log.warn('AWS %s.%s attempt: %d', client.constructor.__super__.serviceIdentifier, funcName, number);
    }
    return client[funcName](client, params).promise()
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
  constructor (client, streamName, {highWaterMark = 16, logger} = {}) {
    super({objectMode: true, highWaterMark: Math.min(highWaterMark, 500)})
    this.cork()

    this.client = client
    this.streamName = streamName
    this.logger = logger || {debug: () => null, info: () => null, warn: () => null}
    if (highWaterMark > 500) {
      this.logger.warn('The maximum number of records that can be added is 500, not %d', highWaterMark)
      highWaterMark = 500
    }
    this.highWaterMark = highWaterMark

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
    this.logger.debug('Writing %d records to Kinesis', this.queue.length)

    const records = this.queue.map(this.prepRecord.bind(this))

    retryAWS(this.client, 'putRecords', {
      Records: records,
      StreamName: this.streamName,
    })
    .then((response) => {
      this.logger.info('Wrote %d records to Kinesis', records.length - response.FailedRecordCount)

      if (response.FailedRecordCount !== 0) {
        this.logger.warn('Failed writing %d records to Kinesis', response.FailedRecordCount)

        const failedRecords = []

        response.Records.forEach((record, index) => {
          if (record.ErrorCode) {
            this.logger.warn('Failed record with message: %s', record.ErrorMessage)
            failedRecords.push(this.queue[index])
          }
        })

        this.queue = failedRecords

        return callback(new Error('Failed to write ' + failedRecords.length + ' records'))
      }

      this.queue = []

      return callback()
    })
    .catch((err) => callback(err))
  };

  _write (data, encoding, callback) {
    this.logger.debug('Adding to Kinesis queue', data)

    this.queue.push(data)

    if (this.queue.length >= this.highWaterMark) {
      process.nextTick(() => {
        this.uncork()
        callback()
      })
    }

    return callback()
  }

  _writev (chunks, callback) {
    console.log('_writev', chunks, this.queue)
    if (this.queue.length === 0) {
      return callback()
    }

    return this.writeRecords(callback)
  };
};

module.exports = KinesisWritable
