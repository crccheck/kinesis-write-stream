const { Readable } = require('stream')
const KinesisWritable = require('kinesis-write-stream')
const AWS = require('aws-sdk')
const bunyan = require('bunyan')

const WAIT = 500

const logger = bunyan.createLogger({name: 'demo', level: 'debug'})

const client = new AWS.Kinesis()

const stream = new KinesisWritable(client, process.argv[2] || 'demo', {logger, wait: WAIT})

class NoiseReadable extends Readable {
  constructor (options = {}) {
    options.objectMode = true
    super(options)
    this._alphabet = '0123456789ABCDEF'.split('')
  }

  _read (size) {
    const throbber = this._alphabet.shift()
    const data = throbber
    // const data = {foo: throbber}
    this._alphabet.push(throbber)

    setTimeout(() => this.push(data), WAIT * 1.1 * Math.random())
  }
}

new NoiseReadable().pipe(stream)
