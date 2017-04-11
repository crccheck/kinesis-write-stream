const { Readable } = require('stream')
const KinesisWritable = require('kinesis-write-stream')
const AWS = require('aws-sdk')
const bunyan = require('bunyan')

const logger = bunyan.createLogger({name: 'demo', level: 'debug'})

const client = new AWS.Kinesis()

const stream = new KinesisWritable(client, process.argv[2] || 'demo', {logger})

class NoiseReadable extends Readable {
  constructor (options = {}) {
    options.objectMode = true
    super(options)
  }

  _read (size) {
    this.push({foo: 'a'})
    // this.push(JSON.stringify({foo: 'a'}))
  }
}

new NoiseReadable().pipe(stream)
