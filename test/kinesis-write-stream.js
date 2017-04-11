/* eslint-disable no-new,no-unused-expressions */
const chai = require('chai')
const sinon = require('sinon')
const sinonChai = require('sinon-chai')
const streamArray = require('stream-array')
const _ = require('lodash')
const KinesisWritable = require('../')
const recordsFixture = require('./fixture/records')
const successResponseFixture = require('./fixture/success-response')
const errorResponseFixture = require('./fixture/failed-response')
const writeFixture = require('./fixture/write-fixture')

chai.use(sinonChai)

const expect = chai.expect

// Convenience wrapper around Promise to reduce test boilerplate
const AWSPromise = {
  resolves: (value) => {
    return sinon.stub().returns({
      promise: () => Promise.resolve(value),
    })
  },
  rejects: (value) => {
    return sinon.stub().returns({
      promise: () => Promise.reject(value),
    })
  },
}

describe('KinesisWritable', function () {
  beforeEach(function () {
    this.sinon = sinon.sandbox.create()

    this.client = {
      putRecords: sinon.stub(),
      constructor: {
        __super__: {
          serviceIdentifier: 'TestClient',
        },
      },
    }

    this.stream = new KinesisWritable(this.client, 'streamName', {
      highWaterMark: 6,
    })
  })

  afterEach(function () {
    this.sinon.restore()
  })

  describe('constructor', function () {
    it('should throw error on missing client', function () {
      expect(function () {
        new KinesisWritable()
      }).to.Throw(Error, 'client is required')
    })

    it('should throw error on missing streamName', function () {
      expect(function () {
        new KinesisWritable({})
      }).to.Throw(Error, 'streamName is required')
    })

    it('should correct highWaterMark above 500', function () {
      const stream = new KinesisWritable({}, 'test', { highWaterMark: 501 })
      expect(stream.highWaterMark).to.equal(500)
    })
  })

  describe('getPartitionKey', function () {
    xit('should return a random partition key padded to 4 digits', function () {
      var kinesis = new KinesisWritable({}, 'foo')

      this.sinon.stub(_, 'random').returns(10)

      expect(kinesis.getPartitionKey()).to.eq('0010')

      _.random.returns(1000)

      expect(kinesis.getPartitionKey()).to.eq('1000')
    })

    it('should be called with the current record being added', function (done) {
      this.client.putRecords = AWSPromise.resolves(successResponseFixture)
      this.sinon.stub(this.stream, 'getPartitionKey').returns('1234')

      this.stream.on('finish', () => {
        expect(this.stream.getPartitionKey).to.have.been.calledWith(recordsFixture[0])
        done()
      })

      streamArray([recordsFixture[0]]).pipe(this.stream)
    })

    it('should use custom getPartitionKey if defined', function (done) {
      this.client.putRecords = AWSPromise.resolves(successResponseFixture)

      this.stream.getPartitionKey = function () {
        return 'custom-partition'
      }

      this.sinon.spy(this.stream, 'getPartitionKey')

      this.stream.on('finish', () => {
        expect(this.stream.getPartitionKey).to.have.returned('custom-partition')
        done()
      })

      streamArray(recordsFixture).pipe(this.stream)
    })
  })

  describe('_write', function () {
    it('should write to Kinesis when stream is closed', function (done) {
      this.client.putRecords = AWSPromise.resolves(successResponseFixture)
      this.sinon.stub(this.stream, 'getPartitionKey').returns('1234')

      this.stream.on('finish', () => {
        expect(this.client.putRecords).to.have.been.calledOnce

        expect(this.client.putRecords).to.have.been.calledWith({
          Records: writeFixture,
          StreamName: 'streamName',
        })

        done()
      })

      streamArray(recordsFixture).pipe(this.stream)
    })

    it('should do nothing if there is nothing in the queue when the stream is closed', function (done) {
      this.client.putRecords = AWSPromise.resolves(successResponseFixture)

      this.stream.on('finish', () => {
        expect(this.client.putRecords).to.have.been.calledOnce

        done()
      })

      for (var i = 0; i < 6; i++) {
        this.stream.write(recordsFixture)
      }

      this.stream.end()
    })

    it('should buffer records up to highWaterMark', function (done) {
      this.client.putRecords.yields(null, successResponseFixture)

      for (var i = 0; i < 4; i++) {
        this.stream.write(recordsFixture[0])
      }

      this.stream.write(recordsFixture[0], function () {
        expect(this.client.putRecords).to.not.have.been.called

        this.stream.write(recordsFixture[0], function () {
          expect(this.client.putRecords).to.have.been.calledOnce

          done()
        }.bind(this))
      }.bind(this))
    })

    it('should emit error on Kinesis error', function (done) {
      this.client.putRecords = AWSPromise.rejects('Fail')

      this.stream.on('error', function (err) {
        expect(err.message).to.eq('Fail')

        done()
      })

      this.stream.end({ foo: 'bar' })
    })

    it('should emit error on records errors', function (done) {
      this.client.putRecords = AWSPromise.resolves(errorResponseFixture)

      this.stream.on('error', function (err) {
        expect(err).to.be.ok

        done()
      })

      this.stream.end({ foo: 'bar' })
    })

    it('should retry failed records', function (done) {
      this.sinon.stub(this.stream, 'getPartitionKey').returns('1234')

      this.client.putRecords = AWSPromise.rejects({retryable: true})
      this.client.putRecords.onCall(2).returns({promise: () => Promise.resolve(successResponseFixture)})

      this.stream.on('finish', () => {
        expect(this.client.putRecords).to.have.been.calledThrice

        expect(this.client.putRecords.secondCall).to.have.been.calledWith({
          Records: writeFixture,
          StreamName: 'streamName',
        })

        done()
      })

      streamArray(recordsFixture).pipe(this.stream)
    })
  })
})
