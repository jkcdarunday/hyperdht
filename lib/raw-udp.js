const EventEmitter = require('events')
const b4a = require('b4a')

const connections = new WeakMap()
const MAGIC = b4a.from('HyperDHT raw UDP\x01')
const FRAME_SIZE = MAGIC.byteLength + 1
const READY = 1
const ACK = 2

module.exports = class RawUDP extends EventEmitter {
  constructor(dht, connection, { timeout }) {
    super()

    this.connection = connection
    this.closed = false
    this._dht = dht
    this._rawStream = connection.rawStream
    this._socket = this._rawStream.socket
    this._remote = {
      host: this._rawStream.remoteHost,
      port: this._rawStream.remotePort
    }
    this._timeout = timeout
    this._timer = null
    this._closing = null
    this._active = false
    this._receivedReady = false
    this._frame = b4a.alloc(FRAME_SIZE)
    this._offset = 0
    this._ondata = this._ondata.bind(this)
    this._onclose = () => this.close()
    this._onerror = (err) => this._fail(err)
    this._onremotechanged = () => this._fail(new Error('Raw UDP endpoint changed'))
    this.opened = new Promise((resolve, reject) => {
      this._resolve = resolve
      this._reject = reject
    })
    this.opened.catch(() => {})
  }

  static register(rawStream, supported) {
    connections.set(rawStream, { supported: !!supported, direct: false, raw: null })
  }

  static setDirect(rawStream) {
    const state = connections.get(rawStream)
    if (state) state.direct = true
  }

  static convert(dht, connection, opts = {}) {
    const { timeout = 10000 } = opts
    if (!Number.isSafeInteger(timeout) || timeout < 1 || timeout > 0x7fffffff) {
      throw new Error('Invalid raw UDP timeout')
    }
    const rawStream = connection.rawStream
    const state = connections.get(rawStream)
    if (!connection.connected || connection.destroying || !rawStream?.connected) {
      throw new Error('Connection must be open before entering raw UDP mode')
    }
    if (rawStream.udx !== dht.udx || !state?.supported) {
      throw new Error('Connection does not support raw UDP mode')
    }
    if (!state.direct || rawStream._remoteChanging) {
      throw new Error('Raw UDP requires a stable direct connection')
    }
    if (state.raw || connection.listenerCount('data') !== 0) {
      throw new Error('Connection is already in use')
    }
    for (const raw of dht._rawUDP) {
      if (
        raw._socket === rawStream.socket &&
        raw._remote.port === rawStream.remotePort &&
        raw._remote.host === rawStream.remoteHost
      ) {
        throw new Error('UDP endpoint is already in raw UDP mode')
      }
    }

    const raw = new RawUDP(dht, connection, { timeout })
    state.raw = raw
    try {
      raw._start()
    } catch (err) {
      raw._fail(err)
    }
    return raw
  }

  _start() {
    const connection = this.connection
    connection.on('data', this._ondata)
    connection.on('close', this._onclose)
    connection.on('error', this._onerror)
    this._rawStream.on('remote-changed', this._onremotechanged)
    this._socket.on('close', this._onclose)
    // Arm raw reception before advertising readiness. Callers can attach
    // message listeners before awaiting opened, including for early datagrams.
    this._dht._rawUDP.add(this)
    this._timer = setTimeout(
      () => this._fail(new Error('Raw UDP negotiation timed out')),
      this._timeout
    )
    this._write(READY)
    connection.resume()
  }

  _write(type) {
    const frame = b4a.alloc(FRAME_SIZE)
    b4a.copy(MAGIC, frame)
    frame[MAGIC.byteLength] = type
    this.connection.write(frame)
  }

  _ondata(data) {
    let offset = 0
    while (offset < data.byteLength && !this.closed) {
      const length = Math.min(FRAME_SIZE - this._offset, data.byteLength - offset)
      b4a.copy(data, this._frame, this._offset, offset, offset + length)
      this._offset += length
      offset += length
      if (this._offset !== FRAME_SIZE) continue
      this._offset = 0
      if (!b4a.equals(this._frame.subarray(0, MAGIC.byteLength), MAGIC)) {
        this._fail(new Error('Invalid Raw UDP control frame'))
        return
      }
      const type = this._frame[MAGIC.byteLength]
      if (type === READY && !this._receivedReady) {
        this._receivedReady = true
        this._write(ACK)
      } else if (type === ACK && this._receivedReady && !this._active) {
        clearTimeout(this._timer)
        this._active = true
        this._resolve(this)
      } else {
        this._fail(new Error('Unexpected Raw UDP control frame'))
      }
    }
  }

  _onrawmessage(socket, data, address) {
    if (this.closed || socket !== this._socket) return false
    if (address.port !== this._remote.port || address.host !== this._remote.host) return false
    this.emit('message', data, address)
    return true
  }

  address() {
    return this._socket.address()
  }

  async send(data) {
    if (!this._active || this.closed) return false
    return this._socket.send(data, this._remote.port, this._remote.host)
  }

  trySend(data) {
    if (!this._active || this.closed) return false
    this._socket.trySend(data, this._remote.port, this._remote.host)
    return true
  }

  remoteAddress() {
    return { ...this._remote }
  }

  _fail(err) {
    if (this.closed) return
    this._reject(err)
    // Errors after negotiation are observable without requiring an error
    // listener merely to avoid EventEmitter's unhandled-error behavior.
    this.close()
    this.emit('failure', err)
  }

  close() {
    if (this.closed) return this._closing || Promise.resolve()
    this.closed = true
    this._active = false
    clearTimeout(this._timer)
    this._reject(new Error('Raw UDP closed'))
    this._dht._rawUDP.delete(this)
    this.connection.off('data', this._ondata)
    this._rawStream.off('remote-changed', this._onremotechanged)
    this._socket.off('close', this._onclose)
    // The control connection owns the punched socket. Keep its error listener
    // installed through destruction, including errors already queued by UDX.
    const closed = this.connection.destroyed
      ? Promise.resolve()
      : new Promise((resolve) => this.connection.once('close', resolve))
    this.connection.destroy()
    this._closing = closed.then(() => {
      this.connection.off('close', this._onclose)
      this.connection.off('error', this._onerror)
      this.emit('close')
    })
    return this._closing
  }
}
