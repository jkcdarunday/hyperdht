const { EventEmitter } = require('events')

module.exports = class RawSocket extends EventEmitter {
  constructor() {
    super()

    this.rawStream = null
    this.destroying = false
  }

  start(rawStream, opts) {
    if (this.destroying || !rawStream) return

    this.rawStream = rawStream

    const address = rawStream.socket.address()
    const socket = {
      localPort: address.port,
      remoteHost: rawStream.remoteHost,
      remotePort: rawStream.remotePort
    }

    rawStream.destroy()
    this.emit('socket', socket)
  }

  destroy(err) {
    if (this.destroying) return
    this.destroying = true

    if (this.rawStream) this.rawStream.destroy()
    this.emit('close', err)
  }

  setKeepAlive() {}
}
