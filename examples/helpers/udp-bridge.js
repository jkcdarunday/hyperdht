// Application-side forwarding policy. HyperDHT itself only exposes raw UDP.
module.exports = async function createUDPBridge(node, connection, opts) {
  const { port, bindPort = 0, timeout = 10000 } = opts
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error('A local WireGuard listen port is required')
  }
  const socket = node.udx.createSocket()
  let raw = null
  let closing = null
  const close = () => {
    if (!closing) closing = Promise.all([socket.close(), raw ? raw.close() : null])
    return closing
  }

  try {
    socket.bind(bindPort, '127.0.0.1')
    raw = node.toRaw(connection, { timeout })
    raw.on('message', (data) => socket.trySend(data, port, '127.0.0.1'))
    raw.on('close', close)
    socket.on('message', (data, address) => {
      if (address.host === '127.0.0.1' && address.port === port) raw.trySend(data)
    })
    await raw.opened
    return { raw, address: () => socket.address(), close }
  } catch (err) {
    await close()
    throw err
  }
}
