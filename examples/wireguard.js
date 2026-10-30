const DHT = require('../')
const goodbye = require('graceful-goodbye')
const createUDPBridge = require('./helpers/udp-bridge')

// Usage: node examples/wireguard.js <local-wireguard-port> [server-public-key]
// Configure WireGuard independently on both machines before starting.
const port = Number(process.argv[2])
const remotePublicKey = process.argv[3]

if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error('Usage: node examples/wireguard.js <local-wireguard-port> [server-public-key]')
}

const node = new DHT()
goodbye(() => node.destroy())

async function bridge(socket) {
  socket.on('error', (err) => console.error('Connection:', err.message))
  try {
    if (!(await socket.opened)) throw new Error('Connection did not open')
    const udp = await createUDPBridge(node, socket, { port })
    const address = udp.address()
    console.log('Connected to HyperDHT peer:', socket.remotePublicKey.toString('hex'))
    console.log(`Set this WireGuard peer's Endpoint to ${address.host}:${address.port}`)
    udp.raw.on('failure', (err) => console.error('Raw UDP:', err.message))
    udp.raw.on('close', () => console.log('UDP bridge closed'))
  } catch (err) {
    console.error('Cannot start UDP bridge:', err.message)
    socket.destroy()
  }
}

async function main() {
  if (remotePublicKey) {
    await bridge(node.connect(remotePublicKey))
    return
  }

  const server = node.createServer(bridge)
  await server.listen()
  console.log('HyperDHT server public key:', server.publicKey.toString('hex'))
}

main().catch(async (err) => {
  console.error(err)
  await node.destroy()
  process.exitCode = 1
})
