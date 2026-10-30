const test = require('brittle')
const b4a = require('b4a')
const { once } = require('events')
const { swarm } = require('./helpers')
const createUDPBridge = require('../examples/helpers/udp-bridge')

test(
  'UDP proxy - bidirectional forwarding over holepunched ports',
  { timeout: 15000 },
  async (t) => {
    const { a, b, client, remote } = await pair(t)
    const clientPort = client.rawStream.socket.address().port
    const serverPort = remote.rawStream.socket.address().port
    t.ok(a._socketPool.lookup(remote.rawStream.socket), 'server holepunched a pooled socket')
    t.ok(b._socketPool.lookup(client.rawStream.socket), 'client holepunched a pooled socket')

    // These ordinary UDP sockets stand in for the two local WireGuard listeners.
    const left = a.udx.createSocket()
    const right = b.udx.createSocket()
    const stranger = a.udx.createSocket()
    for (const socket of [left, right, stranger]) socket.bind(0, '127.0.0.1')
    t.teardown(() => Promise.all([left.close(), right.close(), stranger.close()]))

    const [ab, ba] = await Promise.all([
      createUDPBridge(b, client, { port: right.address().port }),
      createUDPBridge(a, remote, { port: left.address().port })
    ])
    t.teardown(() => Promise.all([ab.close(), ba.close()]))
    const aProxy = ba.address()
    const bProxy = ab.address()
    t.is(aProxy.host, '127.0.0.1', 'server proxy binds to loopback')
    t.is(bProxy.host, '127.0.0.1', 'client proxy binds to loopback')
    t.is(ba.raw.address().port, serverPort, 'server retains its punched network port')
    t.is(ab.raw.address().port, clientPort, 'client retains its punched network port')

    const large = b4a.alloc(1400, 255)
    large.set([4, 0, 0, 0])
    for (const packet of [b4a.from([1, 0, 0, 0, 42]), b4a.from([3, 0, 0, 0]), large]) {
      // Local A -> proxy A -> punched network sockets -> proxy B -> local B.
      const received = wireGuardPacket(right)
      await left.send(packet, aProxy.port)
      const [actual, from] = await received
      t.alike(actual, packet, 'A to B preserves the complete datagram')
      t.is(from.port, bProxy.port, 'B receives from its configured proxy endpoint')

      // Reply through that same local endpoint, as WireGuard would do.
      const reply = wireGuardPacket(left)
      await right.send(packet, from.port)
      const [response, replyFrom] = await reply
      t.alike(response, packet, 'B to A preserves the complete datagram')
      t.is(replyFrom.port, aProxy.port, 'A receives from its configured proxy endpoint')
    }

    const allowed = b4a.from([4, 0, 0, 0, 7])
    const next = wireGuardPacket(right)
    await stranger.send(b4a.from([4, 0, 0, 0, 99]), aProxy.port)
    await left.send(allowed, aProxy.port)
    t.alike((await next)[0], allowed, 'proxy rejects packets from other local ports')

    const remoteClosed = once(ba.raw, 'close')
    await ab.close()
    await remoteClosed
    await ba.close()
    t.ok(ab.raw.closed && ba.raw.closed, 'closing one proxy closes both raw endpoints')
    t.is(a._rawUDP.size + b._rawUDP.size, 0, 'both raw routes are removed')

    // Rebinding proves both application-owned loopback sockets were released.
    const reboundA = a.udx.createSocket()
    const reboundB = b.udx.createSocket()
    t.teardown(() => Promise.all([reboundA.close(), reboundB.close()]))
    reboundA.bind(aProxy.port, '127.0.0.1')
    reboundB.bind(bProxy.port, '127.0.0.1')
    t.pass('both proxy ports can be rebound after cleanup')
  }
)

test(
  'UDP proxy - failed raw negotiation releases the loopback port',
  { timeout: 10000 },
  async (t) => {
    const { b, client, remote } = await pair(t)
    const reservation = b.udx.createSocket()
    reservation.bind(0, '127.0.0.1')
    const bindPort = reservation.address().port
    await reservation.close()
    const remoteClosed = new Promise((resolve) => remote.once('close', resolve))

    // The remote application never opts into raw mode.
    await t.exception(
      () => createUDPBridge(b, client, { port: 51820, bindPort, timeout: 50 }),
      /negotiation timed out/
    )
    await remoteClosed
    t.is(b._rawUDP.size, 0, 'failed negotiation removes the raw route')
    const rebound = b.udx.createSocket()
    t.teardown(() => rebound.close())
    rebound.bind(bindPort, '127.0.0.1')
    t.pass('failed setup releases the application-owned proxy socket')
  }
)

async function pair(t) {
  const net = await swarm(t)
  const a = net.createNode({ quickFirewall: false })
  const b = net.createNode({ quickFirewall: false })
  await Promise.all([a.fullyBootstrapped(), b.fullyBootstrapped()])
  const server = a.createServer({ shareLocalAddress: false })
  server.on('connection', (socket) => socket.on('error', () => {}))
  await server.listen()
  const incoming = once(server, 'connection')
  const client = b.connect(server.publicKey, { localConnection: false })
  client.on('error', () => {})
  const [[remote]] = await Promise.all([incoming, once(client, 'open')])
  await remote.opened
  return { a, b, client, remote }
}

function wireGuardPacket(socket) {
  // Ignore final holepunch probes and DHT traffic still arriving on the raw port.
  // Packet authentication and key exchange are outside this transport test.
  return new Promise((resolve) => {
    socket.on('message', function onmessage(data, address) {
      if (data.byteLength < 4 || data[0] < 1 || data[0] > 4) return
      if (data[1] !== 0 || data[2] !== 0 || data[3] !== 0) return
      socket.off('message', onmessage)
      resolve([data, address])
    })
  })
}
