const test = require('brittle')
const b4a = require('b4a')
const c = require('compact-encoding')
const { once } = require('events')
const { swarm } = require('./helpers')
const RawUDP = require('../lib/raw-udp')
const createUDPBridge = require('../examples/helpers/udp-bridge')
const messages = require('../lib/messages')

const LOSS_MARKER = b4a.from('raw-udp-loss-test:')

for (const proxy of [false, true]) {
  test(
    `raw UDP - ${proxy ? 'application proxy' : 'send and trySend'} does not retransmit lost datagrams`,
    { timeout: 15000 },
    async (t) => {
      const { a, b, client, remote } = await pair(t, true)
      const link = await lossyLink(t, a, b, client, remote)
      let ab, ba, left, right
      if (proxy) {
        left = a.udx.createSocket()
        right = b.udx.createSocket()
        left.bind(0, '127.0.0.1')
        right.bind(0, '127.0.0.1')
        t.teardown(() => Promise.all([left.close(), right.close()]))
        const bridges = await Promise.all([
          createUDPBridge(b, client, { port: right.address().port }),
          createUDPBridge(a, remote, { port: left.address().port })
        ])
        ab = bridges[0]
        ba = bridges[1]
      } else {
        ab = b.toRaw(client)
        ba = a.toRaw(remote)
        await Promise.all([ab.opened, ba.opened])
        left = ba
        right = ab
      }
      t.teardown(() => Promise.all([ab.close(), ba.close()]))

      // Keep reliable control traffic flowing through the same lossy link.
      // Only tagged application datagrams are dropped or counted as resends.
      client.setKeepAlive(100)
      remote.setKeepAlive(100)
      const receivedA = Array(8).fill(0)
      const receivedB = Array(8).fill(0)
      const record = (counts) => (data) => {
        const id = lossPacketId(data)
        if (id !== -1) counts[id]++
      }
      left.on('message', record(receivedA))
      right.on('message', record(receivedB))
      const doneA = packetFrom(left, lossPacket(7))
      const doneB = packetFrom(right, lossPacket(7))
      const controlBefore = link.controlPackets

      for (let id = 0; id < 8; id++) {
        const packet = lossPacket(id)
        if (proxy) {
          await left.send(packet, ba.address().port)
          await right.send(packet, ab.address().port)
        } else if (id % 2 === 0) {
          t.ok(await ba.send(packet))
          t.ok(await ab.send(packet))
        } else {
          t.ok(ba.trySend(packet))
          t.ok(ab.trySend(packet))
        }
      }
      await Promise.all([doneA, doneB])

      // Drops happen only on the first attempt. A retransmission would get
      // through and be visible both on the wire and at the receiver. Observe
      // for three seconds, spanning multiple normal local UDX retry intervals.
      await new Promise((resolve) => setTimeout(resolve, 3000))
      t.alike(link.aToB.attempts, Array(8).fill(1), 'A sends each datagram exactly once')
      t.alike(link.bToA.attempts, Array(8).fill(1), 'B sends each datagram exactly once')
      t.is(link.aToB.dropped, 3, 'three A to B datagrams were actually dropped')
      t.is(link.bToA.dropped, 3, 'three B to A datagrams were actually dropped')
      const expected = [0, 1, 0, 1, 1, 0, 1, 1]
      t.alike(receivedA, expected, 'lost B to A datagrams stay lost; survivors arrive once')
      t.alike(receivedB, expected, 'lost A to B datagrams stay lost; survivors arrive once')
      t.ok(link.controlPackets > controlBefore, 'reliable control traffic is still flowing')
      t.absent(client.destroying || remote.destroying, 'packet loss does not close raw mode')
    }
  )
}

test(
  'raw UDP - coordination, datagrams and shared socket lifecycle',
  { timeout: 10000 },
  async (t) => {
    const { a, b, client, remote } = await pair(t)
    const clientPort = client.rawStream.socket.address().port
    const serverPort = remote.rawStream.socket.address().port
    let socketsCreated = 0
    for (const node of [a, b]) {
      const createSocket = node.udx.createSocket.bind(node.udx)
      node.udx.createSocket = (...args) => {
        socketsCreated++
        return createSocket(...args)
      }
    }
    const write = client.write.bind(client)
    client.write = (data) => {
      for (let i = 0; i < data.byteLength; i++) write(data.subarray(i, i + 1))
      return true
    }

    const ab = b.toRaw(client)
    t.absent(ab.trySend(b4a.from('too early')), 'outbound traffic waits for readiness')
    const earlyPacket = b4a.from([1, 0, 0, 0, 99])
    const earlyReceived = packetFrom(ab, earlyPacket)
    const writeRemote = remote.write.bind(remote)
    remote.write = (frame) => {
      if (frame[frame.byteLength - 1] !== 2) return writeRemote(frame)
      remote.rawStream.socket.trySend(
        earlyPacket,
        remote.rawStream.remotePort,
        remote.rawStream.remoteHost
      )
      earlyReceived.then(([data]) => {
        t.alike(data, earlyPacket, 'raw reception is armed before ACK arrives')
        t.absent(ab._active, 'outbound traffic still waits for ACK')
        writeRemote(frame)
      })
      return true
    }
    const ba = a.toRaw(remote)
    await Promise.all([ab.opened, ba.opened])
    t.is(socketsCreated, 0, 'conversion creates no sockets')
    t.is(ab.address().port, clientPort, 'client keeps its punched port')
    t.is(ba.address().port, serverPort, 'server keeps its punched port')
    t.is(ab.remoteAddress().port, serverPort)

    for (const packet of [
      b4a.from([1, 0, 0, 0, 42]),
      b4a.from([3, 0, 0, 0]),
      b4a.from([127]),
      b4a.alloc(1400, 255)
    ]) {
      const received = once(ab, 'message')
      t.ok(await ba.send(packet), 'datagram sent')
      const [actual, from] = await received
      t.alike(actual, packet, 'packet arrives without added framing')
      t.is(from.port, serverPort, 'packet comes from punched port')
      const reply = once(ba, 'message')
      t.ok(ab.trySend(packet))
      t.alike((await reply)[0], packet, 'reverse forwarding preserves the datagram')
    }
    const [third] = await swarm(t, 1)
    await third.ping(a.address())
    t.pass('DHT traffic from other endpoints still works')
    const remoteClosed = once(ba, 'close')
    await ab.close()
    await remoteClosed
    t.ok(ab.closed && ba.closed)
    t.absent(await ab.send(b4a.from('closed')))
    t.is(a._rawUDP.size + b._rawUDP.size, 0, 'raw routes removed')
    await ab.close()
  }
)

test('raw UDP - peer must opt in before raw transmission', { timeout: 10000 }, async (t) => {
  const { a, b, client, remote } = await pair(t)
  const remoteClosed = new Promise((resolve) => remote.once('close', resolve))
  const raw = b.toRaw(client, { timeout: 50 })
  await t.exception(() => raw.opened, /negotiation timed out/)
  await raw.close()
  t.is(b._rawUDP.size, 0, 'timed-out route removed')
  await remoteClosed
  t.ok(remote.destroyed)
  t.is(a._rawUDP.size, 0)
})

test('raw UDP - forced holepunch retains pooled sockets', { timeout: 15000 }, async (t) => {
  const { a, b, client, remote } = await pair(t, true)
  t.ok(a._socketPool.lookup(remote.rawStream.socket), 'server uses a punched socket')
  t.ok(b._socketPool.lookup(client.rawStream.socket), 'client uses a punched socket')
  const ab = b.toRaw(client)
  const ba = a.toRaw(remote)
  await Promise.all([ab.opened, ba.opened])
  const packet = b4a.from([4, 0, 0, 0, 1])
  const message = packetFrom(ab, packet)
  await ba.send(packet)
  t.alike((await message)[0], packet)
  const closed = once(ba, 'close')
  await ab.close()
  await closed
})

test(
  'raw UDP - rejects unsupported, relayed, busy and duplicate connections',
  { timeout: 10000 },
  async (t) => {
    const { a, b, client, remote } = await pair(t)
    RawUDP.register(client.rawStream, false)
    t.exception(() => b.toRaw(client), /does not support/)
    RawUDP.register(client.rawStream, true)
    t.exception(() => b.toRaw(client), /stable direct/)
    RawUDP.setDirect(client.rawStream)
    t.exception(() => a.toRaw(client), /does not support/)
    t.exception(() => b.toRaw(client, { timeout: 0 }), /timeout/)
    const listener = () => {}
    client.on('data', listener)
    t.exception(() => b.toRaw(client), /already in use/)
    client.off('data', listener)
    const ab = b.toRaw(client)
    const ba = a.toRaw(remote)
    await Promise.all([ab.opened, ba.opened])
    t.exception(() => b.toRaw(client), /already in use/)
    const closed = once(ba, 'close')
    await ab.close()
    await closed
  }
)

test('raw UDP - DHT destruction closes the raw endpoint', { timeout: 10000 }, async (t) => {
  const { a, b, client, remote } = await pair(t)
  const ab = b.toRaw(client)
  const ba = a.toRaw(remote)
  await Promise.all([ab.opened, ba.opened])
  const closed = once(ab, 'close')
  const remoteClosed = once(ba, 'close')
  await b.destroy()
  await Promise.all([closed, remoteClosed])
  t.ok(ab.closed && ba.closed)
})

test('raw UDP - capability flag preserves existing UDX features', (t) => {
  const payload = {
    error: 0,
    firewall: 0,
    udx: { id: 42, seq: 0, reusableSocket: true, rawUDP: true }
  }
  const decoded = c.decode(messages.noisePayload, c.encode(messages.noisePayload, payload))
  t.alike(decoded.udx, { version: 1, ...payload.udx })
  payload.udx.rawUDP = false
  t.is(c.decode(messages.noisePayload, c.encode(messages.noisePayload, payload)).udx.rawUDP, false)
})

async function pair(t, holepunch = false) {
  const net = await swarm(t, holepunch ? 32 : 2)
  const a = holepunch ? net.createNode({ quickFirewall: false }) : net.nodes[0]
  const b = holepunch ? net.createNode({ quickFirewall: false }) : net.nodes[1]
  await Promise.all([a.fullyBootstrapped(), b.fullyBootstrapped()])
  const server = a.createServer({ shareLocalAddress: !holepunch })
  server.on('connection', (socket) => socket.on('error', () => {}))
  await server.listen()
  const incoming = once(server, 'connection')
  const client = b.connect(server.publicKey, { localConnection: !holepunch })
  client.on('error', () => {})
  const [[remote]] = await Promise.all([incoming, once(client, 'open')])
  await Promise.all([client.opened, remote.opened])
  return { a, b, client, remote }
}

function packetFrom(socket, expected) {
  // Raw sockets may also receive in-flight DHT replies or final punch probes.
  return new Promise((resolve) => {
    socket.on('message', function onmessage(data, address) {
      if (!b4a.equals(data, expected)) return
      socket.off('message', onmessage)
      resolve([data, address])
    })
  })
}

function lossPacket(id) {
  return b4a.concat([b4a.from([4, 0, 0, 0]), LOSS_MARKER, b4a.from([id])])
}

function lossPacketId(data) {
  if (data.byteLength !== 4 + LOSS_MARKER.byteLength + 1) return -1
  if (!b4a.equals(data.subarray(4, -1), LOSS_MARKER)) return -1
  const id = data[data.byteLength - 1]
  return id < 8 ? id : -1
}

async function lossyLink(t, a, b, client, remote) {
  const nearA = a.udx.createSocket()
  const nearB = b.udx.createSocket()
  nearA.bind(0, '127.0.0.1')
  nearB.bind(0, '127.0.0.1')
  t.teardown(() => Promise.all([nearA.close(), nearB.close()]))
  const aPort = remote.rawStream.socket.address().port
  const bPort = client.rawStream.socket.address().port
  const stats = {
    aToB: { attempts: Array(8).fill(0), dropped: 0 },
    bToA: { attempts: Array(8).fill(0), dropped: 0 },
    controlPackets: 0
  }
  const drop = new Set([0, 2, 5])
  const forward = (socket, port, direction) => (data) => {
    const id = lossPacketId(data)
    if (id === -1) {
      stats.controlPackets++
    } else {
      direction.attempts[id]++
      if (drop.has(id) && direction.attempts[id] === 1) {
        direction.dropped++
        return
      }
    }
    socket.trySend(data, port, '127.0.0.1')
  }
  nearA.on('message', forward(nearB, bPort, stats.aToB))
  nearB.on('message', forward(nearA, aPort, stats.bToA))

  // Move the established UDX path through real UDP forwarding sockets before
  // entering raw mode. This intercepts actual wire datagrams, not send() calls
  // or receiver callbacks, and leaves the punched local ports unchanged.
  await Promise.all([
    remote.rawStream.changeRemote(
      remote.rawStream.socket,
      client.rawStream.id,
      nearA.address().port,
      '127.0.0.1'
    ),
    client.rawStream.changeRemote(
      client.rawStream.socket,
      remote.rawStream.id,
      nearB.address().port,
      '127.0.0.1'
    )
  ])
  return stats
}
