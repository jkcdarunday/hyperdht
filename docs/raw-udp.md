# Raw UDP mode protocol

The authenticated Noise handshake's UDX feature flags advertise raw UDP
support with bit `2` (`1 << 1`). Bit `1` continues to advertise reusable sockets.
Absence of the new bit prevents raw mode negotiation; existing streams and older
peers otherwise keep their previous behavior.

Both applications opt in by calling `toRaw` on an open stream at an
agreed application boundary. The API consumes the reliable encrypted stream from
that point onward. There is no automatic takeover on receiving arbitrary stream
data, and no unauthenticated UDP switch command.

Each control frame is the 17-byte prefix `HyperDHT raw UDP\x01`, followed by one
command byte: `1` for READY or `2` for ACK. Frames are parsed independently of
stream chunk boundaries. Each peer sends READY once and responds to the other
peer's READY with ACK once. Unexpected frames close the connection.

Before sending READY, a peer registers the existing network socket and peer
IP/port for incoming raw datagrams. Receipt of ACK after READY enables raw sends
and resolves `raw.opened`. Callers attach message listeners to the immediately
returned raw endpoint before awaiting readiness. Reception is already
armed, so packets may arrive before the reliable ACK is delivered. This provides
coordinated readiness without requiring simultaneous clocks or switches.

UDX continues to handle its own protocol packets, including the reliable encrypted
control connection's acknowledgements and keepalives. Other datagrams from the
reserved endpoint bypass DHT decoding and are emitted unchanged as `message`
events. No socket is created and no extra network header is added. Packet
authentication, encryption, buffering, forwarding, and local socket policy belong
to the application, not HyperDHT.

The control stream remains alive for the lifetime of raw mode. Closing it
notifies the peer and releases any pooled socket using the existing lifecycle.
Raw mode never closes a shared network socket directly. A negotiation timeout,
invalid control frame, or endpoint migration destroys the connection and closes
the raw endpoint. A new connection is required to resume stream application
traffic or recover from migration. Relayed connections and duplicate raw endpoint
owners are rejected before negotiation.
