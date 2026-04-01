# OSCAR Federation Protocol

## Overview

Federation allows two or more Open Oscar Server instances to exchange instant messages, typing notifications, and presence (online/offline) events. Users on one server can communicate with users on another server by addressing them as `screenname@networkname`.

Federation is built on top of the existing OSCAR wire protocol. Servers communicate over a dedicated TCP port using FLAP frames containing SNAC messages in a custom **Federation** food group (`0x0100`). Each server authenticates its peers using HMAC-SHA256 with a pre-shared secret.

### Terminology

| Term | Meaning |
|------|---------|
| **Network name** | A unique identifier for a server instance (e.g., `retra.im`, `chivanet.org`). |
| **Local user** | A user whose account exists on the current server. |
| **Remote user** | A user on a federated peer server, addressed as `user@network`. |
| **Peer** | Another Open Oscar Server instance configured for federation. |
| **Federation link** | The persistent TCP connection between two federated peers. |

### Example Setup

```
Server A: retra.im       (federation port 5195)
Server B: chivanet.org   (federation port 5196)

Users:
  Compy    -> account on retra.im
  Serena   -> account on chivanet.org

Compy's buddy list includes:    serena@chivanet.org
Serena's buddy list includes:   compy@retra.im
```

---

## Configuration

Federation is configured through three environment variables:

```bash
# Unique name for this server. Becomes the @suffix for remote addressing.
FEDERATION_NETWORK_NAME=retra.im

# TCP listener for incoming peer connections.
FEDERATION_LISTENER=0.0.0.0:5195

# Peers to federate with. Format: NAME@HOST:PORT:SECRET
FEDERATION_PEERS=chivanet.org@peering.chivanet.org:5196:MySharedSecret
```

Both servers in a pair must configure each other with the same shared secret. Federation is disabled when `FEDERATION_NETWORK_NAME` is empty.

---

## Wire Format

All federation messages are standard OSCAR SNAC frames carried inside FLAP data frames over a persistent TCP connection. The federation food group code is `0x0100`.

### FLAP Frame

```
+---+------+--------+---------+
| * | Type | SeqNum | Payload |
+---+------+--------+---------+
  1    1       2     variable

* = 0x2A (start marker)
Type: 0x02 = Data, 0x05 = KeepAlive, 0x04 = Signoff
```

### SNAC Frame (inside FLAP payload)

```
+-----------+----------+-------+-----------+
| FoodGroup | SubGroup | Flags | RequestID |
+-----------+----------+-------+-----------+
    2 bytes    2 bytes  2 bytes   4 bytes
```

All federation SNACs use FoodGroup `0x0100`. The SubGroup identifies the operation.

---

## SNAC Types Reference

| SubGroup | Name | Description |
|----------|------|-------------|
| `0x0001` | FedAuthRequest | Initiate authentication with challenge |
| `0x0002` | FedAuthResponse | Reply with HMAC digest |
| `0x0003` | FedAuthResult | Authentication success or failure |
| `0x0004` | FedMessage | Relay an instant message |
| `0x0005` | FedMessageAck | Acknowledge message delivery |
| `0x0006` | FedMessageErr | Report message delivery failure |
| `0x0007` | FedPresenceSubscribe | Subscribe to a user's online status |
| `0x0008` | FedPresenceUnsubscribe | Cancel a presence subscription |
| `0x0009` | FedPresenceNotify | Notify that a user came online/offline |
| `0x000A` | FedTypingEvent | Relay a typing notification |
| `0x000B` | FedKeepAlive | Keep the link alive (empty payload) |
| `0x000C` | FedPresenceSubscribeAck | Acknowledge subscription with current status |

---

## Packet Definitions

### FedAuthRequest (0x0001)

Sent by each side during the authentication handshake. Contains the sender's network name, a 32-byte random challenge, and a protocol version number.

```
+-------------------+-----------+---------+
| NetworkName       | Challenge | Version |
+-------------------+-----------+---------+
  uint16-len string   32 bytes    uint16

NetworkName:  The sender's network name (e.g., "retra.im").
              Prefixed with a uint16 length.
Challenge:    32 bytes of cryptographically random data.
Version:      Protocol version (currently 1).
```

### FedAuthResponse (0x0002)

Sent after receiving the peer's FedAuthRequest. Contains the sender's network name and an HMAC-SHA256 digest proving knowledge of the shared secret.

```
+-------------------+--------+
| NetworkName       | Digest |
+-------------------+--------+
  uint16-len string  32 bytes

NetworkName:  The sender's network name.
Digest:       HMAC-SHA256(challenge + peer_network_name, shared_secret)
              Where challenge is the 32 bytes from the peer's FedAuthRequest,
              and peer_network_name is the peer's network name as bytes.
```

### FedAuthResult (0x0003)

Sent after verifying the peer's FedAuthResponse. Indicates whether authentication succeeded.

```
+------+
| Code |
+------+
 uint16

Code values:
  0x0000  Success
  0x0001  Failed (invalid digest)
  0x0002  Unknown (peer not in config)
```

### FedMessage (0x0004)

Relays an instant message from a local user to a user on the remote server. The TLV block carries the original ICBM message payload (channel data, formatting, etc.).

```
+--------+----------+--------+-----------+-----------+
| Cookie | FromUser | ToUser | ChannelID | TLVs ...  |
+--------+----------+--------+-----------+-----------+
 uint64   uint8-len  uint8-len  uint16    rest block
          string     string

Cookie:     Message cookie (unique identifier for ack correlation).
FromUser:   Sender's local screen name (no @network suffix).
ToUser:     Recipient's local screen name on the remote server (no @network).
ChannelID:  ICBM channel (0x0001 = IM, 0x0002 = rendezvous, etc.).
TLVs:       The original ICBM TLV payload (message text, formatting, etc.).
```

### FedMessageAck (0x0005)

Sent by the receiving server to confirm that a FedMessage was delivered to the local recipient.

```
+--------+
| Cookie |
+--------+
 uint64

Cookie:  Matches the Cookie from the original FedMessage.
```

### FedMessageErr (0x0006)

Sent by the receiving server when a FedMessage could not be delivered.

```
+--------+------+
| Cookie | Code |
+--------+------+
 uint64   uint16

Cookie:  Matches the Cookie from the original FedMessage.
Code:    OSCAR error code indicating the reason:
           0x0004  User not logged on
           0x0010  Blocked by recipient's permit/deny settings
           0x001C  General failure
```

### FedPresenceSubscribe (0x0007)

Requests that the remote server send presence notifications for a specific user. Sent when a local user has a federated buddy in their buddy list.

```
+----------+--------+
| FromUser | ToUser |
+----------+--------+
 uint8-len  uint8-len
 string     string

FromUser:  The local user who wants to receive presence updates.
ToUser:    The user on the remote server whose status is being watched.
           This is the local part only (no @network suffix).
```

### FedPresenceUnsubscribe (0x0008)

Cancels a presence subscription. Sent when a local user removes a federated buddy from their buddy list.

```
+----------+--------+
| FromUser | ToUser |
+----------+--------+
 uint8-len  uint8-len
 string     string

(Same layout as FedPresenceSubscribe.)
```

### FedPresenceNotify (0x0009)

Informs a peer that a local user has come online or gone offline. Sent to peers that have active presence subscriptions for this user.

```
+------------+--------+-----------+
| ScreenName | Online | TLVs ...  |
+------------+--------+-----------+
  uint8-len    uint8    rest block
  string

ScreenName:  The local user whose status changed (no @network).
Online:      1 = user came online, 0 = user went offline.
TLVs:        Optional user info TLVs (present when Online=1).
```

### FedTypingEvent (0x000A)

Relays a typing notification between federated users.

```
+--------+----------+--------+-----------+-------+
| Cookie | FromUser | ToUser | ChannelID | Event |
+--------+----------+--------+-----------+-------+
 uint64   uint8-len  uint8-len  uint16    uint16
          string     string

Cookie:     Typing event cookie.
FromUser:   Sender's local screen name.
ToUser:     Recipient's local screen name on the remote server.
ChannelID:  ICBM channel.
Event:      Typing state:
              0x0000  Finished typing / text cleared
              0x0001  Text typed (content present)
              0x0002  Currently typing
```

### FedKeepAlive (0x000B)

Empty payload. Sent every 30 seconds to keep the TCP connection alive. If no frame is received for 90 seconds, the connection is considered dead.

### FedPresenceSubscribeAck (0x000C)

Sent in response to FedPresenceSubscribe. Provides the requested user's current online status so the subscriber gets an immediate answer rather than waiting for the next status change.

```
+------------+--------+-----------+
| ScreenName | Online | TLVs ...  |
+------------+--------+-----------+
  uint8-len    uint8    rest block
  string

ScreenName:  The user whose status was requested (no @network).
Online:      1 = currently online, 0 = currently offline.
TLVs:        Optional user info TLVs (present when Online=1).
```

---

## Protocol Flows

### 1. Connection Establishment and Authentication

Both servers simultaneously attempt outbound connections to each other. Duplicate connections are resolved by convention: the server with the lexicographically smaller network name keeps its outbound connection; the other uses the inbound.

The authentication handshake is a mutual challenge-response using HMAC-SHA256:

```
  retra.im                                              chivanet.org
     |                                                       |
     |  ---- TCP connect to chivanet.org:5196 -------------> |
     |                                                       |
     |  -- FedAuthRequest(name="retra.im", challenge=A) ---> |
     |  <-- FedAuthRequest(name="chivanet.org", challenge=B) |
     |                                                       |
     |  Both sides compute HMAC-SHA256 digests:              |
     |  retra.im computes:    HMAC(B + "retra.im", secret)   |
     |  chivanet.org computes: HMAC(A + "chivanet.org", secret)
     |                                                       |
     |  -- FedAuthResponse(name="retra.im", digest) -------> |
     |  <-- FedAuthResponse(name="chivanet.org", digest) --- |
     |                                                       |
     |  Both sides verify the received digest.               |
     |                                                       |
     |  -- FedAuthResult(code=0x0000 Success) -------------> |
     |  <-- FedAuthResult(code=0x0000 Success) ------------- |
     |                                                       |
     |  === Link established. Steady state begins. ========= |
```

If verification fails at any point, the verifying side sends `FedAuthResult(code=0x0001 Failed)` and closes the connection.

After authentication, the connecting server re-subscribes to presence for all local users who have federated buddies on the peer (see "Reconnection" below).

### 2. User Sign-On with Federated Buddies

When Compy signs into retra.im and has `serena@chivanet.org` in their buddy list:

```
  retra.im                                              chivanet.org
     |                                                       |
     |  Compy completes BOS sign-on (ClientOnline)           |
     |                                                       |
     |  1. retra.im scans Compy's feedbag, finds             |
     |     "serena@chivanet.org" as a buddy entry.           |
     |                                                       |
     |  -- FedPresenceSubscribe(from="compy",to="serena") -> |
     |                                                       |
     |  2. chivanet.org checks if Serena is online.          |
     |     It records compy@retra.im as a subscriber of      |
     |     Serena's presence.                                |
     |                                                       |
     |  <-- FedPresenceSubscribeAck(name="serena",online=1)  |
     |      (or online=0 if Serena is offline)               |
     |                                                       |
     |  3. retra.im receives the ack. If online=1, it        |
     |     delivers a BuddyArrived SNAC to Compy's client    |
     |     with ScreenName="serena@chivanet.org".            |
     |                                                       |
     |  4. retra.im also notifies chivanet.org that Compy    |
     |     is now online (for any remote subscribers):        |
     |                                                       |
     |  -- FedPresenceNotify(name="compy", online=1) ------> |
     |                                                       |
     |  5. chivanet.org checks if any local user has          |
     |     "compy@retra.im" in their buddy list. If Serena   |
     |     does, it delivers BuddyArrived to Serena's client |
     |     with ScreenName="compy@retra.im".                 |
```

### 3. Sending an Instant Message

Compy on retra.im sends an IM to `serena@chivanet.org`:

```
  Compy's Client             retra.im                  chivanet.org          Serena's Client
       |                        |                            |                       |
       | -- ICBMChannelMsg      |                            |                       |
       |    ToHost -----------> |                            |                       |
       |    (to="serena@        |                            |                       |
       |     chivanet.org")     |                            |                       |
       |                        |                            |                       |
       |                        | retra.im sees the          |                       |
       |                        | @chivanet.org suffix       |                       |
       |                        | and routes via federation: |                       |
       |                        |                            |                       |
       |                        | -- FedMessage -----------> |                       |
       |                        |    from="compy"            |                       |
       |                        |    to="serena"             |                       |
       |                        |    TLVs=[msg data]         |                       |
       |                        |                            |                       |
       |                        |            chivanet.org    |                       |
       |                        |            looks up Serena |                       |
       |                        |            locally, checks |                       |
       |                        |            blocking, and   |                       |
       |                        |            delivers:       |                       |
       |                        |                            |                       |
       |                        |                            | -- ICBMChannelMsg --> |
       |                        |                            |    ToClient           |
       |                        |                            |    from=              |
       |                        |                            |    "compy@retra.im"   |
       |                        |                            |                       |
       |                        | <-- FedMessageAck -------- |                       |
       |                        |     (cookie)               |                       |
       |                        |                            |                       |
       | <-- ICBMHostAck ------ |                            |                       |
       |     (if ack requested) |                            |                       |
```

Key details:
- The `FromUser` field in FedMessage contains just `"compy"` (no @suffix). The receiving server prepends `@retra.im` when constructing the ICBM to deliver to Serena.
- The `ToUser` field contains just `"serena"` (no @suffix). The sending server strips the `@chivanet.org` before sending.
- Original ICBM TLVs (message text, formatting, etc.) are forwarded as-is in the TLV rest block.

### 4. Message Delivery Failure

If the recipient is offline or has blocked the sender:

```
  retra.im                                              chivanet.org
     |                                                       |
     |  -- FedMessage(from="compy", to="serena") ----------> |
     |                                                       |
     |     Serena is offline:                                |
     |  <-- FedMessageErr(cookie, code=0x0004) ------------- |
     |                                                       |
     |  retra.im returns an ICBM error to Compy's client.   |

     --- OR ---

     |     Serena has blocked compy@retra.im:                |
     |  <-- FedMessageErr(cookie, code=0x0010) ------------- |
```

### 5. Typing Notifications

When Compy starts typing a message to `serena@chivanet.org`:

```
  retra.im                                              chivanet.org
     |                                                       |
     |  -- FedTypingEvent(from="compy", to="serena",         |
     |     event=0x0002 [typing]) --------------------------> |
     |                                                       |
     |  chivanet.org relays as ICBMClientEvent to Serena.    |
```

Typing events are fire-and-forget. There is no acknowledgment. If the peer is disconnected or the send queue is full, the event is silently dropped.

### 6. User Sign-Off and Departure Notification

When Compy signs off retra.im:

```
  retra.im                                              chivanet.org
     |                                                       |
     |  Compy's session closes. BuddyService                |
     |  .BroadcastBuddyDeparted() is called.                |
     |                                                       |
     |  -- FedPresenceNotify(name="compy", online=0) ------> |
     |                                                       |
     |  chivanet.org finds local users who have              |
     |  "compy@retra.im" in their buddy list and             |
     |  delivers BuddyDeparted to each.                      |
```

### 7. Adding a Federated Buddy

When Serena adds `compy@retra.im` to her buddy list while already signed in:

```
  Serena's Client             chivanet.org                   retra.im
       |                         |                              |
       | -- FeedbagUpsertItem    |                              |
       |    (name="compy@        |                              |
       |     retra.im",          |                              |
       |     class=Buddy) -----> |                              |
       |                         |                              |
       |                         | FeedbagService saves the     |
       |                         | item, then detects the       |
       |                         | @retra.im suffix:            |
       |                         |                              |
       |                         | -- FedPresenceSubscribe      |
       |                         |    (from="serena",           |
       |                         |     to="compy") -----------> |
       |                         |                              |
       |                         | <-- FedPresenceSubscribeAck  |
       |                         |    (name="compy", online=1)  |
       |                         |                              |
       |                         | Delivers BuddyArrived to     |
       | <-- BuddyArrived ------ | Serena with ScreenName=      |
       |    (compy@retra.im)     | "compy@retra.im"             |
```

### 8. Removing a Federated Buddy

When Serena removes `compy@retra.im` from her buddy list:

```
  Serena's Client             chivanet.org                   retra.im
       |                         |                              |
       | -- FeedbagDeleteItem    |                              |
       |    (name="compy@        |                              |
       |     retra.im") -------> |                              |
       |                         |                              |
       |                         | FeedbagService deletes the   |
       |                         | item, then detects the       |
       |                         | @retra.im suffix:            |
       |                         |                              |
       |                         | -- FedPresenceUnsubscribe    |
       |                         |    (from="serena",           |
       |                         |     to="compy") -----------> |
       |                         |                              |
       |                         | retra.im removes the         |
       |                         | subscription record.         |
```

No acknowledgment is sent for unsubscribe. The remote server simply stops sending FedPresenceNotify for that user/subscriber pair.

### 9. Reconnection After Link Failure

When the federation TCP connection drops (network failure, server restart, etc.):

```
  retra.im                                              chivanet.org
     |                                                       |
     |  TCP connection drops.                                |
     |  Read loop detects EOF.                               |
     |                                                       |
     |  Outbound maintainer waits with exponential backoff   |
     |  (1s, 2s, 4s, ... up to 60s max) then retries:       |
     |                                                       |
     |  ---- TCP connect ------------------------------------> |
     |  ---- Auth handshake ---------------------------------> |
     |                                                       |
     |  After successful auth, retra.im re-subscribes to     |
     |  presence for ALL local users' federated buddies:      |
     |                                                       |
     |  -- FedPresenceSubscribe(from="compy",to="serena") -> |
     |  <-- FedPresenceSubscribeAck(name="serena",online=1)  |
     |                                                       |
     |  Compy's client receives BuddyArrived for             |
     |  serena@chivanet.org (if she's online).               |
```

The re-subscription sweep scans every online local user's feedbag for buddies with the reconnected peer's `@network` suffix and sends a FedPresenceSubscribe for each.

### 10. Keepalive

```
  retra.im                                              chivanet.org
     |                                                       |
     |  -- FLAP KeepAlive (every 30s) --------------------->  |
     |  <-- FLAP KeepAlive (every 30s) -------------------- |
     |                                                       |
```

Keepalive frames are standard FLAP frames with type `0x05` and no payload. They are not SNAC messages. If no frame of any type is received for 90 seconds, the connection is considered dead and reconnection begins.

---

## Duplicate Connection Resolution

Since both servers simultaneously attempt outbound connections to each other, a convention prevents duplicate links:

1. Both servers connect and authenticate.
2. When a server receives an inbound connection from a peer it already has an outbound connection to, it compares network names lexicographically.
3. The server with the **smaller** network name keeps its **outbound** connection and rejects the inbound.
4. The server with the **larger** network name accepts the inbound and tears down its own outbound.

Example: `"chivanet.org" < "retra.im"`, so chivanet.org keeps its outbound connection to retra.im, and retra.im uses the inbound from chivanet.org.

---

## Screen Name Addressing

Federated screen names follow the format `localpart@networkname`:

| Viewed from | Compy's name | Serena's name |
|---|---|---|
| retra.im (Compy's server) | `compy` | `serena@chivanet.org` |
| chivanet.org (Serena's server) | `compy@retra.im` | `serena` |
| On the federation wire | `compy` (no suffix) | `serena` (no suffix) |

The `@network` suffix is **always stripped** before sending over the federation link. The receiving server knows the sender's network from the authenticated peer connection and reconstructs the full federated name for local delivery.

The `@` character is not valid in standard AIM screen names, making it an unambiguous delimiter. The local part is normalized the same way as regular screen names (lowercase, spaces removed). The network part is lowercased.

---

## Blocking Federated Users

Local users can block federated users by adding their full federated name (e.g., `compy@retra.im`) to their deny list via the standard feedbag permit/deny mechanism. When a FedMessage arrives, the receiving server checks the local recipient's relationship with the federated sender and returns `FedMessageErr(code=0x0010)` if blocked.

Blocking is enforced on the receiving side only. The sending server does not check whether the remote user has blocked the sender.

---

## Error Handling Summary

| Scenario | Behavior |
|---|---|
| Federation link down | Message returns ICBM error `0x0004` (not logged on) to sender |
| Remote user offline | Peer sends `FedMessageErr(0x0004)`, sender gets ICBM error |
| Remote user blocked sender | Peer sends `FedMessageErr(0x0010)`, sender gets ICBM error |
| Auth failure | Connection closed, retry with exponential backoff |
| Unknown peer connects | `FedAuthResult(0x0002)` sent, connection closed |
| Send queue full | Message/event silently dropped, ICBM error `0x0005` (service unavailable) returned to sender for messages |
| Typing event to offline peer | Silently dropped (no error to sender) |
