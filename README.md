# CS 4390 peer-to-peer file sharing

A tracker server plus a peer program, in C with POSIX threads. No outside
libraries are used; MD5 is implemented in `md5.c` from RFC 1321.

## Build

    make -f makefile

This makes `tracker` here and a `peer` program in each of `peer1`, `peer2`, `peer3`.

## Run by hand

Start the tracker, then each peer from inside its own folder:

    ./tracker
    cd peer1 && ./peer

To use two machines, put the tracker machine's IP address on line 2 of each
peer's `clientThreadConfig.cfg`. Anything in a peer's `shared` folder is shared
automatically when the peer starts. A peer accepts these typed commands:

    REQ LIST
    GET filename.track
    createtracker filename filesize description md5 ip-address port-number
    updatetracker filename start_byte end_byte ip-address port-number
    quit

## Run the final demo

    ./start_demo.sh

This follows the final demo timeline with 13 peers, and at the end compares
every downloaded file with the original. All settings are in `demo.cfg`, and the
output is saved in `demo.log`.

## Config files

| File | Contents |
| --- | --- |
| `sconfig` | port; tracker file folder; seconds before a silent peer is dead |
| `clientThreadConfig.cfg` | tracker port; tracker IP; update interval in seconds |
| `serverThreadConfig.cfg` | peer's listening port; shared folder name |

## Assumptions where the project description is open

- **Byte ranges:** `end_byte` is one past the last byte, matching the seed line
  `0:109283519` for a 109283519-byte file.
- **Peer-to-peer messages:** the description does not define them, so a chunk is
  requested with `<GET filename start end>\n` and answered with
  `<REP DATA start end>\n` plus the raw bytes. A request over 1024 bytes gets
  `<GET invalid>\n`, and bytes the peer does not hold get `<GET fail>\n`.
- **GET checksum:** `FileMD5` in `<REP GET END FileMD5>` is the MD5 of the tracker
  file contents, because the peer must check "the received data" against it.
- **Unknown tracker file:** `<REP GET ferr>\n`, by analogy with the other replies.
- **Peer ranges:** a tracker entry holds one range, so a peer reports its longest
  unbroken run of complete segments.
- **Segment order:** segments are taken in order, but each peer starts at its own
  position in the file. Otherwise, when the seed leaves at 1:30 with the large
  file still downloading, no peer would hold the end of the file.
- **Peer choice:** the newest timestamp wins among the peers least busy with this
  download, so that threads fetch from different peers.
- **Dead peers:** the tracker needs its own copy of the timeout, as the third line
  of `sconfig`. Set it a little above the peers' update interval.
- **File names:** must not contain spaces, since messages are space-separated.
- **Number of peers:** the final demo text says 5 peers join at 30 seconds but its
  steps say peers 3 to 8, which is 6. The demo follows the steps.
- **Demo speed:** `CHUNK_DELAY_MS` in `demo.cfg` slows downloads on purpose,
  because on one machine a transfer would otherwise finish almost instantly.
