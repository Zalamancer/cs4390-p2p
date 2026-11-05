# Builds the tracker in this folder and a copy of the peer in peer1, peer2, peer3.
CC      = gcc
CFLAGS  = -Wall -Wextra -O2 -pthread
COMMON  = common.c md5.c
HEADERS = common.h md5.h
PEERS   = peer1 peer2 peer3

all: tracker $(addsuffix /peer,$(PEERS))

tracker: tracker.c $(COMMON) $(HEADERS)
	$(CC) $(CFLAGS) -o $@ tracker.c $(COMMON)

$(addsuffix /peer,$(PEERS)): peer.c $(COMMON) $(HEADERS)
	$(CC) $(CFLAGS) -o $@ peer.c $(COMMON)

clean:
	rm -f tracker $(addsuffix /peer,$(PEERS))
	rm -rf run torrents demo.log

.PHONY: all clean
