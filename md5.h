/* md5.h - MD5 checksums (RFC 1321), written for this project. */
#ifndef MD5_H
#define MD5_H

#include <stddef.h>
#include <stdint.h>

typedef struct {
    uint32_t state[4];
    uint64_t total_bytes;
    uint8_t  block[64];
    size_t   used;
} md5_ctx;

void md5_init(md5_ctx *ctx);
void md5_update(md5_ctx *ctx, const void *data, size_t len);
void md5_final(md5_ctx *ctx, char hex[33]);

/* Checksum of a memory buffer, as 32 hex characters. */
void md5_buffer(const void *data, size_t len, char hex[33]);

/* Checksum of a whole file. Returns 0 on success, -1 if it cannot be read. */
int md5_file(const char *path, char hex[33]);

#endif
