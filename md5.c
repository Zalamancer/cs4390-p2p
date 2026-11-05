/* md5.c - MD5 checksums, implemented from the RFC 1321 description. */
#include "md5.h"

#include <stdio.h>
#include <string.h>

static const uint32_t K[64] = {
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a,
    0xa8304613, 0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
    0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340,
    0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
    0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8,
    0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c,
    0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
    0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
    0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92,
    0xffeff47d, 0x85845dd1, 0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
    0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391
};

static const uint8_t SHIFT[64] = {
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
    5,  9, 14, 20, 5,  9, 14, 20, 5,  9, 14, 20, 5,  9, 14, 20,
    4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
    6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21
};

static uint32_t rotate_left(uint32_t x, int n)
{
    return (x << n) | (x >> (32 - n));
}

/* Mix one 64-byte block into the running state. */
static void process_block(md5_ctx *ctx, const uint8_t *p)
{
    uint32_t m[16];
    uint32_t a = ctx->state[0], b = ctx->state[1];
    uint32_t c = ctx->state[2], d = ctx->state[3];

    for (int i = 0; i < 16; i++)
        m[i] = (uint32_t)p[i * 4] | (uint32_t)p[i * 4 + 1] << 8 |
               (uint32_t)p[i * 4 + 2] << 16 | (uint32_t)p[i * 4 + 3] << 24;

    for (int i = 0; i < 64; i++) {
        uint32_t f;
        int g;
        if (i < 16)      { f = (b & c) | (~b & d); g = i; }
        else if (i < 32) { f = (d & b) | (~d & c); g = (5 * i + 1) % 16; }
        else if (i < 48) { f = b ^ c ^ d;          g = (3 * i + 5) % 16; }
        else             { f = c ^ (b | ~d);       g = (7 * i) % 16; }

        f += a + K[i] + m[g];
        a = d;
        d = c;
        c = b;
        b += rotate_left(f, SHIFT[i]);
    }

    ctx->state[0] += a;
    ctx->state[1] += b;
    ctx->state[2] += c;
    ctx->state[3] += d;
}

void md5_init(md5_ctx *ctx)
{
    ctx->state[0] = 0x67452301;
    ctx->state[1] = 0xefcdab89;
    ctx->state[2] = 0x98badcfe;
    ctx->state[3] = 0x10325476;
    ctx->total_bytes = 0;
    ctx->used = 0;
}

void md5_update(md5_ctx *ctx, const void *data, size_t len)
{
    const uint8_t *p = data;
    ctx->total_bytes += len;

    while (len > 0) {
        size_t n = 64 - ctx->used;
        if (n > len)
            n = len;
        memcpy(ctx->block + ctx->used, p, n);
        ctx->used += n;
        p += n;
        len -= n;
        if (ctx->used == 64) {
            process_block(ctx, ctx->block);
            ctx->used = 0;
        }
    }
}

void md5_final(md5_ctx *ctx, char hex[33])
{
    static const uint8_t padding[64] = { 0x80 };
    uint64_t bits = ctx->total_bytes * 8;
    uint8_t length[8];

    for (int i = 0; i < 8; i++)
        length[i] = (uint8_t)(bits >> (8 * i));

    /* Pad to 56 bytes in the last block, leaving room for the length. */
    size_t pad = (ctx->used < 56) ? 56 - ctx->used : 120 - ctx->used;
    md5_update(ctx, padding, pad);
    md5_update(ctx, length, 8);

    for (int i = 0; i < 4; i++)
        for (int j = 0; j < 4; j++)
            snprintf(hex + (i * 8) + (j * 2), 3, "%02x",
                     (ctx->state[i] >> (8 * j)) & 0xff);
    hex[32] = '\0';
}

void md5_buffer(const void *data, size_t len, char hex[33])
{
    md5_ctx ctx;
    md5_init(&ctx);
    md5_update(&ctx, data, len);
    md5_final(&ctx, hex);
}

int md5_file(const char *path, char hex[33])
{
    unsigned char buf[65536];
    size_t n;
    md5_ctx ctx;
    FILE *f = fopen(path, "rb");

    if (f == NULL)
        return -1;
    md5_init(&ctx);
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0)
        md5_update(&ctx, buf, n);
    int failed = ferror(f);
    fclose(f);
    if (failed)
        return -1;
    md5_final(&ctx, hex);
    return 0;
}
