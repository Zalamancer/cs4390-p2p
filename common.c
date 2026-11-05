/* common.c - sockets, parsing and tracker file helpers. */
#include "common.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

/* Fill in an IPv4 address. Returns 0 on success, -1 for a bad address. */
static int make_address(struct sockaddr_in *addr, const char *ip, int port)
{
    memset(addr, 0, sizeof(*addr));
    addr->sin_family = AF_INET;
    addr->sin_port = htons((unsigned short)port);
    return inet_pton(AF_INET, ip, &addr->sin_addr) == 1 ? 0 : -1;
}

/* Set the read/write timeouts and turn off send batching, which would
 * otherwise slow the small request/reply messages. */
void socket_prepare(int fd)
{
    struct timeval tv = { IO_TIMEOUT_SEC, 0 };
    int on = 1;

    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
}

/* Connect with a timeout, so an offline peer listed in a stale tracker file
 * cannot hold a download thread for minutes. Returns a socket or -1. */
int tcp_connect(const char *ip, int port)
{
    struct sockaddr_in addr;
    int fd;

    if (make_address(&addr, ip, port) != 0)
        return -1;
    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
        return -1;

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        if (errno != EINPROGRESS) {
            close(fd);
            return -1;
        }
        fd_set writable;
        struct timeval tv = { IO_TIMEOUT_SEC / 3, 0 };
        int err = 0;
        socklen_t err_len = sizeof(err);

        FD_ZERO(&writable);
        FD_SET(fd, &writable);
        if (select(fd + 1, NULL, &writable, NULL, &tv) <= 0 ||
            getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &err_len) < 0 ||
            err != 0) {
            close(fd);
            return -1;
        }
    }

    fcntl(fd, F_SETFL, flags);
    socket_prepare(fd);
    return fd;
}

/* Open a listening socket on every local address. Returns a socket or -1. */
int tcp_listen(int port)
{
    struct sockaddr_in addr;
    int on = 1;
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0)
        return -1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
        listen(fd, 128) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

int send_all(int fd, const void *buf, size_t len)
{
    const char *p = buf;

    while (len > 0) {
        ssize_t n = send(fd, p, len, 0);
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            return -1;
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

int recv_all(int fd, void *buf, size_t len)
{
    char *p = buf;

    while (len > 0) {
        ssize_t n = recv(fd, p, len, 0);
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            return -1;
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

/* Read one line, keeping the newline. It reads a byte at a time so that raw
 * file data following the line stays in the socket for recv_all.
 * Returns the line length, or -1 on error, closed socket or oversized line. */
int recv_line(int fd, char *buf, size_t cap)
{
    size_t used = 0;

    while (used + 1 < cap) {
        char c;
        ssize_t n = recv(fd, &c, 1, 0);
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            return -1;
        buf[used++] = c;
        if (c == '\n') {
            buf[used] = '\0';
            return (int)used;
        }
    }
    return -1;
}

/* Find this machine's address on the route towards remote_ip. A UDP "connect"
 * sends no packets; it only makes the system choose the network interface. */
int local_ip_towards(const char *remote_ip, int port, char *out, size_t cap)
{
    struct sockaddr_in addr, local;
    socklen_t local_len = sizeof(local);
    int fd, result = -1;

    if (make_address(&addr, remote_ip, port) != 0)
        return -1;
    if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
        return -1;
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0 &&
        getsockname(fd, (struct sockaddr *)&local, &local_len) == 0 &&
        inet_ntop(AF_INET, &local.sin_addr, out, (socklen_t)cap) != NULL)
        result = 0;
    close(fd);
    return result;
}

/* Split "<word word word>\n" into words, in place. The angle brackets are
 * optional so that commands can also be typed by hand.
 * Returns the number of words. */
int split_message(char *line, char **tokens, int max_tokens)
{
    size_t len;
    char *save = NULL;
    char *word;
    int count = 0;

    while (*line != '\0' && isspace((unsigned char)*line))
        line++;
    len = strlen(line);
    while (len > 0 && isspace((unsigned char)line[len - 1]))
        line[--len] = '\0';
    if (len > 0 && line[0] == '<')
        line++;
    len = strlen(line);
    if (len > 0 && line[len - 1] == '>')
        line[len - 1] = '\0';

    for (word = strtok_r(line, " \t", &save);
         word != NULL && count < max_tokens;
         word = strtok_r(NULL, " \t", &save))
        tokens[count++] = word;
    return count;
}

/* Parse a whole non-negative number. Returns 0 on success, -1 otherwise. */
int parse_number(const char *text, long long *out)
{
    char *end;
    long long value;

    if (*text == '\0' || !isdigit((unsigned char)*text))
        return -1;
    errno = 0;
    value = strtoll(text, &end, 10);
    if (errno != 0 || *end != '\0')
        return -1;
    *out = value;
    return 0;
}

/* Names come from the network, so refuse anything that could escape the
 * shared folder or break the space-separated message format. */
int valid_filename(const char *name)
{
    size_t len = strlen(name);

    if (len == 0 || len >= MAX_NAME || name[0] == '.')
        return 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)name[i];
        if (c == '/' || c == '\\' || c == '<' || c == '>' || isspace(c) ||
            iscntrl(c))
            return 0;
    }
    return 1;
}

int valid_md5(const char *text)
{
    if (strlen(text) != 32)
        return 0;
    for (int i = 0; i < 32; i++)
        if (!isxdigit((unsigned char)text[i]))
            return 0;
    return 1;
}

int valid_ip(const char *text)
{
    struct in_addr addr;
    return inet_pton(AF_INET, text, &addr) == 1;
}

int valid_port(long long port)
{
    return port >= 1 && port <= 65535;
}

/* Read the non-empty, non-comment lines of a config file.
 * Returns the number of lines, or -1 if the file cannot be opened. */
int read_config(const char *path, char lines[][MAX_LINE], int max_lines)
{
    char buf[MAX_LINE];
    int count = 0;
    FILE *f = fopen(path, "r");

    if (f == NULL)
        return -1;
    while (count < max_lines && fgets(buf, sizeof(buf), f) != NULL) {
        char *start = buf;
        size_t len;

        while (isspace((unsigned char)*start))
            start++;
        len = strlen(start);
        while (len > 0 && isspace((unsigned char)start[len - 1]))
            start[--len] = '\0';
        if (len == 0 || start[0] == '#')
            continue;
        memcpy(lines[count++], start, len + 1);
    }
    fclose(f);
    return count;
}

/* Returns a malloc'd, NUL-terminated copy of a file, or NULL on error. */
char *read_whole_file(const char *path, size_t *len)
{
    struct stat st;
    char *data;
    size_t got;
    FILE *f = fopen(path, "rb");

    if (f == NULL)
        return NULL;
    if (fstat(fileno(f), &st) != 0 || (data = malloc((size_t)st.st_size + 1)) == NULL) {
        fclose(f);
        return NULL;
    }
    got = fread(data, 1, (size_t)st.st_size, f);
    fclose(f);
    if (got != (size_t)st.st_size) {
        free(data);
        return NULL;
    }
    data[got] = '\0';
    if (len != NULL)
        *len = got;
    return data;
}

/* Write to a temporary file and rename it, so that a crash half way through
 * can never leave a damaged file behind. */
int write_file_atomic(const char *path, const char *data, size_t len)
{
    char tmp[MAX_LINE];
    FILE *f;
    int failed;

    if (snprintf(tmp, sizeof(tmp), "%s.tmp", path) >= (int)sizeof(tmp))
        return -1;
    if ((f = fopen(tmp, "wb")) == NULL)
        return -1;
    failed = fwrite(data, 1, len, f) != len;
    failed |= fclose(f) != 0;
    if (failed || rename(tmp, path) != 0) {
        unlink(tmp);
        return -1;
    }
    return 0;
}

/* Parse tracker file text. Lines starting with # are ignored.
 * Returns 0 on success, -1 if a required field is missing. */
int tracker_parse(const char *text, tracker_info *info)
{
    char *copy = strdup(text);
    char *save = NULL;
    int capacity = 0;
    int have_name = 0, have_size = 0, have_md5 = 0;

    memset(info, 0, sizeof(*info));
    if (copy == NULL)
        return -1;

    for (char *line = strtok_r(copy, "\n", &save); line != NULL;
         line = strtok_r(NULL, "\n", &save)) {
        peer_entry entry;

        line[strcspn(line, "\r")] = '\0';
        while (*line == ' ')
            line++;
        if (*line == '#' || *line == '\0')
            continue;

        if (strncmp(line, "Filename: ", 10) == 0) {
            snprintf(info->filename, sizeof(info->filename), "%s", line + 10);
            have_name = 1;
        } else if (strncmp(line, "Filesize: ", 10) == 0) {
            have_size = parse_number(line + 10, &info->filesize) == 0;
        } else if (strncmp(line, "Description: ", 13) == 0) {
            snprintf(info->description, sizeof(info->description), "%s", line + 13);
        } else if (strncmp(line, "MD5: ", 5) == 0) {
            snprintf(info->md5, sizeof(info->md5), "%s", line + 5);
            have_md5 = 1;
        } else if (sscanf(line, "%63[^:]:%d:%lld:%lld:%lld", entry.ip, &entry.port,
                          &entry.start, &entry.end, &entry.timestamp) == 5) {
            if (info->peer_count == capacity) {
                int bigger = capacity ? capacity * 2 : 16;
                peer_entry *grown = realloc(info->peers, (size_t)bigger * sizeof(*grown));
                if (grown == NULL) {
                    free(copy);
                    tracker_free(info);
                    return -1;
                }
                info->peers = grown;
                capacity = bigger;
            }
            info->peers[info->peer_count++] = entry;
        }
    }

    free(copy);
    if (!have_name || !have_size || !have_md5) {
        tracker_free(info);
        return -1;
    }
    return 0;
}

/* Returns the malloc'd text of a tracker file, or NULL if out of memory. */
char *tracker_format(const tracker_info *info)
{
    size_t cap = 1024 + MAX_NAME + MAX_DESC + (size_t)info->peer_count * 128;
    char *text = malloc(cap);
    size_t used;

    if (text == NULL)
        return NULL;
    used = (size_t)snprintf(text, cap,
                            "Filename: %s\nFilesize: %lld\nDescription: %s\nMD5: %s\n"
                            "#list of peers follows next\n",
                            info->filename, info->filesize, info->description, info->md5);
    for (int i = 0; i < info->peer_count; i++) {
        const peer_entry *p = &info->peers[i];
        used += (size_t)snprintf(text + used, cap - used, "%s:%d:%lld:%lld:%lld\n",
                                 p->ip, p->port, p->start, p->end, p->timestamp);
    }
    return text;
}

void tracker_free(tracker_info *info)
{
    free(info->peers);
    info->peers = NULL;
    info->peer_count = 0;
}
