/* common.h - code shared by the tracker server and the peer program. */
#ifndef COMMON_H
#define COMMON_H

#include <stddef.h>

#define MAX_CHUNK      1024   /* largest file chunk one GET may request */
#define MAX_LINE       2048   /* longest protocol or config line */
#define MAX_NAME       256    /* longest file name */
#define MAX_DESC       512    /* longest file description */
#define MAX_IP         64
#define MAX_TOKENS     16
#define MAX_CONFIG     8      /* most lines read from a config file */
#define IO_TIMEOUT_SEC 30     /* give up on a silent socket after this long */

/* One "ip:port:start:end:timestamp" line of a tracker file.
 * start is the first byte the peer has, end is one past the last byte. */
typedef struct {
    char ip[MAX_IP];
    int port;
    long long start;
    long long end;
    long long timestamp;
} peer_entry;

/* The parsed contents of a tracker file. */
typedef struct {
    char filename[MAX_NAME];
    long long filesize;
    char description[MAX_DESC];
    char md5[33];
    peer_entry *peers;
    int peer_count;
} tracker_info;

/* Sockets */
int tcp_connect(const char *ip, int port);
int tcp_listen(int port);
void socket_prepare(int fd);
int send_all(int fd, const void *buf, size_t len);
int recv_all(int fd, void *buf, size_t len);
int recv_line(int fd, char *buf, size_t cap);
int local_ip_towards(const char *remote_ip, int port, char *out, size_t cap);

/* Messages and input checking */
int split_message(char *line, char **tokens, int max_tokens);
int parse_number(const char *text, long long *out);
int valid_filename(const char *name);
int valid_md5(const char *text);
int valid_ip(const char *text);
int valid_port(long long port);

/* Files */
int read_config(const char *path, char lines[][MAX_LINE], int max_lines);
char *read_whole_file(const char *path, size_t *len);
int write_file_atomic(const char *path, const char *data, size_t len);

/* Tracker files */
int tracker_parse(const char *text, tracker_info *info);
char *tracker_format(const tracker_info *info);
void tracker_free(tracker_info *info);

#endif
