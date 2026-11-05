/*
 * tracker.c - the central tracker server.
 *
 * It keeps one <filename>.track file per shared file, listing which peers
 * hold which bytes. Each connection gets its own worker thread, which handles
 * a single command (LIST, GET, createtracker or updatetracker), sends the
 * reply and closes the connection.
 *
 * Settings come from the file "sconfig": first line port number, second line
 * shared folder, optional third line seconds of silence before a peer counts
 * as dead (this should be a little longer than the peers' update interval).
 */
#include "common.h"
#include "md5.h"

#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_DEAD_SECONDS 900

static char shared_dir[MAX_LINE];
static long long dead_seconds = DEFAULT_DEAD_SECONDS;

/* Workers read and rewrite the same tracker files, one at a time. */
static pthread_mutex_t files_lock = PTHREAD_MUTEX_INITIALIZER;

static void log_event(const char *format, ...)
{
    char text[MAX_LINE];
    va_list args;

    va_start(args, format);
    vsnprintf(text, sizeof(text), format, args);
    va_end(args);
    printf("Tracker: %s\n", text);
}

static void reply(int fd, const char *format, ...)
{
    char text[MAX_LINE];
    va_list args;

    va_start(args, format);
    vsnprintf(text, sizeof(text), format, args);
    va_end(args);
    send_all(fd, text, strlen(text));
}

static int track_path(const char *filename, char *path, size_t cap)
{
    return snprintf(path, cap, "%s/%s.track", shared_dir, filename) < (int)cap ? 0 : -1;
}

static int save_tracker(const char *path, const tracker_info *info)
{
    char *text = tracker_format(info);
    int result;

    if (text == NULL)
        return -1;
    result = write_file_atomic(path, text, strlen(text));
    free(text);
    return result;
}

/* LIST: send the name, size and checksum of every tracker file. */
static void handle_list(int fd)
{
    char *body = NULL;
    size_t body_len = 0;
    int count = 0;
    DIR *dir;
    struct dirent *item;

    pthread_mutex_lock(&files_lock);
    if ((dir = opendir(shared_dir)) != NULL) {
        while ((item = readdir(dir)) != NULL) {
            char path[MAX_LINE];
            char line[MAX_LINE];
            char *text, *grown;
            size_t name_len = strlen(item->d_name);
            tracker_info info;
            int line_len;

            if (name_len <= 6 || strcmp(item->d_name + name_len - 6, ".track") != 0)
                continue;
            snprintf(path, sizeof(path), "%s/%s", shared_dir, item->d_name);
            if ((text = read_whole_file(path, NULL)) == NULL)
                continue;
            if (tracker_parse(text, &info) == 0) {
                line_len = snprintf(line, sizeof(line), "<%d %s %lld %s>\n", count + 1,
                                    info.filename, info.filesize, info.md5);
                grown = realloc(body, body_len + (size_t)line_len + 1);
                if (grown != NULL) {
                    body = grown;
                    memcpy(body + body_len, line, (size_t)line_len + 1);
                    body_len += (size_t)line_len;
                    count++;
                }
                tracker_free(&info);
            }
            free(text);
        }
        closedir(dir);
    }
    pthread_mutex_unlock(&files_lock);

    reply(fd, "<REP LIST %d>\n", count);
    if (body != NULL)
        send_all(fd, body, body_len);
    reply(fd, "<REP LIST END>\n");
    free(body);
    log_event("LIST answered with %d file(s)", count);
}

/* GET: send a tracker file followed by the MD5 of the data just sent. */
static void handle_get(int fd, const char *requested)
{
    char name[MAX_NAME];
    char path[MAX_LINE];
    char md5[33];
    char *text;
    size_t len = 0;

    snprintf(name, sizeof(name), "%s", requested);
    size_t name_len = strlen(name);
    if (name_len > 6 && strcmp(name + name_len - 6, ".track") == 0)
        name[name_len - 6] = '\0';

    if (!valid_filename(name) || track_path(name, path, sizeof(path)) != 0) {
        reply(fd, "<GET invalid>\n");
        return;
    }

    pthread_mutex_lock(&files_lock);
    text = read_whole_file(path, &len);
    pthread_mutex_unlock(&files_lock);

    if (text == NULL) {
        reply(fd, "<REP GET ferr>\n");
        log_event("GET %s.track failed: no such tracker file", name);
        return;
    }

    md5_buffer(text, len, md5);
    reply(fd, "<REP GET BEGIN>\n<");
    send_all(fd, text, len);
    reply(fd, ">\n<REP GET END %s>\n", md5);
    free(text);
    log_event("GET %s.track answered", name);
}

/* createtracker: filename filesize description md5 ip port */
static void handle_create(int fd, char **tok, int count)
{
    char path[MAX_LINE];
    struct stat st;
    long long filesize, port;
    tracker_info info;
    peer_entry seed;

    if (count != 7 || !valid_filename(tok[1]) || parse_number(tok[2], &filesize) != 0 ||
        strlen(tok[3]) >= MAX_DESC || !valid_md5(tok[4]) || !valid_ip(tok[5]) ||
        strlen(tok[5]) >= MAX_IP || parse_number(tok[6], &port) != 0 ||
        !valid_port(port) || track_path(tok[1], path, sizeof(path)) != 0) {
        reply(fd, "<createtracker fail>\n");
        log_event("createtracker rejected: bad message");
        return;
    }

    memset(&info, 0, sizeof(info));
    snprintf(info.filename, sizeof(info.filename), "%s", tok[1]);
    snprintf(info.description, sizeof(info.description), "%s", tok[3]);
    snprintf(info.md5, sizeof(info.md5), "%s", tok[4]);
    info.filesize = filesize;

    snprintf(seed.ip, sizeof(seed.ip), "%s", tok[5]);
    seed.port = (int)port;
    seed.start = 0;
    seed.end = filesize;
    seed.timestamp = (long long)time(NULL);
    info.peers = &seed;
    info.peer_count = 1;

    pthread_mutex_lock(&files_lock);
    if (stat(path, &st) == 0) {
        pthread_mutex_unlock(&files_lock);
        reply(fd, "<createtracker ferr>\n");
        log_event("createtracker %s: tracker file already exists", tok[1]);
    } else if (save_tracker(path, &info) != 0) {
        pthread_mutex_unlock(&files_lock);
        reply(fd, "<createtracker fail>\n");
        log_event("createtracker %s failed: cannot write file", tok[1]);
    } else {
        pthread_mutex_unlock(&files_lock);
        reply(fd, "<createtracker succ>\n");
        log_event("created %s.track for %s:%d", tok[1], tok[5], (int)port);
    }
}

/* Add or refresh a peer's entry and drop every peer that has gone silent.
 * Returns 0 on success, -1 if out of memory. */
static int apply_update(tracker_info *info, const char *ip, int port,
                        long long start, long long end)
{
    long long now = (long long)time(NULL);
    int kept = 0, found = 0;
    peer_entry *grown;

    for (int i = 0; i < info->peer_count; i++) {
        peer_entry *p = &info->peers[i];

        if (strcmp(p->ip, ip) == 0 && p->port == port) {
            p->start = start;
            p->end = end;
            p->timestamp = now;
            found = 1;
        } else if (now - p->timestamp > dead_seconds) {
            log_event("removed dead peer %s:%d from %s", p->ip, p->port, info->filename);
            continue;
        }
        info->peers[kept++] = *p;
    }
    info->peer_count = kept;
    if (found)
        return 0;

    grown = realloc(info->peers, (size_t)(kept + 1) * sizeof(*grown));
    if (grown == NULL)
        return -1;
    info->peers = grown;
    snprintf(grown[kept].ip, sizeof(grown[kept].ip), "%s", ip);
    grown[kept].port = port;
    grown[kept].start = start;
    grown[kept].end = end;
    grown[kept].timestamp = now;
    info->peer_count = kept + 1;
    return 0;
}

/* updatetracker: filename start_byte end_byte ip port */
static void handle_update(int fd, char **tok, int count)
{
    char path[MAX_LINE];
    char *text;
    long long start, end, port;
    tracker_info info;
    const char *name = count >= 2 ? tok[1] : "unknown";

    if (count != 6 || !valid_filename(tok[1]) || parse_number(tok[2], &start) != 0 ||
        parse_number(tok[3], &end) != 0 || start > end || !valid_ip(tok[4]) ||
        strlen(tok[4]) >= MAX_IP || parse_number(tok[5], &port) != 0 ||
        !valid_port(port) || track_path(tok[1], path, sizeof(path)) != 0) {
        reply(fd, "<updatetracker %s fail>\n", name);
        log_event("updatetracker rejected: bad message");
        return;
    }

    pthread_mutex_lock(&files_lock);
    if ((text = read_whole_file(path, NULL)) == NULL) {
        pthread_mutex_unlock(&files_lock);
        if (errno == ENOENT) {
            reply(fd, "<updatetracker %s ferr>\n", name);
            log_event("updatetracker %s: no such tracker file", name);
        } else {
            reply(fd, "<updatetracker %s fail>\n", name);
        }
        return;
    }

    int ok = tracker_parse(text, &info) == 0;
    free(text);
    if (ok) {
        ok = end <= info.filesize && apply_update(&info, tok[4], (int)port, start, end) == 0 &&
             save_tracker(path, &info) == 0;
        tracker_free(&info);
    }
    pthread_mutex_unlock(&files_lock);

    if (ok) {
        reply(fd, "<updatetracker %s succ>\n", name);
        log_event("updated %s: %s:%d has bytes %lld to %lld", name, tok[4], (int)port, start, end);
    } else {
        reply(fd, "<updatetracker %s fail>\n", name);
        log_event("updatetracker %s failed", name);
    }
}

/* Worker thread: serve one command from one peer, then finish. */
static void *worker(void *arg)
{
    int fd = *(int *)arg;
    char line[MAX_LINE];
    char *tok[MAX_TOKENS];
    int count;

    free(arg);
    socket_prepare(fd);

    if (recv_line(fd, line, sizeof(line)) > 0) {
        count = split_message(line, tok, MAX_TOKENS);
        if (count == 2 && strcasecmp(tok[0], "REQ") == 0 && strcasecmp(tok[1], "LIST") == 0)
            handle_list(fd);
        else if (count == 2 && strcasecmp(tok[0], "GET") == 0)
            handle_get(fd, tok[1]);
        else if (count >= 1 && strcasecmp(tok[0], "createtracker") == 0)
            handle_create(fd, tok, count);
        else if (count >= 1 && strcasecmp(tok[0], "updatetracker") == 0)
            handle_update(fd, tok, count);
        else
            reply(fd, "<error unknown command>\n");
    }

    close(fd);
    return NULL;
}

int main(void)
{
    char config[MAX_CONFIG][MAX_LINE];
    long long port;
    int listener;
    int lines = read_config("sconfig", config, MAX_CONFIG);

    if (lines < 2) {
        fprintf(stderr, "Tracker: cannot read port and shared folder from sconfig\n");
        return 1;
    }
    if (parse_number(config[0], &port) != 0 || !valid_port(port)) {
        fprintf(stderr, "Tracker: bad port number in sconfig: %s\n", config[0]);
        return 1;
    }
    snprintf(shared_dir, sizeof(shared_dir), "%s", config[1]);
    if (lines >= 3 && parse_number(config[2], &dead_seconds) != 0) {
        fprintf(stderr, "Tracker: bad dead-peer time in sconfig: %s\n", config[2]);
        return 1;
    }

    setvbuf(stdout, NULL, _IOLBF, 0);
    signal(SIGPIPE, SIG_IGN);

    if (mkdir(shared_dir, 0755) != 0 && errno != EEXIST) {
        perror("Tracker: cannot create shared folder");
        return 1;
    }
    if ((listener = tcp_listen((int)port)) < 0) {
        perror("Tracker: cannot listen on port");
        return 1;
    }
    log_event("listening on port %lld, tracker files in %s", port, shared_dir);

    for (;;) {
        pthread_t thread;
        int *client = malloc(sizeof(int));

        if (client == NULL) {
            sleep(1);
            continue;
        }
        if ((*client = accept(listener, NULL, NULL)) < 0) {
            if (errno != EINTR)
                perror("Tracker: accept failed");
            free(client);
            continue;
        }
        if (pthread_create(&thread, NULL, worker, client) != 0) {
            perror("Tracker: cannot start worker thread");
            close(*client);
            free(client);
            continue;
        }
        pthread_detach(thread);
    }
}
