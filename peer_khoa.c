/*
 * peer_khoa.c - peer program for the CS 4390 P2P file sharing project.
 * Author: Khoa Ho
 *
 * Threads:
 *   main thread      reads typed commands (REQ LIST, GET, createtracker, updatetracker, quit)
 *   server thread    accepts other peers; each connection gets its own handler thread
 *   periodic thread  sends updatetracker every interval and refreshes unfinished downloads
 *   download threads one manager per file, which runs several worker threads
 *
 * Peer-to-peer messages (same format as the team's other peer, so they work together):
 *   request: <GET filename start_byte end_byte>\n      end_byte is exclusive, at most 1024 bytes
 *   reply:   <REP DATA start_byte end_byte>\n then the raw bytes
 *   errors:  <GET invalid>\n (bad request or chunk over 1024)  <GET fail>\n (bytes not held)
 *
 * Files:
 *   <shared>/name         complete file
 *   <shared>/name.part    file still downloading
 *   cache/name.track      saved tracker file for an unfinished download
 *   cache/name.state      one '0' or '1' per segment, so a restart resumes the download
 */
#include "common.h"
#include "md5.h"

#include <dirent.h>
#include <fcntl.h>
#include <getopt.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define CACHE_DIR   "cache"
#define PATH_LEN    (MAX_LINE + MAX_NAME + 16)
#define MAX_BAD     64
#define MAX_TRIES   3

/* Settings from the two config files and the command line. */
static struct {
    char name[64];
    char tracker_ip[MAX_IP];
    int tracker_port;
    int interval;
    int listen_port;
    char shared[MAX_LINE];
    char my_ip[MAX_IP];
    int auto_download;
    int threads;
    long long segment;
    int delay_ms;
    int retry_sec;
    int bad_sec;
} cfg;

/* A peer we failed to reach, skipped until 'until'. */
typedef struct {
    char ip[MAX_IP];
    int port;
    time_t until;
} bad_peer;

/* One file being downloaded. */
typedef struct download {
    char name[MAX_NAME];
    long long size;
    char md5[33];
    int segments;
    int remaining;
    unsigned char *done;
    unsigned char *busy;
    int fd;
    tracker_info tracker;
    bad_peer bad[MAX_BAD];
    int bad_count;
    pthread_mutex_t lock;
    struct download *next;
} download;

static download *downloads = NULL;
static pthread_mutex_t downloads_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;

/* ---------- logging and paths ---------- */

static void log_event(const char *format, ...)
{
    char text[MAX_LINE];
    va_list args;

    va_start(args, format);
    vsnprintf(text, sizeof(text), format, args);
    va_end(args);
    pthread_mutex_lock(&log_lock);
    printf("%s: %s\n", cfg.name, text);
    fflush(stdout);
    pthread_mutex_unlock(&log_lock);
}

static void shared_path(char *out, const char *name, const char *suffix)
{
    snprintf(out, PATH_LEN, "%s/%s%s", cfg.shared, name, suffix);
}

static void cache_path(char *out, const char *name, const char *suffix)
{
    snprintf(out, PATH_LEN, "%s/%s%s", CACHE_DIR, name, suffix);
}

static int file_size(const char *path, long long *size)
{
    struct stat st;

    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode))
        return -1;
    *size = (long long)st.st_size;
    return 0;
}

/* ---------- talking to the tracker ---------- */

/* Send one message and read one reply line. Returns 0 on success. */
static int tracker_send(const char *message, char *reply, size_t cap)
{
    int fd = tcp_connect(cfg.tracker_ip, cfg.tracker_port);
    int result = -1;

    if (fd < 0)
        return -1;
    socket_prepare(fd);
    if (send_all(fd, message, strlen(message)) == 0 && recv_line(fd, reply, cap) > 0)
        result = 0;
    close(fd);
    return result;
}

/* createtracker for a complete file in the shared folder. */
static void create_tracker(const char *name)
{
    char path[PATH_LEN], md5[33], message[MAX_LINE], reply[MAX_LINE];
    long long size;

    shared_path(path, name, "");
    if (file_size(path, &size) != 0 || md5_file(path, md5) != 0)
        return;
    snprintf(message, sizeof(message), "<createtracker %s %lld Shared_by_%s %s %s %d>\n",
             name, size, cfg.name, md5, cfg.my_ip, cfg.listen_port);
    if (tracker_send(message, reply, sizeof(reply)) != 0) {
        log_event("createtracker %s failed: cannot reach the tracker", name);
        return;
    }
    reply[strcspn(reply, "\n")] = '\0';
    log_event("createtracker %s %lld Shared_by_%s %s %s %d -> %s",
              name, size, cfg.name, md5, cfg.my_ip, cfg.listen_port, reply);
}

/* updatetracker for bytes start..end. Returns 1 if the tracker has no such file. */
static int update_tracker(const char *name, long long start, long long end)
{
    char message[MAX_LINE], reply[MAX_LINE];

    snprintf(message, sizeof(message), "<updatetracker %s %lld %lld %s %d>\n",
             name, start, end, cfg.my_ip, cfg.listen_port);
    if (tracker_send(message, reply, sizeof(reply)) != 0) {
        log_event("updatetracker %s failed: cannot reach the tracker", name);
        return 0;
    }
    reply[strcspn(reply, "\n")] = '\0';
    log_event("updatetracker %s %lld %lld -> %s", name, start, end, reply);
    return strstr(reply, "ferr") != NULL;
}

/* REQ LIST. Prints the files and, if names is not NULL, stores up to max of them. */
static int request_list(char names[][MAX_NAME], int max)
{
    char line[MAX_LINE];
    char *tok[MAX_TOKENS];
    const char *message = "<REQ LIST>\n";
    int fd = tcp_connect(cfg.tracker_ip, cfg.tracker_port);
    int count, stored = 0;

    if (fd < 0) {
        log_event("REQ LIST failed: cannot reach the tracker");
        return -1;
    }
    socket_prepare(fd);
    log_event("REQ LIST");
    if (send_all(fd, message, strlen(message)) != 0 || recv_line(fd, line, sizeof(line)) <= 0 ||
        split_message(line, tok, MAX_TOKENS) != 3 || strcmp(tok[1], "LIST") != 0) {
        log_event("REQ LIST failed: bad reply");
        close(fd);
        return -1;
    }
    count = atoi(tok[2]);
    log_event("tracker has %d file(s)", count);
    for (int i = 0; i < count; i++) {
        if (recv_line(fd, line, sizeof(line)) <= 0)
            break;
        if (split_message(line, tok, MAX_TOKENS) >= 4) {
            log_event("  %s %s bytes MD5 %s", tok[1], tok[2], tok[3]);
            if (names != NULL && stored < max)
                snprintf(names[stored++], MAX_NAME, "%s", tok[1]);
        }
    }
    recv_line(fd, line, sizeof(line));
    close(fd);
    return stored;
}

/* GET name.track. Checks the MD5, saves a copy in the cache, fills info. Returns 0 on success. */
static int get_tracker(const char *name, tracker_info *info, int quiet)
{
    char message[MAX_LINE], line[MAX_LINE], md5[33], path[PATH_LEN];
    char *body = NULL;
    size_t len = 0;
    int ok = -1;
    int fd = tcp_connect(cfg.tracker_ip, cfg.tracker_port);

    if (fd < 0) {
        log_event("GET %s.track failed: cannot reach the tracker", name);
        return -1;
    }
    socket_prepare(fd);
    if (!quiet)
        log_event("GET %s.track", name);
    snprintf(message, sizeof(message), "<GET %s.track>\n", name);
    if (send_all(fd, message, strlen(message)) != 0 || recv_line(fd, line, sizeof(line)) <= 0 ||
        strcmp(line, "<REP GET BEGIN>\n") != 0) {
        log_event("GET %s.track failed: no such tracker file", name);
        close(fd);
        return -1;
    }

    /* The body comes as <content>\n, then <REP GET END md5>. The MD5 covers content. */
    for (;;) {
        char *tok[MAX_TOKENS];
        int n = recv_line(fd, line, sizeof(line));

        if (n <= 0)
            break;
        if (strncmp(line, "<REP GET END ", 13) == 0) {
            if (split_message(line, tok, MAX_TOKENS) == 4 && body != NULL && len >= 3 &&
                body[0] == '<' && body[len - 2] == '>') {
                body[len - 2] = '\0';
                md5_buffer(body + 1, len - 3, md5);
                if (strcasecmp(md5, tok[3]) != 0) {
                    log_event("GET %s.track failed: MD5 mismatch", name);
                } else if (tracker_parse(body + 1, info) == 0) {
                    cache_path(path, name, ".track");
                    write_file_atomic(path, body + 1, len - 3);
                    ok = 0;
                }
            }
            break;
        }
        char *grown = realloc(body, len + (size_t)n + 1);
        if (grown == NULL)
            break;
        body = grown;
        memcpy(body + len, line, (size_t)n + 1);
        len += (size_t)n;
    }
    free(body);
    close(fd);
    return ok;
}

/* ---------- download bookkeeping ---------- */

static download *find_download(const char *name)
{
    download *d;

    pthread_mutex_lock(&downloads_lock);
    for (d = downloads; d != NULL; d = d->next)
        if (strcmp(d->name, name) == 0)
            break;
    pthread_mutex_unlock(&downloads_lock);
    return d;
}

static void remove_download(download *target)
{
    download **link;

    pthread_mutex_lock(&downloads_lock);
    for (link = &downloads; *link != NULL; link = &(*link)->next)
        if (*link == target) {
            *link = target->next;
            break;
        }
    pthread_mutex_unlock(&downloads_lock);
}

/* Write the segment map to cache/name.state. Caller holds d->lock. */
static void save_state(download *d)
{
    char path[PATH_LEN];
    char *text = malloc((size_t)d->segments + 1);

    if (text == NULL)
        return;
    for (int i = 0; i < d->segments; i++)
        text[i] = d->done[i] ? '1' : '0';
    text[d->segments] = '\n';
    cache_path(path, d->name, ".state");
    write_file_atomic(path, text, (size_t)d->segments + 1);
    free(text);
}

/* Longest run of finished segments, as a byte range. Caller holds d->lock. */
static int done_range(download *d, long long *start, long long *end)
{
    int best = 0, best_at = 0, run = 0;

    for (int i = 0; i < d->segments; i++) {
        run = d->done[i] ? run + 1 : 0;
        if (run > best) {
            best = run;
            best_at = i - run + 1;
        }
    }
    if (best == 0)
        return -1;
    *start = (long long)best_at * cfg.segment;
    *end = (long long)(best_at + best) * cfg.segment;
    if (*end > d->size)
        *end = d->size;
    return 0;
}

static int is_bad(download *d, const char *ip, int port)
{
    time_t now = time(NULL);

    for (int i = 0; i < d->bad_count; i++)
        if (d->bad[i].port == port && strcmp(d->bad[i].ip, ip) == 0 && d->bad[i].until > now)
            return 1;
    return 0;
}

static void mark_bad(download *d, const char *ip, int port)
{
    int slot = d->bad_count < MAX_BAD ? d->bad_count++ : 0;

    snprintf(d->bad[slot].ip, MAX_IP, "%s", ip);
    d->bad[slot].port = port;
    d->bad[slot].until = time(NULL) + cfg.bad_sec;
}

/* Pick the newest-timestamp peer holding bytes start..end. Caller holds d->lock. */
static int pick_peer(download *d, long long start, long long end, char *ip, int *port)
{
    long long newest = -1;

    for (int i = 0; i < d->tracker.peer_count; i++) {
        peer_entry *p = &d->tracker.peers[i];

        if (p->start > start || p->end < end)
            continue;
        if (p->port == cfg.listen_port && strcmp(p->ip, cfg.my_ip) == 0)
            continue;
        if (is_bad(d, p->ip, p->port))
            continue;
        if (p->timestamp > newest) {
            newest = p->timestamp;
            snprintf(ip, MAX_IP, "%s", p->ip);
            *port = p->port;
        }
    }
    return newest >= 0 ? 0 : -1;
}

/* Get a fresh tracker file so new peers can be used. */
static void refresh_tracker(download *d)
{
    tracker_info fresh;

    if (get_tracker(d->name, &fresh, 1) != 0)
        return;
    pthread_mutex_lock(&d->lock);
    tracker_free(&d->tracker);
    d->tracker = fresh;
    pthread_mutex_unlock(&d->lock);
}

/* ---------- downloading ---------- */

/* Fetch bytes start..end over an open connection in chunks of at most 1024 bytes. */
static int fetch_range(int sock, download *d, long long start, long long end)
{
    char message[MAX_LINE], line[MAX_LINE], data[MAX_CHUNK];

    while (start < end) {
        long long stop = start + MAX_CHUNK < end ? start + MAX_CHUNK : end;
        size_t len = (size_t)(stop - start);
        long long got_start, got_end;

        snprintf(message, sizeof(message), "<GET %s %lld %lld>\n", d->name, start, stop);
        if (send_all(sock, message, strlen(message)) != 0 ||
            recv_line(sock, line, sizeof(line)) <= 0 ||
            sscanf(line, "<REP DATA %lld %lld>", &got_start, &got_end) != 2 ||
            got_start != start || got_end != stop ||
            recv_all(sock, data, len) != 0 ||
            pwrite(d->fd, data, len, start) != (ssize_t)len)
            return -1;
        start = stop;
        if (cfg.delay_ms > 0)
            usleep((useconds_t)cfg.delay_ms * 1000);
    }
    return 0;
}

/* Worker thread: take the next missing segment in order and fetch it. */
static void *download_worker(void *arg)
{
    download *d = arg;

    for (;;) {
        char ip[MAX_IP];
        int port = 0, seg = -1;
        long long start, end;

        pthread_mutex_lock(&d->lock);
        if (d->remaining == 0) {
            pthread_mutex_unlock(&d->lock);
            return NULL;
        }
        for (int i = 0; i < d->segments; i++)
            if (!d->done[i] && !d->busy[i]) {
                seg = i;
                break;
            }
        if (seg < 0) {
            pthread_mutex_unlock(&d->lock);
            sleep(1);
            continue;
        }
        start = (long long)seg * cfg.segment;
        end = start + cfg.segment < d->size ? start + cfg.segment : d->size;
        if (pick_peer(d, start, end, ip, &port) != 0) {
            pthread_mutex_unlock(&d->lock);
            log_event("no peer has bytes %lld to %lld of %s yet, retrying", start, end, d->name);
            sleep((unsigned)cfg.retry_sec);
            refresh_tracker(d);
            continue;
        }
        d->busy[seg] = 1;
        pthread_mutex_unlock(&d->lock);

        log_event("downloading %lld to %lld bytes of %s from %s %d", start, end, d->name, ip, port);
        int sock = tcp_connect(ip, port);
        int ok = -1;
        if (sock >= 0) {
            socket_prepare(sock);
            ok = fetch_range(sock, d, start, end);
            close(sock);
        }

        pthread_mutex_lock(&d->lock);
        d->busy[seg] = 0;
        if (ok != 0) {
            mark_bad(d, ip, port);
            pthread_mutex_unlock(&d->lock);
            log_event("lost connection to %s %d, trying another peer", ip, port);
            continue;
        }
        long long range_start = 0, range_end = 0;
        d->done[seg] = 1;
        d->remaining--;
        save_state(d);
        int have_range = done_range(d, &range_start, &range_end) == 0;
        pthread_mutex_unlock(&d->lock);

        /* Tell the tracker only after a whole segment is saved. */
        if (have_range)
            update_tracker(d->name, range_start, range_end);
    }
}

/* Manager thread: run the workers, then check the MD5 and finish the file. */
static void *download_manager(void *arg)
{
    download *d = arg;
    char part[PATH_LEN], final[PATH_LEN], path[PATH_LEN], md5[33];
    pthread_t *workers = calloc((size_t)cfg.threads, sizeof(pthread_t));

    if (workers == NULL)
        return NULL;
    for (int attempt = 1; attempt <= MAX_TRIES; attempt++) {
        for (int i = 0; i < cfg.threads; i++)
            pthread_create(&workers[i], NULL, download_worker, d);
        for (int i = 0; i < cfg.threads; i++)
            pthread_join(workers[i], NULL);

        shared_path(part, d->name, ".part");
        if (md5_file(part, md5) == 0 && strcasecmp(md5, d->md5) == 0) {
            shared_path(final, d->name, "");
            close(d->fd);
            rename(part, final);
            cache_path(path, d->name, ".track");
            unlink(path);
            cache_path(path, d->name, ".state");
            unlink(path);
            remove_download(d);
            log_event("File %s download complete, MD5 verified", d->name);
            update_tracker(d->name, 0, d->size);
            free(workers);
            return NULL;
        }

        /* MD5 mismatch: start over with every segment missing. */
        log_event("File %s failed the MD5 check (attempt %d of %d)", d->name, attempt, MAX_TRIES);
        pthread_mutex_lock(&d->lock);
        memset(d->done, 0, (size_t)d->segments);
        d->remaining = d->segments;
        save_state(d);
        pthread_mutex_unlock(&d->lock);
    }
    log_event("File %s download gave up after %d attempts", d->name, MAX_TRIES);
    free(workers);
    return NULL;
}

/* Start (or resume) downloading a file. resume = 1 uses the tracker file saved in the cache. */
static void start_download(const char *name, int resume)
{
    char path[PATH_LEN];
    long long size;
    tracker_info info;
    download *d;
    pthread_t thread;

    shared_path(path, name, "");
    if (file_size(path, &size) == 0) {
        log_event("%s is already complete here", name);
        return;
    }
    if (find_download(name) != NULL) {
        log_event("%s is already downloading", name);
        return;
    }

    if (resume) {
        char *text;
        size_t len;
        cache_path(path, name, ".track");
        if ((text = read_whole_file(path, &len)) == NULL)
            return;
        int bad = tracker_parse(text, &info);
        free(text);
        if (bad != 0)
            return;
    } else if (get_tracker(name, &info, 0) != 0) {
        return;
    }

    if ((d = calloc(1, sizeof(*d))) == NULL)
        return;
    snprintf(d->name, sizeof(d->name), "%s", name);
    snprintf(d->md5, sizeof(d->md5), "%s", info.md5);
    d->size = info.filesize;
    d->tracker = info;
    d->segments = (int)((d->size + cfg.segment - 1) / cfg.segment);
    if (d->segments == 0)
        d->segments = 1;
    d->done = calloc((size_t)d->segments, 1);
    d->busy = calloc((size_t)d->segments, 1);
    pthread_mutex_init(&d->lock, NULL);

    shared_path(path, name, ".part");
    d->fd = open(path, O_RDWR | O_CREAT, 0644);
    if (d->done == NULL || d->busy == NULL || d->fd < 0 || ftruncate(d->fd, d->size) != 0) {
        log_event("cannot create %s", path);
        return;
    }

    /* Keep segments already finished before a crash. */
    char *state;
    size_t state_len;
    cache_path(path, name, ".state");
    if ((state = read_whole_file(path, &state_len)) != NULL) {
        for (int i = 0; i < d->segments && (size_t)i < state_len; i++)
            d->done[i] = state[i] == '1';
        free(state);
    }
    d->remaining = 0;
    for (int i = 0; i < d->segments; i++)
        d->remaining += !d->done[i];
    if (resume)
        log_event("resuming %s: %d of %d segments already here", name,
                  d->segments - d->remaining, d->segments);
    else
        log_event("downloading %s (%lld bytes, %d segments, %d threads)", name, d->size,
                  d->segments, cfg.threads);

    pthread_mutex_lock(&downloads_lock);
    d->next = downloads;
    downloads = d;
    pthread_mutex_unlock(&downloads_lock);

    pthread_create(&thread, NULL, download_manager, d);
    pthread_detach(thread);
}

/* ---------- serving other peers ---------- */

/* Find which local file holds bytes start..end of name. Returns 0 and fills path if found. */
static int locate_bytes(const char *name, long long start, long long end, char *path)
{
    long long size;
    download *d;
    int ok = 1;

    shared_path(path, name, "");
    if (file_size(path, &size) == 0)
        return end <= size ? 0 : -1;
    if ((d = find_download(name)) == NULL)
        return -1;
    pthread_mutex_lock(&d->lock);
    if (end > d->size) {
        ok = 0;
    } else {
        for (long long i = start / cfg.segment; i <= (end - 1) / cfg.segment && ok; i++)
            ok = d->done[i];
    }
    pthread_mutex_unlock(&d->lock);
    shared_path(path, name, ".part");
    return ok ? 0 : -1;
}

/* Handler thread: answer chunk requests until the other peer hangs up. */
static void *peer_handler(void *arg)
{
    static const char invalid[] = "<GET invalid>\n", failed[] = "<GET fail>\n";
    int sock = *(int *)arg;
    char line[MAX_LINE], path[PATH_LEN], buffer[128 + MAX_CHUNK];

    free(arg);
    socket_prepare(sock);
    while (recv_line(sock, line, sizeof(line)) > 0) {
        char *tok[MAX_TOKENS];
        long long start, end;
        int count = split_message(line, tok, MAX_TOKENS), result;

        if (count != 4 || strcasecmp(tok[0], "GET") != 0 || !valid_filename(tok[1]) ||
            parse_number(tok[2], &start) != 0 || parse_number(tok[3], &end) != 0 ||
            start >= end || end - start > MAX_CHUNK) {
            result = send_all(sock, invalid, strlen(invalid));
        } else if (locate_bytes(tok[1], start, end, path) != 0) {
            result = send_all(sock, failed, strlen(failed));
        } else {
            int file = open(path, O_RDONLY);
            int head = snprintf(buffer, 128, "<REP DATA %lld %lld>\n", start, end);
            ssize_t got = file >= 0 ? pread(file, buffer + head, (size_t)(end - start), start) : -1;

            if (file >= 0)
                close(file);
            if (got == end - start)
                result = send_all(sock, buffer, (size_t)head + (size_t)got);
            else
                result = send_all(sock, failed, strlen(failed));
        }
        if (result != 0)
            break;
    }
    close(sock);
    return NULL;
}

/* Server thread: accept peers and hand each one to a handler thread. */
static void *server_thread(void *arg)
{
    int listener = *(int *)arg;

    for (;;) {
        int *sock = malloc(sizeof(int));
        pthread_t thread;

        if (sock == NULL)
            continue;
        if ((*sock = accept(listener, NULL, NULL)) < 0) {
            free(sock);
            continue;
        }
        if (pthread_create(&thread, NULL, peer_handler, sock) != 0) {
            close(*sock);
            free(sock);
            continue;
        }
        pthread_detach(thread);
    }
    return NULL;
}

/* ---------- periodic tracker refresh ---------- */

/* Calls fn for every complete file in the shared folder. */
static void for_each_shared(void (*fn)(const char *name))
{
    DIR *dir = opendir(cfg.shared);
    struct dirent *entry;

    if (dir == NULL)
        return;
    while ((entry = readdir(dir)) != NULL) {
        size_t len = strlen(entry->d_name);
        char path[PATH_LEN];
        long long size;

        if (entry->d_name[0] == '.' || (len > 5 && strcmp(entry->d_name + len - 5, ".part") == 0))
            continue;
        shared_path(path, entry->d_name, "");
        if (valid_filename(entry->d_name) && file_size(path, &size) == 0)
            fn(entry->d_name);
    }
    closedir(dir);
}

static void refresh_shared_file(const char *name)
{
    char path[PATH_LEN];
    long long size;

    shared_path(path, name, "");
    if (file_size(path, &size) == 0 && update_tracker(name, 0, size))
        create_tracker(name);
}

static void *periodic_thread(void *arg)
{
    (void)arg;
    for (;;) {
        sleep((unsigned)cfg.interval);
        for_each_shared(refresh_shared_file);

        /* For unfinished downloads: report what we have and get a fresh tracker file. */
        pthread_mutex_lock(&downloads_lock);
        int count = 0;
        char names[64][MAX_NAME];
        for (download *d = downloads; d != NULL && count < 64; d = d->next)
            snprintf(names[count++], MAX_NAME, "%s", d->name);
        pthread_mutex_unlock(&downloads_lock);

        for (int i = 0; i < count; i++) {
            download *d = find_download(names[i]);
            long long start, end;

            if (d == NULL)
                continue;
            pthread_mutex_lock(&d->lock);
            int have = done_range(d, &start, &end) == 0;
            pthread_mutex_unlock(&d->lock);
            if (have)
                update_tracker(d->name, start, end);
            refresh_tracker(d);
        }
    }
    return NULL;
}

/* ---------- startup ---------- */

static void on_signal(int sig)
{
    char text[128];
    int len = snprintf(text, sizeof(text), "%s terminated\n", cfg.name);

    (void)sig;
    if (write(STDOUT_FILENO, text, (size_t)len) < 0)
        _exit(1);
    _exit(0);
}

static void load_config(void)
{
    char client[MAX_CONFIG][MAX_LINE], server[MAX_CONFIG][MAX_LINE];
    int c = read_config("clientThreadConfig.cfg", client, MAX_CONFIG);
    int s = read_config("serverThreadConfig.cfg", server, MAX_CONFIG);

    if (c < 3 || s < 2) {
        fprintf(stderr, "need clientThreadConfig.cfg (port, ip, interval) and "
                        "serverThreadConfig.cfg (port, shared folder)\n");
        exit(1);
    }
    cfg.tracker_port = atoi(client[0]);
    snprintf(cfg.tracker_ip, sizeof(cfg.tracker_ip), "%s", client[1]);
    cfg.interval = atoi(client[c - 1]);
    cfg.listen_port = atoi(server[0]);
    snprintf(cfg.shared, sizeof(cfg.shared), "%s", server[s - 1]);
    if (!valid_port(cfg.tracker_port) || !valid_port(cfg.listen_port) || cfg.interval <= 0) {
        fprintf(stderr, "bad value in a config file\n");
        exit(1);
    }
}

static void parse_options(int argc, char **argv)
{
    static const struct option options[] = {
        {"name", required_argument, NULL, 'n'},
        {"auto", no_argument, NULL, 'a'},
        {"threads", required_argument, NULL, 't'},
        {"segment-bytes", required_argument, NULL, 's'},
        {"chunk-delay-ms", required_argument, NULL, 'd'},
        {"retry-sec", required_argument, NULL, 'r'},
        {"bad-peer-sec", required_argument, NULL, 'b'},
        {NULL, 0, NULL, 0}};
    int option;

    while ((option = getopt_long(argc, argv, "", options, NULL)) != -1) {
        switch (option) {
        case 'n': snprintf(cfg.name, sizeof(cfg.name), "%s", optarg); break;
        case 'a': cfg.auto_download = 1; break;
        case 't': cfg.threads = atoi(optarg); break;
        case 's': cfg.segment = atoll(optarg); break;
        case 'd': cfg.delay_ms = atoi(optarg); break;
        case 'r': cfg.retry_sec = atoi(optarg); break;
        case 'b': cfg.bad_sec = atoi(optarg); break;
        default: exit(1);
        }
    }
    if (cfg.threads <= 0) cfg.threads = 4;
    if (cfg.segment <= 0) cfg.segment = 65536;
    if (cfg.retry_sec <= 0) cfg.retry_sec = 3;
    if (cfg.bad_sec <= 0) cfg.bad_sec = 30;
}

/* Resume every download left in the cache by an earlier run. */
static void resume_downloads(void)
{
    DIR *dir = opendir(CACHE_DIR);
    struct dirent *entry;

    if (dir == NULL)
        return;
    while ((entry = readdir(dir)) != NULL) {
        size_t len = strlen(entry->d_name);
        char name[MAX_NAME];

        if (len <= 6 || len >= MAX_NAME || strcmp(entry->d_name + len - 6, ".track") != 0)
            continue;
        snprintf(name, sizeof(name), "%.*s", (int)(len - 6), entry->d_name);
        start_download(name, 1);
    }
    closedir(dir);
}

/* Send a typed createtracker/updatetracker line as-is and print the reply. */
static void send_raw(char *typed)
{
    char message[MAX_LINE], reply[MAX_LINE];

    typed[strcspn(typed, "\r\n")] = '\0';
    snprintf(message, sizeof(message), "<%s>\n", typed);
    if (tracker_send(message, reply, sizeof(reply)) != 0) {
        log_event("cannot reach the tracker");
        return;
    }
    reply[strcspn(reply, "\n")] = '\0';
    log_event("%s -> %s", typed, reply);
}

int main(int argc, char **argv)
{
    char cwd[MAX_LINE], line[MAX_LINE];
    pthread_t server, periodic;
    int listener;

    signal(SIGPIPE, SIG_IGN);
    signal(SIGTERM, on_signal);
    signal(SIGINT, on_signal);

    parse_options(argc, argv);
    if (cfg.name[0] == '\0') {
        const char *base = getcwd(cwd, sizeof(cwd)) ? strrchr(cwd, '/') : NULL;
        snprintf(cfg.name, sizeof(cfg.name), "%s", base ? base + 1 : "peer");
    }
    load_config();
    mkdir(cfg.shared, 0755);
    mkdir(CACHE_DIR, 0755);
    if (local_ip_towards(cfg.tracker_ip, cfg.tracker_port, cfg.my_ip, sizeof(cfg.my_ip)) != 0)
        snprintf(cfg.my_ip, sizeof(cfg.my_ip), "127.0.0.1");

    if ((listener = tcp_listen(cfg.listen_port)) < 0) {
        fprintf(stderr, "cannot listen on port %d\n", cfg.listen_port);
        return 1;
    }
    log_event("listening on %s %d, tracker at %s %d", cfg.my_ip, cfg.listen_port,
              cfg.tracker_ip, cfg.tracker_port);
    pthread_create(&server, NULL, server_thread, &listener);
    pthread_detach(server);

    for_each_shared(create_tracker);
    resume_downloads();
    pthread_create(&periodic, NULL, periodic_thread, NULL);
    pthread_detach(periodic);

    if (cfg.auto_download) {
        char names[64][MAX_NAME];
        int count = request_list(names, 64);
        for (int i = 0; i < count; i++)
            start_download(names[i], 0);
    }

    /* Typed commands. With no terminal (stdin closed), keep serving until killed. */
    while (fgets(line, sizeof(line), stdin) != NULL) {
        char copy[MAX_LINE];
        char *tok[MAX_TOKENS];
        int count;

        snprintf(copy, sizeof(copy), "%s", line);
        count = split_message(copy, tok, MAX_TOKENS);
        if (count == 0)
            continue;
        if (strcasecmp(tok[0], "quit") == 0 || strcasecmp(tok[0], "exit") == 0) {
            log_event("terminated");
            return 0;
        } else if ((count == 2 && strcasecmp(tok[0], "REQ") == 0 && strcasecmp(tok[1], "LIST") == 0) ||
                   (count == 1 && strcasecmp(tok[0], "LIST") == 0)) {
            request_list(NULL, 0);
        } else if (count == 2 && strcasecmp(tok[0], "GET") == 0) {
            size_t len = strlen(tok[1]);
            if (len > 6 && strcmp(tok[1] + len - 6, ".track") == 0)
                tok[1][len - 6] = '\0';
            start_download(tok[1], 0);
        } else if (strcasecmp(tok[0], "createtracker") == 0 || strcasecmp(tok[0], "updatetracker") == 0) {
            send_raw(line);
        } else {
            log_event("commands: REQ LIST | GET name.track | createtracker ... | updatetracker ... | quit");
        }
    }
    for (;;)
        pause();
}
