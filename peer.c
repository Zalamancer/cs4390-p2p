/*
 * peer.c - the peer program: shares files and downloads them from other peers.
 *
 * Threads:
 *   server thread     listens for other peers; one handler per connection
 *   periodic thread   every update interval, refreshes the tracker
 *   one manager per download, which runs several worker threads
 *   main thread       reads commands typed by the user
 *
 * How a download works: the file is divided into segments. Each worker takes
 * the next missing segment, picks the peer with the newest timestamp that
 * holds it and is not already busy serving this download, and fetches the
 * segment in chunks of at most 1024 bytes. After each complete segment the
 * peer saves its progress and tells the tracker. Segments are taken in order
 * but each peer starts at its own position in the file, so that a group of
 * peers holds the whole file even if the original seed leaves early.
 *
 * Peer-to-peer messages (the project description leaves these to us):
 *   request:  <GET filename start_byte end_byte>\n   end_byte is exclusive
 *   reply:    <REP DATA start_byte end_byte>\n followed by the raw bytes
 *   errors:   <GET invalid>\n for a chunk over 1024 bytes or a bad request
 *             <GET fail>\n if this peer does not hold those bytes
 *
 * Files in the peer's folder:
 *   <shared>/name            a complete file
 *   <shared>/name.part       a file still downloading
 *   cache/name.track         saved tracker file for an unfinished download
 *   cache/name.state         which segments of name.part are complete
 */
#include "common.h"
#include "md5.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define CACHE_DIR  "cache"
#define PATH_SIZE  (MAX_LINE + MAX_NAME)

/* Settings that can be changed on the command line. */
#define DEFAULT_SEGMENT_BYTES 65536
#define DEFAULT_THREADS       4
#define DEFAULT_RETRY_SEC     3
#define DEFAULT_BAD_PEER_SEC  30
#define MAX_VERIFY_ATTEMPTS   3

enum { SEG_MISSING, SEG_CLAIMED, SEG_DONE };
enum { CLAIM_OK, CLAIM_WAIT, CLAIM_ALL_DONE };

/* A peer we can download from. */
typedef struct {
    peer_entry entry;
    time_t bad_until;   /* skip this peer until then, after a failure */
    int load;           /* how many of our workers are using it now */
} candidate;

/* One file being downloaded. */
typedef struct download {
    char name[MAX_NAME];
    char md5[33];
    long long size;
    long long segment_bytes;
    int segment_count;
    int first_segment;           /* where this peer starts in the file */
    int fd;                      /* the .part file */
    unsigned char *segments;     /* SEG_* state of each segment */
    candidate *candidates;
    int candidate_count;
    long long reported_bytes;    /* largest range told to the tracker */
    time_t last_refresh;
    int warned_stuck;
    int finished;
    pthread_mutex_t lock;
    struct download *next;
} download;

/* Everything read from the config files and the command line. */
static struct {
    char name[MAX_NAME];
    char tracker_ip[MAX_IP];
    int tracker_port;
    int interval_sec;
    int listen_port;
    char shared_dir[MAX_LINE];
    char self_ip[MAX_IP];
    int auto_download;
    int threads;
    int chunk_delay_ms;
    int retry_sec;
    int bad_peer_sec;
    long long segment_bytes;
} cfg;

static download *downloads = NULL;
static pthread_mutex_t downloads_lock = PTHREAD_MUTEX_INITIALIZER;

static void log_event(const char *format, ...)
{
    char text[MAX_LINE];
    va_list args;

    va_start(args, format);
    vsnprintf(text, sizeof(text), format, args);
    va_end(args);
    printf("%s: %s\n", cfg.name, text);
}

static void shared_path(char *path, const char *name, const char *suffix)
{
    snprintf(path, PATH_SIZE, "%s/%s%s", cfg.shared_dir, name, suffix);
}

static void cache_path(char *path, const char *name, const char *suffix)
{
    snprintf(path, PATH_SIZE, "%s/%s%s", CACHE_DIR, name, suffix);
}

/* ---------- talking to the tracker ---------- */

/* Send one command and read the one-line reply into reply_text. */
static int tracker_exchange(const char *message, char *reply_text, size_t cap)
{
    int fd = tcp_connect(cfg.tracker_ip, cfg.tracker_port);
    int result = -1;

    if (fd < 0)
        return -1;
    if (send_all(fd, message, strlen(message)) == 0 && recv_line(fd, reply_text, cap) > 0)
        result = 0;
    close(fd);
    return result;
}

/* Send an updatetracker message. Returns 0 on success, 1 if the tracker has
 * no such file, and -1 for any other failure. */
static int send_update(const char *name, long long start, long long end)
{
    char message[MAX_LINE], answer[MAX_LINE];
    char *tok[MAX_TOKENS];

    snprintf(message, sizeof(message), "<updatetracker %s %lld %lld %s %d>\n", name, start,
             end, cfg.self_ip, cfg.listen_port);
    if (tracker_exchange(message, answer, sizeof(answer)) != 0) {
        log_event("updatetracker %s failed: cannot reach the tracker", name);
        return -1;
    }
    int count = split_message(answer, tok, MAX_TOKENS);
    const char *status = count == 3 ? tok[2] : "fail";

    log_event("updatetracker %s %lld %lld %s %d -> %s", name, start, end, cfg.self_ip,
              cfg.listen_port, status);
    if (strcmp(status, "succ") == 0)
        return 0;
    return strcmp(status, "ferr") == 0 ? 1 : -1;
}

/* Tell the tracker about a complete file we own, creating its tracker file
 * if nobody has yet. */
static void announce_file(const char *name)
{
    char path[PATH_SIZE], md5[33];
    char message[MAX_LINE], answer[MAX_LINE];
    char *tok[MAX_TOKENS];
    struct stat st;

    shared_path(path, name, "");
    if (stat(path, &st) != 0 || md5_file(path, md5) != 0) {
        log_event("cannot read %s", name);
        return;
    }

    snprintf(message, sizeof(message), "<createtracker %s %lld Shared_by_%s %s %s %d>\n", name,
             (long long)st.st_size, cfg.name, md5, cfg.self_ip, cfg.listen_port);
    if (tracker_exchange(message, answer, sizeof(answer)) != 0) {
        log_event("createtracker %s failed: cannot reach the tracker", name);
        return;
    }
    int count = split_message(answer, tok, MAX_TOKENS);
    const char *status = count == 2 ? tok[1] : "fail";

    log_event("createtracker %s %lld Shared_by_%s %s %s %d -> %s", name,
              (long long)st.st_size, cfg.name, md5, cfg.self_ip, cfg.listen_port, status);
    if (strcmp(status, "ferr") == 0)
        send_update(name, 0, (long long)st.st_size);
}

/* Fetch a tracker file and check it against the MD5 in the reply.
 * Returns the malloc'd contents, or NULL on any failure. */
static char *fetch_tracker(const char *name)
{
    char message[MAX_LINE], line[MAX_LINE], md5[33];
    char *body = NULL, *text = NULL;
    size_t len = 0;
    int fd = tcp_connect(cfg.tracker_ip, cfg.tracker_port);

    if (fd < 0) {
        log_event("GET %s.track failed: cannot reach the tracker", name);
        return NULL;
    }
    log_event("GET %s.track", name);
    snprintf(message, sizeof(message), "<GET %s.track>\n", name);

    if (send_all(fd, message, strlen(message)) != 0 ||
        recv_line(fd, line, sizeof(line)) <= 0 || strcmp(line, "<REP GET BEGIN>\n") != 0) {
        log_event("GET %s.track failed: the tracker has no such file", name);
        close(fd);
        return NULL;
    }

    for (;;) {
        char *tok[MAX_TOKENS];
        int line_len = recv_line(fd, line, sizeof(line));

        if (line_len <= 0)
            break;
        if (strncmp(line, "<REP GET END ", 13) == 0) {
            /* The body arrives as <content>\n, and the MD5 covers content. */
            if (split_message(line, tok, MAX_TOKENS) == 4 && body != NULL && len >= 3 &&
                body[0] == '<' && body[len - 2] == '>') {
                body[len - 2] = '\0';
                md5_buffer(body + 1, len - 3, md5);
                if (strcasecmp(md5, tok[3]) == 0)
                    text = strdup(body + 1);
                else
                    log_event("GET %s.track failed: checksum mismatch", name);
            }
            break;
        }
        char *grown = realloc(body, len + (size_t)line_len + 1);
        if (grown == NULL)
            break;
        body = grown;
        memcpy(body + len, line, (size_t)line_len + 1);
        len += (size_t)line_len;
    }

    free(body);
    close(fd);
    return text;
}

/* Ask for the file list. Each name is passed to on_file when it is not NULL.
 * Returns the number of files, or -1 on failure. */
static int request_list(void (*on_file)(const char *name))
{
    char line[MAX_LINE];
    char *tok[MAX_TOKENS];
    const char *message = "<REQ LIST>\n";
    int fd = tcp_connect(cfg.tracker_ip, cfg.tracker_port);
    int count = -1;

    if (fd < 0) {
        log_event("LIST failed: cannot reach the tracker");
        return -1;
    }
    log_event("REQ LIST");
    if (send_all(fd, message, strlen(message)) == 0 &&
        recv_line(fd, line, sizeof(line)) > 0 &&
        split_message(line, tok, MAX_TOKENS) == 3 && strcmp(tok[1], "LIST") == 0) {
        count = atoi(tok[2]);
        log_event("REP LIST %d", count);
        while (recv_line(fd, line, sizeof(line)) > 0) {
            int words = split_message(line, tok, MAX_TOKENS);
            if (words == 3 && strcmp(tok[1], "LIST") == 0 && strcmp(tok[2], "END") == 0)
                break;
            if (words == 4) {
                log_event("%s. %s, %s bytes, MD5 %s", tok[0], tok[1], tok[2], tok[3]);
                if (on_file != NULL)
                    on_file(tok[1]);
            }
        }
    } else {
        log_event("LIST failed: bad reply from the tracker");
    }
    close(fd);
    return count;
}

/* ---------- download bookkeeping ---------- */

static void segment_range(const download *dl, int seg, long long *start, long long *end)
{
    *start = seg * dl->segment_bytes;
    *end = *start + dl->segment_bytes;
    if (*end > dl->size)
        *end = dl->size;
}

/* Replace the list of download sources, leaving out ourselves and keeping what
 * we already learned about the peers that are still listed. dl must be locked. */
static void set_candidates(download *dl, const tracker_info *info)
{
    candidate *fresh = calloc((size_t)info->peer_count + 1, sizeof(*fresh));
    int count = 0;

    if (fresh == NULL)
        return;
    for (int i = 0; i < info->peer_count; i++) {
        const peer_entry *p = &info->peers[i];

        if (strcmp(p->ip, cfg.self_ip) == 0 && p->port == cfg.listen_port)
            continue;
        fresh[count].entry = *p;
        for (int j = 0; j < dl->candidate_count; j++) {
            const candidate *old = &dl->candidates[j];
            if (strcmp(old->entry.ip, p->ip) == 0 && old->entry.port == p->port) {
                fresh[count].bad_until = old->bad_until;
                fresh[count].load = old->load;
                break;
            }
        }
        count++;
    }
    free(dl->candidates);
    dl->candidates = fresh;
    dl->candidate_count = count;
}

/* Find the longest unbroken run of complete segments, which becomes the range
 * we report: a tracker entry can hold one range per peer. dl must be locked. */
static void best_range(const download *dl, long long *start, long long *end)
{
    int best_first = 0, best_len = 0, run = 0;

    for (int i = 0; i < dl->segment_count; i++) {
        run = dl->segments[i] == SEG_DONE ? run + 1 : 0;
        if (run > best_len) {
            best_len = run;
            best_first = i - run + 1;
        }
    }
    *start = *end = 0;
    if (best_len > 0) {
        long long ignored;
        segment_range(dl, best_first, start, &ignored);
        segment_range(dl, best_first + best_len - 1, &ignored, end);
    }
}

static void save_state(const download *dl)
{
    char path[PATH_SIZE];
    size_t cap = (size_t)dl->segment_count + 64;
    char *text = malloc(cap);

    if (text == NULL)
        return;
    int used = snprintf(text, cap, "%lld %lld\n", dl->size, dl->segment_bytes);
    for (int i = 0; i < dl->segment_count; i++)
        text[used++] = dl->segments[i] == SEG_DONE ? '1' : '0';
    text[used++] = '\n';

    cache_path(path, dl->name, ".state");
    if (write_file_atomic(path, text, (size_t)used) != 0)
        log_event("cannot save progress for %s", dl->name);
    free(text);
}

/* Reload progress left by an earlier run. Anything that does not match this
 * download exactly is ignored, and the file then starts from nothing. */
static void load_state(download *dl)
{
    char path[PATH_SIZE];
    struct stat st;
    long long size, segment_bytes;
    int offset = 0, done = 0;
    char *text;

    shared_path(path, dl->name, ".part");
    if (stat(path, &st) != 0 || (long long)st.st_size != dl->size)
        return;
    cache_path(path, dl->name, ".state");
    if ((text = read_whole_file(path, NULL)) == NULL)
        return;

    if (sscanf(text, "%lld %lld\n%n", &size, &segment_bytes, &offset) == 2 &&
        size == dl->size && segment_bytes == dl->segment_bytes &&
        strlen(text + offset) >= (size_t)dl->segment_count) {
        for (int i = 0; i < dl->segment_count; i++) {
            if (text[offset + i] == '1') {
                dl->segments[i] = SEG_DONE;
                done++;
            }
        }
    }
    free(text);
    if (done > 0)
        log_event("resuming %s: %d of %d segments already here", dl->name, done,
                  dl->segment_count);
}

/* Choose the next missing segment and a peer for it, trying segments in order
 * from this peer's starting position. */
static int claim_segment(download *dl, int *seg_out, char *ip, int *port)
{
    time_t now = time(NULL);
    int done = 0;
    int result = CLAIM_WAIT;

    pthread_mutex_lock(&dl->lock);
    for (int i = 0; i < dl->segment_count; i++)
        done += dl->segments[i] == SEG_DONE;

    if (done == dl->segment_count) {
        result = CLAIM_ALL_DONE;
    } else {
        for (int i = 0; i < dl->segment_count && result == CLAIM_WAIT; i++) {
            int seg = (dl->first_segment + i) % dl->segment_count;
            candidate *best = NULL;
            long long start, end;

            if (dl->segments[seg] != SEG_MISSING)
                continue;
            segment_range(dl, seg, &start, &end);

            /* The newest timestamp wins, but a peer that is less busy with
             * this download comes first so workers use different peers. */
            for (int c = 0; c < dl->candidate_count; c++) {
                candidate *p = &dl->candidates[c];

                if (p->entry.start > start || p->entry.end < end || p->bad_until > now)
                    continue;
                if (best == NULL || p->load < best->load ||
                    (p->load == best->load && p->entry.timestamp > best->entry.timestamp))
                    best = p;
            }
            if (best != NULL) {
                best->load++;
                dl->segments[seg] = SEG_CLAIMED;
                *seg_out = seg;
                snprintf(ip, MAX_IP, "%s", best->entry.ip);
                *port = best->entry.port;
                dl->warned_stuck = 0;
                result = CLAIM_OK;
            }
        }
    }
    pthread_mutex_unlock(&dl->lock);
    return result;
}

/* Hand a claimed peer back; on failure avoid that peer for a while. dl must
 * be locked. */
static void release_peer(download *dl, const char *ip, int port, int failed)
{
    for (int c = 0; c < dl->candidate_count; c++) {
        candidate *p = &dl->candidates[c];

        if (strcmp(p->entry.ip, ip) == 0 && p->entry.port == port) {
            if (p->load > 0)
                p->load--;
            if (failed)
                p->bad_until = time(NULL) + cfg.bad_peer_sec;
            return;
        }
    }
}

static void segment_failed(download *dl, int seg, const char *ip, int port)
{
    pthread_mutex_lock(&dl->lock);
    dl->segments[seg] = SEG_MISSING;
    release_peer(dl, ip, port, 1);
    pthread_mutex_unlock(&dl->lock);
    log_event("lost connection to %s %d, trying another peer", ip, port);
}

/* Record a finished segment, save progress and update the tracker. */
static void segment_finished(download *dl, int seg, const char *ip, int port)
{
    long long start, end;
    int tell_tracker = 0;

    pthread_mutex_lock(&dl->lock);
    dl->segments[seg] = SEG_DONE;
    release_peer(dl, ip, port, 0);
    save_state(dl);
    best_range(dl, &start, &end);
    if (end - start > dl->reported_bytes) {
        dl->reported_bytes = end - start;
        tell_tracker = 1;
    }
    pthread_mutex_unlock(&dl->lock);

    if (tell_tracker)
        send_update(dl->name, start, end);
}

/* Get the latest tracker file and use it for this download. */
static void refresh_download(download *dl)
{
    char *text = fetch_tracker(dl->name);
    tracker_info info;

    if (text == NULL)
        return;
    if (tracker_parse(text, &info) == 0) {
        char path[PATH_SIZE];

        cache_path(path, dl->name, ".track");
        pthread_mutex_lock(&dl->lock);
        if (!dl->finished)
            write_file_atomic(path, text, strlen(text));
        set_candidates(dl, &info);
        pthread_mutex_unlock(&dl->lock);
        tracker_free(&info);
    }
    free(text);
}

/* Called when a worker has nothing to do. If segments still lack a source,
 * look again at the tracker, at most once every retry_sec seconds. */
static void wait_for_sources(download *dl)
{
    time_t now = time(NULL);
    int refresh = 0;
    int warn = 0;

    pthread_mutex_lock(&dl->lock);
    if (now - dl->last_refresh >= cfg.retry_sec) {
        dl->last_refresh = now;
        refresh = 1;
    }
    for (int i = 0; i < dl->segment_count && !warn; i++)
        warn = dl->segments[i] == SEG_MISSING;
    if (warn && dl->warned_stuck)
        warn = 0;
    else if (warn)
        dl->warned_stuck = 1;
    pthread_mutex_unlock(&dl->lock);

    if (warn)
        log_event("waiting for a peer that holds the rest of %s", dl->name);
    if (refresh)
        refresh_download(dl);
    usleep(500 * 1000);
}

/* ---------- download threads ---------- */

/* Fetch bytes start..end of a file over an open connection, chunk by chunk.
 * Returns 0 on success, -1 on any failure. */
static int fetch_range(int fd, download *dl, long long start, long long end)
{
    char message[MAX_LINE], line[MAX_LINE];
    char data[MAX_CHUNK];

    while (start < end) {
        long long stop = start + MAX_CHUNK < end ? start + MAX_CHUNK : end;
        size_t len = (size_t)(stop - start);
        long long got_start, got_end;

        snprintf(message, sizeof(message), "<GET %s %lld %lld>\n", dl->name, start, stop);
        if (send_all(fd, message, strlen(message)) != 0 ||
            recv_line(fd, line, sizeof(line)) <= 0 ||
            sscanf(line, "<REP DATA %lld %lld>", &got_start, &got_end) != 2 ||
            got_start != start || got_end != stop ||
            recv_all(fd, data, len) != 0 ||
            pwrite(dl->fd, data, len, start) != (ssize_t)len)
            return -1;

        start = stop;
        if (cfg.chunk_delay_ms > 0)
            usleep((useconds_t)cfg.chunk_delay_ms * 1000);
    }
    return 0;
}

/* Worker thread: fetch segments until the whole file is here. */
static void *download_worker(void *arg)
{
    download *dl = arg;
    char connected_ip[MAX_IP] = "";
    int connected_port = 0;
    int fd = -1;

    for (;;) {
        char ip[MAX_IP];
        int port, seg;
        long long start, end;
        int status = claim_segment(dl, &seg, ip, &port);

        if (status == CLAIM_ALL_DONE)
            break;
        if (status == CLAIM_WAIT) {
            wait_for_sources(dl);
            continue;
        }

        if (fd < 0 || strcmp(ip, connected_ip) != 0 || port != connected_port) {
            if (fd >= 0)
                close(fd);
            fd = tcp_connect(ip, port);
            if (fd < 0) {
                segment_failed(dl, seg, ip, port);
                continue;
            }
            snprintf(connected_ip, sizeof(connected_ip), "%s", ip);
            connected_port = port;
        }

        segment_range(dl, seg, &start, &end);
        log_event("downloading %lld to %lld bytes of %s from %s %d", start, end, dl->name, ip, port);
        if (fetch_range(fd, dl, start, end) != 0) {
            close(fd);
            fd = -1;
            segment_failed(dl, seg, ip, port);
            continue;
        }
        segment_finished(dl, seg, ip, port);
    }

    if (fd >= 0)
        close(fd);
    return NULL;
}

/* Manager thread: run the workers, check the finished file and publish it. */
static void *download_manager(void *arg)
{
    download *dl = arg;
    char part[PATH_SIZE], final_path[PATH_SIZE], path[PATH_SIZE], md5[33];

    shared_path(part, dl->name, ".part");
    shared_path(final_path, dl->name, "");
    refresh_download(dl);

    for (int attempt = 1; attempt <= MAX_VERIFY_ATTEMPTS; attempt++) {
        pthread_t *workers = calloc((size_t)cfg.threads, sizeof(*workers));
        int started = 0;

        if (workers == NULL) {
            log_event("out of memory downloading %s", dl->name);
            return NULL;
        }
        for (int i = 0; i < cfg.threads; i++)
            if (pthread_create(&workers[started], NULL, download_worker, dl) == 0)
                started++;
        if (started == 0) {
            log_event("cannot start download threads for %s", dl->name);
            free(workers);
            return NULL;
        }
        for (int i = 0; i < started; i++)
            pthread_join(workers[i], NULL);
        free(workers);

        if (md5_file(part, md5) == 0 && strcasecmp(md5, dl->md5) == 0) {
            pthread_mutex_lock(&dl->lock);
            dl->finished = 1;
            pthread_mutex_unlock(&dl->lock);

            if (rename(part, final_path) != 0) {
                log_event("cannot save finished file %s", dl->name);
                return NULL;
            }
            close(dl->fd);
            dl->fd = -1;
            cache_path(path, dl->name, ".track");
            unlink(path);
            cache_path(path, dl->name, ".state");
            unlink(path);
            send_update(dl->name, 0, dl->size);
            log_event("File %s download complete, MD5 verified", dl->name);
            return NULL;
        }

        /* A bad checksum means some peer sent wrong data, so start over. */
        log_event("%s failed its MD5 check (attempt %d of %d)", dl->name, attempt,
                  MAX_VERIFY_ATTEMPTS);
        pthread_mutex_lock(&dl->lock);
        memset(dl->segments, SEG_MISSING, (size_t)dl->segment_count);
        dl->reported_bytes = 0;
        save_state(dl);
        pthread_mutex_unlock(&dl->lock);
    }

    log_event("giving up on %s", dl->name);
    return NULL;
}

/* Set up a download from a tracker file and start its manager thread. */
static void start_download(const tracker_info *info)
{
    download *dl = calloc(1, sizeof(*dl));
    char path[PATH_SIZE];
    pthread_t thread;
    int segments = 0;

    if (dl != NULL) {
        segments = (int)((info->filesize + cfg.segment_bytes - 1) / cfg.segment_bytes);
        dl->segments = calloc((size_t)segments + 1, 1);
    }
    if (dl == NULL || dl->segments == NULL) {
        log_event("out of memory starting %s", info->filename);
        free(dl);
        return;
    }

    snprintf(dl->name, sizeof(dl->name), "%s", info->filename);
    snprintf(dl->md5, sizeof(dl->md5), "%s", info->md5);
    dl->size = info->filesize;
    dl->segment_bytes = cfg.segment_bytes;
    dl->segment_count = segments;
    pthread_mutex_init(&dl->lock, NULL);

    /* Spread peers' starting positions evenly through the file. Consecutive
     * port numbers land far apart thanks to this multiplier (2^32 / golden
     * ratio), so no part of the file is held only by the original seed. */
    uint32_t spread = (uint32_t)cfg.listen_port * 2654435761u;
    dl->first_segment = (int)(((uint64_t)spread * (uint64_t)segments) >> 32);

    load_state(dl);
    shared_path(path, dl->name, ".part");
    dl->fd = open(path, O_RDWR | O_CREAT, 0644);
    if (dl->fd < 0 || ftruncate(dl->fd, dl->size) != 0) {
        log_event("cannot create %s", path);
        if (dl->fd >= 0)
            close(dl->fd);
        free(dl->segments);
        free(dl);
        return;
    }
    set_candidates(dl, info);

    pthread_mutex_lock(&downloads_lock);
    dl->next = downloads;
    downloads = dl;
    pthread_mutex_unlock(&downloads_lock);

    if (pthread_create(&thread, NULL, download_manager, dl) != 0) {
        log_event("cannot start download of %s", dl->name);
        return;
    }
    pthread_detach(thread);
}

static download *find_download(const char *name)
{
    download *dl;

    pthread_mutex_lock(&downloads_lock);
    for (dl = downloads; dl != NULL; dl = dl->next)
        if (strcmp(dl->name, name) == 0)
            break;
    pthread_mutex_unlock(&downloads_lock);
    return dl;
}

/* Get a tracker file, save it to the local cache and start downloading. */
static void request_file(const char *requested)
{
    char name[MAX_NAME], path[PATH_SIZE];
    char *text;
    struct stat st;
    tracker_info info;

    snprintf(name, sizeof(name), "%s", requested);
    size_t len = strlen(name);
    if (len > 6 && strcmp(name + len - 6, ".track") == 0)
        name[len - 6] = '\0';

    if (!valid_filename(name)) {
        log_event("bad file name: %s", requested);
        return;
    }
    shared_path(path, name, "");
    if (stat(path, &st) == 0) {
        log_event("already have %s", name);
        return;
    }
    if (find_download(name) != NULL) {
        log_event("already downloading %s", name);
        return;
    }
    if ((text = fetch_tracker(name)) == NULL)
        return;

    if (tracker_parse(text, &info) != 0 || !valid_filename(info.filename) ||
        strcmp(info.filename, name) != 0) {
        log_event("bad tracker file for %s", name);
        free(text);
        return;
    }
    cache_path(path, name, ".track");
    if (write_file_atomic(path, text, strlen(text)) != 0) {
        log_event("cannot save tracker file for %s", name);
    } else {
        log_event("file %s requested from %d peer(s)", name, info.peer_count);
        start_download(&info);
    }
    tracker_free(&info);
    free(text);
}

/* After a crash or restart, carry on with whatever the cache says is unfinished. */
static void resume_downloads(void)
{
    DIR *dir = opendir(CACHE_DIR);
    struct dirent *item;

    if (dir == NULL)
        return;
    while ((item = readdir(dir)) != NULL) {
        char path[PATH_SIZE];
        char *text;
        size_t len = strlen(item->d_name);
        tracker_info info;

        if (len <= 6 || strcmp(item->d_name + len - 6, ".track") != 0 ||
            len >= MAX_NAME)
            continue;
        snprintf(path, sizeof(path), "%s/%s", CACHE_DIR, item->d_name);
        if ((text = read_whole_file(path, NULL)) == NULL)
            continue;
        if (tracker_parse(text, &info) == 0) {
            if (valid_filename(info.filename) && find_download(info.filename) == NULL) {
                log_event("found unfinished download %s", info.filename);
                start_download(&info);
            }
            tracker_free(&info);
        }
        free(text);
    }
    closedir(dir);
}

/* ---------- serving other peers ---------- */

/* Work out which file on disk can supply a byte range.
 * Returns 0 and fills in path if we hold all of it, -1 otherwise. */
static int locate_range(const char *name, long long start, long long end, char *path)
{
    struct stat st;
    download *dl;
    int ok = 1;

    shared_path(path, name, "");
    if (stat(path, &st) == 0)
        return S_ISREG(st.st_mode) && end <= (long long)st.st_size ? 0 : -1;

    if ((dl = find_download(name)) == NULL)
        return -1;
    shared_path(path, name, ".part");
    pthread_mutex_lock(&dl->lock);
    if (end > dl->size || dl->segment_count == 0) {
        ok = 0;
    } else {
        int first = (int)(start / dl->segment_bytes);
        int last = (int)((end - 1) / dl->segment_bytes);
        for (int i = first; i <= last && ok; i++)
            ok = dl->segments[i] == SEG_DONE;
    }
    pthread_mutex_unlock(&dl->lock);
    return ok ? 0 : -1;
}

/* Answer one chunk request. Returns 0 to keep the connection, -1 to close it. */
static int serve_chunk(int fd, const char *name, long long start, long long end)
{
    static const char failed[] = "<GET fail>\n";
    char path[PATH_SIZE], header[128];
    char reply[sizeof(header) + MAX_CHUNK];
    size_t len = (size_t)(end - start);
    int file;

    if (locate_range(name, start, end, path) != 0)
        return send_all(fd, failed, strlen(failed));
    if ((file = open(path, O_RDONLY)) < 0)
        return send_all(fd, failed, strlen(failed));

    int header_len = snprintf(header, sizeof(header), "<REP DATA %lld %lld>\n", start, end);
    memcpy(reply, header, (size_t)header_len);
    ssize_t got = pread(file, reply + header_len, len, start);
    close(file);
    if (got != (ssize_t)len)
        return send_all(fd, failed, strlen(failed));
    return send_all(fd, reply, (size_t)header_len + len);
}

/* Handler thread: answer chunk requests until the other peer hangs up. */
static void *connection_handler(void *arg)
{
    static const char invalid[] = "<GET invalid>\n";
    int fd = *(int *)arg;
    char line[MAX_LINE];

    free(arg);
    socket_prepare(fd);

    while (recv_line(fd, line, sizeof(line)) > 0) {
        char *tok[MAX_TOKENS];
        long long start, end;
        int count = split_message(line, tok, MAX_TOKENS);
        int result;

        if (count == 4 && strcasecmp(tok[0], "GET") == 0 && valid_filename(tok[1]) &&
            parse_number(tok[2], &start) == 0 && parse_number(tok[3], &end) == 0 &&
            start < end && end - start <= MAX_CHUNK)
            result = serve_chunk(fd, tok[1], start, end);
        else
            result = send_all(fd, invalid, strlen(invalid));
        if (result != 0)
            break;
    }

    close(fd);
    return NULL;
}

/* Server thread: accept connections from other peers. */
static void *server_thread(void *arg)
{
    int listener = *(int *)arg;

    for (;;) {
        pthread_t thread;
        int *client = malloc(sizeof(int));

        if (client == NULL) {
            sleep(1);
            continue;
        }
        if ((*client = accept(listener, NULL, NULL)) < 0) {
            free(client);
            continue;
        }
        if (pthread_create(&thread, NULL, connection_handler, client) != 0) {
            close(*client);
            free(client);
            continue;
        }
        pthread_detach(thread);
    }
    return NULL;
}

/* ---------- periodic work ---------- */

/* Go through the shared folder and tell the tracker what we hold. With
 * first_pass set, complete files are announced with createtracker. */
static void refresh_shared_files(int first_pass)
{
    DIR *dir = opendir(cfg.shared_dir);
    struct dirent *item;

    if (dir == NULL) {
        log_event("cannot open shared folder %s", cfg.shared_dir);
        return;
    }
    while ((item = readdir(dir)) != NULL) {
        char path[PATH_SIZE];
        struct stat st;
        size_t len = strlen(item->d_name);

        if (item->d_name[0] == '.' || (len > 5 && strcmp(item->d_name + len - 5, ".part") == 0))
            continue;
        if (!valid_filename(item->d_name)) {
            if (first_pass)
                log_event("not sharing \"%s\": spaces are not allowed in file names", item->d_name);
            continue;
        }
        shared_path(path, item->d_name, "");
        if (stat(path, &st) != 0 || !S_ISREG(st.st_mode))
            continue;

        if (first_pass || send_update(item->d_name, 0, (long long)st.st_size) == 1)
            announce_file(item->d_name);
    }
    closedir(dir);
}

/* Every interval: update the tracker, and for files that are still incomplete,
 * report progress and fetch the latest tracker file. */
static void *periodic_thread(void *arg)
{
    (void)arg;

    for (;;) {
        sleep((unsigned int)cfg.interval_sec);
        refresh_shared_files(0);

        pthread_mutex_lock(&downloads_lock);
        download *first = downloads;
        pthread_mutex_unlock(&downloads_lock);

        /* New downloads go on the front, so this part of the list is stable. */
        for (download *dl = first; dl != NULL; dl = dl->next) {
            long long start, end;
            int finished;

            pthread_mutex_lock(&dl->lock);
            finished = dl->finished;
            best_range(dl, &start, &end);
            pthread_mutex_unlock(&dl->lock);

            if (finished)
                continue;
            if (end > start)
                send_update(dl->name, start, end);
            refresh_download(dl);
        }
    }
    return NULL;
}

/* ---------- startup and commands ---------- */

static void read_configs(void)
{
    char lines[MAX_CONFIG][MAX_LINE];
    long long number;
    int count = read_config("clientThreadConfig.cfg", lines, MAX_CONFIG);

    if (count < 3) {
        fprintf(stderr, "cannot read clientThreadConfig.cfg (need port, IP, interval)\n");
        exit(1);
    }
    if (parse_number(lines[0], &number) != 0 || !valid_port(number)) {
        fprintf(stderr, "bad tracker port in clientThreadConfig.cfg: %s\n", lines[0]);
        exit(1);
    }
    cfg.tracker_port = (int)number;
    if (!valid_ip(lines[1]) || strlen(lines[1]) >= MAX_IP) {
        fprintf(stderr, "bad tracker IP in clientThreadConfig.cfg: %s\n", lines[1]);
        exit(1);
    }
    snprintf(cfg.tracker_ip, sizeof(cfg.tracker_ip), "%s", lines[1]);
    if (parse_number(lines[count - 1], &number) != 0 || number < 1 || number > INT_MAX) {
        fprintf(stderr, "bad interval in clientThreadConfig.cfg: %s\n", lines[count - 1]);
        exit(1);
    }
    cfg.interval_sec = (int)number;

    count = read_config("serverThreadConfig.cfg", lines, MAX_CONFIG);
    if (count < 2) {
        fprintf(stderr, "cannot read serverThreadConfig.cfg (need port, shared folder)\n");
        exit(1);
    }
    if (parse_number(lines[0], &number) != 0 || !valid_port(number)) {
        fprintf(stderr, "bad port in serverThreadConfig.cfg: %s\n", lines[0]);
        exit(1);
    }
    cfg.listen_port = (int)number;
    snprintf(cfg.shared_dir, sizeof(cfg.shared_dir), "%s", lines[count - 1]);
}

static int positive_option(const char *text, const char *what)
{
    long long number;

    if (parse_number(text, &number) != 0 || number > INT_MAX) {
        fprintf(stderr, "bad value for %s: %s\n", what, text);
        exit(1);
    }
    return (int)number;
}

static void read_options(int argc, char *argv[])
{
    static const struct option options[] = {
        { "name",           required_argument, NULL, 'n' },
        { "auto",           no_argument,       NULL, 'a' },
        { "threads",        required_argument, NULL, 't' },
        { "segment-bytes",  required_argument, NULL, 's' },
        { "chunk-delay-ms", required_argument, NULL, 'd' },
        { "retry-sec",      required_argument, NULL, 'r' },
        { "bad-peer-sec",   required_argument, NULL, 'b' },
        { NULL, 0, NULL, 0 }
    };
    char cwd[MAX_LINE];
    int option;

    cfg.threads = DEFAULT_THREADS;
    cfg.segment_bytes = DEFAULT_SEGMENT_BYTES;
    cfg.retry_sec = DEFAULT_RETRY_SEC;
    cfg.bad_peer_sec = DEFAULT_BAD_PEER_SEC;

    /* A peer is named after its folder unless told otherwise. */
    snprintf(cfg.name, sizeof(cfg.name), "peer");
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        char *base = strrchr(cwd, '/');
        snprintf(cfg.name, sizeof(cfg.name), "%s", base != NULL ? base + 1 : cwd);
    }

    while ((option = getopt_long(argc, argv, "", options, NULL)) != -1) {
        switch (option) {
        case 'n': snprintf(cfg.name, sizeof(cfg.name), "%s", optarg); break;
        case 'a': cfg.auto_download = 1; break;
        case 't': cfg.threads = positive_option(optarg, "--threads"); break;
        case 's': cfg.segment_bytes = positive_option(optarg, "--segment-bytes"); break;
        case 'd': cfg.chunk_delay_ms = positive_option(optarg, "--chunk-delay-ms"); break;
        case 'r': cfg.retry_sec = positive_option(optarg, "--retry-sec"); break;
        case 'b': cfg.bad_peer_sec = positive_option(optarg, "--bad-peer-sec"); break;
        default:
            fprintf(stderr, "usage: ./peer [--name N] [--auto] [--threads N] [--segment-bytes N]\n"
                            "              [--chunk-delay-ms N] [--retry-sec N] [--bad-peer-sec N]\n");
            exit(1);
        }
    }
    if (cfg.threads < 1 || cfg.segment_bytes < 1) {
        fprintf(stderr, "--threads and --segment-bytes must be at least 1\n");
        exit(1);
    }
}

/* Send a typed createtracker or updatetracker command as it is. */
static void send_typed_command(const char *typed)
{
    char message[MAX_LINE], answer[MAX_LINE];

    snprintf(message, sizeof(message), "<%s>\n", typed);
    if (tracker_exchange(message, answer, sizeof(answer)) != 0) {
        log_event("cannot reach the tracker");
        return;
    }
    answer[strcspn(answer, "\n")] = '\0';
    log_event("%s -> %s", typed, answer);
}

static void print_help(void)
{
    printf("Commands:\n"
           "  REQ LIST\n"
           "  GET filename.track\n"
           "  createtracker filename filesize description md5 ip-address port-number\n"
           "  updatetracker filename start_byte end_byte ip-address port-number\n"
           "  quit\n");
}

/* Read and run typed commands. Returns at end of input or on quit. */
static void command_loop(void)
{
    char line[MAX_LINE];

    while (fgets(line, sizeof(line), stdin) != NULL) {
        char copy[MAX_LINE];
        char *tok[MAX_TOKENS];
        int count;

        line[strcspn(line, "\r\n")] = '\0';
        snprintf(copy, sizeof(copy), "%s", line);
        count = split_message(copy, tok, MAX_TOKENS);
        if (count == 0)
            continue;

        if (count == 2 && strcasecmp(tok[0], "REQ") == 0 && strcasecmp(tok[1], "LIST") == 0) {
            request_list(NULL);
        } else if (count == 2 && strcasecmp(tok[0], "GET") == 0) {
            request_file(tok[1]);
        } else if (strcasecmp(tok[0], "createtracker") == 0 ||
                   strcasecmp(tok[0], "updatetracker") == 0) {
            char *start = line;
            size_t len;

            while (*start == ' ' || *start == '<')
                start++;
            len = strlen(start);
            if (len > 0 && start[len - 1] == '>')
                start[len - 1] = '\0';
            send_typed_command(start);
        } else if (strcasecmp(tok[0], "quit") == 0) {
            log_event("terminated");
            exit(0);
        } else {
            print_help();
        }
    }
}

/* For the demo: list the tracker's files and download all of them. */
static void auto_download(void)
{
    while (request_list(request_file) <= 0)
        sleep((unsigned int)cfg.retry_sec);
}

int main(int argc, char *argv[])
{
    pthread_t thread;
    int listener;

    read_options(argc, argv);
    read_configs();

    setvbuf(stdout, NULL, _IOLBF, 0);
    signal(SIGPIPE, SIG_IGN);

    if (local_ip_towards(cfg.tracker_ip, cfg.tracker_port, cfg.self_ip, sizeof(cfg.self_ip)) != 0) {
        fprintf(stderr, "cannot work out this machine's IP address\n");
        return 1;
    }
    if ((mkdir(cfg.shared_dir, 0755) != 0 && errno != EEXIST) ||
        (mkdir(CACHE_DIR, 0755) != 0 && errno != EEXIST)) {
        perror("cannot create shared or cache folder");
        return 1;
    }
    if ((listener = tcp_listen(cfg.listen_port)) < 0) {
        perror("cannot listen on port");
        return 1;
    }
    log_event("started at %s %d, tracker at %s %d", cfg.self_ip, cfg.listen_port,
              cfg.tracker_ip, cfg.tracker_port);

    if (pthread_create(&thread, NULL, server_thread, &listener) != 0) {
        perror("cannot start server thread");
        return 1;
    }
    pthread_detach(thread);

    refresh_shared_files(1);
    resume_downloads();

    if (pthread_create(&thread, NULL, periodic_thread, NULL) != 0) {
        perror("cannot start update thread");
        return 1;
    }
    pthread_detach(thread);

    if (cfg.auto_download)
        auto_download();
    else if (isatty(STDIN_FILENO))
        print_help();
    command_loop();

    /* With no more input, keep sharing until the process is stopped. */
    for (;;)
        pause();
}
