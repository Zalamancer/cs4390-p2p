#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <openssl/evp.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <dirent.h>
#include <pthread.h>
#include <time.h>

#define DEFAULT_PORT 3490
#define BACKLOG 10
#define PEER_TIMEOUT_SECONDS 900

pthread_mutex_t tracker_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Calculate the MD5 checksum of an entire file. */
int calculate_file_md5(const char *path, char *output)
{
    FILE *file = fopen(path, "rb");
    if (file == NULL)
        return -1;

    EVP_MD_CTX *context = EVP_MD_CTX_new();
    if (context == NULL)
    {
        fclose(file);
        return -1;
    }

    unsigned char buffer[4096];
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_length = 0;
    size_t bytes_read;

    if (EVP_DigestInit_ex(context, EVP_md5(), NULL) != 1)
    {
        EVP_MD_CTX_free(context);
        fclose(file);
        return -1;
    }

    while ((bytes_read = fread(buffer, 1, sizeof(buffer), file)) > 0)
    {
        if (EVP_DigestUpdate(context, buffer, bytes_read) != 1)
        {
            EVP_MD_CTX_free(context);
            fclose(file);
            return -1;
        }
    }

    if (ferror(file))
    {
        EVP_MD_CTX_free(context);
        fclose(file);
        return -1;
    }

    fclose(file);

    if (EVP_DigestFinal_ex(context, digest, &digest_length) != 1)
    {
        EVP_MD_CTX_free(context);
        return -1;
    }

    EVP_MD_CTX_free(context);

    for (unsigned int i = 0; i < digest_length; i++)
        sprintf(output + (i * 2), "%02x", digest[i]);

    output[digest_length * 2] = '\0';

    return 0;
}

int send_all(int socket, const char *buffer, size_t length)
{
    size_t total_sent = 0;

    while (total_sent < length)
    {
        ssize_t sent = send(
            socket,
            buffer + total_sent,
            length - total_sent,
            0
        );

        if (sent <= 0)
            return -1;

        total_sent += sent;
    }

    return 0;
}

int remove_inactive_peers(const char *filename);

/* Read the shared filename, file size, and MD5 from a tracker file. */
int read_tracker_metadata(
    const char *path,
    char *filename,
    size_t filename_size,
    char *filesize,
    size_t filesize_size,
    char *md5,
    size_t md5_size
)
{
    if (filename_size == 0 || filesize_size == 0 || md5_size == 0)
    {
        return -1;
    }

    FILE *file = fopen(path, "r");

    if (file == NULL)
    {
        return -1;
    }

    char line[1024];

    filename[0] = '\0';
    filesize[0] = '\0';
    md5[0] = '\0';

    while (fgets(line, sizeof(line), file) != NULL)
    {
        if (strncmp(line, "Filename:", 9) == 0)
        {
            sscanf(line + 9, "%255s", filename);
            filename[filename_size - 1] = '\0';
        }
        else if (strncmp(line, "Filesize:", 9) == 0)
        {
            sscanf(line + 9, "%63s", filesize);
            filesize[filesize_size - 1] = '\0';
        }
        else if (strncmp(line, "MD5:", 4) == 0)
        {
            sscanf(line + 4, "%63s", md5);
            md5[md5_size - 1] = '\0';
        }
    }

    fclose(file);

    if (filename[0] == '\0' ||
        filesize[0] == '\0' ||
        md5[0] == '\0')
    {
        return -1;
    }

    return 0;
}

/* Create a tracker file for a newly shared file. */
static int create_tracker_file_unlocked(
    const char *filename,
    const char *filesize,
    const char *description,
    const char *md5,
    const char *ip,
    const char *port
)
{
    char path[512];

    /* Reject filenames that could escape tracker_data. */
    if (strchr(filename, '/') != NULL ||
        strchr(filename, '\\') != NULL ||
        filename[0] == '\0')
    {
        return -1;
    }

    snprintf(path, sizeof(path), "tracker_data/%s.track", filename);

    /* Do not overwrite an existing tracker file. */
    FILE *check = fopen(path, "r");
    if (check != NULL)
    {
        fclose(check);
        return 1;
    }

    FILE *file = fopen(path, "w");
    if (file == NULL)
    {
        return -1;
    }

    time_t now = time(NULL);

    fprintf(file, "Filename: %s\n", filename);
    fprintf(file, "Filesize: %s\n", filesize);
    fprintf(file, "Description: %s\n", description);
    fprintf(file, "MD5: %s\n", md5);

    long long size = atoll(filesize);

    fprintf(file, "%s:%s:0:%lld:%ld\n",
            ip, port, size, (long)now);

    if (fclose(file) != 0)
    {
        return -1;
    }

    return 0;
}

int create_tracker_file(
    const char *filename,
    const char *filesize,
    const char *description,
    const char *md5,
    const char *ip,
    const char *port
)
{
    pthread_mutex_lock(&tracker_mutex);

    int result = create_tracker_file_unlocked(
        filename,
        filesize,
        description,
        md5,
        ip,
        port
    );

    pthread_mutex_unlock(&tracker_mutex);

    return result;
}

/* Update an existing peer record or add a new one. */
static int update_tracker_file_unlocked(
    const char *filename,
    const char *start_bytes,
    const char *end_bytes,
    const char *ip,
    const char *port
)
{
    char path[512];
    char temp_path[] = "tracker_data/.updateXXXXXX";
    char *start_end;
    char *end_end;

    long long start = strtoll(start_bytes, &start_end, 10);
    long long end = strtoll(end_bytes, &end_end, 10);

    /* Validate the requested byte range. */
    if (*start_bytes == '\0' || *start_end != '\0' ||
        *end_bytes == '\0' || *end_end != '\0')
    {
        return -1;
    }

    if (start < 0 || end < start)
    {
        return -1;
    }

    /* Reject invalid filenames and paths. */
    if (filename[0] == '\0' ||
        strchr(filename, '/') != NULL ||
        strchr(filename, '\\') != NULL)
    {
        return -1;
    }

    snprintf(path, sizeof(path), "tracker_data/%s.track", filename);

    /* Open the existing tracker file; never create a missing one. */
    FILE *file = fopen(path, "r");
    if (file == NULL)
    {
        return -1;
    }

    char line[512];
    long long filesize = -1;

    /* Read the declared file size. */
    while (fgets(line, sizeof(line), file) != NULL)
    {
        if (sscanf(line, "Filesize: %lld", &filesize) == 1)
        {
            break;
        }
    }

    fclose(file);

    if (filesize <= 0 || start >= end || end > filesize)
    {
        return -1;
    }

    /* Create a unique temporary file for the rewrite. */
    int temp_fd = mkstemp(temp_path);
    if (temp_fd == -1)
    {
        return -1;
    }

    FILE *temp = fdopen(temp_fd, "w");
    if (temp == NULL)
    {
        close(temp_fd);
        unlink(temp_path);
        return -1;
    }

    file = fopen(path, "r");
    if (file == NULL)
    {
        fclose(temp);
        unlink(temp_path);
        return -1;
    }

    int peer_found = 0;
    time_t now = time(NULL);

    /* Preserve metadata and replace the matching peer record. */
    while (fgets(line, sizeof(line), file) != NULL)
    {
        char existing_ip[64];
        char existing_port[32];
        char existing_start[32];
        char existing_end[32];
        long existing_time;

        if (sscanf(line, "%63[^:]:%31[^:]:%31[^:]:%31[^:]:%ld",
                   existing_ip, existing_port, existing_start,
                   existing_end, &existing_time) == 5 &&
            strcmp(existing_ip, ip) == 0 &&
            strcmp(existing_port, port) == 0)
        {
            if (!peer_found)
            {
                fprintf(temp, "%s:%s:%s:%s:%ld\n",
                        ip, port, start_bytes, end_bytes, (long)now);
                peer_found = 1;
            }

            /* Skip duplicate records for this same peer. */
        }
        else
        {
            fputs(line, temp);
        }
    }

    int read_error = ferror(file);
    fclose(file);

    /* Add the peer if no matching record existed. */
    if (!peer_found && !read_error)
    {
        fprintf(temp, "%s:%s:%s:%s:%ld\n",
                ip, port, start_bytes, end_bytes, (long)now);
    }

    int write_error = ferror(temp);

    if (fclose(temp) != 0)
    {
        write_error = 1;
    }

    if (read_error || write_error)
    {
        unlink(temp_path);
        return -1;
    }

    /* Replace the original file with the completed version. */
    if (rename(temp_path, path) != 0)
    {
        unlink(temp_path);
        return -1;
    }

    return 0;
}

int update_tracker_file(
    const char *filename,
    const char *start_bytes,
    const char *end_bytes,
    const char *ip,
    const char *port
)
{
    pthread_mutex_lock(&tracker_mutex);

    int result = update_tracker_file_unlocked(
        filename,
        start_bytes,
        end_bytes,
        ip,
        port
    );

    if (result == 0)
    {
        remove_inactive_peers(filename);
    }

    pthread_mutex_unlock(&tracker_mutex);

    return result;
}

/* Remove peer records that have not refreshed recently. */
int remove_inactive_peers(const char *filename)
{
    char path[512];
    char temp_path[] = "tracker_data/.cleanupXXXXXX";
    char line[512];

    if (filename[0] == '\0' ||
        strchr(filename, '/') != NULL ||
        strchr(filename, '\\') != NULL)
    {
        return -1;
    }

    snprintf(path, sizeof(path),
             "tracker_data/%s.track", filename);

    FILE *file = fopen(path, "r");
    if (file == NULL)
    {
        return -1;
    }

    int temp_fd = mkstemp(temp_path);
    if (temp_fd == -1)
    {
        fclose(file);
        return -1;
    }

    FILE *temp = fdopen(temp_fd, "w");
    if (temp == NULL)
    {
        close(temp_fd);
        unlink(temp_path);
        fclose(file);
        return -1;
    }

    time_t now = time(NULL);
    int write_error = 0;

    while (fgets(line, sizeof(line), file) != NULL)
    {
        char ip[64], port[32], start[32], end[32];
        long timestamp;

        if (sscanf(line, "%63[^:]:%31[^:]:%31[^:]:%31[^:]:%ld",
                   ip, port, start, end, &timestamp) == 5)
        {
            if (now >= timestamp &&
                now - timestamp > PEER_TIMEOUT_SECONDS)
            {
                continue;
            }
        }

        if (fputs(line, temp) == EOF)
        {
            write_error = 1;
            break;
        }
    }

    if (ferror(file))
    {
        write_error = 1;
    }

    fclose(file);

    if (fclose(temp) != 0)
    {
        write_error = 1;
    }

    if (write_error)
    {
        unlink(temp_path);
        return -1;
    }

    if (rename(temp_path, path) != 0)
    {
        unlink(temp_path);
        return -1;
    }

    return 0;
}

// Collect the names of tracker files in the tracker_data directory.
int get_tracker_files(char files[][256], int max_files)
{
    DIR *dir = opendir("tracker_data");

    if (dir == NULL)
    {
        perror("opendir");
        return -1;
    }

    int count = 0;
    struct dirent *entry;

    while ((entry = readdir(dir)) != NULL)
    {
        size_t len = strlen(entry->d_name);

        // Count only filenames ending in ".track".
        if (len > 6 &&
            strcmp(entry->d_name + len - 6, ".track") == 0)
        {
            if (count < max_files)
            {
                snprintf(files[count], 256, "%s", entry->d_name);
            }

            count++;
        }
    }

    closedir(dir);
    return count;
}


/* Handle one connected peer in its own thread. */
void *handle_client(void *arg)
{
    int client_socket = *(int *)arg;
    free(arg);

    char buffer[1024];
    size_t total_received = 0;
    int bytes_received = 0;

        while (total_received < sizeof(buffer) - 1)
    {
        bytes_received = recv(
            client_socket,
            buffer + total_received,
            sizeof(buffer) - 1 - total_received,
            0
        );

        if (bytes_received <= 0)
            break;

        total_received += bytes_received;

        if (memchr(buffer, '\n', total_received) != NULL)
        {
            total_received = (size_t)((char *)memchr(buffer, '\n', total_received) - buffer) + 1;
            break;
        }
    }

    buffer[total_received] = '\0';

    if (total_received > 0 &&
        memchr(buffer, '\n', total_received) != NULL)
    {
        printf("Received: %s\n", buffer);

        if (strcmp(buffer, "REQ LIST\n") == 0)
        {
            char files[100][256];
            pthread_mutex_lock(&tracker_mutex);
            int count = get_tracker_files(files, 100);

            if (count >= 0)
            {
                char response[1024];

                int used = snprintf(
                    response,
                    sizeof(response),
                    "<REP LIST %d>\n",
                    count
                );

                for (int i = 0; i < count && i < 100; i++)
                {
                    if (used >= (int)sizeof(response))
                    {
                        break;
                    }

                        char path[512];
                        char filename[256];
                        char filesize[64];
                        char md5[64];

                        snprintf(path, sizeof(path), "tracker_data/%s", files[i]);

                        if (read_tracker_metadata(
                                path,
                                filename, sizeof(filename),
                                filesize, sizeof(filesize),
                                md5, sizeof(md5)) == 0)
                        {
                            int written = snprintf(
                                response + used,
                                sizeof(response) - (size_t)used,
                                "%d %s %s %s\n",
                                i + 1,
                                filename,
                                filesize,
                                md5
                            );

                            if (written < 0 ||
                                (size_t)written >= sizeof(response) - (size_t)used)
                            {
                                break;
                            }

                            used += written;
                        }
                }

                if (used < (int)sizeof(response))
                {
                    snprintf(
                        response + used,
                        sizeof(response) - used,
                        "<REP LIST END>\n"
                    );
                }

                send_all(
                    client_socket,
                    response,
                    strlen(response)
                );
            }

            pthread_mutex_unlock(&tracker_mutex);
        }
        else if (strncmp(buffer, "createtracker ", 13) == 0)
        {
            char filename[256];
            char filesize[64];
            char description[256];
            char md5[64];
            char ip[64];
            char port[16];

            int parsed = sscanf(
                buffer,
                "createtracker %255s %63s %255s %63s %63s %15s",
                filename,
                filesize,
                description,
                md5,
                ip,
                port
            );

            if (parsed != 6)
            {
                send_all(client_socket, "<createtracker fail>\n",
                        strlen("<createtracker fail>\n"));
            }
            else
            {
                int result = create_tracker_file(
                    filename, filesize, description, md5, ip, port
                );

                if (result == 0)
                {
                    send_all(client_socket, "<createtracker succ>\n",
                            strlen("<createtracker succ>\n"));
                }
                else if (result == 1)
                {
                    send_all(client_socket, "<createtracker ferr>\n",
                            strlen("<createtracker ferr>\n"));
                }
                else
                {
                    send_all(client_socket, "<createtracker fail>\n",
                            strlen("<createtracker fail>\n"));
                }
            }
        }
        else if (strncmp(buffer, "updatetracker ", 14) == 0)
        {
            char filename[256];
            char start_bytes[64];
            char end_bytes[64];
            char ip[64];
            char port[16];

            int parsed = sscanf(
                buffer,
                "updatetracker %255s %63s %63s %63s %15s",
                filename,
                start_bytes,
                end_bytes,
                ip,
                port
            );

            if (parsed != 5)
            {
                send_all(client_socket, "<updatetracker filename fail>\n",
                        strlen("<updatetracker filename fail>\n"));
            }
            else
            {
                int result = update_tracker_file(
                    filename, start_bytes, end_bytes, ip, port
                );

                if (result == 0)
                {
                    char response[320];
                    snprintf(response, sizeof(response),
                            "<updatetracker %s succ>\n", filename);
                    send_all(client_socket, response, strlen(response));
                }
                else
                {
                    char response[320];
                    char path[512];

                    snprintf(path, sizeof(path),
                            "tracker_data/%s.track", filename);

                    FILE *check = fopen(path, "r");

                    if (check == NULL)
                    {
                        snprintf(response, sizeof(response),
                                "<updatetracker %s ferr>\n", filename);
                    }
                    else
                    {
                        fclose(check);

                        snprintf(response, sizeof(response),
                                "<updatetracker %s fail>\n", filename);
                    }

                    send_all(client_socket, response, strlen(response));
                }
            }
        }
        // Handle a request for a specific tracker file.
        else if (strncmp(buffer, "GET ", 4) == 0)
        {
            char filename[256];
            char path[512];

            if (sscanf(buffer, "GET %255s", filename) != 1 ||
                strstr(filename, "/") != NULL ||
                strstr(filename, "\\") != NULL)
            {
                send_all(client_socket, "<GET invalid>\n",
                        strlen("<GET invalid>\n"));
            }
            else
            {
                size_t len = strlen(filename);

                if (len <= 6 ||
                    strcmp(filename + len - 6, ".track") != 0)
                {
                    send_all(client_socket, "<GET invalid>\n",
                            strlen("<GET invalid>\n"));
                }
                else
                {
                    snprintf(
                        path,
                        sizeof(path),
                        "tracker_data/%s",
                        filename
                    );

                    FILE *file = fopen(path, "r");

                    if (file == NULL)
                    {
                        send_all(client_socket, "<GET invalid>\n",
                                strlen("<GET invalid>\n"));
                    }
                    else
                    {
                        char line[1024];
                        char tracker_md5[33];

                        if (calculate_file_md5(path, tracker_md5) != 0)
                        {
                            fclose(file);
                            send_all(client_socket, "<GET invalid>\n",
                                    strlen("<GET invalid>\n"));
                        }
                        else
                        {
                            send_all(
                                client_socket,
                                "<REP GET BEGIN>\n",
                                strlen("<REP GET BEGIN>\n")
                            );

                            while (fgets(line, sizeof(line), file) != NULL)
                            {
                                if (send_all(client_socket, line, strlen(line)) != 0)
                                    break;
                            }

                            fclose(file);

                            char end_marker[128];

                            snprintf(end_marker, sizeof(end_marker),
                                    "<REP GET END %s>\n", tracker_md5);

                            send_all(client_socket, end_marker, strlen(end_marker));
                        }
                    }
                }
            }
        }
    }

    close(client_socket);
    return NULL;
}

int main(void)
{
    int server_socket;
    int client_socket;

    struct sockaddr_in server_addr;
    struct sockaddr_in client_addr;

    socklen_t client_len = sizeof(client_addr);

    /* Create TCP socket */
    server_socket = socket(AF_INET, SOCK_STREAM, 0);

    if (server_socket < 0)
    {
        perror("socket");
        return 1;
    }

    /* Configure server address */
    memset(&server_addr, 0, sizeof(server_addr));

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(DEFAULT_PORT);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    /* Bind socket to the port */
    if (bind(server_socket,
             (struct sockaddr *)&server_addr,
             sizeof(server_addr)) < 0)
    {
        perror("bind");
        close(server_socket);
        return 1;
    }

    /* Start listening */
    if (listen(server_socket, BACKLOG) < 0)
    {
        perror("listen");
        close(server_socket);
        return 1;
    }

    printf("Tracker listening on port %d...\n", DEFAULT_PORT);

    /* Accept connections and assign each peer to a worker thread. */
    while (1)
    {
        client_len = sizeof(client_addr);

        client_socket = accept(
            server_socket,
            (struct sockaddr *)&client_addr,
            &client_len
        );

        if (client_socket < 0)
        {
            perror("accept");
            continue;
        }

        // Allocate memory to pass the socket to the worker.
        int *client_arg = malloc(sizeof(int));

        if (client_arg == NULL)
        {
            perror("malloc");
            close(client_socket);
            continue;
        }

        *client_arg = client_socket;

        pthread_t thread;

        if (pthread_create(
                &thread,
                NULL,
                handle_client,
                client_arg
            ) != 0)
        {
            perror("pthread_create");
            free(client_arg);
            close(client_socket);
            continue;
        }

        // Reclaim thread resources when the worker finishes.
        pthread_detach(thread);
    }
}
