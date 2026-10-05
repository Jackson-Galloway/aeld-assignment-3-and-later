// aesdsocket - multi-threaded stream socket server for assignment 6
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <syslog.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <pthread.h>
#include <sys/queue.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#ifndef USE_AESD_CHAR_DEVICE
#define USE_AESD_CHAR_DEVICE 1
#endif

#if USE_AESD_CHAR_DEVICE
#include "../aesd-char-driver/aesd_ioctl.h"
#endif

#define PORT 9000
#if USE_AESD_CHAR_DEVICE
#define DATAFILE "/dev/aesdchar"
#else
#define DATAFILE "/var/tmp/aesdsocketdata"
#endif
#define BACKLOG 10
#define RECV_CHUNK 1024
#define TIMESTAMP_PERIOD_SECONDS 10

static volatile sig_atomic_t exit_requested = 0;
static int listen_fd = -1;
static pthread_mutex_t file_mutex = PTHREAD_MUTEX_INITIALIZER;

struct thread_node {
    pthread_t tid;
    int client_fd;
    char ip_str[INET_ADDRSTRLEN];
    volatile int complete;
    SLIST_ENTRY(thread_node) entries;
};

SLIST_HEAD(slisthead, thread_node);
static struct slisthead thread_list = SLIST_HEAD_INITIALIZER(thread_list);

static void signal_handler(int signo)
{
    (void)signo;
    exit_requested = 1;
}

static void block_exit_signals(void)
{
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &set, NULL);
}

static int append_to_buffer(char **buf, size_t *len, size_t *cap, const char *data, size_t datalen)
{
    if (*len + datalen > *cap) {
        size_t newcap = (*cap == 0) ? RECV_CHUNK : *cap;
        while (newcap < *len + datalen) {
            newcap *= 2;
        }
        char *tmp = realloc(*buf, newcap);
        if (tmp == NULL) {
            return 0;
        }
        *buf = tmp;
        *cap = newcap;
    }
    memcpy(*buf + *len, data, datalen);
    *len += datalen;
    return 1;
}

// Caller must hold file_mutex.
static int append_packet_to_file(const char *data, size_t len)
{
    int fd = open(DATAFILE, O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (fd == -1) {
        return 0;
    }
    size_t total = 0;
    while (total < len) {
        ssize_t w = write(fd, data + total, len - total);
        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            close(fd);
            return 0;
        }
        total += (size_t)w;
    }
    close(fd);
    return 1;
}

// Caller must hold file_mutex. Sends from fd's current file position
// through EOF, without touching the position beforehand.
static int send_fd_contents(int fd, int fd_out)
{
    char buf[RECV_CHUNK];
    ssize_t n;
    while ((n = read(fd, buf, sizeof(buf))) > 0) {
        ssize_t sent = 0;
        while (sent < n) {
            ssize_t s = send(fd_out, buf + sent, (size_t)(n - sent), 0);
            if (s < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return 0;
            }
            sent += s;
        }
    }
    return (n == 0) ? 1 : 0;
}

// Caller must hold file_mutex.
static int send_file_contents(int fd_out)
{
    int fd = open(DATAFILE, O_RDONLY);
    if (fd == -1) {
        return 0;
    }
    int ok = send_fd_contents(fd, fd_out);
    close(fd);
    return ok;
}

#if USE_AESD_CHAR_DEVICE
#define SEEKTO_CMD_PREFIX "AESDCHAR_IOCSEEKTO:"

// Caller must hold file_mutex. Checks whether packet is an
// "AESDCHAR_IOCSEEKTO:X,Y" command; if so, issues the ioctl and sends the
// device's contents from the resulting file position back over fd_out,
// using the same fd for both so the seek set by the ioctl is honored.
// Returns 1 if packet was a seekto command that was handled successfully,
// -1 if it was a seekto command that failed, or 0 if packet was not a
// seekto command (the caller should fall back to treating it as plain
// write data).
static int try_handle_seekto_command(const char *packet, size_t packet_len, int fd_out)
{
    char cmdbuf[64];
    struct aesd_seekto seekto;
    unsigned int write_cmd, write_cmd_offset;
    int fd;
    int ok;

    if (packet_len < strlen(SEEKTO_CMD_PREFIX) || packet_len >= sizeof(cmdbuf)) {
        return 0;
    }
    if (strncmp(packet, SEEKTO_CMD_PREFIX, strlen(SEEKTO_CMD_PREFIX)) != 0) {
        return 0;
    }

    memcpy(cmdbuf, packet, packet_len);
    cmdbuf[packet_len] = '\0';
    if (sscanf(cmdbuf + strlen(SEEKTO_CMD_PREFIX), "%u,%u", &write_cmd, &write_cmd_offset) != 2) {
        return 0;
    }
    seekto.write_cmd = write_cmd;
    seekto.write_cmd_offset = write_cmd_offset;

    fd = open(DATAFILE, O_RDWR);
    if (fd == -1) {
        syslog(LOG_ERR, "failed opening %s for seekto ioctl: %s", DATAFILE, strerror(errno));
        return -1;
    }

    if (ioctl(fd, AESDCHAR_IOCSEEKTO, &seekto) != 0) {
        syslog(LOG_ERR, "AESDCHAR_IOCSEEKTO ioctl failed: %s", strerror(errno));
        close(fd);
        return -1;
    }

    ok = send_fd_contents(fd, fd_out);
    close(fd);
    return ok ? 1 : -1;
}
#endif

// Caller must hold file_mutex. Returns 1 on success, 0 on failure.
static int process_received_packet(const char *packet, size_t packet_len, int fd_out)
{
#if USE_AESD_CHAR_DEVICE
    int seekto_result = try_handle_seekto_command(packet, packet_len, fd_out);
    if (seekto_result != 0) {
        return seekto_result == 1;
    }
#endif
    if (!append_packet_to_file(packet, packet_len)) {
        return 0;
    }
    return send_file_contents(fd_out);
}

static void handle_connection(int fd)
{
    char recvbuf[RECV_CHUNK];
    char *packet = NULL;
    size_t packet_len = 0;
    size_t packet_cap = 0;
    int connection_error = 0;

    while (!exit_requested && !connection_error) {
        ssize_t n = recv(fd, recvbuf, sizeof(recvbuf), 0);
        if (n < 0) {
            if (errno == EINTR) {
                break;
            }
            syslog(LOG_ERR, "recv failed: %s", strerror(errno));
            break;
        }
        if (n == 0) {
            break;
        }

        ssize_t start = 0;
        for (ssize_t i = 0; i < n && !connection_error; i++) {
            if (recvbuf[i] == '\n') {
                size_t chunk_len = (size_t)(i - start + 1);
                if (!append_to_buffer(&packet, &packet_len, &packet_cap, recvbuf + start, chunk_len)) {
                    syslog(LOG_ERR, "malloc failed, discarding oversized packet");
                    free(packet);
                    packet = NULL;
                    packet_len = 0;
                    packet_cap = 0;
                } else {
                    pthread_mutex_lock(&file_mutex);
                    int ok = process_received_packet(packet, packet_len, fd);
                    pthread_mutex_unlock(&file_mutex);

                    if (!ok) {
                        syslog(LOG_ERR, "failed processing packet for %s", DATAFILE);
                        connection_error = 1;
                    }
                    free(packet);
                    packet = NULL;
                    packet_len = 0;
                    packet_cap = 0;
                }
                start = i + 1;
            }
        }
        if (!connection_error && n > start) {
            if (!append_to_buffer(&packet, &packet_len, &packet_cap, recvbuf + start, (size_t)(n - start))) {
                syslog(LOG_ERR, "malloc failed, discarding oversized packet");
                free(packet);
                packet = NULL;
                packet_len = 0;
                packet_cap = 0;
            }
        }
    }
    free(packet);
}

static void *connection_thread(void *arg)
{
    struct thread_node *node = (struct thread_node *)arg;
    block_exit_signals();

    handle_connection(node->client_fd);

    syslog(LOG_INFO, "Closed connection from %s", node->ip_str);
    node->complete = 1;
    return NULL;
}

#if !USE_AESD_CHAR_DEVICE
static void *timestamp_thread(void *arg)
{
    (void)arg;
    block_exit_signals();

    while (!exit_requested) {
        for (int i = 0; i < TIMESTAMP_PERIOD_SECONDS && !exit_requested; i++) {
            sleep(1);
        }
        if (exit_requested) {
            break;
        }

        time_t now = time(NULL);
        struct tm tm_now;
        localtime_r(&now, &tm_now);
        char timebuf[64];
        strftime(timebuf, sizeof(timebuf), "%a, %d %b %Y %H:%M:%S %z", &tm_now);
        char line[96];
        int len = snprintf(line, sizeof(line), "timestamp:%s\n", timebuf);
        if (len < 0) {
            continue;
        }

        pthread_mutex_lock(&file_mutex);
        if (!append_packet_to_file(line, (size_t)len)) {
            syslog(LOG_ERR, "failed writing timestamp to %s: %s", DATAFILE, strerror(errno));
        }
        pthread_mutex_unlock(&file_mutex);
    }
    return NULL;
}
#endif

// Joins and frees any thread_node whose thread has finished.
static void reap_completed_threads(void)
{
    struct thread_node *node = SLIST_FIRST(&thread_list);
    while (node != NULL) {
        struct thread_node *next = SLIST_NEXT(node, entries);
        if (node->complete) {
            pthread_join(node->tid, NULL);
            close(node->client_fd);
            SLIST_REMOVE(&thread_list, node, thread_node, entries);
            free(node);
        }
        node = next;
    }
}

// Forces every in-progress connection thread to unblock, then joins all of them.
static void shutdown_all_threads(void)
{
    struct thread_node *node;
    SLIST_FOREACH(node, &thread_list, entries) {
        shutdown(node->client_fd, SHUT_RDWR);
    }
    while (!SLIST_EMPTY(&thread_list)) {
        node = SLIST_FIRST(&thread_list);
        pthread_join(node->tid, NULL);
        close(node->client_fd);
        SLIST_REMOVE_HEAD(&thread_list, entries);
        free(node);
    }
}

int main(int argc, char *argv[])
{
    int daemon_mode = 0;
    int opt;
    while ((opt = getopt(argc, argv, "d")) != -1) {
        if (opt == 'd') {
            daemon_mode = 1;
        }
    }

    openlog("aesdsocket", LOG_PID, LOG_USER);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd == -1) {
        syslog(LOG_ERR, "socket failed: %s", strerror(errno));
        closelog();
        return -1;
    }

    int yes = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(PORT);

    // A just-exited previous instance can briefly hold the port even with
    // SO_REUSEADDR set; retry for a few seconds before giving up.
    int bind_attempts = 20;
    while (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        if (errno != EADDRINUSE || --bind_attempts <= 0) {
            syslog(LOG_ERR, "bind failed: %s", strerror(errno));
            close(listen_fd);
            closelog();
            return -1;
        }
        usleep(250000);
    }

    if (daemon_mode) {
        pid_t pid = fork();
        if (pid < 0) {
            syslog(LOG_ERR, "fork failed: %s", strerror(errno));
            close(listen_fd);
            closelog();
            return -1;
        }
        if (pid > 0) {
            closelog();
            return 0;
        }
        // Threads (timer, per-connection) are created below, only reached in
        // the child from here on -- pthreads do not survive fork().
        if (setsid() == -1) {
            syslog(LOG_ERR, "setsid failed: %s", strerror(errno));
            close(listen_fd);
            closelog();
            return -1;
        }
        int devnull = open("/dev/null", O_RDWR);
        if (devnull != -1) {
            dup2(devnull, STDIN_FILENO);
            dup2(devnull, STDOUT_FILENO);
            dup2(devnull, STDERR_FILENO);
            if (devnull > STDERR_FILENO) {
                close(devnull);
            }
        }
    }

    if (listen(listen_fd, BACKLOG) == -1) {
        syslog(LOG_ERR, "listen failed: %s", strerror(errno));
        close(listen_fd);
        closelog();
        return -1;
    }

#if !USE_AESD_CHAR_DEVICE
    pthread_t timer_tid;
    if (pthread_create(&timer_tid, NULL, timestamp_thread, NULL) != 0) {
        syslog(LOG_ERR, "failed to create timestamp thread: %s", strerror(errno));
        close(listen_fd);
        closelog();
        return -1;
    }
#endif

    while (!exit_requested) {
        reap_completed_threads();

        struct sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);
        int fd = accept(listen_fd, (struct sockaddr *)&client_addr, &addrlen);
        if (fd == -1) {
            if (errno == EINTR) {
                break;
            }
            syslog(LOG_ERR, "accept failed: %s", strerror(errno));
            continue;
        }

        struct thread_node *node = malloc(sizeof(*node));
        if (node == NULL) {
            syslog(LOG_ERR, "malloc failed for thread node, dropping connection");
            close(fd);
            continue;
        }
        node->client_fd = fd;
        node->complete = 0;
        inet_ntop(AF_INET, &client_addr.sin_addr, node->ip_str, sizeof(node->ip_str));
        syslog(LOG_INFO, "Accepted connection from %s", node->ip_str);

        if (pthread_create(&node->tid, NULL, connection_thread, node) != 0) {
            syslog(LOG_ERR, "pthread_create failed: %s", strerror(errno));
            close(fd);
            free(node);
            continue;
        }
        SLIST_INSERT_HEAD(&thread_list, node, entries);
    }

    syslog(LOG_INFO, "Caught signal, exiting");

    shutdown_all_threads();
#if !USE_AESD_CHAR_DEVICE
    pthread_join(timer_tid, NULL);
#endif

    close(listen_fd);
    listen_fd = -1;
#if !USE_AESD_CHAR_DEVICE
    remove(DATAFILE);
#endif
    closelog();
    return 0;
}
