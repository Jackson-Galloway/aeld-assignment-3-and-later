// aesdsocket - stream socket server for assignment 5
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <syslog.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define PORT 9000
#define DATAFILE "/var/tmp/aesdsocketdata"
#define BACKLOG 10
#define RECV_CHUNK 1024

static volatile sig_atomic_t exit_requested = 0;
static int listen_fd = -1;
static int client_fd = -1;

static void signal_handler(int signo)
{
    (void)signo;
    exit_requested = 1;
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

static int send_file_contents(int fd_out)
{
    int fd = open(DATAFILE, O_RDONLY);
    if (fd == -1) {
        return 0;
    }
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
                close(fd);
                return 0;
            }
            sent += s;
        }
    }
    close(fd);
    return (n == 0) ? 1 : 0;
}

static void handle_connection(int fd)
{
    char recvbuf[RECV_CHUNK];
    char *packet = NULL;
    size_t packet_len = 0;
    size_t packet_cap = 0;

    while (!exit_requested) {
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
        for (ssize_t i = 0; i < n; i++) {
            if (recvbuf[i] == '\n') {
                size_t chunk_len = (size_t)(i - start + 1);
                if (!append_to_buffer(&packet, &packet_len, &packet_cap, recvbuf + start, chunk_len)) {
                    syslog(LOG_ERR, "malloc failed, discarding oversized packet");
                    free(packet);
                    packet = NULL;
                    packet_len = 0;
                    packet_cap = 0;
                } else {
                    if (!append_packet_to_file(packet, packet_len)) {
                        syslog(LOG_ERR, "failed writing to %s: %s", DATAFILE, strerror(errno));
                    } else if (!send_file_contents(fd)) {
                        syslog(LOG_ERR, "failed sending %s to client: %s", DATAFILE, strerror(errno));
                    }
                    free(packet);
                    packet = NULL;
                    packet_len = 0;
                    packet_cap = 0;
                }
                start = i + 1;
            }
        }
        if (n > start) {
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

static void cleanup(void)
{
    if (client_fd != -1) {
        close(client_fd);
        client_fd = -1;
    }
    if (listen_fd != -1) {
        close(listen_fd);
        listen_fd = -1;
    }
    remove(DATAFILE);
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

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        syslog(LOG_ERR, "bind failed: %s", strerror(errno));
        close(listen_fd);
        closelog();
        return -1;
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

    while (!exit_requested) {
        struct sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);
        client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &addrlen);
        if (client_fd == -1) {
            if (errno == EINTR) {
                break;
            }
            syslog(LOG_ERR, "accept failed: %s", strerror(errno));
            continue;
        }

        char ip_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, ip_str, sizeof(ip_str));
        syslog(LOG_INFO, "Accepted connection from %s", ip_str);

        handle_connection(client_fd);

        close(client_fd);
        client_fd = -1;
        syslog(LOG_INFO, "Closed connection from %s", ip_str);
    }

    syslog(LOG_INFO, "Caught signal, exiting");
    cleanup();
    closelog();
    return 0;
}
