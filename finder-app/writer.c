#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <syslog.h>

int main(int argc, char *argv[])
{
    openlog("writer", LOG_PID, LOG_USER);

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <writefile> <writestr>\n", argv[0]);
        syslog(LOG_ERR, "Usage: %s <writefile> <writestr>", argv[0]);
        closelog();
        exit(1);
    }

    char *writefile = argv[1];
    char *writestr = argv[2];

    int fd = open(writefile, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        syslog(LOG_ERR, "could not open %s: %s", writefile, strerror(errno));
        closelog();
        exit(1);
    }

    syslog(LOG_DEBUG, "Writing %s to %s", writestr, writefile);

    size_t len = strlen(writestr);
    size_t written = 0;
    while (written < len) {
        ssize_t n = write(fd, writestr + written, len - written);
        if (n == -1) {
            syslog(LOG_ERR, "could not write to %s: %s", writefile, strerror(errno));
            close(fd);
            closelog();
            exit(1);
        }
        written += n;
    }

    close(fd);
    closelog();
    return 0;
}
