#include "systemcalls.h"
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>

// Runs cmd via system(), returns true if it exited with status 0
bool do_system(const char *cmd)
{
    int status = system(cmd);
    return status != -1 && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

// Runs command[0] (must be an absolute path) with fork/execv/waitpid
bool do_exec(int count, ...)
{
    va_list args;
    va_start(args, count);
    char *command[count + 1];
    for (int i = 0; i < count; i++)
        command[i] = va_arg(args, char *);
    command[count] = NULL;
    va_end(args);

    pid_t pid = fork();
    if (pid == -1)
        return false;

    if (pid == 0)
    {
        execv(command[0], command);
        exit(EXIT_FAILURE);
    }

    int status;
    waitpid(pid, &status, 0);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

// Same as do_exec, but redirects stdout to outputfile
bool do_exec_redirect(const char *outputfile, int count, ...)
{
    va_list args;
    va_start(args, count);
    char *command[count + 1];
    for (int i = 0; i < count; i++)
        command[i] = va_arg(args, char *);
    command[count] = NULL;
    va_end(args);

    int fd = open(outputfile, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
        return false;

    pid_t pid = fork();
    if (pid == -1)
    {
        close(fd);
        return false;
    }

    if (pid == 0)
    {
        dup2(fd, STDOUT_FILENO);
        close(fd);
        execv(command[0], command);
        exit(EXIT_FAILURE);
    }

    close(fd);
    int status;
    waitpid(pid, &status, 0);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}
