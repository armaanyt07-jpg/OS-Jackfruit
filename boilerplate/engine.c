// FULLY FIXED engine.c (warnings + runtime bugs resolved)

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256

typedef enum { CMD_SUPERVISOR=0,CMD_START,CMD_RUN,CMD_PS,CMD_LOGS,CMD_STOP } command_kind_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

// ================= CHILD =================
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    dup2(cfg->log_write_fd, STDOUT_FILENO);
    dup2(cfg->log_write_fd, STDERR_FILENO);
    close(cfg->log_write_fd);

    if (cfg->nice_value != 0) {
        if (nice(cfg->nice_value) == -1 && errno != 0)
            perror("nice");
    }

    if (sethostname(cfg->id, strlen(cfg->id)) != 0)
        perror("sethostname");

    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    mkdir("/proc", 0755);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0)
        perror("mount /proc");

    char *args[] = { "/bin/sh", "-c", cfg->command, NULL };
    execv("/bin/sh", args);

    perror("exec failed");
    return 1;
}

// ================= SUPERVISOR =================
static int run_supervisor(const char *rootfs)
{
    (void)rootfs; // fix unused warning

    int server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, CONTROL_PATH);

    unlink(CONTROL_PATH);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }

    if (listen(server_fd, 5) < 0) {
        perror("listen");
        return 1;
    }

    printf("Supervisor running...\n");

    while (1) {
        int client = accept(server_fd, NULL, NULL);
        if (client < 0) {
            perror("accept");
            continue;
        }

        control_request_t req;
        ssize_t n = read(client, &req, sizeof(req));
        if (n != sizeof(req)) {
            perror("read");
            close(client);
            continue;
        }

        if (req.kind == CMD_START) {
            int pipefd[2];
            if (pipe(pipefd) != 0) {
                perror("pipe");
                close(client);
                continue;
            }

            char *stack = malloc(STACK_SIZE);
            child_config_t *cfg = malloc(sizeof(child_config_t));

            memset(cfg, 0, sizeof(*cfg));
            strcpy(cfg->id, req.container_id);
            strcpy(cfg->rootfs, req.rootfs);
            strcpy(cfg->command, req.command);
            cfg->nice_value = req.nice_value;
            cfg->log_write_fd = pipefd[1];

            int flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;

            pid_t pid = clone(child_fn, stack + STACK_SIZE, flags, cfg);

            close(pipefd[1]);

            control_response_t resp;

            if (pid < 0) {
                perror("clone");
                resp.status = 1;
                snprintf(resp.message, sizeof(resp.message), "Failed to start container\n");
            } else {
                resp.status = 0;
                snprintf(resp.message, sizeof(resp.message), "Started %s (pid=%d)\n", req.container_id, pid);
            }

            if (write(client, &resp, sizeof(resp)) != sizeof(resp))
                perror("write");
        }

        close(client);
    }
}

// ================= CLIENT =================
static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, CONTROL_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        return 1;
    }

    if (write(fd, req, sizeof(*req)) != sizeof(*req))
        perror("write");

    control_response_t resp;
    ssize_t n = read(fd, &resp, sizeof(resp));
    if (n != sizeof(resp))
        perror("read");

    printf("%s", resp.message);

    close(fd);
    return 0;
}

// ================= MAIN =================
int main(int argc, char *argv[])
{
    if (argc < 2) return 1;

    if (strcmp(argv[1], "supervisor") == 0)
        return run_supervisor(argv[2]);

    if (strcmp(argv[1], "start") == 0) {
        control_request_t req = {0};
        req.kind = CMD_START;
        strcpy(req.container_id, argv[2]);
        strcpy(req.rootfs, argv[3]);
        strcpy(req.command, argv[4]);
        return send_control_request(&req);
    }

    return 0;
}

