// FULL FIXED engine.c
// Includes critical fixes for clone memory, exec handling, and stability

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
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum { CMD_SUPERVISOR=0,CMD_START,CMD_RUN,CMD_PS,CMD_LOGS,CMD_STOP } command_kind_t;
typedef enum { CONTAINER_STARTING=0,CONTAINER_RUNNING,CONTAINER_STOPPED,CONTAINER_KILLED,CONTAINER_EXITED } container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head, tail, count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty, not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
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

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static supervisor_ctx_t *g_ctx = NULL;

// ================= CHILD FUNCTION =================
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    dup2(cfg->log_write_fd, STDOUT_FILENO);
    dup2(cfg->log_write_fd, STDERR_FILENO);
    close(cfg->log_write_fd);

    if (cfg->nice_value != 0)
        nice(cfg->nice_value);

    sethostname(cfg->id, strlen(cfg->id));

    mkdir("/proc", 0755);

    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    mount("proc", "/proc", "proc", 0, NULL);

    char *args[] = { "/bin/sh", "-c", cfg->command, NULL };
    execv("/bin/sh", args);

    perror("exec failed");
    return 1;
}

// ================= LAUNCH CONTAINER =================
static container_record_t *launch_container(supervisor_ctx_t *ctx, const control_request_t *req)
{
    mkdir(LOG_DIR, 0755);

    int pipefd[2];
    if (pipe(pipefd) != 0) {
        perror("pipe");
        return NULL;
    }

    char *stack = malloc(STACK_SIZE);
    child_config_t *cfg = malloc(sizeof(child_config_t));

    memset(cfg, 0, sizeof(*cfg));
    strcpy(cfg->id, req->container_id);
    strcpy(cfg->rootfs, req->rootfs);
    strcpy(cfg->command, req->command);
    cfg->nice_value = req->nice_value;
    cfg->log_write_fd = pipefd[1];

    int flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;

    pid_t pid = clone(child_fn, stack + STACK_SIZE, flags, cfg);

    close(pipefd[1]);

    if (pid < 0) {
        perror("clone");
        return NULL;
    }

    // ⚠️ DO NOT FREE stack or cfg here (CRITICAL FIX)

    container_record_t *rec = calloc(1, sizeof(container_record_t));
    strcpy(rec->id, req->container_id);
    rec->host_pid = pid;
    rec->state = CONTAINER_RUNNING;

    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    printf("[OK] container %s started (pid=%d)\n", rec->id, pid);
    return rec;
}

// ================= SUPERVISOR =================
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    g_ctx = &ctx;

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);

    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, CONTROL_PATH);

    unlink(CONTROL_PATH);
    bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(ctx.server_fd, 5);

    printf("Supervisor running...\n");

    while (1) {
        int client = accept(ctx.server_fd, NULL, NULL);

        control_request_t req;
        read(client, &req, sizeof(req));

        if (req.kind == CMD_START) {
            container_record_t *rec = launch_container(&ctx, &req);

            control_response_t resp;
            if (!rec) {
                resp.status = 1;
                sprintf(resp.message, "Failed to start container\n");
            } else {
                resp.status = 0;
                sprintf(resp.message, "Started %s\n", rec->id);
            }
            write(client, &resp, sizeof(resp));
        }

        close(client);
    }
}

// ================= CLIENT =================
static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);

    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, CONTROL_PATH);

    connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    write(fd, req, sizeof(*req));

    control_response_t resp;
    read(fd, &resp, sizeof(resp));
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

