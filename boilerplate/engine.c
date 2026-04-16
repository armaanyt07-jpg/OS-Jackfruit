/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

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

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

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
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
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

/* Global supervisor context pointer for signal handlers */
static supervisor_ctx_t *g_ctx = NULL;

/* Suppress warn_unused_result on write() calls - we intentionally ignore them */
#pragma GCC diagnostic ignored "-Wunused-result"
#pragma GCC diagnostic ignored "-Wformat-truncation"

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag, const char *value, unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req, int argc, char *argv[], int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr, "Invalid value for --nice (expected -20..19): %s\n", argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ─── Bounded Buffer ─────────────────────────────────────────────────────── */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;
    memset(buffer, 0, sizeof(*buffer));
    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0) return rc;
    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) { pthread_mutex_destroy(&buffer->mutex); return rc; }
    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * Producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * Consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0) {
        /* shutting down and empty */
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ─── Logging Thread ─────────────────────────────────────────────────────── */

/*
 * Logging consumer thread.
 *
 * Responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (1) {
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0)
            break; /* shutdown + empty */

        /* Find the log file for this container */
        char log_path[PATH_MAX];
        log_path[0] = '\0';

        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *rec = ctx->containers;
        while (rec) {
            if (strcmp(rec->id, item.container_id) == 0) {
                snprintf(log_path, sizeof(log_path), "%s", rec->log_path);
                break;
            }
            rec = rec->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (log_path[0] == '\0')
            continue;

        int fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0)
            continue;
        (void)write(fd, item.data, item.length);
        close(fd);
    }

    /* Drain any remaining items after shutdown signal */
    while (1) {
        pthread_mutex_lock(&ctx->log_buffer.mutex);
        if (ctx->log_buffer.count == 0) {
            pthread_mutex_unlock(&ctx->log_buffer.mutex);
            break;
        }
        log_item_t drain_item = ctx->log_buffer.items[ctx->log_buffer.head];
        ctx->log_buffer.head = (ctx->log_buffer.head + 1) % LOG_BUFFER_CAPACITY;
        ctx->log_buffer.count--;
        pthread_mutex_unlock(&ctx->log_buffer.mutex);

        char log_path[PATH_MAX];
        log_path[0] = '\0';
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *r = ctx->containers;
        while (r) {
            if (strcmp(r->id, drain_item.container_id) == 0) {
                snprintf(log_path, sizeof(log_path), "%s", r->log_path);
                break;
            }
            r = r->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (log_path[0] != '\0') {
            int fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (fd >= 0) {
                (void)write(fd, drain_item.data, drain_item.length);
                close(fd);
            }
        }
    }

    fprintf(stderr, "[logger] thread exiting cleanly\n");
    return NULL;
}

/* ─── Producer Thread (reads pipe from one container) ───────────────────── */

typedef struct {
    int pipe_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *log_buffer;
} producer_arg_t;

void *producer_thread(void *arg)
{
    producer_arg_t *parg = (producer_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    while (1) {
        memset(&item, 0, sizeof(item));
        snprintf(item.container_id, CONTAINER_ID_LEN, "%s", parg->container_id);

        n = read(parg->pipe_fd, item.data, LOG_CHUNK_SIZE - 1);
        if (n <= 0)
            break;
        item.length = (size_t)n;
        bounded_buffer_push(parg->log_buffer, &item);
    }

    close(parg->pipe_fd);
    free(parg);
    return NULL;
}

/* ─── Container Child Entry ──────────────────────────────────────────────── */

/*
 * clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Redirect stdout and stderr to the pipe write end */
    dup2(cfg->log_write_fd, STDOUT_FILENO);
    dup2(cfg->log_write_fd, STDERR_FILENO);
    close(cfg->log_write_fd);

    /* Set nice value */
    if (cfg->nice_value != 0)
        (void)nice(cfg->nice_value);

    /* Set hostname to container id */
    (void)sethostname(cfg->id, strlen(cfg->id));

    /* Mount /proc */
    char proc_path[PATH_MAX];
    snprintf(proc_path, sizeof(proc_path) - 1, "%s/proc", cfg->rootfs);
    mkdir(proc_path, 0755);

    /* chroot into container rootfs */
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    /* Mount /proc inside chroot */
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount /proc");
        /* non-fatal, continue */
    }

    /* Execute command */
    char *args[] = { cfg->command, NULL };
    execv(cfg->command, args);

    /* Try with /bin/sh if direct exec fails */
    char *sh_args[] = { "/bin/sh", "-c", cfg->command, NULL };
    execv("/bin/sh", sh_args);

    perror("execv");
    return 1;
}

/* ─── Monitor ioctl helpers ──────────────────────────────────────────────── */

int register_with_monitor(int monitor_fd, const char *container_id,
                          pid_t host_pid, unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    snprintf(req.container_id, sizeof(req.container_id), "%s", container_id);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    snprintf(req.container_id, sizeof(req.container_id), "%s", container_id);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

/* ─── Container Launch ───────────────────────────────────────────────────── */

static container_record_t *launch_container(supervisor_ctx_t *ctx,
                                            const control_request_t *req)
{
    /* Check for duplicate id */
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *existing = ctx->containers;
    while (existing) {
        if (strcmp(existing->id, req->container_id) == 0 &&
            existing->state == CONTAINER_RUNNING) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            fprintf(stderr, "[supervisor] container '%s' already running\n", req->container_id);
            return NULL;
        }
        existing = existing->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Create log directory and file */
    mkdir(LOG_DIR, 0755);
    char log_path[PATH_MAX];
    snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, req->container_id);

    /* Create pipe for container output */
    int pipefd[2];
    if (pipe(pipefd) != 0) {
        perror("pipe");
        return NULL;
    }

    /* Allocate stack for clone */
    char *stack = malloc(STACK_SIZE);
    if (!stack) {
        perror("malloc stack");
        close(pipefd[0]);
        close(pipefd[1]);
        return NULL;
    }

    /* Set up child config */
    child_config_t *cfg = malloc(sizeof(child_config_t));
    if (!cfg) {
        free(stack);
        close(pipefd[0]);
        close(pipefd[1]);
        return NULL;
    }
    memset(cfg, 0, sizeof(*cfg));
    snprintf(cfg->id, CONTAINER_ID_LEN, "%s", req->container_id);
    snprintf(cfg->rootfs, PATH_MAX, "%s", req->rootfs);
    snprintf(cfg->command, CHILD_COMMAND_LEN, "%s", req->command);
    cfg->nice_value = req->nice_value;
    cfg->log_write_fd = pipefd[1];

    /* Clone with namespaces */
    int flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
    pid_t pid = clone(child_fn, stack + STACK_SIZE, flags, cfg);

    close(pipefd[1]); /* parent doesn't write */

    if (pid < 0) {
        perror("clone");
        free(stack);
        free(cfg);
        close(pipefd[0]);
        return NULL;
    }

    free(stack);
    free(cfg);

    /* Register with kernel monitor */
    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd, req->container_id, pid,
                                  req->soft_limit_bytes, req->hard_limit_bytes) != 0)
            fprintf(stderr, "[supervisor] warning: failed to register '%s' with monitor\n",
                    req->container_id);
    }

    /* Start producer thread for this container's pipe */
    producer_arg_t *parg = malloc(sizeof(producer_arg_t));
    if (parg) {
        parg->pipe_fd = pipefd[0];
        snprintf(parg->container_id, CONTAINER_ID_LEN, "%s", req->container_id);
        parg->log_buffer = &ctx->log_buffer;
        pthread_t prod_tid;
        pthread_create(&prod_tid, NULL, producer_thread, parg);
        pthread_detach(prod_tid);
    } else {
        close(pipefd[0]);
    }

    /* Create and register metadata */
    container_record_t *rec = calloc(1, sizeof(container_record_t));
    if (!rec) {
        kill(pid, SIGKILL);
        return NULL;
    }
    snprintf(rec->id, CONTAINER_ID_LEN, "%s", req->container_id);
    rec->host_pid = pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    snprintf(rec->log_path, PATH_MAX, "%s", log_path);

    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    fprintf(stderr, "[supervisor] started container '%s' pid=%d\n", rec->id, (int)pid);
    return rec;
}

/* ─── SIGCHLD handler ────────────────────────────────────────────────────── */

static void sigchld_handler(int sig)
{
    (void)sig;
    if (!g_ctx) return;

    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *rec = g_ctx->containers;
        while (rec) {
            if (rec->host_pid == pid) {
                if (WIFEXITED(status)) {
                    rec->exit_code = WEXITSTATUS(status);
                    rec->state = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    rec->exit_signal = WTERMSIG(status);
                    if (rec->stop_requested)
                        rec->state = CONTAINER_STOPPED;
                    else if (rec->exit_signal == SIGKILL)
                        rec->state = CONTAINER_KILLED; /* hard_limit_killed */
                    else
                        rec->state = CONTAINER_KILLED;
                }
                fprintf(stderr, "[supervisor] container '%s' exited state=%s\n",
                        rec->id, state_to_string(rec->state));

                /* Unregister from kernel monitor */
                if (g_ctx->monitor_fd >= 0)
                    unregister_from_monitor(g_ctx->monitor_fd, rec->id, pid);
                break;
            }
            rec = rec->next;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }
}

static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx)
        g_ctx->should_stop = 1;
}

/* ─── Request Handlers ───────────────────────────────────────────────────── */

static void handle_ps(supervisor_ctx_t *ctx, int client_fd)
{
    char buf[4096];
    int len = 0;

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;

    len += snprintf(buf + len, sizeof(buf) - len,
                    "%-16s %-8s %-12s %-10s %-12s %-12s %s\n",
                    "ID", "PID", "STATE", "SOFT(MiB)", "HARD(MiB)", "EXIT", "LOG");

    while (rec && len < (int)sizeof(buf) - 200) {
        char started[32];
        struct tm *tm_info = localtime(&rec->started_at);
        strftime(started, sizeof(started), "%H:%M:%S", tm_info);

        len += snprintf(buf + len, sizeof(buf) - len,
                        "%-16s %-8d %-12s %-10lu %-12lu %-12d %s\n",
                        rec->id,
                        (int)rec->host_pid,
                        state_to_string(rec->state),
                        rec->soft_limit_bytes >> 20,
                        rec->hard_limit_bytes >> 20,
                        rec->exit_code,
                        rec->log_path);
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    control_response_t resp;
    resp.status = 0;
    buf[sizeof(resp.message) - 1] = '\0';
    snprintf(resp.message, sizeof(resp.message), "%s", buf);
    (void)write(client_fd, &resp, sizeof(resp));
}

static void handle_logs(supervisor_ctx_t *ctx, int client_fd, const char *id)
{
    char log_path[PATH_MAX] = {0};

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;
    while (rec) {
        if (strcmp(rec->id, id) == 0) {
            snprintf(log_path, PATH_MAX, "%s", rec->log_path);
            break;
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    control_response_t resp;
    if (log_path[0] == '\0') {
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message), "Container '%s' not found\n", id);
        (void)write(client_fd, &resp, sizeof(resp));
        return;
    }

    /* Send the log file contents back */
    resp.status = 0;
    snprintf(resp.message, sizeof(resp.message), "Log file: %s\n", log_path);
    (void)write(client_fd, &resp, sizeof(resp));

    int fd = open(log_path, O_RDONLY);
    if (fd < 0) {
        return;
    }
    char chunk[1024];
    ssize_t n;
    while ((n = read(fd, chunk, sizeof(chunk))) > 0)
        (void)write(client_fd, chunk, n);
    close(fd);
}

static void handle_stop(supervisor_ctx_t *ctx, int client_fd, const char *id)
{
    control_response_t resp;

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;
    while (rec) {
        if (strcmp(rec->id, id) == 0) break;
        rec = rec->next;
    }

    if (!rec || rec->state != CONTAINER_RUNNING) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message),
                 "Container '%s' not found or not running\n", id);
        (void)write(client_fd, &resp, sizeof(resp));
        return;
    }

    rec->stop_requested = 1;
    pid_t pid = rec->host_pid;
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Try graceful SIGTERM first, then SIGKILL */
    kill(pid, SIGTERM);
    usleep(500000);
    kill(pid, SIGKILL);

    resp.status = 0;
    snprintf(resp.message, sizeof(resp.message), "Stopped container '%s'\n", id);
    (void)write(client_fd, &resp, sizeof(resp));
}

static void handle_start(supervisor_ctx_t *ctx, int client_fd,
                         const control_request_t *req, int block)
{
    control_response_t resp;
    container_record_t *rec = launch_container(ctx, req);

    if (!rec) {
        resp.status = 1;
        snprintf(resp.message, sizeof(resp.message),
                 "Failed to start container '%s'\n", req->container_id);
        (void)write(client_fd, &resp, sizeof(resp));
        return;
    }

    resp.status = 0;
    snprintf(resp.message, sizeof(resp.message),
             "Started container '%s' pid=%d\n", rec->id, (int)rec->host_pid);
    (void)write(client_fd, &resp, sizeof(resp));

    if (block) {
        /* For 'run': wait for the container to exit */
        waitpid(rec->host_pid, NULL, 0);
        snprintf(resp.message, sizeof(resp.message),
                 "Container '%s' finished, state=%s\n",
                 rec->id, state_to_string(rec->state));
        (void)write(client_fd, &resp, sizeof(resp));
    }
}

/* ─── Supervisor Event Loop ──────────────────────────────────────────────── */

/*
 * The long-running supervisor process.
 *
 * Responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    g_ctx = &ctx;

    fprintf(stderr, "[supervisor] starting, base-rootfs: %s\n", rootfs);

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) { errno = rc; perror("bounded_buffer_init"); return 1; }

    /* 1) open /dev/container_monitor */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] warning: cannot open /dev/container_monitor: %s\n",
                strerror(errno));

    /* 2) create the control socket / FIFO / shared-memory channel */
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", CONTROL_PATH);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(ctx.server_fd, 10) < 0) {
        perror("listen"); return 1;
    }
    fprintf(stderr, "[supervisor] control socket at %s\n", CONTROL_PATH);

    /* 3) install SIGCHLD / SIGINT / SIGTERM handling */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = sigterm_handler;
    sa.sa_flags = SA_RESTART;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* 4) spawn the logger thread */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) { errno = rc; perror("pthread_create logger"); return 1; }

    fprintf(stderr, "[supervisor] ready, waiting for commands...\n");

    /* 5) enter the supervisor event loop */
    while (!ctx.should_stop) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(ctx.server_fd, &readfds);

        struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };
        int sel = select(ctx.server_fd + 1, &readfds, NULL, NULL, &tv);
        if (sel < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }
        if (sel == 0) continue;

        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }

        control_request_t req;
        ssize_t n = read(client_fd, &req, sizeof(req));
        if (n != sizeof(req)) {
            close(client_fd);
            continue;
        }

        switch (req.kind) {
        case CMD_START:
            handle_start(&ctx, client_fd, &req, 0);
            break;
        case CMD_RUN:
            handle_start(&ctx, client_fd, &req, 1);
            break;
        case CMD_PS:
            handle_ps(&ctx, client_fd);
            break;
        case CMD_LOGS:
            handle_logs(&ctx, client_fd, req.container_id);
            break;
        case CMD_STOP:
            handle_stop(&ctx, client_fd, req.container_id);
            break;
        default:
            break;
        }

        close(client_fd);
    }

    /* Shutdown sequence */
    fprintf(stderr, "[supervisor] shutting down...\n");

    /* Stop all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *rec = ctx.containers;
    while (rec) {
        if (rec->state == CONTAINER_RUNNING) {
            rec->stop_requested = 1;
            kill(rec->host_pid, SIGTERM);
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Wait for children */
    while (waitpid(-1, NULL, WNOHANG) > 0);
    sleep(1);
    while (waitpid(-1, NULL, WNOHANG) > 0);

    /* Stop logger */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    /* Cleanup */
    close(ctx.server_fd);
    unlink(CONTROL_PATH);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);

    /* Free metadata */
    pthread_mutex_lock(&ctx.metadata_lock);
    rec = ctx.containers;
    while (rec) {
        container_record_t *next = rec->next;
        free(rec);
        rec = next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);

    fprintf(stderr, "[supervisor] clean exit\n");
    return 0;
}

/* ─── CLI Client ─────────────────────────────────────────────────────────── */

/*
 * Client-side control request path.
 *
 * The CLI commands use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is used here as it is the most
 * direct option for a local supervisor/client design.
 */
static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", CONTROL_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect: is the supervisor running?");
        close(fd);
        return 1;
    }

    if (write(fd, req, sizeof(*req)) != sizeof(*req)) {
        perror("write");
        close(fd);
        return 1;
    }

    /* Read response(s) */
    control_response_t resp;
    ssize_t n;
    while ((n = read(fd, &resp, sizeof(resp))) == sizeof(resp)) {
        printf("%s", resp.message);
        if (resp.status != 0) break;
        /* For logs, also read raw bytes until EOF */
        if (req->kind == CMD_LOGS) {
            char buf[1024];
            ssize_t r;
            while ((r = read(fd, buf, sizeof(buf))) > 0)
                fwrite(buf, 1, r, stdout);
            break;
        }
        break;
    }

    close(fd);
    return 0;
}

/* ─── Command Dispatch ───────────────────────────────────────────────────── */

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr, "Usage: %s start <id> <container-rootfs> <command> [opts]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    snprintf(req.container_id, sizeof(req.container_id), "%s", argv[2]);
    snprintf(req.rootfs, sizeof(req.rootfs), "%s", argv[3]);
    snprintf(req.command, sizeof(req.command), "%s", argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr, "Usage: %s run <id> <container-rootfs> <command> [opts]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    snprintf(req.container_id, sizeof(req.container_id), "%s", argv[2]);
    snprintf(req.rootfs, sizeof(req.rootfs), "%s", argv[3]);
    snprintf(req.command, sizeof(req.command), "%s", argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    /*
     * The supervisor responds with container metadata.
     * Rendering format is kept simple for demos and debugging.
     */
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) { fprintf(stderr, "Usage: %s logs <id>\n", argv[0]); return 1; }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    snprintf(req.container_id, sizeof(req.container_id), "%s", argv[2]);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) { fprintf(stderr, "Usage: %s stop <id>\n", argv[0]); return 1; }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    snprintf(req.container_id, sizeof(req.container_id), "%s", argv[2]);
    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) { fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]); return 1; }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
