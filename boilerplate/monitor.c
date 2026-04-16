/*
 * monitor.c - Multi-Container Memory Monitor (Linux Kernel Module)
 *
 * Architecture:
 *   - A struct timer_list fires every CHECK_INTERVAL_SEC seconds.
 *   - The timer callback is in softirq context and CANNOT sleep, so it
 *     immediately schedules a work item onto the system workqueue.
 *   - The work function runs in process context (kworker thread) where
 *     mutex_lock(), get_task_mm(), mmput(), and kmalloc(GFP_KERNEL)
 *     are all safe to call.
 *   - A single mutex protects the linked list across the work function
 *     and the ioctl handler.
 */

#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/jiffies.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>
#include <linux/workqueue.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME        "container_monitor"
#define CHECK_INTERVAL_SEC  1

/* ================================================================
 * Linked-list node
 * ================================================================ */
struct monitor_entry {
    pid_t            pid;
    char             container_id[64];
    unsigned long    soft_limit_bytes;
    unsigned long    hard_limit_bytes;
    int              soft_limit_triggered; /* 1 once soft warning emitted */
    struct list_head list;
};

/* ================================================================
 * Globals
 *
 * Lock choice — mutex, not spinlock:
 *   The work function calls get_task_mm() / mmput() (may sleep) and
 *   holds the lock during that window.  The ioctl path calls
 *   kmalloc(GFP_KERNEL) (may sleep).  Neither path is in hard-IRQ or
 *   softirq context, so a sleepable mutex is both correct and safe.
 * ================================================================ */
static LIST_HEAD(monitor_list);
static DEFINE_MUTEX(monitor_lock);

static struct timer_list monitor_timer;
static struct work_struct monitor_work;

static dev_t         dev_num;
static struct cdev   c_dev;
static struct class *cl;

/* ================================================================
 * RSS helper — returns resident bytes for pid, or -1 if gone.
 * ================================================================ */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct   *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) {
        rss_pages = get_mm_rss(mm);
        mmput(mm);
    }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}

/* ================================================================
 * Limit event helpers
 * ================================================================ */
static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ================================================================
 * Work function — runs in process context (kworker thread).
 *
 *   - Iterates the list safely (list_for_each_entry_safe).
 *   - Removes entries for dead processes.
 *   - Emits soft-limit warning exactly once per entry.
 *   - Kills and removes entries that breach the hard limit.
 * ================================================================ */
static void monitor_work_fn(struct work_struct *w)
{
    struct monitor_entry *entry, *tmp;
    long rss;

    mutex_lock(&monitor_lock);

    list_for_each_entry_safe(entry, tmp, &monitor_list, list) {

        rss = get_rss_bytes(entry->pid);

        /* Process has exited — remove and free */
        if (rss < 0) {
            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        /* Soft limit: warn once */
        if (rss > (long)entry->soft_limit_bytes &&
            !entry->soft_limit_triggered) {
            log_soft_limit_event(entry->container_id,
                                 entry->pid,
                                 entry->soft_limit_bytes,
                                 rss);
            entry->soft_limit_triggered = 1;
        }

        /* Hard limit: kill and remove */
        if (rss > (long)entry->hard_limit_bytes) {
            kill_process(entry->container_id,
                         entry->pid,
                         entry->hard_limit_bytes,
                         rss);
            list_del(&entry->list);
            kfree(entry);
            /*
             * 'entry' is freed here; this is safe because
             * list_for_each_entry_safe already saved the next
             * pointer in 'tmp' before we entered the loop body.
             */
        }
    }

    mutex_unlock(&monitor_lock);
}

/* ================================================================
 * Timer callback — softirq context, MUST NOT sleep.
 *
 * Only schedules the work item and re-arms itself.
 * ================================================================ */
static void timer_callback(struct timer_list *t)
{
    schedule_work(&monitor_work);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
}

/* ================================================================
 * IOCTL handler
 *
 * MONITOR_REGISTER   — add a new entry to the monitored list.
 * MONITOR_UNREGISTER — remove an entry by pid + container_id.
 * ================================================================ */
static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    /* All locals declared at top — required by kernel -Wdeclaration-after-statement */
    struct monitor_request  req;
    struct monitor_entry   *new_entry;
    struct monitor_entry   *entry, *tmp;

    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -EINVAL;

    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
        return -EFAULT;

    /* ---- REGISTER ---- */
    if (cmd == MONITOR_REGISTER) {

        /* Sanity: soft limit must not exceed hard limit */
        if (req.soft_limit_bytes > req.hard_limit_bytes)
            return -EINVAL;

        new_entry = kmalloc(sizeof(*new_entry), GFP_KERNEL);
        if (!new_entry)
            return -ENOMEM;

        new_entry->pid = req.pid;
        strscpy(new_entry->container_id, req.container_id,
                sizeof(new_entry->container_id));
        new_entry->soft_limit_bytes     = req.soft_limit_bytes;
        new_entry->hard_limit_bytes     = req.hard_limit_bytes;
        new_entry->soft_limit_triggered = 0;
        INIT_LIST_HEAD(&new_entry->list);

        mutex_lock(&monitor_lock);
        list_add_tail(&new_entry->list, &monitor_list);
        mutex_unlock(&monitor_lock);

        printk(KERN_INFO
               "[container_monitor] Registered container=%s pid=%d "
               "soft=%lu hard=%lu\n",
               req.container_id, req.pid,
               req.soft_limit_bytes, req.hard_limit_bytes);
        return 0;
    }

    /* ---- UNREGISTER ---- */
    mutex_lock(&monitor_lock);

    list_for_each_entry_safe(entry, tmp, &monitor_list, list) {
        if (entry->pid == req.pid &&
            strncmp(entry->container_id, req.container_id,
                    sizeof(entry->container_id)) == 0) {

            list_del(&entry->list);
            kfree(entry);
            mutex_unlock(&monitor_lock);

            printk(KERN_INFO
                   "[container_monitor] Unregistered container=%s pid=%d\n",
                   req.container_id, req.pid);
            return 0;
        }
    }

    mutex_unlock(&monitor_lock);

    printk(KERN_WARNING
           "[container_monitor] Unregister: no entry for container=%s pid=%d\n",
           req.container_id, req.pid);
    return -ENOENT;
}

/* ================================================================
 * File operations
 * ================================================================ */
static struct file_operations fops = {
    .owner          = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* ================================================================
 * Module init
 * ================================================================ */
static int __init monitor_init(void)
{
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif

    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    /* Initialise the work item before arming the timer */
    INIT_WORK(&monitor_work, monitor_work_fn);

    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO "[container_monitor] Module loaded. Device: /dev/%s\n",
           DEVICE_NAME);
    return 0;
}

/* ================================================================
 * Module exit
 *
 * Shutdown order matters:
 *   1. Stop the timer  — no new work items will be queued after this.
 *   2. Cancel the work — drains any already-queued work item.
 *   3. Free the list   — no concurrent access possible now.
 *   4. Tear down the character device.
 * ================================================================ */
static void __exit monitor_exit(void)
{
    struct monitor_entry *entry, *tmp;

    /* 1. Stop the periodic timer */
    timer_shutdown_sync(&monitor_timer);

    /* 2. Wait for any in-flight work to finish, then cancel */
    cancel_work_sync(&monitor_work);

    /* 3. Free remaining list entries */
    mutex_lock(&monitor_lock);

    list_for_each_entry_safe(entry, tmp, &monitor_list, list) {
        list_del(&entry->list);
        kfree(entry);
    }

    mutex_unlock(&monitor_lock);

    /* 4. Remove the character device */
    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded.\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Multi-container memory monitor");
