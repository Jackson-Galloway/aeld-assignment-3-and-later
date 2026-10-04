/**
 * @file aesdchar.c
 * @brief Functions and data related to the AESD char driver implementation
 *
 * Based on the implementation of the "scull" device driver, found in
 * Linux Device Drivers example code.
 *
 * @author Dan Walkes
 * @date 2019-10-22
 * @copyright Copyright (c) 2019
 *
 */

#include <linux/module.h>
#include <linux/init.h>
#include <linux/printk.h>
#include <linux/types.h>
#include <linux/cdev.h>
#include <linux/fs.h> // file_operations
#include <linux/slab.h> // kmalloc/kfree
#include <linux/uaccess.h> // copy_to_user/copy_from_user
#include <linux/mutex.h>
#include "aesdchar.h"
int aesd_major =   0; // use dynamic major
int aesd_minor =   0;

MODULE_AUTHOR("Jackson Galloway");
MODULE_LICENSE("Dual BSD/GPL");

struct aesd_dev aesd_device;

static int aesd_open(struct inode *inode, struct file *filp)
{
    PDEBUG("open");
    filp->private_data = &aesd_device;
    return 0;
}

static int aesd_release(struct inode *inode, struct file *filp)
{
    PDEBUG("release");
    return 0;
}

static ssize_t aesd_read(struct file *filp, char __user *buf, size_t count,
                loff_t *f_pos)
{
    ssize_t retval = 0;
    struct aesd_dev *dev = filp->private_data;
    struct aesd_buffer_entry *entry;
    size_t entry_offset;
    size_t bytes_available;
    size_t bytes_to_copy;

    PDEBUG("read %zu bytes with offset %lld",count,*f_pos);

    if (mutex_lock_interruptible(&dev->lock)) {
        return -ERESTARTSYS;
    }

    entry = aesd_circular_buffer_find_entry_offset_for_fpos(&dev->buffer, *f_pos, &entry_offset);
    if (entry == NULL) {
        /* f_pos is at or past the end of all currently buffered data */
        goto out;
    }

    bytes_available = entry->size - entry_offset;
    bytes_to_copy = (count < bytes_available) ? count : bytes_available;

    if (copy_to_user(buf, entry->buffptr + entry_offset, bytes_to_copy)) {
        retval = -EFAULT;
        goto out;
    }

    *f_pos += bytes_to_copy;
    retval = bytes_to_copy;

out:
    mutex_unlock(&dev->lock);
    return retval;
}

/**
 * Appends data to dev->pending_buf, growing it with a fresh kmalloc since the
 * final command length isn't known until a newline shows up. Caller must
 * hold dev->lock. Returns 0 on success, -ENOMEM on allocation failure (in
 * which case dev->pending_buf/pending_size are left unchanged).
 */
static int aesd_append_to_pending(struct aesd_dev *dev, const char *data, size_t len)
{
    char *grown = kmalloc(dev->pending_size + len, GFP_KERNEL);

    if (!grown) {
        return -ENOMEM;
    }
    if (dev->pending_buf) {
        memcpy(grown, dev->pending_buf, dev->pending_size);
        kfree(dev->pending_buf);
    }
    memcpy(grown + dev->pending_size, data, len);

    dev->pending_buf = grown;
    dev->pending_size += len;
    return 0;
}

/**
 * Hands the accumulated pending buffer off to the circular buffer as a
 * completed write command, evicting the oldest command if the buffer is
 * already at its 10-entry limit. Caller must hold dev->lock.
 */
static void aesd_commit_pending_entry(struct aesd_dev *dev)
{
    struct aesd_buffer_entry new_entry = {
        .buffptr = dev->pending_buf,
        .size = dev->pending_size,
    };

    if (dev->buffer.full) {
        kfree(dev->buffer.entry[dev->buffer.in_offs].buffptr);
    }
    aesd_circular_buffer_add_entry(&dev->buffer, &new_entry);

    dev->pending_buf = NULL;
    dev->pending_size = 0;
}

static ssize_t aesd_write(struct file *filp, const char __user *buf, size_t count,
                loff_t *f_pos)
{
    struct aesd_dev *dev = filp->private_data;
    char *kbuf;
    size_t line_start = 0;
    size_t i;
    int err;

    PDEBUG("write %zu bytes with offset %lld",count,*f_pos);

    if (count == 0) {
        return 0;
    }

    kbuf = kmalloc(count, GFP_KERNEL);
    if (!kbuf) {
        return -ENOMEM;
    }

    if (copy_from_user(kbuf, buf, count)) {
        kfree(kbuf);
        return -EFAULT;
    }

    if (mutex_lock_interruptible(&dev->lock)) {
        kfree(kbuf);
        return -ERESTARTSYS;
    }

    /* A single write() can contain several newline-terminated commands
     * (or none). Commit one circular-buffer entry per newline found, and
     * carry forward anything after the last newline as a partial command.
     */
    for (i = 0; i < count; i++) {
        if (kbuf[i] != '\n') {
            continue;
        }

        err = aesd_append_to_pending(dev, kbuf + line_start, i - line_start + 1);
        if (err) {
            mutex_unlock(&dev->lock);
            kfree(kbuf);
            return err;
        }
        aesd_commit_pending_entry(dev);
        line_start = i + 1;
    }

    if (line_start < count) {
        err = aesd_append_to_pending(dev, kbuf + line_start, count - line_start);
        if (err) {
            mutex_unlock(&dev->lock);
            kfree(kbuf);
            return err;
        }
    }

    mutex_unlock(&dev->lock);
    kfree(kbuf);
    return count;
}
struct file_operations aesd_fops = {
    .owner =    THIS_MODULE,
    .read =     aesd_read,
    .write =    aesd_write,
    .open =     aesd_open,
    .release =  aesd_release,
};

static int aesd_setup_cdev(struct aesd_dev *dev)
{
    int err, devno = MKDEV(aesd_major, aesd_minor);

    cdev_init(&dev->cdev, &aesd_fops);
    dev->cdev.owner = THIS_MODULE;
    dev->cdev.ops = &aesd_fops;
    err = cdev_add (&dev->cdev, devno, 1);
    if (err) {
        printk(KERN_ERR "Error %d adding aesd cdev", err);
    }
    return err;
}



static int __init aesd_init_module(void)
{
    dev_t dev = 0;
    int result;
    result = alloc_chrdev_region(&dev, aesd_minor, 1,
            "aesdchar");
    aesd_major = MAJOR(dev);
    if (result < 0) {
        printk(KERN_WARNING "Can't get major %d\n", aesd_major);
        return result;
    }
    memset(&aesd_device,0,sizeof(struct aesd_dev));

    aesd_circular_buffer_init(&aesd_device.buffer);
    mutex_init(&aesd_device.lock);
    aesd_device.pending_buf = NULL;
    aesd_device.pending_size = 0;

    result = aesd_setup_cdev(&aesd_device);

    if( result ) {
        unregister_chrdev_region(dev, 1);
    }
    return result;

}

static void __exit aesd_cleanup_module(void)
{
    dev_t devno = MKDEV(aesd_major, aesd_minor);
    struct aesd_buffer_entry *entry;
    uint8_t index;

    cdev_del(&aesd_device.cdev);

    AESD_CIRCULAR_BUFFER_FOREACH(entry, &aesd_device.buffer, index) {
        kfree(entry->buffptr);
    }
    kfree(aesd_device.pending_buf);

    unregister_chrdev_region(devno, 1);
}



module_init(aesd_init_module);
module_exit(aesd_cleanup_module);
