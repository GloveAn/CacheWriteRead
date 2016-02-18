#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/bio.h>
#include <linux/device-mapper.h>
#include <linux/list.h>
#include <linux/log2.h>

/* Note
 * sector_t = u64 = unsigned long long
 */

#define WRITE_CACHE_SIZE 200
#define READ_CACHE_SIZE 200

/* seek value =
 * MAX(24bit) {
 *     [max(0, k - unit_size) *
 *     (read_flag + t * write_flag) /
 *     k
 * } *
 * (T1 + T2 * seek_distance)
 *
 * note that the kernel does not support float arithmetic,
 */
#define K  512
#define T   90/100
#define T1   1
#define T2   100

/* data could be stored on hdd(COLD), SSD(CACHED for READ or WRITE) */
#define COLD        1 << 0
#define WRITE_CACHE 1 << 1
#define READ_CACHE  1 << 2

struct cwr_unit_meta
{
    // logic location is indicated by array index
    unsigned int z_value;
    unsigned int read_count;
    unsigned int write_count;
    struct dm_dev* dev;
    sector_t offset;
    struct list_head list;
};

struct cwr_context
{
    sector_t unit_size;
    sector_t last_unit;

    struct dm_dev* cold_dev;
    struct dm_dev* read_dev;
    struct dm_dev* write_dev;

    sector_t cold_dev_size;
    sector_t read_dev_size;
    sector_t write_dev_size;

    struct cwr_unit_meta* unit_meta;
    struct list_head read_list;
    struct list_head write_list;
};

/*
 * dmsetup create dev_name --table
 * '0 102400 cwr unit_size /dev/hdd /dev/write_cache /dev/read-cache'
 */
static int cwr_map(struct dm_target *dt, struct bio *bio, union map_info *mi)
{
    struct cwr_context* cc = (struct cwr_context*)dt->private;
    sector_t offset, unit_index, seek_distance;
    int read_flag, write_flag;
    unsigned int z_value;

    offset = bio->bi_sector - dt->begin;
    unit_index = offset >> (cc->unit_size - 1);
    // Is seek_distance an absolute value?
    seek_distance = cc->last_unit - unit_index;
    if(seek_distance < 0) seek_distance = -seek_distance;

    read_flag = bio->bi_rw & READ;
    write_flag = bio->bi_rw & WRITE;

    // if seek distance is small, we consider it as sequential IO.
    if(seek_distance < 5)
    {
        z_value = 0;
    }
    else
    {
        z_value = K - cc->unit_size;
        if(z_value < 0) z_value = 0;
        z_value = z_value * (read_flag + write_flag * T);
        do_div(z_value, K);
        if(z_value >= 0xFFFFFF) z_value = 0xFFFFFF;
        do_div(seek_distance, T2);
        z_value = (T1 + seek_distance) << z_value;
    }

    cc->unit_meta[unit_index].read_count += read_flag;
    cc->unit_meta[unit_index].write_count += write_flag;
    cc->unit_meta[unit_index].z_value += z_value;

    return 0;
}

static int cwr_ctr(struct dm_target *dt, unsigned int argc, char *argv[])
{
    struct cwr_context* cc;
    int re = 0;
    int unit_amount, i;

    if(argc != 4)
    {
        dt->error = "dm-cwr: invalid argument count";
        return -EINVAL;
    }

    cc = kzalloc(sizeof(struct cwr_context), GFP_KERNEL);
    if(cc == 0)
    {
        dt->error = "dm-cwr: cannot allocate cwr context";
        return -ENOMEM;
    }

    /// read devices' sector sizes
    if(sscanf(argv[0], "%llu", &cc->unit_size) != 1)
    {
        dt->error = "dm-cwr: unit size read error";
        re = -EINVAL;
        goto unit_invalid;
    }
    if(is_power_of_2(cc->unit_size) == 0)
    {
        dt->error = "dm-cwr: unit size is not power of 2";
        re = -EINVAL;
        goto unit_invalid;
    }
    unit_amount = dt->len >> ilog2(cc->unit_size);

    cc->write_dev_size = WRITE_CACHE_SIZE * cc->unit_size;
    cc->read_dev_size = READ_CACHE_SIZE * cc->unit_size;
    cc->cold_dev_size = dt->len - cc->write_dev_size - cc->read_dev_size;

    cc->unit_meta = kzalloc(sizeof(struct cwr_unit_meta) * unit_amount,
                            GFP_KERNEL);
    if(cc->unit_meta == 0)
    {
        dt->error = "dm-cwr: cannot allocate cwr unit meta";
        re = -ENOMEM;
        goto meta_invalid;
    }

    /// get mapped targets
    re |= dm_get_device(dt, argv[1], 0, cc->cold_dev_size,
                        dm_table_get_mode(dt->table), &cc->cold_dev);
    re |= dm_get_device(dt, argv[2], 0, cc->write_dev_size,
                        dm_table_get_mode(dt->table), &cc->write_dev);
    re |= dm_get_device(dt, argv[4], 0, cc->read_dev_size,
                        dm_table_get_mode(dt->table), &cc->read_dev);
    if(re != 0) goto device_invalid;

    // disk size check
    if((i_size_read(cc->cold_dev->bdev->bd_inode) > (cc->cold_dev_size << 9))
    || (i_size_read(cc->write_dev->bdev->bd_inode) > (cc->write_dev_size << 9))
    || (i_size_read(cc->read_dev->bdev->bd_inode) > (cc->read_dev_size << 9)))
    {   // i_size_read returns the size measured by bytes
        dt->error = "dm-cwr: disk is too small";
        re = -EINVAL;
    }
    // alignment check
    if((cc->cold_dev_size & (cc->unit_size - 1))
    || (cc->write_dev_size & (cc->unit_size - 1))
    || (cc->read_dev_size & (cc->unit_size - 1)))
    {
        dt->error = "dm-cwr: disk size is not alignt";
        re = -EINVAL;
    }
    if(re != 0) goto size_invalid;

    cc->last_unit = 0;

    INIT_LIST_HEAD(&cc->read_list);
    INIT_LIST_HEAD(&cc->write_list);
    for(i = 0; i < unit_amount; i++)
    {
        INIT_LIST_HEAD(&cc->unit_meta[i].list);
        // initially add all nodes to read list
        list_add(&cc->unit_meta[i].list, &cc->read_list);
    }

    dt->private = cc;

    return 0;

size_invalid:
device_invalid:
    if(cc->cold_dev) dm_put_device(dt, cc->cold_dev);
    if(cc->write_dev) dm_put_device(dt, cc->write_dev);
    if(cc->read_dev) dm_put_device(dt, cc->read_dev);
meta_invalid:
unit_invalid:
    kfree(cc);
    return re;
}

static void cwr_dtr(struct dm_target *dt)
{
    struct cwr_context* cc = (struct cwr_context*)dt->private;

    dm_put_device(dt, cc->cold_dev);
    dm_put_device(dt, cc->write_dev);
    dm_put_device(dt, cc->read_dev);
    kfree(cc->unit_meta);
    kfree(cc);
}

static struct target_type cwr_target = {
	.name    = "cwr",
	.version = {0, 0, 0},
	.module  = THIS_MODULE,
	.ctr     = cwr_ctr,
	.dtr     = cwr_dtr,
	.map     = cwr_map,
};

/* module related functions */
static int __init cwr_init(void)
{
    int re;

    re = dm_register_target(&cwr_target);
    if(re < 0)
    {
        printk(KERN_ERR "regist cwr target fail.");
        return re;
    }

    return 0;
}

static void __exit cwr_done(void)
{
    dm_unregister_target(&cwr_target);
}

module_init(cwr_init);
module_exit(cwr_done);
MODULE_LICENSE("CC");
