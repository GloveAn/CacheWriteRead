#include <linux/module.h>	/* Needed by all modules */
#include <linux/init.h>		/* Needed for the macros */

/* Note
 * sector_t = u64 = unsigned long long
 */

#define W_CACHE 200
#define R_CACHE 200

struct cwr_context
{
    sector_t u_size;

    struct dm_dev* c_dev;
    struct dm_dev* r_dev;
    struct dm_dev* w_dev;

    sector_t c_size;
    sector_t r_size;
    sector_t w_size;
};

/*
 * dmsetup create dev_name --table
 * '0 102400 cwr unit_size /dev/hdd /dev/write_cache /dev/read-cache'
 */
static int cwr_map(struct dm_target *dt, struct bio *bio, union map_info *mi)
{
    ;
}

static int cwr_ctr(struct dm_target *dt, unsigned int argc, char *argv[])
{
    struct cwr_context* cc;
    int re = 0;

    if(argc != 4)
    {
        dt->error = "dm-cwr: invalid argument count";
        return -EINVAL;
    }

    cc = kzalloc(sizeof(struct cwr_context), GFP_KERNEL);
    if(ct == 0)
    {
        dt->error = "dm-cwr: cannot allocate cwr context";
        return -ENOMEM;
    }

    /* read devices' sector sizes */
    if(sscanf(argv[0], "%llu", &cc->u_size) != 1) re = -EINVAL;
    cc->w_size = W_CACHE * cc->u_size;
    cc->r_size = R_CACHE * cc->u_size;
    cc->c_size = dt->len - cc->w_size - cc->r_size;
    // disk size check
    if((i_size_read(cc->c_dev->bdev->bd_inode) > (cc->c_size << 9))
    || (i_size_read(cc->w_dev->bdev->bd_inode) > (cc->w_size << 9))
    || (i_size_read(cc->r_dev->bdev->bd_inode) > (cc->r_size << 9)))
    {   // i_size_read returns the size measured by bytes
        dt->error = "dm-cwr: disk is too small";
        re = -EINVAL;
    }
    // alignment check
    if((cc->c_size % cc->u_size)
    || (cc->w_size % cc->u_size)
    || (cc->r_size % cc->u_size))
    {
        dt-error = "dm-cwr: disk size is not alignt"
        re = -EINVAL;
    }
    if(re == -EINVAL) goto size_invalid;

    /* get mapped targets */
    re |= dm_get_device(dt, argv[1], 0, cc->c_size,
                        dm_table_get_mode(dt->table), &cc->c_dev);
    re |= dm_get_device(dt, argv[2], 0, cc->w_size,
                        dm_table_get_mode(dt->table), &cc->w_dev);
    re |= dm_get_device(dt, argv[4], 0, cc->r_size,
                        dm_table_get_mode(dt->table), &cc->r_dev);
    if(re != 0) goto device_invalid;

    dt->private = cc;

device_invalid:
    if(cc->c_dev) dm_put_device(dt, cc->c_dev);
    if(cc->w_dev) dm_put_device(dt, cc->w_dev);
    if(cc->r_dev) dm_put_device(dt, cc->r_dev);
size_invalid:
    kfree(cc);
    return -EINVAL;
}

static void cwr_dtr(struct dm_target *dt)
{
    struct cwr_context* cc = (struct cwr_context*)dt->private;

    dm_put_device(dt, cc->c_dev);
    dm_put_device(dt, cc->w_dev);
    dm_put_device(dt, cc->r_dev);
    kfree(cc);
}

static struct target_type cwr_target = {
	.name    = "cwr",
	.version = {0, 0, 1},
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
