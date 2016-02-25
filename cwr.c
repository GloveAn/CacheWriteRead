#include "cwr.h"

static struct kmem_cache *bio_info_cache;
static mempool_t *bio_info_pool;

#if 0
static DECLARE_WAIT_QUEUE_HEAD(wait_queue);

static int list_sort_cmp(void *priv, struct list_head* a, struct list_head* b)
{
    /*
     * The comparison function @cmp must return a negative value if @a
     * should sort before @b
    */
    struct cwr_unit_meta* unit_meta_a, * unit_meta_b;
    unit_meta_a = list_entry(a, struct cwr_unit_meta, list);
    unit_meta_b = list_entry(b, struct cwr_unit_meta, list);
    if(unit_meta_a->z_value == unit_meta_b->z_value) return 0;
    return unit_meta_a->z_value < unit_meta_b->z_value;
}

static void manage_unit(unsigned long data)
{
    struct cwr_context *cc = (struct cwr_context *)data;
    struct list_head *cur_node, *next_node, *move_out, *move_in;
    struct cwr_unit_meta* cur_unit;
    unsigned int read_list_length = 0;
    unsigned int write_list_length = 0;
    unsigned int io_frenquency;

    io_frenquency = (cc->io_count - cc->old_io_count) / UNIT_MANAGE_INTERVAL;
    cc->old_io_count = cc->io_count;

    // if the cwr device is already under data moving state, we should quit
    if(cc->state != CWR_STATE_MOVING &&
       cc->io_count >= IO_COUNT_THRESHOLD &&
       io_frenquency == UNIT_MANAGE_THRESHOLD)
    {
        cc->state = CWR_STATE_MOVING;

        /// clear read count and write count to prevent overflow
        list_for_each(cur_node, &cc->read_list)
        {
	        cur_unit = list_entry(cur_node, struct cwr_unit_meta, list);
            if(cur_unit->read_count > cur_unit->write_count)
            {
                cur_unit->read_count -= cur_unit->write_count;
                cur_unit->write_count = 0;
            }
            else
            {
                cur_unit->write_count -= cur_unit->read_count;
                cur_unit->read_count = 0;
            }
	    }
        list_for_each(cur_node, &cc->write_list)
        {
	        cur_unit = list_entry(cur_node, struct cwr_unit_meta, list);
            if(cur_unit->read_count > cur_unit->write_count)
            {
                cur_unit->read_count -= cur_unit->write_count;
                cur_unit->write_count = 0;
            }
            else
            {
                cur_unit->write_count -= cur_unit->read_count;
                cur_unit->read_count = 0;
            }
	    }

        /// arrange list node by its read count and write count
        list_for_each_safe(cur_node, next_node, &cc->read_list)
        {
            cur_unit = list_entry(cur_node, struct cwr_unit_meta, list);
            if(cur_unit->write_count > RW_STATE_THRESHOLD)
            {
                list_del_init(cur_node);
                list_add(cur_node, &cc->write_list);
            }
            else
            {
                read_list_length++;
            }
        }
        list_for_each_safe(cur_node, next_node, &cc->write_list)
        {
            cur_unit = list_entry(cur_node, struct cwr_unit_meta, list);
            if(cur_unit->read_count > RW_STATE_THRESHOLD)
            {
                list_del_init(cur_node);
                list_add(cur_node, &cc->read_list);
            }
            else
            {
                write_list_length++;
            }
        }

        /// sort hot data to the front of lists
        list_sort(0, &cc->read_list, list_sort_cmp);
        list_sort(0, &cc->write_list, list_sort_cmp);

        /// FIXME! consider no data block is marked read or write,
        /// then the the device cannot hold all the data.
        if(read_list_length < READ_CACHE_SIZE ||
           write_list_length < WRITE_CACHE_SIZE)
           printk_once(KERN_ALERT "Not enough data to fill cache!");

        /// time for moving data
        for(move_out = cc->read_list.next, move_in = cc->read_list.next;
            move_out != &cc->read_list && move_in != &cc->read_list;
            move_out = move_out->next, move_in = move_in->next)
        {
            ;
        }

        cc->io_count = 0;
        cc->old_io_count = 0;

        cc->state = CWR_STATE_READY;
    }

    cc->unit_manage_timer.expires = jiffies + UNIT_MANAGE_INTERVAL * HZ;
    add_timer(&cc->unit_manage_timer);
}
#endif
static inline sector_t get_cell_id(struct dm_target *dt,
                                   struct cwr_context *cc,
                                   struct bio *bio)
{
    return (bio->bi_sector - dt->begin) >> (cc->cell_size - 1);
}

static void cwr_end_io(struct bio *bio, int error)
{
    // @error is set by bio_endio()
    struct cwr_bio_info *bio_info = (struct cwr_bio_info*)bio->bi_private;
    struct bio *origin_bio = bio_info->bio;
    struct cwr_context *cc = bio_info->cc;
    sector_t cell_id = get_cell_id(bio_info->dt, cc, bio);

    cc->cell_meta[cell_id].bio_count--;
    if(cc->cell_meta[cell_id].bio_count == 0)
    {
        cc->cell_meta[cell_id].state &= !CELL_STATE_ACCESSING;
        // the cell finshed all operations
        if(!(cc->cell_meta[cell_id].state & CELL_STATE_MIGRATING))
            cc->cell_meta[cell_id].state = CELL_STATE_READY;
    }

    bio_endio(origin_bio, error);
    mempool_free(bio_info, bio_info_pool);
}

static int cwr_map(struct dm_target *dt, struct bio *bio, union map_info *mi)
{
    struct cwr_context* cc = (struct cwr_context*)dt->private;
    sector_t cell_id, seek_distance;
    int read_flag, write_flag;
    unsigned int z_value;
    struct bio *cwr_bio;
    struct cwr_bio_info *bio_info;

    // no need to protect io_count against race condition,
    // it could be imprecisely.
    cc->io_count++;

    read_flag = bio_data_dir(bio) & READ;
    write_flag = bio_data_dir(bio) & WRITE;

    cell_id = get_cell_id(dt, cc, bio);

    /* update z value */
    seek_distance = cc->last_cell - cell_id;
    // make seek_distance an absolute value
    if(seek_distance < 0) seek_distance = -seek_distance;
    if(seek_distance < 5)
    {   // if seek distance is small, we consider it as sequential IO.
        z_value = 0;
    }
    else
    {
        z_value = K - cc->cell_size;
        if(z_value < 0) z_value = 0;
        z_value = z_value * (read_flag + write_flag * T);
        do_div(z_value, K); // do_div: int64/int32
        if(z_value >= 0xFFFFFF) z_value = 0xFFFFFF;
        do_div(seek_distance, T2);
        z_value = (T1 + seek_distance) << z_value;
    }
    cc->cell_meta[cell_id].z_value += z_value;

    /* update r/w count */
    cc->cell_meta[cell_id].read_count += read_flag;
    cc->cell_meta[cell_id].write_count += write_flag;

    bio->bi_bdev = cc->cell_meta[cell_id].dev->bdev;
    bio->bi_sector = cc->cell_meta[cell_id].offset * cc->cell_size;

    /* clone bio to handle cell states */
    cwr_bio = bio_clone(bio, GFP_NOIO);
    if(cwr_bio == 0) return DM_MAPIO_REQUEUE; // try again later
    // memory freed in cwr_end_io()
    bio_info = (struct cwr_bio_info *)mempool_alloc(bio_info_pool, GFP_NOIO);

    bio_info->bio = bio;
    bio_info->dt = dt;
    bio_info->cc = cc;

    cwr_bio->bi_end_io = cwr_end_io;
    cwr_bio->bi_private = bio_info;

    cc->cell_meta[cell_id].state &= !CELL_STATE_READY;
    cc->cell_meta[cell_id].state |= CELL_STATE_ACCESSING;
    cc->cell_meta[cell_id].bio_count++;
    if(cc->cell_meta[cell_id].state & CELL_STATE_MIGRATING)
    {
        // pend bio when migrating
        bio_list_add(&cc->cell_meta[cell_id].bio_list, cwr_bio);
    }
    else
    {
        generic_make_request(cwr_bio);
    }

    return DM_MAPIO_SUBMITTED;
}

/*
 * dmsetup create dev_name --table
 * '0 102400 cwr unit_size /dev/hdd /dev/write_cache /dev/read-cache'
 */
static int cwr_ctr(struct dm_target *dt, unsigned int argc, char *argv[])
{
    struct cwr_context* cc;
    sector_t cell_size, cell_amount;
    int re = 0;
    unsigned int i, j;

    if(argc != 4)
    {
        dt->error = "cwr: invalid argument count";
        return -EINVAL;
    }

    if(sscanf(argv[0], "%llu", &cell_size) != 1)
    {
        dt->error = "cwr: cell size read error";
        return -EINVAL;
    }
    if(is_power_of_2(cell_size) == 0)
    {
        dt->error = "cwr: cell size is not power of 2";
        return -EINVAL;
    }
    cell_amount = dt->len >> ilog2(cell_size);

    cc = kzalloc(sizeof(struct cwr_context) +
                 sizeof(struct cwr_cell_meta) * cell_amount,
                 GFP_KERNEL);
    if(cc == 0)
    {
        dt->error = "cwr: cannot allocate cwr context";
        return -ENOMEM;
    }

    cc->cell_size = cell_size;

    /// get mapped targets
    re |= dm_get_device(dt, argv[1], 0, cc->cold_dev_size,
                        dm_table_get_mode(dt->table), &cc->cold_dev);
    re |= dm_get_device(dt, argv[2], 0, cc->write_dev_size,
                        dm_table_get_mode(dt->table), &cc->write_dev);
    re |= dm_get_device(dt, argv[3], 0, cc->read_dev_size,
                        dm_table_get_mode(dt->table), &cc->read_dev);
    if(re != 0) goto device_invalid;

    cc->write_dev_size = WRITE_CACHE_SIZE * cc->cell_size;
    cc->read_dev_size = READ_CACHE_SIZE * cc->cell_size;
    cc->cold_dev_size = dt->len - cc->write_dev_size - cc->read_dev_size;

    // disk size check
    // i_size_read returns device size by bytes
    if((i_size_read(cc->cold_dev->bdev->bd_inode) < (cc->cold_dev_size << 9))
    || (i_size_read(cc->write_dev->bdev->bd_inode) < (cc->write_dev_size << 9))
    || (i_size_read(cc->read_dev->bdev->bd_inode) < (cc->read_dev_size << 9)))
    {
        dt->error = "cwr: disk is too small";
        re = -EINVAL;
        goto size_invalid;
    }
    // alignment check (may be reduntant)
    if((cc->cold_dev_size & (cc->cell_size - 1))
    || (cc->write_dev_size & (cc->cell_size - 1))
    || (cc->read_dev_size & (cc->cell_size - 1)))
    {
        dt->error = "dm-cwr: disk size is not alignt";
        re = -EINVAL;
        goto size_invalid;
    }

    cc->cell_meta = (struct cwr_cell_meta *)
                    ((unsigned char *)cc + sizeof(struct cwr_context));

    INIT_LIST_HEAD(&cc->read_list);
    INIT_LIST_HEAD(&cc->write_list);
    i = 0;
    // map first logical part to write cache
    for(j = 0; j < WRITE_CACHE_SIZE; i++, j++)
    {
        cc->cell_meta[i].dev = cc->write_dev;
        cc->cell_meta[i].offset = j;
        cc->cell_meta[i].state = CELL_STATE_READY;

        INIT_LIST_HEAD(&cc->cell_meta[i].rw_list);
        list_add(&cc->cell_meta[i].rw_list, &cc->write_list);
    }
    // map second logical part to read cache
    for(j = 0; j < READ_CACHE_SIZE; i++, j++)
    {
        cc->cell_meta[i].dev = cc->read_dev;
        cc->cell_meta[i].offset = j;
        cc->cell_meta[i].state = CELL_STATE_READY;

        INIT_LIST_HEAD(&cc->cell_meta[i].rw_list);
        list_add(&cc->cell_meta[i].rw_list, &cc->read_list);
    }
    // map last logical part to cold hdd
    for(j = 0; i < cell_amount; i++, j++)
    {
        cc->cell_meta[i].dev = cc->cold_dev;
        cc->cell_meta[i].offset = j;
        cc->cell_meta[i].state = CELL_STATE_READY;

        INIT_LIST_HEAD(&cc->cell_meta[i].rw_list);
        // conside it as write oriented data
        list_add(&cc->cell_meta[i].rw_list, &cc->write_list);
    }

    /*init_timer(&cc->cell_manage_timer);
    cc->cell_manage_timer.data = (unsigned long)cc;
    cc->cell_manage_timer.function = cell_manager;
    cc->cell_manage_timer.expires = jiffies + CELL_MANAGE_INTERVAL * HZ;
    add_timer(&cc->cell_manage_timer);*/

    dt->private = cc;

    printk(KERN_DEBUG "a cwr device is constructed.");
    printk(KERN_DEBUG "cell size: %llu", cc->cell_size);
    printk(KERN_DEBUG "c: %s, w:%s, r:%s",
           cc->cold_dev->name, cc->write_dev->name, cc->read_dev->name);
    return 0;

size_invalid:
device_invalid:
    if(cc->cold_dev) dm_put_device(dt, cc->cold_dev);
    if(cc->write_dev) dm_put_device(dt, cc->write_dev);
    if(cc->read_dev) dm_put_device(dt, cc->read_dev);
    kfree(cc);
    return re;
}

static void cwr_dtr(struct dm_target *dt)
{
    struct cwr_context* cc = (struct cwr_context*)dt->private;

    /*// wait until the last timer returns
    del_timer_sync(&cc->cell_manage_timer);*/

    dm_put_device(dt, cc->cold_dev);
    dm_put_device(dt, cc->write_dev);
    dm_put_device(dt, cc->read_dev);
    kfree(cc);

    printk(KERN_DEBUG "a cwr device is destructed.");
    printk(KERN_DEBUG "c: %s, w:%s, r:%s",
           cc->cold_dev->name, cc->write_dev->name, cc->read_dev->name);
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

    /* init bio info pool */
    bio_info_cache = kmem_cache_create("cwr_bio_info",
                                       sizeof(struct cwr_bio_info),
                                       0, 0, 0);
    if(bio_info_cache == 0)
    {
        printk(KERN_ERR "allocate bio info cache fail.");
        return -ENOMEM;
    }
    bio_info_pool = mempool_create(MIN_BIO_INFO_AMOUNT,
                                   mempool_alloc_slab, mempool_free_slab,
                                   bio_info_cache);

    re = dm_register_target(&cwr_target);
    if(re < 0)
    {
        mempool_destroy(bio_info_pool);
        kmem_cache_destroy(bio_info_cache);
        printk(KERN_ERR "regist cwr target fail.");
        return re;
    }

    printk(KERN_DEBUG "cwr loaded.");
    return 0;
}

static void __exit cwr_done(void)
{
    dm_unregister_target(&cwr_target);
    mempool_destroy(bio_info_pool);
    kmem_cache_destroy(bio_info_cache);
    printk(KERN_DEBUG "cwr exited.");
}

module_init(cwr_init);
module_exit(cwr_done);
MODULE_LICENSE("GPL");
