#include "cwr.h"

static struct kmem_cache *bio_info_cache;
static mempool_t *bio_info_pool;

LIST_HEAD(swap_list);
struct workqueue_struct *swap_work_queue;

static inline void finish_pending_bio(struct cwr_context *cc,
                                      struct cwr_cell_meta *ccm)
{
    struct bio *bio;

    while(!bio_list_empty(&ccm->bio_list))
    {
        bio = bio_list_pop(&ccm->bio_list);
        bio->bi_next = 0; // single bio instead of a bio list
        bio->bi_sector = ccm->offset + (bio->bi_sector & cc->cell_mask);
        bio->bi_bdev = ccm->dev->bdev;

        generic_make_request(bio);
    }
}

static inline int dm_io_sync_vm(unsigned num_regions,
                                struct dm_io_region *where, int rw, void *data,
                                unsigned long *error_bits,
                                struct cwr_context *cc)
{
    struct dm_io_request dir;

    dir.bi_rw = rw;
    dir.mem.type = DM_IO_VMA;
    dir.mem.ptr.vma = data;

	// set notify.fn to be async IO. NULL means sync IO
    dir.notify.fn = 0;
    dir.client = cc->io_client;

    return dm_io(&dir, num_regions, where, error_bits);
}

static inline struct dm_io_region make_io_region(struct cwr_context *cc,
                                                 struct cwr_cell_meta *ccm)
{
    struct dm_io_region where;

    where.bdev = ccm->dev->bdev;
    where.sector = ccm->offset;
    where.count = cc->cell_size;

    return where;
}

static void swap_worker(struct work_struct *ws)
{
    struct cwr_context *cc;
    struct cwr_swap_info *csi;
    void *mem1, *mem2;
    struct dm_io_region dir1, dir2;
    struct dm_dev *tmp_dev;
    sector_t tmp_offset;
    unsigned long error_bits;
    int re;

    cc = container_of(ws, struct cwr_context, swap_work);
    csi = list_first_entry(&swap_list, struct cwr_swap_info, swap_list);

    list_del(&csi->swap_list);

    mem1 = vmalloc(cc->cell_size << 9);
    mem2 = vmalloc(cc->cell_size << 9);
    if(mem1 == 0 || mem2 == 0) goto exit_swap;

    /* read cell data to memory */
    dir1 = make_io_region(cc, csi->ccm1);
    re = dm_io_sync_vm(1, &dir1, READ, mem1, &error_bits, cc);
    if(re < 0)
    {
        printk(KERN_ERR "cwr: read cells fail.");
        goto exit_swap;
    }
    dir2 = make_io_region(cc, csi->ccm2);
    re = dm_io_sync_vm(1, &dir2, READ, mem2, &error_bits, cc);
    if(re < 0)
    {
        printk(KERN_ERR "cwr: read cells fail.");
        goto exit_swap;
    }

    /* exchange and write data to cells */
    re = dm_io_sync_vm(1, &dir2, WRITE, mem1, &error_bits, cc);
    if(re < 0)
    {
        printk(KERN_ERR "cwr: write cells fail.");
        goto exit_swap;
    }
    re = dm_io_sync_vm(1, &dir1, WRITE, mem2, &error_bits, cc);
    if(re < 0)
    {
        printk(KERN_ERR "cwr: write cells fail.");
        goto exit_swap;
    }

    /* swap meta info */
    tmp_dev = csi->ccm1->dev;
    csi->ccm1->dev = csi->ccm2->dev;
    csi->ccm2->dev = tmp_dev;

    tmp_offset = csi->ccm1->offset;
    csi->ccm1->offset = csi->ccm2->offset;
    csi->ccm2->offset = tmp_offset;

    printk_once(KERN_DEBUG "cwr: swap worker success.");

exit_swap:
    spin_lock(&cc->lock);

    finish_pending_bio(cc, csi->ccm1);
    finish_pending_bio(cc, csi->ccm2);

    csi->ccm1->state &= !CELL_STATE_MIGRATING;
    csi->ccm2->state &= !CELL_STATE_MIGRATING;

    spin_unlock(&cc->lock);

    if(mem1) vfree(mem1);
    if(mem2) vfree(mem2);
    kfree(csi);
}

static inline void enqueue_pair(struct cwr_cell_meta *ccm1,
                                struct cwr_cell_meta *ccm2,
                                struct cwr_context *cc)
{
    struct cwr_swap_info *csi;

    spin_lock(&cc->lock);

    if(ccm1->state & CELL_STATE_ACCESSING &&
       ccm2->state & CELL_STATE_ACCESSING)
    {
        spin_unlock(&cc->lock);
        return;
    }

    ccm1->state |= CELL_STATE_MIGRATING;
    ccm2->state |= CELL_STATE_MIGRATING;

    spin_unlock(&cc->lock);

    csi = kzalloc(sizeof(struct cwr_swap_info), GFP_ATOMIC);
    if(csi == 0) return;

    csi->ccm1 = ccm1;
    csi->ccm2 = ccm2;
    csi->cc = cc;
    INIT_LIST_HEAD(&csi->swap_list);

    // swap info will be used by worker of work queue
    list_add_tail(&csi->swap_list, &swap_list);
    queue_work(swap_work_queue, &cc->swap_work);
}

static inline void enqueue_pairs(struct cwr_context *cc)
{
    struct list_head wc_node, cw_node, wr_node, rw_node, rc_node, cr_node;
    struct list_head *cur_node, *next_node, *swap_node1, *swap_node2;
    struct cwr_cell_meta *ccm;
    unsigned int i;

    INIT_LIST_HEAD(&wc_node);
    INIT_LIST_HEAD(&cw_node);
    INIT_LIST_HEAD(&wr_node);
    INIT_LIST_HEAD(&rw_node);
    INIT_LIST_HEAD(&rc_node);
    INIT_LIST_HEAD(&cr_node);

    /* classify cells */
    i = 0;
    list_for_each(cur_node, &cc->read_list)
    {
        ccm = list_entry(cur_node, struct cwr_cell_meta, rw_list);

        if(ccm->state & CELL_STATE_ACCESSING) continue;
        if(i < READ_CACHE_SIZE)
        {
            if(ccm->dev == cc->cold_dev) // c -> r
                list_add(&ccm->class_list, &cr_node);
            if(ccm->dev == cc->write_dev) // w -> r
                list_add(&ccm->class_list, &wr_node);
        }
        else // r -> c
        {
            if(ccm->dev == cc->read_dev)
                list_add(&ccm->class_list, &rc_node);
        }
        i++;
    }

    i = 0;
    list_for_each(cur_node, &cc->write_list)
    {
        ccm = list_entry(cur_node, struct cwr_cell_meta, rw_list);

        if(ccm->state & CELL_STATE_ACCESSING) continue;
        if(i < WRITE_CACHE_SIZE)
        {
            if(ccm->dev == cc->cold_dev) // c -> w
                list_add(&ccm->class_list, &cw_node);
            if(ccm->dev == cc->read_dev) // r -> w
                list_add(&ccm->class_list, &rw_node);
        }
        else // w -> c
        {
            if(ccm->dev == cc->write_dev)
                list_add(&ccm->class_list, &wc_node);
        }
        i++;
    }

    for(swap_node1 = wc_node.next, swap_node2 = cw_node.next;
        swap_node1->next != &wc_node && swap_node2 != &cw_node;
        swap_node1 = swap_node1->next, swap_node2 = swap_node2->next)
        enqueue_pair(list_entry(swap_node1, struct cwr_cell_meta, class_list),
                     list_entry(swap_node2, struct cwr_cell_meta, class_list),
                     cc);
    for(swap_node1 = rc_node.next, swap_node2 = cr_node.next;
        swap_node1->next != &rc_node && swap_node2 != &cr_node;
        swap_node1 = swap_node1->next, swap_node2 = swap_node2->next)
        enqueue_pair(list_entry(swap_node1, struct cwr_cell_meta, class_list),
                     list_entry(swap_node2, struct cwr_cell_meta, class_list),
                     cc);
    for(swap_node1 = wr_node.next, swap_node2 = rw_node.next;
        swap_node1->next != &wr_node && swap_node2 != &rw_node;
        swap_node1 = swap_node1->next, swap_node2 = swap_node2->next)
        enqueue_pair(list_entry(swap_node1, struct cwr_cell_meta, class_list),
                     list_entry(swap_node2, struct cwr_cell_meta, class_list),
                     cc);

    list_for_each_safe(cur_node, next_node, &wc_node) list_del_init(cur_node);
    list_for_each_safe(cur_node, next_node, &cw_node) list_del_init(cur_node);
    list_for_each_safe(cur_node, next_node, &rc_node) list_del_init(cur_node);
    list_for_each_safe(cur_node, next_node, &cr_node) list_del_init(cur_node);
    list_for_each_safe(cur_node, next_node, &rw_node) list_del_init(cur_node);
    list_for_each_safe(cur_node, next_node, &wr_node) list_del_init(cur_node);
}

static int list_sort_cmp(void *priv, struct list_head* a, struct list_head* b)
{
    /*
     * The comparison function @cmp must return a negative value if @a
     * should sort before @b
    */
    struct cwr_cell_meta *ccm1, *ccm2;
    ccm1 = list_entry(a, struct cwr_cell_meta, rw_list);
    ccm2 = list_entry(b, struct cwr_cell_meta, rw_list);
    if(ccm1->z_value == ccm2->z_value) return 0;
    return ccm1->z_value < ccm2->z_value;
}

static void cell_manager(unsigned long data)
{
    /* as the timer is set when this function returns,
     * there will be at most one timer running.
     * so no worry about concurrency.
     * all io operations here are sync io.
     */
    struct cwr_context *cc = (struct cwr_context *)data;
    struct list_head *cur_node, *next_node;
    struct cwr_cell_meta *ccm;
    int io_frenquency;
    int min_z_value = INT_MAX;

    io_frenquency = cc->io_count - cc->old_io_count / CELL_MANAGE_INTERVAL;
    cc->old_io_count = cc->io_count;

    printk_once(KERN_DEBUG "cwr: cell manager trigerred");
    printk_once(KERN_DEBUG "    io freq:%d, io count:%d",
                io_frenquency, cc->old_io_count);

    // trigger under certain conditions
    if(cc->io_count >= IO_COUNT_THRESHOLD &&
       io_frenquency <= CELL_MANAGE_THRESHOLD)
    {
        printk_once(KERN_DEBUG "cwr: cell manager is activated.");

        /* clear read count / write count, and z value to prevent overflow */
        list_for_each(cur_node, &cc->read_list)
        {
	        ccm = list_entry(cur_node, struct cwr_cell_meta, rw_list);

            if(min_z_value > ccm->z_value) min_z_value = ccm->z_value;

            if(ccm->read_count > ccm->write_count)
            {
                ccm->read_count -= ccm->write_count;
                ccm->write_count = 0;
            }
            else
            {
                ccm->write_count -= ccm->read_count;
                ccm->read_count = 0;
            }
	    }
        list_for_each(cur_node, &cc->write_list)
        {
	        ccm = list_entry(cur_node, struct cwr_cell_meta, rw_list);

            if(min_z_value > ccm->z_value) min_z_value = ccm->z_value;

            if(ccm->read_count > ccm->write_count)
            {
                ccm->read_count -= ccm->write_count;
                ccm->write_count = 0;
            }
            else
            {
                ccm->write_count -= ccm->read_count;
                ccm->read_count = 0;
            }
	    }

        /* clear z value,
         * migrate list node by its read count and write count
         */
        list_for_each_safe(cur_node, next_node, &cc->read_list)
        {
            ccm = list_entry(cur_node, struct cwr_cell_meta, rw_list);

            if(ccm->write_count > RW_STATE_THRESHOLD)
            {
                list_del_init(cur_node);
                list_add(cur_node, &cc->write_list);
            }
            else
            {
                ccm->z_value -= min_z_value;
            }
        }
        list_for_each_safe(cur_node, next_node, &cc->write_list)
        {
            ccm = list_entry(cur_node, struct cwr_cell_meta, rw_list);

            ccm->z_value -= min_z_value;

            if(ccm->read_count > RW_STATE_THRESHOLD)
            {
                list_del_init(cur_node);
                list_add(cur_node, &cc->read_list);
            }
        }

        /* sort hot cells to the front of lists */
        list_sort(0, &cc->read_list, list_sort_cmp);
        list_sort(0, &cc->write_list, list_sort_cmp);

        /* traverse read list and write list
         * to classify cells into 6 categoris:
         *   1. w -> c; 2. c -> w;
         *   3. w -> r; 4. r -> w;
         *   5. c -> r; 6. r -> c;
         * then we can pair them to work queue for migration
         * some cells won't be migrated:
         *   1. it's in accessing
         *   2. we can't find a pair for it
         */
        enqueue_pairs(cc);

        // reset io count after migration
        cc->io_count = 0;
        cc->old_io_count = 0;
    }

    /* restart timer */
    cc->cell_manage_timer.expires = jiffies + CELL_MANAGE_INTERVAL * HZ;
    add_timer(&cc->cell_manage_timer);
}

static inline sector_t get_cell_id(struct dm_target *dt,
                                   struct cwr_context *cc,
                                   struct bio *bio)
{
    return (bio->bi_sector - dt->begin) >> ilog2(cc->cell_size);
}

static void cwr_end_io(struct bio *bio, int error)
{
    // @error is set by bio_endio()
    struct cwr_bio_info *cbi = (struct cwr_bio_info *)bio->bi_private;
    unsigned long flag;

    bio->bi_sector = cbi->sector;
    bio->bi_bdev = cbi->bdev;
    bio->bi_end_io = cbi->end_io;
    bio->bi_private = cbi->private;

    spin_lock_irqsave(&cbi->cc->lock, flag);

    cbi->ccm->bio_count--;
    if(cbi->ccm->bio_count == 0)
        cbi->ccm->state &= !CELL_STATE_ACCESSING;

    spin_unlock_irqrestore(&cbi->cc->lock, flag);

    bio_endio(bio, error);
    mempool_free(cbi, bio_info_pool);
}

static int cwr_map(struct dm_target *dt, struct bio *bio, union map_info *mi)
{
    struct cwr_context *cc = (struct cwr_context *)dt->private;
    sector_t cell_id, seek_distance;
    int read_flag, write_flag;
    int z_value;
    //struct bio *cwr_bio;
    struct cwr_bio_info *cbi;
    unsigned long flag;

    cell_id = get_cell_id(dt, cc, bio);

    /* no need to protect io_count against race condition,
     * it could be imprecisely.
     */
    cc->io_count++;

    read_flag = bio_data_dir(bio) & READ;
    write_flag = bio_data_dir(bio) & WRITE;

    //printk(KERN_DEBUG "cwr: map cell id: %llu", cell_id); // DEBUG

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

    cbi = (struct cwr_bio_info *)mempool_alloc(bio_info_pool, GFP_NOIO);

    cbi->sector = bio->bi_sector;
    cbi->bdev = bio->bi_bdev;
    cbi->end_io = bio->bi_end_io;
    cbi->private = bio->bi_private;
    cbi->cc = cc;
    cbi->ccm = cc->cell_meta + cell_id;

    bio->bi_end_io = cwr_end_io;
    bio->bi_private = cbi;

    cc->cell_meta[cell_id].read_count += read_flag;
    cc->cell_meta[cell_id].write_count += write_flag;
    // when bio count reaches 0, we will change cwr state to ready.

    spin_lock_irqsave(&cc->lock, flag);

    cc->last_cell = cell_id;

    cc->cell_meta[cell_id].bio_count++;
    cc->cell_meta[cell_id].state |= CELL_STATE_ACCESSING;

    //if(cc->cell_meta[cell_id].state & CELL_STATE_MIGRATING)
    //{
        // pend bio when migrating
        //bio_list_add(&cc->cell_meta[cell_id].bio_list, cwr_bio);
        //spin_unlock(&cc->lock);
    //}
    //else
    //{
        spin_unlock_irqrestore(&cc->lock, flag);
        /* redirect bio */
        bio->bi_bdev = cc->cell_meta[cell_id].dev->bdev;
        bio->bi_sector = cc->cell_meta[cell_id].offset +
                         (bio->bi_sector & cc->cell_mask);
        generic_make_request(bio);
    //}

    return DM_MAPIO_SUBMITTED;
}

// dmsetup create test --table '0 1048576 cwr 16 /dev/sdc1 /dev/sdc2 /dev/sdc3'
static int cwr_ctr(struct dm_target *dt, unsigned int argc, char *argv[])
{
    struct cwr_context *cc;
    unsigned int cell_size, cell_amount;
    int re = 0;
    unsigned int i, j;

    if(argc != 4)
    {
        dt->error = "invalid argument count";
        return -EINVAL;
    }

    if(sscanf(argv[0], "%u", &cell_size) != 1)
    {
        dt->error = "cell size read error";
        return -EINVAL;
    }
    if(is_power_of_2(cell_size) == 0)
    {
        dt->error = "cell size is not power of 2";
        return -EINVAL;
    }
    cell_amount = dt->len >> ilog2(cell_size);

    cc = kzalloc(sizeof(struct cwr_context) +
                 sizeof(struct cwr_cell_meta) * cell_amount,
                 GFP_KERNEL);
    if(cc == 0)
    {
        dt->error = "cannot allocate cwr context";
        return -ENOMEM;
    }

    cc->io_client = dm_io_client_create(0); /* "0" needs verification */
    if(IS_ERR(cc->io_client))
    {
        re = PTR_ERR(cc->io_client);
        dt->error = "create dm io client fail.";
        goto io_client_fail;
    }

    cc->cell_size = cell_size;
    cc->cell_mask = cell_size - 1;

    cc->write_dev_size = WRITE_CACHE_SIZE * cc->cell_size;
    cc->read_dev_size = READ_CACHE_SIZE * cc->cell_size;
    cc->cold_dev_size = dt->len - cc->write_dev_size - cc->read_dev_size;

    /* get mapped targets */
    re |= dm_get_device(dt, argv[1], 0, cc->cold_dev_size,
                        dm_table_get_mode(dt->table), &cc->cold_dev);
    re |= dm_get_device(dt, argv[2], 0, cc->write_dev_size,
                        dm_table_get_mode(dt->table), &cc->write_dev);
    re |= dm_get_device(dt, argv[3], 0, cc->read_dev_size,
                        dm_table_get_mode(dt->table), &cc->read_dev);
    if(re != 0) goto device_invalid;

    // disk size check
    // i_size_read returns device size by bytes
    if((i_size_read(cc->cold_dev->bdev->bd_inode) < (cc->cold_dev_size << 9))
    || (i_size_read(cc->write_dev->bdev->bd_inode) < (cc->write_dev_size << 9))
    || (i_size_read(cc->read_dev->bdev->bd_inode) < (cc->read_dev_size << 9)))
    {
        dt->error = "disk is too small";
        re = -EINVAL;
        goto size_invalid;
    }
    // alignment check (may be reduntant)
    if((cc->cold_dev_size & (cc->cell_size - 1))
    || (cc->write_dev_size & (cc->cell_size - 1))
    || (cc->read_dev_size & (cc->cell_size - 1)))
    {
        dt->error = "disk size is not alignt";
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
        cc->cell_meta[i].offset = j * cell_size;

        INIT_LIST_HEAD(&cc->cell_meta[i].rw_list);
        INIT_LIST_HEAD(&cc->cell_meta[i].class_list);
        bio_list_init(&cc->cell_meta[i].bio_list);

        list_add(&cc->cell_meta[i].rw_list, &cc->write_list);
    }
    // map second logical part to read cache
    for(j = 0; j < READ_CACHE_SIZE; i++, j++)
    {
        cc->cell_meta[i].dev = cc->read_dev;
        cc->cell_meta[i].offset = j * cell_size;

        INIT_LIST_HEAD(&cc->cell_meta[i].rw_list);
        INIT_LIST_HEAD(&cc->cell_meta[i].class_list);
        bio_list_init(&cc->cell_meta[i].bio_list);

        list_add(&cc->cell_meta[i].rw_list, &cc->read_list);
    }
    // map last logical part to cold hdd
    for(j = 0; i < cell_amount; i++, j++)
    {
        cc->cell_meta[i].dev = cc->cold_dev;
        cc->cell_meta[i].offset = j * cell_size;

        INIT_LIST_HEAD(&cc->cell_meta[i].rw_list);
        INIT_LIST_HEAD(&cc->cell_meta[i].class_list);
        bio_list_init(&cc->cell_meta[i].bio_list);

        // conside it as write oriented data
        list_add(&cc->cell_meta[i].rw_list, &cc->write_list);
    }

    INIT_WORK(&cc->swap_work, swap_worker);

    init_timer(&cc->cell_manage_timer);
    cc->cell_manage_timer.data = (unsigned long)cc;
    cc->cell_manage_timer.function = cell_manager;
    cc->cell_manage_timer.expires = jiffies + CELL_MANAGE_INTERVAL * HZ;
    //add_timer(&cc->cell_manage_timer);

    spin_lock_init(&cc->lock);

    dt->private = cc;

    printk(KERN_DEBUG "cwr: a device is constructed.");
    printk(KERN_DEBUG "    cell size: %llu", cc->cell_size);
    printk(KERN_DEBUG "    c: %s, w:%s, r:%s",
           cc->cold_dev->name, cc->write_dev->name, cc->read_dev->name);
    return 0;

size_invalid:
device_invalid:
    if(cc->cold_dev) dm_put_device(dt, cc->cold_dev);
    if(cc->write_dev) dm_put_device(dt, cc->write_dev);
    if(cc->read_dev) dm_put_device(dt, cc->read_dev);
io_client_fail:
    kfree(cc);
    return re;
}

static void cwr_dtr(struct dm_target *dt)
{
    struct cwr_context *cc = (struct cwr_context *)dt->private;

    flush_workqueue(swap_work_queue);
    del_timer_sync(&cc->cell_manage_timer);
    dm_io_client_destroy(cc->io_client);

    dm_put_device(dt, cc->cold_dev);
    dm_put_device(dt, cc->write_dev);
    dm_put_device(dt, cc->read_dev);
    kfree(cc);

    printk(KERN_DEBUG "cwr: a device is destructed.");
    printk(KERN_DEBUG "    c: %s, w:%s, r:%s",
           cc->cold_dev->name, cc->write_dev->name, cc->read_dev->name);
}

static struct target_type cwr_target = {
	.name    = "cwr",
	.version = {0, 9, 0},
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
    // mempool_create will success on return
    bio_info_pool = mempool_create(MIN_BIO_INFO_AMOUNT,
                                   mempool_alloc_slab, mempool_free_slab,
                                   bio_info_cache);

    swap_work_queue = create_workqueue("CWR_WORK_QUEUE");
    if(swap_work_queue == 0)
    {
        printk(KERN_ERR "create work queue fail.");
        re = -ENOMEM;
        goto work_queue_fail;
    }

    re = dm_register_target(&cwr_target);
    if(re < 0)
    {
        printk(KERN_ERR "regist cwr target fail.");
        goto register_target_fail;
    }

    printk(KERN_DEBUG "cwr loaded.");
    return 0;

register_target_fail:
    destroy_workqueue(swap_work_queue);
work_queue_fail:
    mempool_destroy(bio_info_pool);
    kmem_cache_destroy(bio_info_cache);

    return re;
}

static void __exit cwr_done(void)
{
    dm_unregister_target(&cwr_target);
    destroy_workqueue(swap_work_queue);
    mempool_destroy(bio_info_pool);
    kmem_cache_destroy(bio_info_cache);
    printk(KERN_DEBUG "cwr exited.");
}

module_init(cwr_init);
module_exit(cwr_done);
MODULE_LICENSE("GPL");
