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
        bio->bi_sector = ccm->offset * cc->cell_size;
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
    unsigned int tmp_int;
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
    tmp_int = csi->ccm1->z_value;
    csi->ccm1->z_value = csi->ccm2->z_value;
    csi->ccm2->z_value = tmp_int;

    tmp_int = csi->ccm1->read_count;
    csi->ccm1->read_count = csi->ccm2->read_count;
    csi->ccm2->read_count = tmp_int;

    tmp_int = csi->ccm1->write_count;
    csi->ccm1->write_count = csi->ccm2->write_count;
    csi->ccm2->write_count = tmp_int;

    tmp_dev = csi->ccm1->dev;
    csi->ccm1->dev = csi->ccm2->dev;
    csi->ccm2->dev = tmp_dev;

    tmp_offset = csi->ccm1->offset;
    csi->ccm1->offset = csi->ccm2->offset;
    csi->ccm2->offset = tmp_offset;

    finish_pending_bio(cc, csi->ccm1);
    finish_pending_bio(cc, csi->ccm2);

exit_swap:
    if(mem1) vfree(mem1);
    if(mem2) vfree(mem2);
    kfree(csi);
}

static inline void enqueue_pair(struct cwr_cell_meta *ccm1,
                                struct cwr_cell_meta *ccm2,
                                struct cwr_context *cc)
{
    struct cwr_swap_info *csi;

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

    /* cell classify */
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
    unsigned int io_frenquency;

    io_frenquency = (cc->io_count - cc->old_io_count) / CELL_MANAGE_INTERVAL;
    cc->old_io_count = cc->io_count;

    // trigger under certain conditions
    if(cc->io_count >= IO_COUNT_THRESHOLD &&
       io_frenquency <= CELL_MANAGE_THRESHOLD)
    {
        /* clear read count and write count to prevent overflow */
        list_for_each(cur_node, &cc->read_list)
        {
	        ccm = list_entry(cur_node, struct cwr_cell_meta, rw_list);
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

        /* migrate list node by its read count and write count */
        list_for_each_safe(cur_node, next_node, &cc->read_list)
        {
            ccm = list_entry(cur_node, struct cwr_cell_meta, rw_list);
            if(ccm->write_count > RW_STATE_THRESHOLD)
            {
                list_del_init(cur_node);
                list_add(cur_node, &cc->write_list);
            }
        }
        list_for_each_safe(cur_node, next_node, &cc->write_list)
        {
            ccm = list_entry(cur_node, struct cwr_cell_meta, rw_list);
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
    return (bio->bi_sector - dt->begin) >> (cc->cell_size - 1);
}

static void cwr_end_io(struct bio *bio, int error)
{
    // @error is set by bio_endio()
    struct cwr_bio_info *cbi = (struct cwr_bio_info*)bio->bi_private;
    struct bio *origin_bio = cbi->bio;
    struct cwr_context *cc = cbi->cc;
    sector_t cell_id = get_cell_id(cbi->dt, cc, bio);

    cc->cell_meta[cell_id].bio_count--;
    if(cc->cell_meta[cell_id].bio_count == 0)
    {
        // remove state accessing
        cc->cell_meta[cell_id].state &= !CELL_STATE_ACCESSING;
        // the cell finshed all operations
        if(!(cc->cell_meta[cell_id].state & CELL_STATE_MIGRATING))
            cc->cell_meta[cell_id].state = CELL_STATE_READY;
    }

    bio_endio(origin_bio, error);
    mempool_free(cbi, bio_info_pool);
}

static int cwr_map(struct dm_target *dt, struct bio *bio, union map_info *mi)
{
    struct cwr_context *cc = (struct cwr_context*)dt->private;
    sector_t cell_id, seek_distance;
    int read_flag, write_flag;
    unsigned int z_value;
    struct bio *cwr_bio;
    struct cwr_bio_info *cbi;

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

    /* clone bio to handle cell states */
    cwr_bio = bio_clone(bio, GFP_NOIO);
    if(cwr_bio == 0) return DM_MAPIO_REQUEUE; // try again later
    cbi = (struct cwr_bio_info *)mempool_alloc(bio_info_pool, GFP_NOIO);

    cbi->bio = bio;
    cbi->dt = dt;
    cbi->cc = cc;

    cwr_bio->bi_end_io = cwr_end_io;
    cwr_bio->bi_private = cbi;

    // when bio count reaches 0, we will change cwr state to ready.
    cc->cell_meta[cell_id].bio_count++;

    if(cc->cell_meta[cell_id].state & CELL_STATE_MIGRATING)
    {
        // pend bio when migrating
        bio_list_add(&cc->cell_meta[cell_id].bio_list, cwr_bio);
    }
    else
    {
        /* remove state ready, add state accessing */
        cc->cell_meta[cell_id].state &= !CELL_STATE_READY;
        cc->cell_meta[cell_id].state |= CELL_STATE_ACCESSING;

        /* redirect bio */
        cwr_bio->bi_bdev = cc->cell_meta[cell_id].dev->bdev;
        cwr_bio->bi_sector = cc->cell_meta[cell_id].offset * cc->cell_size;
        generic_make_request(cwr_bio);
    }

    return DM_MAPIO_SUBMITTED;
}

/*
 * dmsetup create dev_name --table
 * '0 102400 cwr cell_size /dev/hdd /dev/write_cache /dev/read-cache'
 */
static int cwr_ctr(struct dm_target *dt, unsigned int argc, char *argv[])
{
    struct cwr_context *cc;
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

    cc->io_client = dm_io_client_create(0); /* "0" needs verification */
    if(IS_ERR(cc->io_client))
    {
        re = PTR_ERR(cc->io_client);
        dt->error = "cwr: create dm io client fail.";
        goto io_client_fail;
    }

    /* get mapped targets */
    re |= dm_get_device(dt, argv[1], 0, cc->cold_dev_size,
                        dm_table_get_mode(dt->table), &cc->cold_dev);
    re |= dm_get_device(dt, argv[2], 0, cc->write_dev_size,
                        dm_table_get_mode(dt->table), &cc->write_dev);
    re |= dm_get_device(dt, argv[3], 0, cc->read_dev_size,
                        dm_table_get_mode(dt->table), &cc->read_dev);
    if(re != 0) goto device_invalid;

    cc->cell_size = cell_size;

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
        dt->error = "cwr: disk size is not alignt";
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
        cc->cell_meta[i].state = CELL_STATE_READY;
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
        cc->cell_meta[i].state = CELL_STATE_READY;
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
        cc->cell_meta[i].state = CELL_STATE_READY;
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
    add_timer(&cc->cell_manage_timer);

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
io_client_fail:
    kfree(cc);
    return re;
}

static void cwr_dtr(struct dm_target *dt)
{
    struct cwr_context *cc = (struct cwr_context*)dt->private;

    flush_workqueue(swap_work_queue);
    del_timer_sync(&cc->cell_manage_timer);
    dm_io_client_destroy(cc->io_client);

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
