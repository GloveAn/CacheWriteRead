#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/bio.h>
#include <linux/device-mapper.h>
#include <linux/list.h>
#include <linux/log2.h>

// note: sector_t = u64 = unsigned long long

#define WRITE_CACHE_SIZE 200
#define READ_CACHE_SIZE 200

/// seek value =
/// MAX(24bit) {
///     [max(0, k - unit_size) *
///     (read_flag + t * write_flag) /
///     k
/// } *
/// (T1 + T2 * seek_distance)
/// note: that the kernel does not support float arithmetic,
#define K  512
#define T   90/100
#define T1   1
#define T2   100

/// data could be stored on hdd(COLD), SSD(CACHED for READ or WRITE)
#define COLD        1 << 0
#define WRITE_CACHE 1 << 1
#define READ_CACHE  1 << 2

// when io actions accumulate to the threshold,
// we will move hot data blocks to ssd.
#define IO_COUNT_THRESHOLD 10000
// when the delta value of read count and write count exceed this threshold,
// we move data block from one list to the other.
#define RW_STATE_THRESHOLD 20

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
    /// measured by sector
    sector_t unit_size;
    sector_t last_unit;

    struct dm_dev* cold_dev;
    struct dm_dev* read_dev;
    struct dm_dev* write_dev;

    /// measured by sector
    sector_t cold_dev_size;
    sector_t read_dev_size;
    sector_t write_dev_size;

    struct cwr_unit_meta* unit_meta;
    struct list_head read_list;
    struct list_head write_list;

    unsigned int io_count;
};

/// list sort functions, code snippet from linux 4.4.2
#define MAX_LIST_LENGTH_BITS 20

/*
 * Returns a list organized in an intermediate format suited
 * to chaining of merge() calls: null-terminated, no reserved or
 * sentinel head node, "prev" links not maintained.
 */
static struct list_head *merge(void *priv,
				int (*cmp)(void *priv, struct list_head *a,
					struct list_head *b),
				struct list_head *a, struct list_head *b)
{
	struct list_head head, *tail = &head;

	while (a && b) {
		/* if equal, take 'a' -- important for sort stability */
		if ((*cmp)(priv, a, b) <= 0) {
			tail->next = a;
			a = a->next;
		} else {
			tail->next = b;
			b = b->next;
		}
		tail = tail->next;
	}
	tail->next = a?:b;
	return head.next;
}

/*
 * Combine final list merge with restoration of standard doubly-linked
 * list structure.  This approach duplicates code from merge(), but
 * runs faster than the tidier alternatives of either a separate final
 * prev-link restoration pass, or maintaining the prev links
 * throughout.
 */
static void merge_and_restore_back_links(void *priv,
				int (*cmp)(void *priv, struct list_head *a,
					struct list_head *b),
				struct list_head *head,
				struct list_head *a, struct list_head *b)
{
	struct list_head *tail = head;
	u8 count = 0;

	while (a && b) {
		/* if equal, take 'a' -- important for sort stability */
		if ((*cmp)(priv, a, b) <= 0) {
			tail->next = a;
			a->prev = tail;
			a = a->next;
		} else {
			tail->next = b;
			b->prev = tail;
			b = b->next;
		}
		tail = tail->next;
	}
	tail->next = a ? : b;

	do {
		/*
		 * In worst cases this loop may run many iterations.
		 * Continue callbacks to the client even though no
		 * element comparison is needed, so the client's cmp()
		 * routine can invoke cond_resched() periodically.
		 */
		if (unlikely(!(++count)))
			(*cmp)(priv, tail->next, tail->next);

		tail->next->prev = tail;
		tail = tail->next;
	} while (tail->next);

	tail->next = head;
	head->prev = tail;
}

/**
 * list_sort - sort a list
 * @priv: private data, opaque to list_sort(), passed to @cmp
 * @head: the list to sort
 * @cmp: the elements comparison function
 *
 * This function implements "merge sort", which has O(nlog(n))
 * complexity.
 *
 * The comparison function @cmp must return a negative value if @a
 * should sort before @b, and a positive value if @a should sort after
 * @b. If @a and @b are equivalent, and their original relative
 * ordering is to be preserved, @cmp must return 0.
 */
void list_sort(void *priv, struct list_head *head,
		int (*cmp)(void *priv, struct list_head *a,
			struct list_head *b))
{
	struct list_head *part[MAX_LIST_LENGTH_BITS+1]; /* sorted partial lists
						-- last slot is a sentinel */
	int lev;  /* index into part[] */
	int max_lev = 0;
	struct list_head *list;

	if (list_empty(head))
		return;

	memset(part, 0, sizeof(part));

	head->prev->next = NULL;
	list = head->next;

	while (list) {
		struct list_head *cur = list;
		list = list->next;
		cur->next = NULL;

		for (lev = 0; part[lev]; lev++) {
			cur = merge(priv, cmp, part[lev], cur);
			part[lev] = NULL;
		}
		if (lev > max_lev) {
			if (unlikely(lev >= ARRAY_SIZE(part)-1)) {
				//printk_once(KERN_DEBUG "list too long for efficiency\n");
				lev--;
			}
			max_lev = lev;
		}
		part[lev] = cur;
	}

	for (lev = 0; lev < max_lev; lev++)
		if (part[lev])
			list = merge(priv, cmp, part[lev], list);

	merge_and_restore_back_links(priv, cmp, head, part[max_lev], list);
}

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

static void do_io_count(struct cwr_context* cc)
{
    struct list_head *cur_node, *next_node;
    struct cwr_unit_meta* cur_unit;

    // we don't need to protect io count against race condition,
    // as we don't need to perform swap action precisely.
    cc->io_count++;
    if(cc->io_count >= IO_COUNT_THRESHOLD)
    {
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
        }
        list_for_each_safe(cur_node, next_node, &cc->write_list)
        {
            cur_unit = list_entry(cur_node, struct cwr_unit_meta, list);
            if(cur_unit->read_count > RW_STATE_THRESHOLD)
            {
                list_del_init(cur_node);
                list_add(cur_node, &cc->read_list);
            }
        }

        /// sort hot data to the front of lists
        list_sort(0, &cc->read_list, list_sort_cmp);
        list_sort(0, &cc->write_list, list_sort_cmp);

        cc->io_count = 0;
    }
}

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
        do_div(z_value, K); // do_div: int64/int32
        if(z_value >= 0xFFFFFF) z_value = 0xFFFFFF;
        do_div(seek_distance, T2);
        z_value = (T1 + seek_distance) << z_value;
    }

    cc->unit_meta[unit_index].read_count += read_flag;
    cc->unit_meta[unit_index].write_count += write_flag;
    cc->unit_meta[unit_index].z_value += z_value;

    bio->bi_bdev = cc->unit_meta[unit_index].dev->bdev;
    bio->bi_sector = cc->unit_meta[unit_index].offset * cc->unit_size;

    do_io_count(cc);

    return DM_MAPIO_REMAPPED;
}

/*
 * dmsetup create dev_name --table
 * '0 102400 cwr unit_size /dev/hdd /dev/write_cache /dev/read-cache'
 */
static int cwr_ctr(struct dm_target *dt, unsigned int argc, char *argv[])
{
    struct cwr_context* cc;
    int re = 0;
    int unit_amount, i, j;

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
    re |= dm_get_device(dt, argv[3], 0, cc->read_dev_size,
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
    cc->io_count = 0;

    INIT_LIST_HEAD(&cc->read_list);
    INIT_LIST_HEAD(&cc->write_list);
    i = 0;
    // map first part to read cache
    for(j = 0; j < READ_CACHE_SIZE; i++, j++)
    {
        cc->unit_meta[i].dev = cc->read_dev;
        cc->unit_meta[i].offset = j;

        INIT_LIST_HEAD(&cc->unit_meta[i].list);
        list_add(&cc->unit_meta[i].list, &cc->read_list);
    }
    // map second part to write cache
    for(j = 0; j < WRITE_CACHE_SIZE; i++, j++)
    {
        cc->unit_meta[i].dev = cc->write_dev;
        cc->unit_meta[i].offset = j;

        INIT_LIST_HEAD(&cc->unit_meta[i].list);
        list_add(&cc->unit_meta[i].list, &cc->write_list);
    }
    // map last part to cold hdd
    for(j = 0; i < unit_amount; i++, j++)
    {
        cc->unit_meta[i].dev = cc->cold_dev;
        cc->unit_meta[i].offset = j;

        INIT_LIST_HEAD(&cc->unit_meta[i].list);
        // initially add all nodes to read list
        list_add(&cc->unit_meta[i].list, &cc->write_list);
    }

    dt->private = cc;

    printk(KERN_DEBUG "a new cwr device is constructed.");
    printk(KERN_DEBUG "unit size: %llu", cc->unit_size);
    printk(KERN_DEBUG "c: %s, w:%s, r:%s",
           cc->cold_dev->name, cc->write_dev->name, cc->read_dev->name);
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

    re = dm_register_target(&cwr_target);
    if(re < 0)
    {
        printk(KERN_ERR "regist cwr target fail.");
        return re;
    }

    printk(KERN_DEBUG "cwr loaded.");
    return 0;
}

static void __exit cwr_done(void)
{
    dm_unregister_target(&cwr_target);
    printk(KERN_DEBUG "cwr unloaded.");
}

module_init(cwr_init);
module_exit(cwr_done);
MODULE_LICENSE("GPL");
