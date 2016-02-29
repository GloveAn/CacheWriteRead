#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/bio.h>
#include <linux/device-mapper.h>
#include <linux/dm-io.h>
#include <linux/list.h>
#include <linux/log2.h>
#include <linux/timer.h>
#include <linux/sched.h>
#include <linux/spinlock.h>
#include <asm/atomic.h>

// NOTE: sector_t = u64 = unsigned long long

/* unit in cell */
#define WRITE_CACHE_SIZE 200
#define READ_CACHE_SIZE  200

/*
 * z value = MAX(24bit) {
 *      [max(0, k - unit_size) *
 *      (read_flag + t * write_flag) /
 *      k
 *  } * (T1 + T2 * seek_distance)
 *  NOTE: the kernel does not support float arithmetic,
 */
#define K  512
#define T   90/100
#define T1   1
#define T2   100

// when io actions reaches the threshold, move hot data cells to ssd.
#define IO_COUNT_THRESHOLD 10000
// when the delta value of read count and write count exceeds this threshold,
// move data cell from one list to the other.
#define RW_STATE_THRESHOLD 20

// time interval for managing data cells
#define CELL_MANAGE_INTERVAL  20 // unit in second
// io workload to trigger data cells management
#define CELL_MANAGE_THRESHOLD 0

/* cell state for handling bio request */
#define CELL_STATE_READY     0
#define CELL_STATE_ACCESSING 1 << 0
#define CELL_STATE_MIGRATING 1 << 1

/* bio info minimal available amount */
#define MIN_BIO_INFO_AMOUNT 256

struct cwr_swap_info
{
    struct cwr_cell_meta *ccm1;
    struct cwr_cell_meta *ccm2;
    struct cwr_context *cc;
    struct list_head swap_list;
};

struct cwr_bio_info
{
    struct bio *bio;
    struct dm_target *dt;
    struct cwr_context *cc;
};

struct cwr_cell_meta
{
    // array index implies cell id / logical sector
    unsigned int z_value; // represent data hotness

    unsigned int read_count;
    unsigned int write_count;
    atomic_t bio_count; // for managing cwr state

    struct dm_dev *dev;
    sector_t offset; // physical location on device, unit in sector

    unsigned int state;

    struct list_head rw_list; // list for sorting by data hotness / z value
    struct list_head class_list; // list for migration
    struct bio_list bio_list; // list for waiting for migration
};

struct cwr_context
{
    sector_t cell_size; // unit in sector
    sector_t last_cell; // unit in sector, for calculating z value

    struct dm_dev *cold_dev;
    struct dm_dev *read_dev;
    struct dm_dev *write_dev;

    sector_t cold_dev_size; // unit in sector
    sector_t read_dev_size; // unit in sector
    sector_t write_dev_size; // unit in sector

    struct cwr_cell_meta *cell_meta;
    struct list_head read_list; // store read oriented cells
    struct list_head write_list; // store write oriented cells

    unsigned int io_count;
    unsigned int old_io_count; // for calculating io frequency

    struct work_struct swap_work;
    struct dm_io_client *io_client;

    struct timer_list cell_manage_timer; // for cell management

    spinlock_t lock;
};

/* START // code snippet from linux 4.4.2: linked list sort functions, */
#define MAX_LIST_LENGTH_BITS 20
static struct list_head *merge(void *priv,
				int (*cmp)(void *priv, struct list_head *a,
					struct list_head *b),
				struct list_head *a, struct list_head *b)
{
	struct list_head head, *tail = &head;
	while (a && b) {
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
static void merge_and_restore_back_links(void *priv,
				int (*cmp)(void *priv, struct list_head *a,
					struct list_head *b),
				struct list_head *head,
				struct list_head *a, struct list_head *b)
{
	struct list_head *tail = head;
	u8 count = 0;
	while (a && b) {
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
		if (unlikely(!(++count)))
			(*cmp)(priv, tail->next, tail->next);

		tail->next->prev = tail;
		tail = tail->next;
	} while (tail->next);

	tail->next = head;
	head->prev = tail;
}
void list_sort(void *priv, struct list_head *head,
		int (*cmp)(void *priv, struct list_head *a,
			struct list_head *b))
{
	struct list_head *part[MAX_LIST_LENGTH_BITS+1];
	int lev;
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
/* END // code snippet from linux 4.4.2: linked list sort functions, */
