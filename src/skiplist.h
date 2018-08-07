/*
 * Implement Lock-free skiplist based VM.
 *
 * Copyright (C) 2017 Miao Cai <miaogecm@gmail.com>, Shenming Liu <liushenming2@gmail.com>
 * Nanjing University
 *
 * Implementation of the lock-free skiplist data-structure created by Maurice Herlihy,
 * Yossi Lev, and Nir Shavit. See Herlihy's and Shivit's book "The Art of Multiprocessor 
 * Programming" revised reprint. 
 * https://www.amazon.com/Art-Multiprocessor-Programming-Revised-Reprint/dp/0123973376/
 *
 * See also Kir Fraser's dissertation "Practical Lock Freedom".
 * www.cl.cam.ac.uk/techreports/UCAM-CL-TR-579.pdf
 *
 * I've generalized the data structure to support update operations like set() and 
 * CAS() in addition to the normal add() and remove() operations.
 *
 * Warning: This code is written for the x86 memory-model. The algorithim depends 
 * on certain stores and loads being ordered. This code won't work correctly on 
 * platforms with weaker memory models if you don't add memory barriers in the right 
 * places.
 */

#ifndef SKIPLIST_H
#define SKIPLIST_H

/* Setting MAX_LEVELS to 1 essentially makes this data structure the Harris-Michael lock-free list. */
#define MAX_LEVELS 5
#define MAX_NUM_THREADS 32

#define E_KEY_NOT_EXIST		100
#define E_KEY_IN_MAP		101

#define DOES_NOT_EXIST	0


typedef size_t markable_t;

typedef struct {
	unsigned long start;
	unsigned long end;
} map_key_t;  //表示一个vma的地址范围

/* 32 bytes */
struct sl_node {
    map_key_t key;
    unsigned int num_levels;
    int ref_counter;  //用于删除节点时的引用计数
    markable_t *next;  //(指针stealing)，next域是一个有num_levels个markable_t元素的数组，每一个数组的元素，都指向一个sl_node
} __attribute__((aligned(sizeof(long))));

/* 40 bytes */
struct skiplist {
    struct sl_node head;
    int high_water; /* max historic number of levels */
    struct hp_item* HP[MAX_NUM_THREADS];
    int count_of_hp;  /* upper bound of hps in the system. */
} __attribute__((aligned(sizeof(long))));

#define TAG_VALUE(v, tag) ((v) |  tag)
#define IS_TAGGED(v, tag) ((v) &  tag)
#define STRIP_TAG(v, tag) ((v) & ~tag)


#define     MARK_NODE(x) TAG_VALUE((markable_t)(x), 0x1)
#define      HAS_MARK(x) (IS_TAGGED((x), 0x1) == 0x1)  
//x是一个markable_t的值
#define      GET_NODE(x) ((struct sl_node *)(x))
#define    STRIP_MARK(x) ((struct sl_node *)STRIP_TAG((x), 0x1))

#define sl_entry(ptr, type, member) \
	container_of(ptr, type, member)


/**
 * sl_for_each_safe - iterate over a skiplist safe against removal of skiplist entry
 * @pos:	the &struct sl_node to use as a loop cursor.
 * @n:		another &struct sl_node to use as temporary storage
 * @head:	the head for your skiplist.
 */
#define sl_for_each_safe(pos, n, head) \
	for (pos = GET_NODE(head->next[0]), n = GET_NODE(pos->next[0]); pos; \
		pos = n, n = GET_NODE(pos->next[0]))

/**
 * sl_first_entry - get the first element from a skiplist
 * @ptr:	the sl node to take the element from.
 * @type:	the type of the struct this is embedded in.
 * @member:	the name of the sl_node within the struct.
 *
 * Note, that skiplist is expected to be not empty.
 */
#define sl_first_entry(ptr, type, member) \
	sl_entry(GET_NODE((ptr)->next[0]), type, member)

/**
 * sl_next_entry - get the next element in skiplist
 * @pos:	the type * to cursor
 * @member:	the name of the sl_node within the struct.
 */
#define sl_next_entry(pos, member) \
	(&pos->member ? sl_entry(GET_NODE((pos)->member.next[0]), typeof(*(pos)), member) : NULL)

/**
 * sl_for_each_entry - iterate over skiplist of given type
 * @pos:	the type * to use as a loop cursor.
 * @head:	the head for your skiplist.
 * @member:	the name of the sl_node within the struct.
 */
#define sl_for_each_entry(pos, head, member)				\
	for (pos = sl_first_entry(head, typeof(*pos), member);	\
	     pos->member.next[0];								\
	     pos = sl_next_entry(pos, member))

/**
 * sl_for_each_entry_safe - iterate over skiplist of given type safe against removal of skiplist entry
 * @pos:	the type * to use as a loop cursor.
 * @n:		another type * to use as temporary storage
 * @head:	the head for your skiplist.
 * @member: the name of the sl_node within the struct.
 */
#define sl_for_each_entry_safe(pos, n, head, member)			\
	for (pos = sl_first_entry(head, typeof(*pos), member),		\
		 n = sl_next_entry(pos, member);						\
		 &(pos->member);										\
		 pos = n, n = sl_next_entry(n, member))

/*
 * empty_skiplist - return true if skiplist is empty 
 */
static inline int empty_skiplist(struct skiplist *sl)
{
	return sl->head.next[0] ? 0 : 1;  //next[0]是bottom-level的节点。
}

#if 1
#define sl_node_free(node)		\
	free((node)->next)
#endif

enum {
	SLKEY_LEFT = -1,
	SLKEY_EQUAL,
	SLKEY_RIGHT
};

/*
 * key_cmp - compare the two given keys, return EQUAL if  
 * key->start <= to_cmp->start <= to_cmp->end <= key->end, return LEFT if 
 * to_cmp->start <= to_cmp->end <= key->start < key->end, return RIGHT if
 * key->start < key->end <= to_cmp->start <= to_cmp->end
 * @key:    key in the skiplist.
 * @to_cmp: key need to compared.
 */
static inline int key_cmp(map_key_t *key, map_key_t *to_cmp)
{
	if ((key->start <= to_cmp->start) && (to_cmp->end <= key->end))
		return SLKEY_EQUAL;
	
	if (key->end > to_cmp->start) {
		/* Fail if an existing vma overlaps the area */
		if (key->start < to_cmp->end)   //不可比的情况，直接返回-1表示错误
			return -1;
		return SLKEY_LEFT;
	} else
		return SLKEY_RIGHT;
}

extern void init_sl(struct skiplist *sl);
extern void free_sl(struct skiplist *sl);
//extern struct sl_node *sl_lookup(struct skiplist * sl, map_key_t key, struct sl_node **preds, struct sl_node **succs, int level, int tid);
extern struct sl_node *sl_lookup(struct skiplist *sl, map_key_t key, int n, int tid);
extern int sl_insert(struct skiplist *sl, struct sl_node *node, map_key_t key, int verbose, int tid);
extern int sl_remove(struct skiplist *sl, map_key_t key, int verbose, int tid);
extern map_key_t sl_min_key(struct skiplist *sl);
extern void sl_print(struct skiplist *sl, int verbose);
extern void sl_print2(struct skiplist *sl, int verbose);
extern void sl_print_lsm(struct skiplist *sl);

#define HP_K 2
typedef size_t hp_t;

struct hp_rnode;

//全局HP链表中的项，每一个thread对应其中的一个
struct hp_item {
	hp_t hp0[MAX_LEVELS];    //hp的值
	hp_t hp1[MAX_LEVELS];
	struct hp_rnode* d_list;  //本thread的d_list头节点
	unsigned int d_count;   //d_list中的节点数，特例是对于头结点hp_list来说
} __attribute__((aligned(sizeof(long))));

//the retire nodes are organized in a list headed by hp_item->d_list.
struct hp_rnode {
        hp_t address;
        struct hp_rnode* next;
} __attribute__((aligned(sizeof(long))));


extern struct hp_item* hp_item_setup(struct skiplist* sl, int tid);
extern void hp_setdown(struct skiplist* sl);
extern void hp_save_addr(struct hp_item* hp, int level, int index, hp_t hp_addr);
extern void hp_clear_addr(struct hp_item* hp, int level, int index);
extern hp_t hp_get_addr(struct hp_item* hp, int level, int index);
extern void hp_clear_all_addr(struct hp_item* hp);
extern void hp_dump_statics(struct skiplist* sl);

extern void hp_retire_node(struct skiplist* sl, struct hp_item* hp, hp_t hp_addr);
extern void hp_retire_hp_item(struct skiplist* sl, int tid);

void hp_scan(struct skiplist* sl, struct hp_item* hp);
unsigned int hp_R(unsigned int R);
#endif /* SKIPLIST_H */


