/*
 * Implement concurrent skiplist based VM.
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

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <pthread.h>
#include "skiplist.h"
#include "utils.h"


enum unlink {
    FORCE_UNLINK,
    ASSIST_UNLINK,
    DONT_UNLINK
};

struct skiplist* sl;

/*
 * 获取一个int类型的随机数
 */
unsigned int get_random_int(void){
	static int seed_set = 1;
	if (seed_set){
		seed_set = 0;
		srand((unsigned)time(NULL));	
	}
	return rand();
}


/*
 * 用于向skiplist中插入一个节点时，获取一个随机的level值
 */
static int random_levels(struct skiplist *sl) 
{
    unsigned int r = get_random_int();
    int z = COUNT_TRAILING_ZEROS(r);
    //int levels = (int)(z / 2);
    int levels = z;
	
    if (levels == 0)
        return 1;
	
    if (levels > MAX_LEVELS) 
	levels = MAX_LEVELS;
    if (levels > sl->high_water) {  //此处可见skiplist的high_water是+1，+1地涨起来的
        levels = SYNC_ADD(&sl->high_water, 1);
        //levels = 1;  //for test
		sl_debug("random_levels(): increased high water mark to %d, levels = %d.\n", sl->high_water, levels);
    }
	
    return levels;
}

int node_count = 0;
int alloc_count = 0;
int retire_count = 0;
int insert_lp_count = 0; //linearization point
int remove_lp_count = 0; //linearization point

static struct sl_node *node_alloc(int num_levels, map_key_t *key, struct sl_node *node)
{
    struct sl_node *item = node;
    size_t sz;
	
    sz = num_levels * sizeof(struct sl_node *);

    item->next = (markable_t *)malloc(sz);
    memset(item->next, 0, sz);  //need!
    item->key.start  = key->start;
    item->key.end    = key->end;
    item->num_levels = num_levels;
    item->ref_counter = num_levels;
    sl_debug("thread-%lx node_alloc(): address = %p, key = [%lu,%lu], level = %d.\n", pthread_self(), (void*)item, key->start, key->end, num_levels);
    SYNC_ADD(&node_count, 1);
    SYNC_ADD(&alloc_count, 1);
    return item;
}

/*
 * 创建一个santinel节点作为head节点，head节点的level是MAX_LEVELS
 */
void init_sl(struct skiplist *sl)
{
    memset(sl, 0, sizeof(struct skiplist)); //need! because the HP array must be clean.
    map_key_t key = { .start = 0, .end = 0 };
    sl->high_water = 1;
    node_alloc(MAX_LEVELS, &key, &sl->head);
}

void free_sl(struct skiplist *sl)
{
    hp_setdown(sl);
    //pred = &sl->head;
    //curr = GET_NODE(STRIP_MARK(pred->next[level]));
	//free(sl->head.next);  //把head sentinel的节点删除即可?
}

/* 
 * find_preds -	search the <sl> with <key>, fill the array <preds> and <succs>,
 * return item with <key>, else NULL if it doesn't exist.
 * @preds:	predecessor array need to be filled.  preds[i]是key对应的节点在level-i层的前驱
 * @succs:	successor array need to be filled.  succs[i]是key对应的节点在level-i层的后继
 * @n:		we record the <preds> and <succs> between level [0, n-1].  //即要查找的是n以下level的对应node的前驱和后继
 * @sl:		skiplist
 * @key:	key to be found.
 * @unlink:	DONT_UNLINK: skip over the marked items and don't remove them.
 *			ASSIST_UNLINK: remove the items while traversing.
 * 			FORCE_UNLINK: skip over the key even it isn't marked as removable.
 */
#if 1
static struct sl_node *find_preds(struct sl_node **preds, struct sl_node **succs, 
					int n, struct skiplist *sl, map_key_t key, enum unlink unlink, int tid) {

    struct hp_item* hp = sl->HP[tid];
    if (hp == NULL) {
		hp = hp_item_setup(sl, tid);
    }
	
	int bottom_level = 0;
	struct sl_node *pred = NULL;
	struct sl_node *curr = NULL;
	struct sl_node *succ = NULL;
    markable_t next;
	int level = n - 1;
	
find_retry:
	while (1) {
		pred = &sl->head;
		//sl_debug("pred is sl->head\n");
		for (level = n - 1; level >= bottom_level; level--) {
			curr = GET_NODE(STRIP_MARK(pred->next[level]));
			while (curr) {
                //if now sb has removed the node-curr from the list, the pred->next[level] can not be equal to curr.
				//curr要在此时记录到hp0中
				hp_t hp_addr = (hp_t)curr;
				hp_save_addr(hp, level, 0, hp_addr); //将hp_addr存到本线程的hp(hp_item)内
                sl_debug("thread-%lx protect hazard pointer address is %p, pred's address is %p.\n", pthread_self(), curr, pred);
				//验证curr是否safe? 验证失败retry
				if (GET_NODE(STRIP_MARK(pred->next[level])) != curr || HAS_MARK(pred->next[level])) {
                    hp_clear_addr(hp, level, 0);
					goto find_retry;
				}
                sl_debug("thread-%lx node curr's address is %p, pred's address if %p.\n", pthread_self(), curr, pred);
				//succ = GET_NODE(STRIP_MARK(curr->next[level]));  //基于curr现在是被hp保护住的前提
                next = curr->next[level];
				if (HAS_MARK(next)) { //curr此时被标记
					//ABA-prone CAS! 物理移除curr，此步CAS要想成功必须：[1].pred未被标记，[2].curr依然是pred的后继
                    succ = GET_NODE(STRIP_MARK(next));
					markable_t old_next = SYNC_CAS(&pred->next[level], (markable_t)curr, (markable_t)succ);
					if (old_next != (markable_t)curr) {
						//物理删除失败，[1].pred在CAS race时被标记了，[2].pred -> curr的关系CAS race时被破坏了
						goto find_retry;
					}
                    if (level == bottom_level) {
                        //linearization point
                        SYNC_ADD(&remove_lp_count, 1);
                    }
					sl_debug("thread-%lx physical remove node %p at level %d, and it's pred was %p.\n", pthread_self(), curr, level, pred);
					//物理移除curr成功，但是这仅仅是一个level上的，调整一下curr的引用计数
					int ref_counter = SYNC_SUB(&curr->ref_counter, 1);
                        if (ref_counter < 0) {
                            printf("dddddd ref_counter = %d.\n", ref_counter);
                        }
					if (ref_counter == 0) {
						//此时这个节点已经可以使用hp_retire_node移除了，带GC机制的free(curr);
                        sl_debug("refcounter is zero, now retire.\n");
						hp_retire_node(sl, hp, hp_addr);
                        SYNC_ADD(&retire_count, 1);
					}

					curr = succ;
				} else {
                    succ = GET_NODE(STRIP_MARK(next));
					if (key_cmp(&curr->key, &key) == SLKEY_RIGHT) {
						//继续向后遍历
						pred = curr;
						//此时把hp0的内容倒到hp1中
                        hp_t hp0 = hp_get_addr(hp, level, 0);
						hp_save_addr(hp, level, 1, hp0);
						curr = succ;
					}else {  //可能是SLKEY_EQUAL/SLKEY_RIGHT/-1
						goto do_records;  //到下一层去
					}
				}
			}
			
do_records:
			//sl_debug("do_records\n");
			//此处记录一下本层的preds和succs
			if (preds){
				preds[level] = pred;	
			}
			if (succs) {
				succs[level] = curr;	
			}
		} // end for
		if (curr && key_cmp(&curr->key, &key) == SLKEY_EQUAL) {
			return curr;
		}
		return NULL;
	}		
}
#endif


#if 1
int sl_insert(struct skiplist *sl, struct sl_node *node, map_key_t key, int verbose, int tid) 
{
    struct hp_item* hp = sl->HP[tid];
    if (hp == NULL) {
	hp = hp_item_setup(sl, tid);
    }

    struct sl_node *preds[MAX_LEVELS], *succs[MAX_LEVELS];
    int bottom_level = 0;
    int level = 0;
    struct sl_node *old_item, *new_item = NULL;

    while(1) {
        old_item = find_preds(preds, succs, MAX_LEVELS, sl, key, ASSIST_UNLINK, tid);
        if (old_item != NULL) {
            sl_debug("sl_insert(): insert_failed! item: [%lu,%lu], old_item: [%lu,%lu].\n", key.start,key.end,old_item->key.start,old_item->key.end);
            hp_clear_all_addr(hp);
            return -E_KEY_IN_MAP;
        }
        //
        level = random_levels(sl);  //新创建出来的node的level数是level
        new_item = node_alloc(level, &key, node);
        for (int i = bottom_level; i < level; i++) {
            new_item->next[i] = (markable_t)succs[i];
        }

        //important! linearization point.
        struct sl_node* pred = preds[bottom_level];
        struct sl_node* succ = succs[bottom_level];
        
        markable_t old_value = SYNC_CAS(&pred->next[bottom_level], (markable_t)succ, (markable_t)new_item);
        if (old_value != (markable_t)succ) {
            //CAS failed!
            sl_debug("thread-%lx sl_insert(): linearization point retry!\n", pthread_self());
            SYNC_SUB(&node_count, 1);
            SYNC_SUB(&alloc_count, 1);
            free(new_item->next);
            continue;
        }
        
        SYNC_ADD(&insert_lp_count, 1);
        for(int i = bottom_level + 1; i < level; i++) {
            while(1) {
                pred = preds[i];
                succ = succs[i];
                //[1].pred cannot be marked. [2].succ should still be the successor of pred.
                old_value = SYNC_CAS(&pred->next[i], (markable_t)succ, (markable_t)new_item);
                if (old_value == (markable_t)succ) {
                    //level-i success!
                    if (HAS_MARK(new_item->next[i])) {
                        sl_debug("Link a marked node at level-%d.\n", i);
                    }
                    break;
                }
                //there could be marked nodes added to the skiplist!
                sl_debug("thread-%lx sl_insert() retry!\n", pthread_self());
                find_preds(preds, succs, level, sl, key, ASSIST_UNLINK, tid);
                
                //adjust all the new_item->next[field] from level-i to level-level.
                for (int j = i; j < level; ++j) {
                    markable_t old_next = new_item->next[j];
                    markable_t other;
                    if ((markable_t)succs[j] == old_next)
                        continue;
                    
                    other = SYNC_CAS(&new_item->next[j], old_next, (markable_t)succs[j]);
                    if (other == old_next) {
                        //succeed to update the next.
                        if (HAS_MARK(other)) {
                            //mark the new_item->next[j].
                            while (!HAS_MARK(new_item->next[j])) {
                                old_next = new_item->next[j];
                                other = SYNC_CAS(&new_item->next[j], old_next, (markable_t)MARK_NODE(new_item->next[j]));
                            }
                        }
                    } else {
                        //fail to update the next field.
                        find_preds(preds, succs, level, sl, key, ASSIST_UNLINK, tid);
                        j--;
                    }
                }
            }
        }
        hp_clear_all_addr(hp);
        return 1;
    }
}
#endif



/* sl_remove - remove the item with <key> in <sl> from top down. 
 * If fails return -E_KEY_NOT_EXIST else 0.
 * @sl:	 skiplist
 * @key: item's key
 * @tid: thread's id
 */
#if 1
int sl_remove(struct skiplist *sl, map_key_t key, int verbose, int tid)
{
    struct hp_item* hp = sl->HP[tid];
    if (hp == NULL) {
	hp = hp_item_setup(sl, tid);
    }

	markable_t old_next;
	int level;
	int bottom_level = 0;
	struct sl_node *preds[MAX_LEVELS];
	/* If <item> == NULL, it is the linearization point of unsuccessful <map_remove> */
	// 以key为key，查找其每一层的前驱，并存放到preds中，
	struct sl_node *item = find_preds(preds, NULL, sl->high_water, sl, key, ASSIST_UNLINK, tid);
	markable_t next;
	if (item == NULL) {
		sl_debug("thread-%lx sl_remove(): doesn't found the node to remove in the skiplist.\n", pthread_self());
                hp_clear_all_addr(hp);
		return -E_KEY_NOT_EXIST;
	}
	sl_debug("thread-%lx sl_remove(): found the item: %p [%lu,%lu] to remove in the skiplist!\n", pthread_self(), item, item->key.start, item->key.end);
	
	/* 1. Mark <item> at each level of <sl> from the top down. If multiple threads try to concurrently remove
	 *    the same item only one of them should succeed. Marking the bottom level establishes which of them succeeds. */
	//从上往下，对item的每一个level上的节点打上标记，除了bottom_level在for循环外单独处理
	for (level = item->num_levels - 1; level > bottom_level; --level) {
		
		next = (markable_t)item->next[level];  //next是item的本level对应的markable_t（next域和mark域共享字段）
		/*
		 * 本层暂时还未被标记过，反复CAS标记之直至成功
		 * 因为CAS期间可能有其他thread在item->next[level]和后继之间插入了新的节点，导致CAS标记操作失败
		 */
		while (!HAS_MARK(next)) { //node item doesn't have a mark at level-level.
			//试图给bottom_level的节点打上标记
			SYNC_CAS(&item->next[level], next, (markable_t)MARK_NODE(next));
			next = item->next[level];
		}
	}

	//单独处理bottom_level的节点
        while(!HAS_MARK(item->next[bottom_level])) {//retry，CAS失败并且bottom_level的节点也没有被标记，其他thread在CAS期间win Race，修改了item->next[bottom_level]，比如在其后插入一个新节点
                next = item->next[bottom_level]; 
		old_next = SYNC_CAS(&item->next[bottom_level], next, (markable_t)MARK_NODE(next)); //试图给bottom_level的节点打上标记，无论成功与否，每一层的逻辑删除已经全部结束了
        }
        if (old_next != item->next[bottom_level]) 
            return 0;
        find_preds(NULL, NULL, sl->high_water, sl, key, ASSIST_UNLINK, tid);
	hp_clear_all_addr(hp);
        return 0;
}
#endif


/* sl_lookup - Fast find that does not help unlink partially removed nodes and does not 
 * return the node's predecessors, return item if found else NULL.
 * @sl:		skiplist
 * @key:	search key
 * @level:  查找0~level的前驱和后继
 */
struct sl_node *sl_lookup(struct skiplist *sl, map_key_t key, int level, int tid)
{
    return find_preds(NULL, NULL, level, sl, key, DONT_UNLINK, tid);
}


/*
 * 返回skiplist中的最小key值:
 * 由于skiplist是按照升序排列，所以第一个未标记节点的key值是最小的key值
 */
map_key_t sl_min_key(struct skiplist *sl)
{
    struct sl_node *item = GET_NODE(sl->head.next[0]);
	map_key_t key = sl->head.key;
	
    while (item != NULL) {
        markable_t next = item->next[0];
        if (!HAS_MARK(next))
            return item->key;
        item = STRIP_MARK(next);
    }
    return key;
}

/*
 * 统计没有被标记的节点数
 */
size_t sl_count(struct skiplist *sl)
{
    size_t count = 0;
    struct sl_node *item = GET_NODE(sl->head.next[0]);   //为什么不直接struct sl_node *item = sl->head?
    while (item) {
        //if (!HAS_MARK(item->next[0])) {
            count++;
        //}
        item = STRIP_MARK(item->next[0]);  //item变为next节点（需要先把最低位的mark给清除）
    }
    return count;
}


void sl_print_lsm(struct skiplist *sl) {
	struct sl_node* pred;
	struct sl_node* curr;
	int level = sl->high_water - 1;
	int bottom_level = 0;
	sl_debug("\n--------------------------------\ndump the skiplist:\n");
	for (; level >= bottom_level; level--) {
		pred = &sl->head;
		sl_debug("Level %d : [head] -> ", level);
		curr = GET_NODE(STRIP_MARK(pred->next[level]));
                while (curr) {
                    markable_t next_field = (markable_t)curr->next[level];
                    map_key_t key_field = curr->key;
                    sl_debug("[%ld,%ld]%c -> ", key_field.start, key_field.end, (HAS_MARK(next_field) ? '@' : ' '));
                    curr = GET_NODE(STRIP_MARK(curr->next[level]));
                }
                sl_debug("NULL\n");
	}
}

map_key_t keys[15] = {
	[0] = {.start = 102, .end = 150},
	[1] = {.start = 22, .end = 35},
	[2] = {.start = 12, .end = 22},
	[3] = {.start = 55, .end = 63},
	[4] = {.start = 200, .end = 300},
	[5] = {.start = 150, .end = 200},
	[6] = {.start = 40, .end = 50},
	[7] = {.start = 68, .end = 72},
	[8] = {.start = 4, .end = 8},
	[9] = {.start = 90, .end = 100},
	[10] = {.start = 82, .end = 90},
	[11] = {.start = 77, .end = 79},
	[12] = {.start = 1, .end = 3},
	[13] = {.start = 250, .end = 279},
	[14] = {.start = 9, .end = 10},
};


//unsigned long counter = 0;
void* thr_func(void* arg){
	unsigned long slave_id = (unsigned long)arg;
        sl_debug("slave %lx is thread %lx.\n", slave_id, pthread_self());
	
	//为本线程创建一个hp_item，并将其lock-free地添加到hp_list中，每个线程持有一个自己的hp_item的指针hp
	struct hp_item* hp = hp_item_setup(sl, slave_id);
	for (int i = 0; i < 1000; i++){
		int index = rand() % 15;
		int add_or_remove = rand() % 2;
		struct sl_node* new_node1 = (struct sl_node*)malloc(sizeof(struct sl_node));
		memset(new_node1, 0, sizeof(struct sl_node));
		if (add_or_remove) {
			sl_debug("\n**slave %lx insert a node\n", slave_id);
			sl_insert(sl, new_node1, keys[index], 0, slave_id);
		} else {
			sl_debug("\n**slave %lx remove a node\n", slave_id);
			sl_remove(sl, keys[index], 0, slave_id);
		}
                sl_lookup(sl, keys[rand()%15], sl->high_water, slave_id);
               // sl_print_lsm(sl);
	}
	return NULL;
}

int main(){
	sl = (struct skiplist*)malloc(sizeof(struct skiplist));
	init_sl(sl);

	pthread_t tid[100];
	for (unsigned long i = 0; i < 4; i++) {
		pthread_create(&tid[i], NULL, thr_func, (void *)i);
	}
	for (int i = 0; i < 4; i++) {
		pthread_join(tid[i], NULL);
	}

        free_sl(sl);
        sl_print_lsm(sl);
        printf("node_count = %d.\n", node_count);
        printf("alloc_count = %d.\n", alloc_count);
        printf("retire_count = %d.\n", retire_count);
        printf("insert_lp_count = %d.\n", insert_lp_count);
        printf("remove_lp_count = %d.\n", remove_lp_count);
        if (sl_count(sl) + 1 == node_count) {
            printf("success!\n");
        }else {
            printf("fail!\n");
        }
          
	/*
	//为本线程创建一个hp_item，并将其lock-free地添加到hp_list中，每个线程持有一个自己的hp_item的指针hp
	struct hp_item* hp = hp_item_setup(hp_list);
	map_key_t new_key;
	
	//insert [3,8]
	struct sl_node* new_node1 = (struct sl_node*)malloc(sizeof(struct sl_node));
	memset(new_node1, 0, sizeof(struct sl_node));
	new_key.start = 3;
	new_key.end = 8;
	sl_insert(sl, new_node1, new_key, 0, hp);
	
	//insert [9,10]
	struct sl_node* new_node2 = (struct sl_node*)malloc(sizeof(struct sl_node));
	memset(new_node2, 0, sizeof(struct sl_node));
	new_key.start = 9, new_key.end = 10;
	sl_insert(sl, new_node2, new_key, 0, hp);
	
	//insert [30,45]
	struct sl_node* new_node3 = (struct sl_node*)malloc(sizeof(struct sl_node));
	memset(new_node3, 0, sizeof(struct sl_node));
	new_key.start = 30, new_key.end = 45;
	sl_insert(sl, new_node3, new_key, 0, hp);
	//sl_remove(sl, new_key, 0, 0, hp);
	
	//insert [23,26]
	struct sl_node* new_node4 = (struct sl_node*)malloc(sizeof(struct sl_node));
	memset(new_node4, 0, sizeof(struct sl_node));
	new_key.start = 23, new_key.end = 26;
	sl_insert(sl, new_node4, new_key, 0, hp);
	//sl_remove(sl, new_key, 0, 0, hp);
	
	//insert [5,7]
	struct sl_node* new_node5 = (struct sl_node*)malloc(sizeof(struct sl_node));
	memset(new_node5, 0, sizeof(struct sl_node));
	new_key.start = 5, new_key.end = 7;
	sl_insert(sl, new_node5, new_key, 0, hp);
	
	sl_print_lsm(sl);
	sl_remove(sl, new_key, 0, 0, hp);
	
	//insert [40,43]
	struct sl_node* new_node6 = (struct sl_node*)malloc(sizeof(struct sl_node));
	memset(new_node6, 0, sizeof(struct sl_node));
	new_key.start = 40, new_key.end = 43;
	sl_insert(sl, new_node6, new_key, 0, hp);
	
	//insert [1,2]
	struct sl_node* new_node7 = (struct sl_node*)malloc(sizeof(struct sl_node));
	memset(new_node7, 0, sizeof(struct sl_node));
	new_key.start = 1, new_key.end = 2;
	sl_insert(sl, new_node7, new_key, 0, hp);
	
	sl_print_lsm(sl);
	printf("--------------------------------------\n");
	*/
	
	
	
	return 0;
}
