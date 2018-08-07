#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include "skiplist.h"
#include "utils.h"
extern int node_count;

//当每个thread开始时，主动调用hp_item_setup来建立自己与hp相关的结构
struct hp_item* hp_item_setup(struct skiplist* sl, int tid) {
        while (1) {
            unsigned int hp_H = sl->count_of_hp;  //How many hps are there in the system.
            //every time there comes a new thread, increase (HP_K * MAXLEVELS) hps in the system.
            unsigned int old_H = SYNC_CAS(&sl->count_of_hp, hp_H, hp_H + HP_K * MAX_LEVELS);  
            if (old_H == hp_H) 
                break;  //success to increment hp_list->d_count.
        }
        
	struct hp_item* item = (struct hp_item*)malloc(sizeof(struct hp_item));
        //memset(item, 0, sizeof(struct hp_item));
	item->d_list = (struct hp_rnode *)malloc(sizeof(struct hp_rnode));  //item->d_list is a head node.
        item->d_list->next = NULL;  //important.
	
        sl->HP[tid] = item;
	return item;
}


//保存hp
void hp_save_addr(struct hp_item* hp, int level, int index, hp_t hp_addr) {
        level >= MAX_LEVELS ? MAX_LEVELS - 1 : level;
	if (index == 0) {
		hp->hp0[level] = hp_addr;
	} else {
		hp->hp1[level] = hp_addr;
	}
}


//释放hp
void hp_clear_addr(struct hp_item* hp, int level, int index) {
        level >= MAX_LEVELS ? MAX_LEVELS - 1 : level;
	if (index == 0) {
		hp->hp0[level] = 0;	
	} else {
		hp->hp1[level] = 0;
	}
}


hp_t hp_get_addr(struct hp_item* hp, int level, int index) {
        level >= MAX_LEVELS ? MAX_LEVELS - 1 : level;
	if (index == 0) {
		return hp->hp0[level];
	} else {
		return hp->hp1[level];
	}
}


void hp_clear_all_addr(struct hp_item* hp) {
    memset(hp->hp0, 0, sizeof(hp_t) * MAX_LEVELS);
    memset(hp->hp1, 0, sizeof(hp_t) * MAX_LEVELS);
}


void hp_dump_statics(struct skiplist* sl) {
    
}


//the master thread call this function at last only once!
void hp_setdown(struct skiplist* sl) {
    //walk through the hp_list and free all the hp_items.
    for (int i = 0; i < MAX_NUM_THREADS; i++) {
        hp_retire_hp_item(sl, i);
    }
}


void hp_retire_node(struct skiplist* sl, struct hp_item* hp, hp_t hp_addr) {
	sl_debug("thread-%lx hp_retire_node(): retire node:%p.\n", pthread_self(), (void *)hp_addr);

        struct hp_rnode *rnode = (struct hp_rnode*)malloc(sizeof(struct hp_rnode));
        rnode->address = hp_addr;

        //push the rnode into the hp->d_list. there's no contention between threads.
        rnode->next = hp->d_list->next;
        hp->d_list->next = rnode;

        hp->d_count++;
	if (hp->d_count >= hp_R(sl->count_of_hp)) {
		hp_scan(sl, hp);
	}
}

unsigned int hp_R(unsigned int R) {
    return R + 2;
}


//when a thread exits, it ought to call hp_retire_hp_item() to retire the hp_item.
void hp_retire_hp_item(struct skiplist* sl, int tid) {
    struct hp_item* hp = sl->HP[tid];
    if (hp == NULL) 
	return;

    struct hp_rnode* rnode = hp->d_list->next;
    while (rnode) {
        SYNC_SUB(&node_count, 1);
		free(((struct sl_node*)(rnode->address))->next);
        free((void *)rnode->address);  //free the skiplist node.
        struct hp_rnode* old_rnode = rnode;
        rnode = rnode->next;
        free(old_rnode);
    }
    free(hp->d_list);

    free((void *) hp);
    sl->HP[tid] = NULL;
}


//4 steps，至少释放R-N个节点（R即d_R，为每一个thread的待释放节点buffer尺寸；N为KP，即P个线程，每个线程K个hps）
void hp_scan(struct skiplist* sl, struct hp_item* hp) {
    sl_debug("thread-%lx hp_scan(): begin scan!\n", pthread_self());
    unsigned int plist_len = HP_K * MAX_LEVELS * MAX_NUM_THREADS;
    hp_t* plist = (hp_t *) malloc(plist_len * sizeof(hp_t));
    unsigned int plist_count = 0;
  
    for (int i = 0; i < MAX_NUM_THREADS; i++) {
		struct hp_item* __item__ = sl->HP[i];
       	if (__item__ == NULL) 
			continue;
        for(int level = 0; level < MAX_LEVELS; level++) {
            hp_t hp0 = ACCESS_ONCE(__item__->hp0[level]);
            hp_t hp1 = ACCESS_ONCE(__item__->hp1[level]);
            if (hp0 != 0)
				plist[plist_count++] = hp0;
            if (hp1 != 0)
				plist[plist_count++] = hp1;
        }
    }

    struct hp_rnode* new_d_list = (struct hp_rnode *)malloc(sizeof(struct hp_rnode));  //new head of the hp->d_list.
    new_d_list->next = NULL;
    unsigned int new_d_count = 0;
	
    //walk through the hp->d_list.
    struct hp_rnode* __rnode__ = hp->d_list->next;  //the first rnode in the hp->d_list.
    while (__rnode__) {
        //pop the __rnode__ from the hp->d_list.
        hp->d_list->next = __rnode__->next;
            
        //search the __rnode__->address in the plist.
        hp_t target_hp = __rnode__->address;
        unsigned int pi = 0;
        for(; pi < plist_count; pi++) {
            if (plist[pi] == target_hp) {
                //found! push __rnode__ into new_d_list.
                sl_debug("thread-%lx hp_scan(): Remain the node at address:%p. sb still hold the hazard pointer, cannot be freed now!\n", pthread_self(), (void *)target_hp);
                __rnode__->next = new_d_list->next;
                new_d_list->next = __rnode__;
                new_d_count++;
                break;
            }
        }
        if (pi == plist_count) {
            //doesn't find target_hp in plist, the skiplist node at target_hp can be freed right now.
            sl_debug("thread-%lx hp_scan(): Free the node at address:%p.\n", pthread_self(), (void *)target_hp);
			free(((struct sl_node*)target_hp)->next);
            free((void *)target_hp);
            //the __rnode__ can be freed too.
            free(__rnode__);
            SYNC_SUB(&node_count, 1);
        }
        __rnode__ = hp->d_list->next;
    }
    free(plist);
    free(hp->d_list);
    hp->d_list = new_d_list;
    hp->d_count = new_d_count;
}




























