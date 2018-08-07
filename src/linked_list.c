#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "utils.h"
#include "linked_list.h"

int node_count = 0;
int retire_count = 0;
int print_count = 0;


//struct ll_node* ll is pointing to a memory location allocated by user.
void ll_init(struct linked_list* ll) {
    memset(ll, 0, sizeof(struct linked_list));
}


struct ll_node* ll_find(struct linked_list* ll, int tid, int key, struct ll_node** __pred, struct ll_node** __succ) {

    struct hp_item* hp = ll->HP[tid];
    if (hp == NULL) {
	hp = hp_item_setup(ll, tid);
    }

    struct ll_node* pred;
    struct ll_node* curr;
    struct ll_node* succ;
retry: 
    {
        pred = &(ll->ll_head);  //iterate the linked_list from it's head sentinel node.
        curr = GET_NODE(pred->next);
        while (curr) {
            hp_save_addr(hp, 0, (hp_t)curr);  //protect curr's address.
            //then we need to validate whether curr is safe.[1].pred should not be marked. [2].pred-->curr
            if (HAS_MARK((markable_t)pred) || pred->next != (markable_t)curr) {
                hp_clear_all_addr(hp);
                goto retry;
            }
            markable_t next = curr->next;
            if (HAS_MARK(next)) {
                //curr is marked, we need to physically remove it.
                succ = GET_NODE(STRIP_MARK(next));
                //[1]. pred must not be marked. [2]. pred--->curr should not be changed.
                markable_t old_value = SYNC_CAS(&pred->next, curr, succ);
                if (old_value != (markable_t)curr) {
                    //CAS failed.
                    goto retry;  //retry.
                }
                //CAS succeed. Go forwards.
                sl_debug("free a node in ll_find().\n");
                //=======================retire the node curr===========================
                hp_retire_node(ll, hp, (hp_t)curr);
                SYNC_ADD(&retire_count, 1);
                curr = succ;
            } else {
                succ = GET_NODE(STRIP_MARK(next));
                if (curr->key >= key) {
                    if (__succ) *__succ = curr;
                    if (__pred) *__pred = pred;
                    return curr;
                }
                //haven't found the target. Go forwards.
                pred = curr;
                hp_t hp0 = hp_get_addr(hp, 0);
                hp_save_addr(hp, 1, hp0);
                curr = succ;
            }
        }
        if (__succ) *__succ = curr;
        if (__pred) *__pred = pred;
        return NULL;
    }
}


int ll_insert(struct linked_list* ll, int tid, int key) {
    struct hp_item* hp = ll->HP[tid];
    if (hp == NULL) {
	hp = hp_item_setup(ll, tid);
    }
    //struct ll_node* head = &(ll->ll_head);
    while (1) {
        struct ll_node *pred = NULL, *succ = NULL;
        struct ll_node* item = ll_find(ll, tid, key, &pred, &succ);
        if (item && item->key == key) {
            //key is now in the linked list.
            hp_clear_all_addr(hp);
            return -1;
        }
        item = (struct ll_node*) malloc (sizeof(struct ll_node));
    
        item->key = key;
        item->next = (markable_t)succ;
    
        //[1]. pred must not be marked. [2]. pred--->succ should not be changed.
        markable_t old_value = SYNC_CAS(&pred->next, succ, item);
        if (old_value != (markable_t)succ) {
            //CAS failed!
            free(item);
            continue;
        }
        //CAS succeed!
        SYNC_ADD(&node_count, 1);
        hp_clear_all_addr(hp);
        return 0;
    }
}


int ll_remove(struct linked_list* ll, int tid, int key) {
    struct hp_item* hp = ll->HP[tid];
    if (hp == NULL) {
	hp = hp_item_setup(ll, tid);
    }
    //struct ll_node* head = &(ll->ll_head);
    struct ll_node *pred = NULL;
    markable_t old_value;
    while (1) {
        struct ll_node* item = ll_find(ll, tid, key, &pred, NULL);
        if (!item || item->key != key) {
            //cannot find key int the ll.
            hp_clear_all_addr(hp);
            return -1;
        } else {
            //logically remove the item.
            //try to mark item.
            markable_t next = item->next;
            old_value = SYNC_CAS(&item->next, next, MARK_NODE(next));
            if (old_value != next) {
                //fail to mark item. Now item could be marked by others OR removed by others OR freed by others OR still there.
                //so we need to retry from list head.
                continue;
            }
            
            ll_find(ll, tid, key, NULL, NULL);
            hp_clear_all_addr(hp);
            return 0;
        }
    }
}


int ll_contains(struct linked_list* ll, int tid, int key) {
    struct hp_item* hp = ll->HP[tid];
    if (hp == NULL) {
	hp = hp_item_setup(ll, tid);
    }

    //struct ll_node* head = &(ll->ll_head);
    struct ll_node* curr;
    ll_find(ll, tid, key, NULL, &curr);
 
    if (curr && curr->key == key) {
        hp_clear_all_addr(hp);
        return 1;
    }
    hp_clear_all_addr(hp);
    return 0;
}


void ll_print(struct linked_list* ll) {
    struct ll_node* head = &(ll->ll_head);
    struct ll_node* curr = GET_NODE(head->next);
    sl_debug("[head] -> ");
    while (curr) {
        SYNC_ADD(&print_count, 1);
        sl_debug("[%d]%c -> ", curr->key, (HAS_MARK(curr->next) ? '*' : ' '));
        curr = GET_NODE(curr->next);
    } 
    sl_debug("NULL.\n");
    sl_debug("print_count = %d.\n", print_count);
}

void ll_destroy(struct linked_list* ll) {
    hp_setdown(ll);
    struct ll_node* head = &(ll->ll_head);
    struct ll_node* curr = GET_NODE(head->next);
    while (curr) {
        struct ll_node* old_curr = curr;
        curr = GET_NODE(curr->next);
        free(old_curr);
        SYNC_SUB(&node_count, 1);
    } 
    free(head);
    printf("node_count = %d.\n", node_count);
    sl_debug("retire_count = %d.\n", retire_count);
    retire_count = 0;
    node_count = 0;
    print_count = 0;
}


/*
 * A thread call hp_item_setup() to allocate a hp_item belongs to it.
 * @ll: a hp_item struct is used for a specified linked_list.(A thread can holds a lot of hp_items when it accesses a lot of linked_lists)
 * @tid:thread-id of the calling thread
 * @return: hp_item owned to the calling thread.
 */
struct hp_item* hp_item_setup(struct linked_list* ll, int tid) {
    //accumulate ll->count_of_hp
    while (1) {
        unsigned int hp_H = ll->count_of_hp;  //How many hps are there in the system.
        //every time there comes a new thread, increase HP_K hps in the system.
        unsigned int old_H = SYNC_CAS(&ll->count_of_hp, hp_H, hp_H + HP_K);  
        if (old_H == hp_H) 
            break;  //success to increment hp_list->d_count.
    }

    struct hp_item* hp = (struct hp_item*) malloc(sizeof(struct hp_item));
    memset(hp, 0, sizeof(struct hp_item));
    hp->d_list = (struct hp_rnode*) malloc(sizeof(struct hp_rnode));
    hp->d_list->next = NULL;
    ll->HP[tid] = hp;
    return hp;
}


//保存hp
void hp_save_addr(struct hp_item* hp, int index, hp_t hp_addr) {
    if (index == 0) {
        hp->hp0 = hp_addr;
    } else {
        hp->hp1 = hp_addr;
    }
}


//释放hp
void hp_clear_addr(struct hp_item* hp, int index) {
    if (index == 0) {
        hp->hp0 = 0;	
    } else {
        hp->hp1 = 0;
    }
}


hp_t hp_get_addr(struct hp_item* hp, int index) {
    if (index == 0) {
        return hp->hp0;
    } else {
        return hp->hp1;
    }
}


void hp_clear_all_addr(struct hp_item* hp) {
    hp->hp0 = 0;
    hp->hp1 = 0;
}


void hp_dump_statics(struct linked_list* ll) {
    
}


/*
 * Call from the ll_destroy() when the hp facility in the linked_list is no more used.
 * @ll:setdown hp of which linked list.
 */
void hp_setdown(struct linked_list* ll) {
    //walk through the hp_list and free all the hp_items.
    for (int i = 0; i < MAX_NUM_THREADS; i++) {
        hp_retire_hp_item(ll, i);
    }
}


void hp_retire_node(struct linked_list* ll, struct hp_item* hp, hp_t hp_addr) {
	sl_debug("thread-%lx hp_retire_node(): retire node:%p.\n", pthread_self(), (void *)hp_addr);

        struct hp_rnode *rnode = (struct hp_rnode*) malloc(sizeof(struct hp_rnode));
        rnode->address = hp_addr;

        //push the rnode into the hp->d_list. there's no contention between threads.
        rnode->next = hp->d_list->next;
        hp->d_list->next = rnode;

        hp->d_count++;
	if (hp->d_count >= hp_R(ll->count_of_hp)) {
		hp_scan(ll, hp);
	}
}

unsigned int hp_R(unsigned int R) {
    return R + 2;
}


//when a thread exits, it ought to call hp_retire_hp_item() to retire the hp_item.
void hp_retire_hp_item(struct linked_list* ll, int tid) {
    struct hp_item* hp = ll->HP[tid];
    if (hp == NULL) 
	return;

    ll->HP[tid] = NULL;
    struct hp_rnode* rnode = hp->d_list;
    rnode = rnode->next;
    while (rnode) {
        SYNC_SUB(&node_count, 1);
        free((void *)rnode->address);  //free the linked_list node.
        struct hp_rnode* old_rnode = rnode;
        rnode = rnode->next;
        free(old_rnode);
        hp->d_count--;
    }
    free(hp->d_list);
    sl_debug("hp[%p] 's final d_count = %d.\n", hp, hp->d_count);
    free((void *) hp);
}


//4 steps，至少释放R-N个节点（R即d_R，为每一个thread的待释放节点buffer尺寸；N为KP，即P个线程，每个线程K个hps）
void hp_scan(struct linked_list* ll, struct hp_item* hp) {
    sl_debug("thread-%lx hp_scan(): begin scan!\n", pthread_self());
    unsigned int plist_len = HP_K * MAX_NUM_THREADS;
    hp_t* plist = (hp_t *) malloc(plist_len * sizeof(hp_t));
    unsigned int plist_count = 0;
  
    for (int i = 0; i < MAX_NUM_THREADS; i++) {
	struct hp_item* __item__ = ll->HP[i];
       	if (__item__ == NULL) 
	    continue;
        hp_t hp0 = ACCESS_ONCE(__item__->hp0);
        hp_t hp1 = ACCESS_ONCE(__item__->hp1);
        if (hp0 != 0)
            plist[plist_count++] = hp0;
        if (hp1 != 0)
            plist[plist_count++] = hp1;
    }

    struct hp_rnode* new_d_list = (struct hp_rnode *) malloc(sizeof(struct hp_rnode));  //new head of the hp->d_list.
    new_d_list->next = 0;
    int new_d_count = 0;
	
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
            free((void *)target_hp);
            //the __rnode__ can be freed too.
            free(__rnode__);
            SYNC_SUB(&node_count, 1);
        }
        __rnode__ = hp->d_list->next;
    }
    free(plist);
    sl_debug("free d_list address = %p.\n", hp->d_list);
    free(hp->d_list);
    hp->d_list = new_d_list;
    hp->d_count = new_d_count;
}




