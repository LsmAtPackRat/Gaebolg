#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "utils.h"
#include "hash_set.h"

static int MASK = 0x00FFFFFF;  //takes the lowest 3 bytes of the hashcode. ???Don't understand.
static int HI_MASK = 0x80000000;  //the MSB of an int-value
int hs_node_count = 0;
int hs_add_success_time = 0;
int hs_add_fail_time = 0;
int hs_remove_success_time = 0;
int hs_remove_fail_time = 0;
int ll_node_count = 0;  //just for test.

void hs_init(struct hash_set* hs, int tid) {
    memset(hs->main_array, 0, MAIN_ARRAY_LEN * sizeof(segment_t*));  //important.
    hs->load_factor = LOAD_FACTOR_DEFAULT;  // 2
    hs->capacity = INIT_NUM_BUCKETS;  // 2 at first
    hs->set_size = 0;
    get_bucket_list(hs, tid, 0);  //allocate the memory of the very first segment
    struct bucket_list** segment_0 = (struct bucket_list **)hs->main_array[0];
    bucket_list_init(&segment_0[0], 0);   //here we go.
}


//init a bucket_list structure. It contains a sentinel node.
void bucket_list_init(struct bucket_list** bucket, int bucket_index) {
    *bucket = (struct bucket_list*) malloc(sizeof(struct bucket_list));  //don't need memset(,0,);
    ll_init(&((*bucket)->bucket_sentinel));  // init the linked_list of the bucket-0.
    (*bucket)->bucket_sentinel.ll_head.key = make_sentinel_key(bucket_index);
}


int reverse(int code) {
    int result = 0;
    for (int i = 0; i < sizeof(int) * 8; i++) {
        int bit = code == (code >> 1) << 1 ? 0 : 1;
        bit = bit << (sizeof(int) * 8 - i - 1);
        result = result | bit;
        code = code >> 1;
    }
    return result;
}


void print_binary(int code, int bit_count) {
    int bit = code == (code >> 1) << 1 ? 0 : 1;  //the current LSB.
    if (bit_count == 1) {
        sl_debug("%d", bit);
        return;
    } else {
        print_binary(code >> 1, bit_count - 1);
        sl_debug("%d", bit);
    }
}


//hash-function
int hash(int key) {
    return key;
}


//set MSB to 1, and reverse the key.
int make_ordinary_key(int key) {
    int code = hash(key) & MASK;
    return reverse(code | HI_MASK);  //mark the MSB and reverse it.
}


//set MSB to 0, and reverse the key.
int make_sentinel_key(int key) {
    return reverse(key & MASK);
}


//1 if key is sentinel key, 0 if key is ordinary key.
int is_sentinel_key(int key) {
    if (key == ((key >> 1) << 1)) {
        return 1;
    }
    return 0;
}


int get_origin_key(int key) {
    key = (key >> 1) << 1;
    return reverse(key);
}


//if the segment of the target bucket_index hasn't been initialized, initialize the segment.
struct bucket_list* get_bucket_list(struct hash_set* hs, int tid, int bucket_index) {
    int main_array_index = bucket_index / SEGMENT_SIZE;
    int segment_index = bucket_index % SEGMENT_SIZE;

    // First, find the segment in the hs->main_array.
    segment_t* p_segment = hs->main_array[main_array_index];
    if (p_segment == NULL) {
        //allocate the segment until the first bucket_list in it is visited.
        p_segment = (segment_t*) malloc(sizeof(segment_t));
        memset(p_segment, 0, sizeof(segment_t));  //important
        segment_t* old_value = SYNC_CAS(&hs->main_array[main_array_index], NULL, p_segment);
        if (old_value != NULL) {  //someone beat us to it.
            free(p_segment);
            p_segment = hs->main_array[main_array_index];
        } else {
            sl_debug("allocate a new segment, main_array_index is %d.\n", main_array_index);
        }
    }
    
    // Second, find the bucket_list in the segment.
    struct bucket_list** buckets = (struct bucket_list**)p_segment;
    return buckets[segment_index];  //may be NULL.
}


//put the new_bucket at the bucket_index.
void set_bucket_list(struct hash_set* hs, int tid, int bucket_index, struct bucket_list* new_bucket){
    int main_array_index = bucket_index / SEGMENT_SIZE;
    int segment_index = bucket_index % SEGMENT_SIZE;

    // First, find the segment in the hs->main_array.
    segment_t* p_segment = hs->main_array[main_array_index];
    if (p_segment == NULL) {
        //allocate the segment until the first bucket_list in it is visited.
        p_segment = (segment_t*) malloc(sizeof(segment_t));
        memset(p_segment, 0, sizeof(segment_t));  //important
        segment_t* old_value = SYNC_CAS(&hs->main_array[main_array_index], NULL, p_segment);
        if (old_value != NULL) {  //someone beat us to it.
            free(p_segment);
            p_segment = hs->main_array[main_array_index];
        } else {
            sl_debug("allocate a new segment, main_array_index is %d.\n", main_array_index);
        }
    }
    
    // Second, find the bucket_list in the segment.
    struct bucket_list** buckets = (struct bucket_list**)p_segment;
    buckets[segment_index] = new_bucket;
}


// return 1 if contains, 0 otherwise.
int bucket_list_contains(struct bucket_list* bucket, int tid, int key) {
   return ll_contains(&bucket->bucket_sentinel, tid, key);
}


//recursively initialize the parent's buckets.
void initialize_bucket(struct hash_set* hs, int tid, int bucket_index) {
    //First, find the parent bucket.If the parent bucket hasn't be initialized, initialize it.
    int parent_index = get_parent_index(hs, bucket_index);
    struct bucket_list* parent_bucket = get_bucket_list(hs, tid, parent_index);
    if (parent_bucket == NULL) {
        sl_debug("parent bucket haven't been setup! bucket-%d 's parent index = %d.\n", bucket_index, parent_index);
        initialize_bucket(hs, tid, parent_index);  //recursive call.
    }

    //Second, find the insert point in the parent bucket's linked_list and use CAS to insert new_bucket->bucket_sentinel->ll_node into the parent bucket.
    parent_bucket = get_bucket_list(hs, tid, parent_index);
    
    //Third, allocate a new bucket_list here.
    struct bucket_list* new_bucket;
    bucket_list_init(&new_bucket, bucket_index);

    //FIXME:We want to insert the a allocated sentinel node into the parent bucket_list. But ll_insert() will also malloc a new memory area. It't duplicated.
    //if (ll_insert(&parent_bucket->bucket_sentinel, tid, make_sentinel_key(bucket_index)) == 0) {
    if (ll_insert_ready_made(&parent_bucket->bucket_sentinel, tid, &(new_bucket->bucket_sentinel.ll_head)) == 0) {
        //success!
        set_bucket_list(hs, tid, bucket_index, new_bucket);
    } else {
        //FIXME:encapsulate a function to free a struct bucket_list.
        free(new_bucket);  //ugly.use function.
    }
}


//get the parent bucket index of the given bucket_index
int get_parent_index(struct hash_set* hs, int bucket_index) {
    int parent_index = hs->capacity;
    do {
        parent_index = parent_index >> 1;
    } while (parent_index > bucket_index);
    parent_index = bucket_index - parent_index;
    //sl_debug("bucket-%d 's parent is bucket-%d.\n", bucket_index, parent_index);
    return parent_index;
}


int hs_add(struct hash_set* hs, int tid, int key) {
    //First, calculate the bucket index by key and capacity of the hash_set.
    int bucket_index = hash(key) % hs->capacity;
    sl_debug("thread-%lx : hs_add origin_key = %d, bucket_index = %d.\n", pthread_self(), key, bucket_index);
    struct bucket_list* bucket = get_bucket_list(hs, tid, bucket_index);  //bucket will be initialized if needed in get_bucket_list()
    if (bucket == NULL) {
        //FIXME: It seems that the bucket will not always be initialized as we expected.
        sl_debug("thread-%lx : bucket-%d haven't been initialized! initialize it first.\n", pthread_self(), bucket_index);
        initialize_bucket(hs, tid, bucket_index);  
        bucket = get_bucket_list(hs, tid, bucket_index);
    }

    if (ll_insert(&bucket->bucket_sentinel, tid, make_ordinary_key(key)) != 0) {
        //fail to insert the key into the hash_set
        sl_debug("thread-%lx : hs_add(%d) fail!\n", pthread_self(), key);
        SYNC_ADD(&hs_add_fail_time, 1);
        return -1;
    }
    sl_debug("thread-%lx : hs_add(%d) success!\n", pthread_self(), key);
    SYNC_ADD(&hs_node_count, 1);
    int set_size_now = SYNC_ADD(&hs->set_size, 1);
    SYNC_ADD(&hs_add_success_time, 1);
    unsigned long capacity_now = hs->capacity;  //we must fetch and store the value before test whether of not to resize. Be careful don't resize multi times.
 
    //Do we need to resize the hash_set?
    if ((1.0) * set_size_now / capacity_now >= hs->load_factor) {
        //sl_debug("need to resize!\n");
        if (capacity_now * 2 <= MAIN_ARRAY_LEN * SEGMENT_SIZE) {
            //sl_debug("try to resize!\n");
            unsigned long old_capacity = SYNC_CAS(&hs->capacity, capacity_now, capacity_now * 2);
            if (old_capacity == capacity_now) {
                //sl_debug("resize succeed!\n");
            }
        } else {
            //sl_debug("cannot resize, the buckets number reaches (MAIN_ARRAY_LEN * SEGMENT_SIZE).\n");
        }
    }
    return 0;
}


// return 1 if hs contains key, 0 otherwise.
int hs_contains(struct hash_set* hs, int tid, int key) {
    // First, get bucket index.
    // Note: it is a corner case. If the hs is resized not long ago. Some elements should be adjusted to new bucket,
    // When we find a key who is in the hs, but haven't been "moved to" the new bucket. We need to "move" it.
    // Actually, we need to  initialize the new bucket and insert it into the hs main_list.
    int bucket_index = hash(key) % hs->capacity;
    struct bucket_list* bucket = get_bucket_list(hs, tid, bucket_index);
    if (bucket == NULL) {
        initialize_bucket(hs, tid, bucket_index);  
        bucket = get_bucket_list(hs, tid, bucket_index);
    }
    return bucket_list_contains(bucket, tid, make_ordinary_key(key));
}


// 0 if succeed, -1 if fail.
int hs_remove(struct hash_set* hs, int tid, int key) {
    // First, get bucket index.
    //if the hash_set is resized now! Maybe we cannot find the new bucket. Just initialize it if the bucket if NULL.
    int bucket_index = hash(key) % hs->capacity;

    struct bucket_list* bucket = get_bucket_list(hs, tid, bucket_index);
    if (bucket == NULL) {
        initialize_bucket(hs, tid, bucket_index);  
        bucket = get_bucket_list(hs, tid, bucket_index);
    }

    int origin_sentinel_key = get_origin_key(bucket->bucket_sentinel.ll_head.key);
    //sl_debug("key - %d 's sentinel origin key is %u.\n", key, origin_sentinel_key);
    // Second, remove the key from the bucket.
    // the node will be taken over by bucket->bucket_sentinel's HP facility. I think it isn't efficient but can work.
    int ret = ll_remove(&bucket->bucket_sentinel, tid, make_ordinary_key(key));
    if (ret == 0) {
        SYNC_SUB(&hs->set_size, 1);
        SYNC_ADD(&hs_remove_success_time, 1);
        //sl_debug("remove %d success!\n", key);
    } else {
        SYNC_ADD(&hs_remove_fail_time, 1);
    }
    return ret;
}


//[sentinel]->(ordinary)->(ordinary)->[sentinel]
void hs_print(struct hash_set* hs, int tid) {
    int print_count = 0;
    int ordinary_count = 0;
    //we need to traverse from the bucket-0's sentinel ll_node to the end.
    struct ll_node* head = &(get_bucket_list(hs, tid, 0)->bucket_sentinel.ll_head);  //now head is the head of the main-list.
    struct ll_node* curr = head;
    while (curr) {
        SYNC_ADD(&print_count, 1);
        if (is_sentinel_key(curr->key)) {
            printf("[%d] -> ", get_origin_key(curr->key));
        } else {
            if (!HAS_MARK(curr->next)) {
                SYNC_ADD(&ordinary_count, 1);
                printf("(%d)%c -> ", get_origin_key(curr->key), (HAS_MARK(curr->next) ? '*' : ' '));
            }
        }
        
        curr = GET_NODE(STRIP_MARK(curr->next));
    } 
    printf("NULL.\n");
    printf("hash_set : set_size = %d.\n", hs->set_size);
    if (hs->set_size != ordinary_count) {
        printf("FAIl! (hs->set_size != ordinary_count)\n");
    } else {
        printf("SUCCESS! (hs->set_size == ordinary_count)\n");
    }
    
    sl_debug("load factor = %f.\n", (1.0) * hs->set_size / hs->capacity);
    sl_debug("print_count = %d.\n", print_count);
    
}


//traverse the hs->main_array and print each bucket_list.
void hs_print_through_bucket(struct hash_set* hs, int tid) {
    //
    for (int i = 0; i < MAIN_ARRAY_LEN; i++) {
        segment_t* p_segment = hs->main_array[i];
        if (p_segment == NULL) {
            continue;
        }
        for (int j = 0; j < SEGMENT_SIZE; j++) {
            struct bucket_list** buckets = (struct bucket_list**)p_segment;
            struct bucket_list* bucket = buckets[j];
            if (bucket == NULL) {
                continue;
            }
            //now traverse the bucket.
            struct ll_node* head = &(bucket->bucket_sentinel.ll_head);
            printf("[%d]%c -> ", get_origin_key(head->key), (HAS_MARK(head->next) ? '*' : ' '));

            struct ll_node* curr = GET_NODE(head->next);
            while (curr) {
                if (is_sentinel_key(curr->key)) {
                    printf("NULL\n");
                    break;
                } else {
                    printf("(%d)%c -> ", get_origin_key(curr->key), (HAS_MARK(curr->next) ? '*' : ' '));
                }
                curr = GET_NODE(STRIP_MARK(curr->next));
            } 
            if (curr == NULL) {
                printf("NULL\n");
            }
        }
    }
    //sl_debug("NULL.\n");
    sl_debug("hash_set : set_size = %lu.\n", hs->set_size);
    sl_debug("load factor = %f.\n", (1.0) * hs->set_size / hs->capacity);
}


void hs_destroy(struct hash_set* hs) { 
    struct ll_node *p1, *p2;

    // First , get the first linked_list of bucket-0.
    struct bucket_list** buckets_0 = (struct bucket_list**)hs->main_array[0];
    struct linked_list* ll_curr = &(buckets_0[0]->bucket_sentinel);  // Now, ll_curr is the most left linked_list.

    while (1) {
        p1 = &(ll_curr->ll_head);
        p2 = GET_NODE(STRIP_MARK(p1->next));

        while (p2 != NULL && !is_sentinel_key(p2->key)) {
            p1 = p2;
            p2 = GET_NODE(STRIP_MARK(p1->next));
        }

        if (p2 == NULL) {
            // ll_curr must be the last linked_list in the main list. Just call ll_destroy() to destroy it.
            ll_destroy(ll_curr);
            break;
        } else {
            // p2 is the head(sentinel) of a linked_list.
            p1->next = 0;
            ll_destroy(ll_curr);
            // Now the ll_curr is destroyed, we need to find the new ll_curr started from the p2.
            // We need to find the linked_list from the main_array using p2->key as the index.
            int bucket_index = get_origin_key(p2->key);
            struct bucket_list* bucket = get_bucket_list(hs, 0, bucket_index);   // tid is an arbitrary value. bucket must not be NULL.
            ll_curr = &(bucket->bucket_sentinel);
        }
    }

    // Second, we have freed the whole linked_list, now we need to free all of the segments.
    for (int i = 0; i < MAIN_ARRAY_LEN; i++) {
        segment_t* segment_curr = hs->main_array[i];
        if (segment_curr == NULL) continue;
        free(segment_curr);
    }

    // Third, free the hash_set.
    free(hs);
    printf("hs_add_success_time = %d, hs_add_fail_time = %d.\nhs_remove_success_time = %d, hs_remove_fail_time = %d.\n", hs_add_success_time, hs_add_fail_time, hs_remove_success_time, hs_remove_fail_time);
    printf("hs_node_count = %d.\n", hs_node_count);
}
















