#ifndef _H_HASH_SET
#define _H_HASH_SET

/* 
  lock-free hash_set : <Split-Ordered Lists:Lock-Free Extensible Hash Tables>
  lock-free hash_set consists of two components:
      1. a lock-free linked list. ("linked_list.h")
      2. an expanding array of references into the list. (logical buckets)

*/
#define MAX_NUM_BUCKETS 32  // a hash_set can have up to MAX_NUM_BUCKETS buckets
#define INIT_NUM_BUCKETS 2  // a hash_set has INIT_NUM_BUCKETS at first
#define LOAD_FACTOR_DEFAULT 0.75
#define MAIN_ARRAY_LEN 16
#define SEGMENT_SIZE 4

#include "linked_list.h"


//each bucket_list is a lock-free linked_list
struct bucket_list {
    struct linked_list bucket_sentinel;   // head the bucket_list.
};

typedef struct bucket_list* segment_t[SEGMENT_SIZE] ;  //a continuous memory area contains SEGMENT_SIZE of pointers (point to bucket_list structure).
struct hash_set {
    segment_t* main_array[MAIN_ARRAY_LEN];  //main_array is static allocated.
    float load_factor;   //expect value of the length of each bucket list
    unsigned long capacity;   //how many buckets are there in the hash_set, capacity <= MAX_NUM_BUCKETS
    int set_size;  //how many items are there in the hash_set
};


extern void hs_init(struct hash_set * hs, int tid);
extern int hs_add(struct hash_set* hs, int tid, int key);
extern int hs_contains(struct hash_set* hs, int tid, int key);
extern int hs_remove(struct hash_set* hs, int tid, int key); 
extern void hs_print(struct hash_set* hs, int tid);
extern void hs_print_through_bucket(struct hash_set* hs, int tid);
extern void hs_destroy(struct hash_set* hs);

void bucket_list_init(struct bucket_list** bucket, int key);
struct bucket_list* get_bucket_list(struct hash_set* hs, int tid, int bucket_index);
void set_bucket_list(struct hash_set* hs, int tid, int bucket_index, struct bucket_list* new_bucket);
void initialize_bucket(struct hash_set* hs, int tid, int bucket_index);
int get_parent_index(struct hash_set* hs, int bucket_index);


int reverse(int code);
void print_binary(int code, int bit_count);

int hash(int key);
int make_ordinary_key(int key);   //will be changed to type T later.
int make_sentinel_key(int key);
int is_sentinel_key(int key);
int get_origin_key(int key);


#endif
