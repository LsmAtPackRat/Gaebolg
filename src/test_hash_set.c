#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include "utils.h"
#include "hash_set.h"

struct hash_set* hs;
int hs_add_time = 0;
int hs_remove_time = 0;

#if 1
int keys[7] = {8,9,13,10,7,22,23};

unsigned int get_random_int(void){
	static int seed_set = 1;
	if (seed_set){
		seed_set = 0;
		srand((unsigned)time(NULL));	
	}
	return rand();
}

void* thr_func(void* arg){
	unsigned long slave_id = (unsigned long)arg;
        sl_debug("slave %lu is thread %lx.\n", slave_id, pthread_self());
	
	for (int i = 0; i < 10000; i++){
		int index = get_random_int() % 7;
		int add_or_remove = get_random_int() % 2;
		if (add_or_remove) {
			//sl_debug("\nslave %lx insert a node\n", slave_id);
			hs_add(hs, slave_id, keys[index]);
                        SYNC_ADD(&hs_add_time, 1);
		} else {
			//sl_debug("\nslave %lx remove a node\n", slave_id);
			hs_remove(hs, slave_id, keys[index]);
                        SYNC_ADD(&hs_remove_time, 1);
		}
	}
	return NULL;
}

#endif



int main() {   
    #if 0
    hs = (struct hash_set*) malloc(sizeof(struct hash_set)); 
    hs_init(hs, 0);
    hs_add(hs, 0, 8);  //+
    hs_print(hs, 0);
    sl_debug("\n");

    hs_add(hs, 0, 8);  //expect fail
    hs_print(hs, 0);
    sl_debug("\n");

    hs_add(hs, 0, 9);  //+
    
    hs_print(hs, 0);
    sl_debug("\n");

    hs_add(hs, 0, 13);  //+
    hs_print(hs, 0);
    if (hs_remove(hs, 0, 9) == -1)   //-
        sl_debug("   fail to remove 9.\n");
    else 
        sl_debug("   remove 9 success.\n");
    sl_debug("\n");


    hs_add(hs, 0, 7);  //+
    hs_print(hs, 0);
    
    if (hs_contains(hs, 0, 9)) sl_debug("hs contains 9\n");
    sl_debug("\n");

    hs_add(hs, 0, 10);  //+
    hs_print(hs, 0);
    if (hs_contains(hs, 0, 8)) sl_debug("hs contains 8\n");
    sl_debug("\n");
    

    hs_add(hs, 0, 13);  //+
    hs_print(hs, 0);
    if (hs_contains(hs, 0, 13)) sl_debug("hs contains 13\n");
    sl_debug("\n");

    hs_add(hs, 0, 22);   //+
    hs_print(hs, 0);
    if (hs_contains(hs, 0, 23)) sl_debug("hs contains 23\n");
    sl_debug("\n");

    if (hs_contains(hs, 0, 10)) sl_debug("hs contains 10\n");
    hs_print(hs, 0);
    hs_destroy(hs);
    #endif
    
    
    #if 1
    for (int i = 0; i < 1; i++) {
        hs = (struct hash_set*) malloc(sizeof(struct hash_set));
        hs_init(hs, 0);
        pthread_t tid[100];
        for (unsigned long i = 0; i < 3; i++) {
            pthread_create(&tid[i], NULL, thr_func, (void *)i);
        }
        for (int i = 0; i < 3; i++) {
            pthread_join(tid[i], NULL);
        }
    
        hs_print(hs, 0);
        hs_print_through_bucket(hs, 0);
        printf("hs_add_time = %d, hs_remove_time = %d.\n", hs_add_time, hs_remove_time);
        hs_destroy(hs);
    }
    #endif
    return 0;
}




