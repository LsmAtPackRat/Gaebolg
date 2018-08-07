#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include "utils.h"
#include "linked_list.h"

struct linked_list* list;
int keys[20] = {1,3,5,7,2,4,6,8,15,17,20,30,33,36,37,39,50,60,77,100};

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
        sl_debug("slave %lx is thread %lx.\n", slave_id, pthread_self());
	
	for (int i = 0; i < 100000; i++){
		int index = get_random_int() % 20;
		int add_or_remove = get_random_int() % 2;
		if (add_or_remove) {
			//sl_debug("\nslave %lx insert a node\n", slave_id);
			ll_insert(list, slave_id, keys[index]);
		} else {
			//sl_debug("\nslave %lx remove a node\n", slave_id);
			ll_remove(list, slave_id, keys[index]);
		}
	}
	return NULL;
}


int main() {   
    for (int i = 0; i < 100; i++) {
        list = (struct linked_list*) malloc(sizeof(struct linked_list));
        ll_init(list);

        pthread_t tid[100];
        for (unsigned long i = 0; i < 30; i++) {
            pthread_create(&tid[i], NULL, thr_func, (void *)i);
        }
        for (int i = 0; i < 30; i++) {
            pthread_join(tid[i], NULL);
        }
    
        ll_print(list);
        ll_destroy(list);
    }
    //pause();
    return 0;
}




