#ifndef _LINKED_LIST_H
#define _LINKED_LIST_H
#define MAX_NUM_THREADS 32
#define HP_K 2

typedef size_t markable_t;
typedef size_t hp_t;

//the retire nodes are organized in a list headed by hp_item->d_list.
struct hp_rnode {
        hp_t address;
        struct hp_rnode* next;
} __attribute__((aligned(sizeof(long))));


struct hp_item {
	hp_t hp0;    //hp的值
	hp_t hp1;
	struct hp_rnode* d_list;  //本thread的d_list头节点
	int d_count;   //how many hp_rnodes are there in the d_list.
} __attribute__((aligned(sizeof(long))));



//elements type in the linked_list
struct ll_node {
    int key;
    markable_t next;
};

struct linked_list {
    struct ll_node ll_head;  //sentinel node, head the linked list.
    struct hp_item* HP[MAX_NUM_THREADS]; //every thread has a hp_item* in it.
    unsigned int count_of_hp; //upper bound of the hps in the system.
};



#define    IS_TAGGED(v, tag)    ((v) &  tag) 
#define    STRIP_TAG(v, tag)    ((v) & ~tag)
#define    TAG_VALUE(v, tag)    ((v) |  tag)

#define    HAS_MARK(markable_t_value)          (IS_TAGGED((markable_t_value), 0x1) == 0x1)
#define    STRIP_MARK(markable_t_value)        (STRIP_TAG((markable_t_value), 0x1))
#define    MARK_NODE(markable_t_value)         (TAG_VALUE(markable_t_value, 0x1))
#define    GET_NODE(markable_t_value)          ((struct ll_node*)markable_t_value) 


extern void ll_init(struct linked_list* ll);
extern struct ll_node* ll_find(struct linked_list* ll, int tid, int key, struct ll_node** pred, struct ll_node** succ);
extern int ll_insert(struct linked_list* ll, int tid, int key);
extern int ll_remove(struct linked_list* ll, int tid, int key);
extern int ll_contains(struct linked_list* ll, int tid, int key);
extern void ll_print(struct linked_list* ll);
extern void ll_destroy(struct linked_list* ll);



extern struct hp_item* hp_item_setup(struct linked_list* ll, int tid);
extern void hp_setdown(struct linked_list* ll);
extern void hp_save_addr(struct hp_item* hp, int index, hp_t hp_addr);
extern void hp_clear_addr(struct hp_item* hp, int index);
extern hp_t hp_get_addr(struct hp_item* hp, int index);
extern void hp_clear_all_addr(struct hp_item* hp);
extern void hp_dump_statics(struct linked_list* ll);

extern void hp_retire_node(struct linked_list* ll, struct hp_item* hp, hp_t hp_addr);
extern void hp_retire_hp_item(struct linked_list* ll, int tid);

void hp_scan(struct linked_list* ll, struct hp_item* hp);
unsigned int hp_R(unsigned int R);


#endif
