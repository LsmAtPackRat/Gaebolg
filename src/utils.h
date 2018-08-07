#ifndef UTILS_H
#define UTILS_H

#define CAS_EXPECT_DOES_NOT_EXIST ( 0)
#define CAS_EXPECT_EXISTS         (-1)
#define CAS_EXPECT_WHATEVER       (-2)

/*
 * gcc提供的内置函数
 */
#define COUNT_TRAILING_ZEROS(x)		__builtin_ctz(x)  //返回x从左起，第一个1之前的0的个数

#define SYNC_SWAP(addr,x)         __sync_lock_test_and_set(addr,x)
#define SYNC_CAS(addr,old,x)      __sync_val_compare_and_swap(addr,old,x)
#define SYNC_ADD(addr,n)          __sync_add_and_fetch(addr,n)
#define SYNC_SUB(addr,n)          __sync_sub_and_fetch(addr,n)
#define ACCESS_ONCE(x) (*(volatile typeof(x) *)&(x))


//#define sl_debug(fmt, args ...)	 do {fprintf(stderr, fmt, ##args);} while (0)
//#define sl_debug(fmt, args ...) do{}while(0)
#endif
