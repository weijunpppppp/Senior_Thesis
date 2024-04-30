// GCC-VERSION - Not tested for -O3 flag for GCC version above 5.

#ifndef __FAIRLOCK_H__
#define __FAIRLOCK_H__

#define _GNU_SOURCE
#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sched.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <linux/futex.h>
#include <pthread.h>
#include "rdtsc.h"
#include "common.h"

typedef unsigned long long ull;

#ifdef DEBUG
typedef struct stats {
    ull reenter;
    ull banned_time;
    ull start;
    ull next_runnable_wait;
    ull prev_slice_wait;
    ull own_slice_wait;
    ull runnable_wait;
    ull succ_wait;
    ull release_succ_wait;
} stats_t;
#endif

typedef struct flthread_info {
    ull banned_until;
    ull next_acquire_time;
    //for debug///////////////////////
    ull here1;
    ull here2;
    ull here3;
    ull here4;
    ull here5;
    ull here6;
    ull here7;
    //for debug//////////////////////
    int bad_acquires; 
    int lock_acquires;
    float detector; // detector = bad_acquires/lock_acquires
    int in_subq; // whether the thread is (good && it is in the currect subqueue)
    ull weight;
    ull slice;
    ull start_ticks;
    int banned;
    ull bad;
#ifdef DEBUG
    stats_t stat;
#endif
} flthread_info_t;

enum qnode_state {
    INIT = 0, // not waiting or after next runnable node
    NEXT,
    RUNNABLE,
    RUNNING
};

typedef struct qnode {
    int state __attribute__ ((aligned (CACHELINE)));
    int is_good __attribute__ ((aligned (CACHELINE)));
    int in_subq __attribute__ ((aligned (CACHELINE)));
    struct qnode *next __attribute__ ((aligned (CACHELINE)));
} qnode_t __attribute__ ((aligned (CACHELINE)));

typedef struct fairlock {
    qnode_t *qtail __attribute__ ((aligned (CACHELINE)));
    qnode_t *qnext __attribute__ ((aligned (CACHELINE)));
    qnode_t *sub_qfirst __attribute__ ((aligned (CACHELINE)));
    qnode_t *sub_qnext __attribute__ ((aligned (CACHELINE)));
    ull slice __attribute__ ((aligned (CACHELINE)));
    ull sub_slice __attribute__ ((aligned (CACHELINE)));
    int slice_valid __attribute__ ((aligned (CACHELINE)));
    int sub_slice_holding __attribute__ ((aligned (CACHELINE)));
    pthread_key_t flthread_info_key;
    ull total_weight;
} fairlock_t __attribute__ ((aligned (CACHELINE)));

static inline qnode_t *flqnode(fairlock_t *lock) {
    return (qnode_t *) ((char *) &lock->qnext - offsetof(qnode_t, next));
}

static inline int futex(int *uaddr, int futex_op, int val, const struct timespec *timeout) {
    return syscall(SYS_futex, uaddr, futex_op, val, timeout, NULL, 0);
}

int fairlock_init(fairlock_t *lock) {
    int rc;

    lock->qtail = NULL;
    lock->qnext = NULL;
    lock->sub_qfirst = NULL;
    lock->sub_qnext = NULL;
    lock->total_weight = 0;
    lock->slice = 0;
    lock->sub_slice = 0;
    lock->slice_valid = 0;
    lock->sub_slice_holding = 0;
    if (0 != (rc = pthread_key_create(&lock->flthread_info_key, NULL))) {
        return rc;
    }
    return 0;
}

static flthread_info_t *flthread_info_create(fairlock_t *lock, int weight) {
    flthread_info_t *info;
    info = malloc(sizeof(flthread_info_t));
    info->banned_until = rdtsc();
    info->next_acquire_time = rdtsc();
    info->bad_acquires = 1;
    info->lock_acquires = 0;
    info->detector = 1;
    info->in_subq = 0;
    //for debug///////////////////////
    info->here1 = 0;
    info->here2 = 0;
    info->here3 = 0;
    info->here4 = 0;
    info->here5 = 0;
    info->here6 = 0;
    info->here7 = 0;
    //for debug//////////////////////
    if (weight == 0) {
        int prio = getpriority(PRIO_PROCESS, 0);
        weight = prio_to_weight[prio+20];
    }
    info->weight = weight;
    __sync_add_and_fetch(&lock->total_weight, weight);
    info->banned = 0;
    info->bad = 0;
    info->slice = 0;
    info->start_ticks = 0;
#ifdef DEBUG
    memset(&info->stat, 0, sizeof(stats_t));
    info->stat.start = info->banned_until;
#endif
    return info;
}

void fairlock_thread_init(fairlock_t *lock, int weight) {
    flthread_info_t *info;
    info = (flthread_info_t *) pthread_getspecific(lock->flthread_info_key);
    if (NULL != info) {
        free(info);
    }
    info = flthread_info_create(lock, weight);
    pthread_setspecific(lock->flthread_info_key, info);
}

int fairlock_destroy(fairlock_t *lock) {
    //return pthread_key_delete(lock->flthread_info_key);
    return 0;
}

void fairlock_acquire(fairlock_t *lock) {
    flthread_info_t *info;
    ull now;

    info = (flthread_info_t *) pthread_getspecific(lock->flthread_info_key);
    if (NULL == info) {
        info = flthread_info_create(lock, 0);
        pthread_setspecific(lock->flthread_info_key, info);
    }

    if (info->lock_acquires > 100){
        info->lock_acquires = 0;
        info->bad_acquires = 1;
    }
    info->lock_acquires++;
    if ((now = rdtsc()) <= info->next_acquire_time){
        info->bad_acquires++;
        info->bad++; 
    }
    info->detector = (double)info->bad_acquires/info->lock_acquires;

    if (info->detector >= 0.1 && info->in_subq == 1){
        info->in_subq = 0;
        goto begin;
    }

    if (readvol(lock->slice_valid)) {
        ull curr_slice = lock->slice;
        qnode_t *succ = readvol(lock->qnext);
    
        if (info->detector<0.1 && info->in_subq == 1 && (now = rdtsc()) < curr_slice){
            info->here1++;
            if (rdtsc()>= lock->slice){
                goto begin;
            }

            while (0 == __sync_bool_compare_and_swap(&lock->sub_slice_holding, 0, 1)){
                info->here6++;
                curr_slice = lock->slice;
                if (rdtsc()>= lock->slice){
                    goto begin;
                }
                ull slice_left = curr_slice - rdtsc();
                struct timespec timeout = {
                    .tv_sec = 0, // slice will be less then 1 sec
                    .tv_nsec = (slice_left / (CYCLE_PER_US * SLEEP_GRANULARITY)) * SLEEP_GRANULARITY * 1000,
                };
                //todo: something wrong here!!!!!!!!!!!!!!!!!!!!!!!!!!!
                futex(&lock->sub_slice_holding, FUTEX_WAIT_PRIVATE, 1, &timeout);
                info->here3++;
                if (rdtsc()>= lock->slice){
                    info->here2++;
                    lock->sub_slice_holding = 0;
                    futex(&lock->sub_slice_holding, FUTEX_WAKE_PRIVATE, 1, NULL);
                    goto begin;
                }
            }


            //invariant: current thread grad the sub_slice lock
            if (NULL == succ){
                if (__sync_bool_compare_and_swap(&lock->qtail, NULL, flqnode(lock)))
                    goto reenter;
                spin_then_yield(SPIN_LIMIT, (now = rdtsc()) < curr_slice && NULL == (succ = readvol(lock->qnext)));
#ifdef DEBUG
                info->stat.own_slice_wait += rdtsc() - now;
#endif
                if (now >= curr_slice){
                    lock->sub_slice_holding = 0;
                    futex(&lock->sub_slice_holding, FUTEX_WAKE_PRIVATE, 1, NULL);
                    goto begin;  
                }      
            }

            if (succ != NULL && succ->is_good){
                succ->in_subq = 1;
            }

            if (succ->state < RUNNABLE || __sync_bool_compare_and_swap(&succ->state, RUNNABLE, NEXT)) {
                goto reenter;
            }
        }


        // If owner of current slice, try to reenter at the beginning of the queue
        else if (curr_slice == info->slice && (now = rdtsc()) < curr_slice) {
            if (NULL == succ) {
                if (__sync_bool_compare_and_swap(&lock->qtail, NULL, flqnode(lock)))
                    goto reenter;
                spin_then_yield(SPIN_LIMIT, (now = rdtsc()) < curr_slice && NULL == (succ = readvol(lock->qnext)));
#ifdef DEBUG
                info->stat.own_slice_wait += rdtsc() - now;
#endif
                // let the succ invalidate the slice, and don't need to wake it up because slice expires naturally
                if (now >= curr_slice)
                    goto begin;
            }
            
            // if state < RUNNABLE, it won't become RUNNABLE unless someone releases lock,
            // but as no one is holding the lock, there is no race

            //todo: here need to introduce and merge next good thread
            if (succ->state < RUNNABLE || __sync_bool_compare_and_swap(&succ->state, RUNNABLE, NEXT)) {
reenter:
#ifdef DEBUG
                info->stat.reenter++;
#endif
                info->start_ticks = now;
                return;
            }
        }
    }
begin:

    if (info->banned) {
        if ((now = rdtsc()) < info->banned_until) {
            ull banned_time = info->banned_until - now;
#ifdef DEBUG
            info->stat.banned_time += banned_time;
#endif
            // sleep with granularity of SLEEP_GRANULARITY us
            while (banned_time > CYCLE_PER_US * SLEEP_GRANULARITY) {
                struct timespec req = {
                    .tv_sec = banned_time / CYCLE_PER_S,
                    .tv_nsec = (banned_time % CYCLE_PER_S / CYCLE_PER_US / SLEEP_GRANULARITY) * SLEEP_GRANULARITY * 1000,
                };
                nanosleep(&req, NULL);
                if ((now = rdtsc()) >= info->banned_until)
                    break;
                banned_time = info->banned_until - now;
            }
            // spin for the remaining (<SLEEP_GRANULARITY us)
            spin_then_yield(SPIN_LIMIT, (now = rdtsc()) < info->banned_until);
        }
    }

    qnode_t n = { 0 };
    if (info->detector < 0.1){
        n.is_good = 1;
    }else{
        n.is_good = 0;
    }
    while (1) {
        qnode_t *prev = readvol(lock->qtail);
        if (__sync_bool_compare_and_swap(&lock->qtail, prev, &n)) {
            // enter the lock queue
            if (NULL == prev) {
                if (n.is_good){
                    info->here4++;
                    info->in_subq = 1;
                    n.in_subq = 1;
                }
                n.state = RUNNABLE;
                lock->qnext = &n;
            } else {
                if (prev == flqnode(lock)) {
                    n.state = NEXT;
                    prev->next = &n;
                } else {
                    //todo: here2 and here3
                    prev->next = &n;
                    if (prev->in_subq = 1 && n.is_good){
                        info->in_subq = 1;
                        n.in_subq = 1;
                    }
                    // wait until we become the next runnable
#ifdef DEBUG
                    now = rdtsc();
#endif
                    do {
                        futex(&n.state, FUTEX_WAIT_PRIVATE, INIT, NULL);
                    } while (INIT == readvol(n.state));
#ifdef DEBUG
                    info->stat.next_runnable_wait += rdtsc() - now;
#endif
                }
            }
            // invariant: n.state >= NEXT

            // wait until the current slice expires
            int slice_valid;
            ull curr_slice;
            //todo: here need to divide in different cases 
            if (prev == flqnode(lock) && n.is_good == 1){
                while ((slice_valid = readvol(lock->slice_valid)) && (now = rdtsc()) + SLEEP_GRANULARITY < (curr_slice = readvol(lock->slice))) {
                    ull slice_left = curr_slice - now;
                    struct timespec timeout = {
                        .tv_sec = 0, // slice will be less then 1 sec
                        .tv_nsec = (slice_left / (CYCLE_PER_US * SLEEP_GRANULARITY)) * SLEEP_GRANULARITY * 1000,
                    };
                    futex(&lock->sub_slice_holding, FUTEX_WAIT_PRIVATE, 1, &timeout);
                    if (1 == __sync_bool_compare_and_swap(&lock->sub_slice_holding, 0, 1)){
                        n.state=RUNNABLE;
                        break;
                    }
                }
                if (rdtsc() >= readvol(lock->slice)){
                    lock->slice_valid = 0;
                    n.in_subq = 1;
                    info->in_subq = 1;
                }
                else{
                    if (n.in_subq == 0){
                        if (slice_valid) {
                            spin_then_yield(SPIN_LIMIT, (slice_valid = readvol(lock->slice_valid)) && rdtsc() < readvol(lock->slice));
                            if (slice_valid)
                                lock->slice_valid = 0;   
                        }
                    }
                }

            }
            //todo: do I need to consider the case: prev ==flqnode(lock) && n.isgood = 0
            //todo: consider change the place of grabbing sublock
            else if (prev != NULL && prev->in_subq && n.in_subq){
                while((now = rdtsc()) < (curr_slice = readvol(lock->slice))){
                    ull slice_left = curr_slice - now;
                    struct timespec timeout = {
                        .tv_sec = 0, // slice will be less then 1 sec
                        .tv_nsec = (slice_left / (CYCLE_PER_US * SLEEP_GRANULARITY)) * SLEEP_GRANULARITY * 1000,
                    };
                    futex(&lock->sub_slice_holding, FUTEX_WAIT_PRIVATE, 1, &timeout);
                    if (1 == __sync_bool_compare_and_swap(&lock->sub_slice_holding, 0, 1)){
                        n.state = RUNNABLE;
                        break;
                    }
                }
                if (rdtsc() >= readvol(lock->slice)){
                    lock->slice_valid = 0;
                }
            }
            else if (prev == NULL && n.is_good){
                info->here7++;
                // wait for the sub lock
                while((now = rdtsc()) < (curr_slice = readvol(lock->slice))){
                    ull slice_left = curr_slice - now;
                    struct timespec timeout = {
                        .tv_sec = 0, // slice will be less then 1 sec
                        .tv_nsec = (slice_left / (CYCLE_PER_US * SLEEP_GRANULARITY)) * SLEEP_GRANULARITY * 1000,
                    };
                    futex(&lock->sub_slice_holding, FUTEX_WAIT_PRIVATE, 1, &timeout);
                    if (1 == __sync_bool_compare_and_swap(&lock->sub_slice_holding, 0, 1)){
                        n.state=RUNNABLE;
                        break;
                    }
                }
                if (rdtsc() >= readvol(lock->slice)){
                    lock->slice_valid = 0;
                }
            }

            else{
                info->here5++;
            while ((slice_valid = readvol(lock->slice_valid)) && (now = rdtsc()) + SLEEP_GRANULARITY < (curr_slice = readvol(lock->slice))) {
                ull slice_left = curr_slice - now;
                struct timespec timeout = {
                    .tv_sec = 0, // slice will be less then 1 sec
                    .tv_nsec = (slice_left / (CYCLE_PER_US * SLEEP_GRANULARITY)) * SLEEP_GRANULARITY * 1000,
                };
                futex(&lock->slice_valid, FUTEX_WAIT_PRIVATE, 0, &timeout);
#ifdef DEBUG
                info->stat.prev_slice_wait += rdtsc() - now;
#endif
            }
            if (n.is_good){
                n.in_subq = 1;
                info->in_subq = 1;
            }
            if (slice_valid) {
                spin_then_yield(SPIN_LIMIT, (slice_valid = readvol(lock->slice_valid)) && rdtsc() < readvol(lock->slice));
                if (slice_valid)
                    lock->slice_valid = 0;
            }
            }
            // invariant: rdtsc() >= curr_slice && lock->slice_valid == 0

#ifdef DEBUG
            now = rdtsc();
#endif
            // spin until RUNNABLE and try to grab the lock


            spin_then_yield(SPIN_LIMIT, RUNNABLE != readvol(n.state) || 0 == __sync_bool_compare_and_swap(&n.state, RUNNABLE, RUNNING));



            // invariant: n.state == RUNNING
#ifdef DEBUG
            info->stat.runnable_wait += rdtsc() - now;
#endif

            // record the successor in the lock so we can notify it when we release
            qnode_t *succ = readvol(n.next);
            if (NULL == succ) {
                lock->qnext = NULL;
                if (0 == __sync_bool_compare_and_swap(&lock->qtail, &n, flqnode(lock))) {
                    spin_then_yield(SPIN_LIMIT, NULL == (succ = readvol(n.next)));
#ifdef DEBUG
                    info->stat.succ_wait += rdtsc() - now;
#endif
                    lock->qnext = succ;
                }
            } else {
                lock->qnext = succ;
            }
            // invariant: NULL == succ <=> lock->qtail == flqnode(lock)

            now = rdtsc();
            info->start_ticks = now;
            info->slice = now + FAIRLOCK_GRANULARITY;
            lock->slice = info->slice;
            lock->slice_valid = 1;
            if (n.is_good){
                if (prev == NULL || n.in_subq == 1){
                    lock->sub_slice_holding = 1;
                    info->in_subq = 1;
                }
            }
            // wake up successor if necessary
            if (succ) {
                succ->state = NEXT;
                futex(&succ->state, FUTEX_WAKE_PRIVATE, 1, NULL);
            }
            return;
        }
    }
}

void fairlock_release(fairlock_t *lock) {
    ull now, cs;
#ifdef DEBUG
    ull succ_start = 0, succ_end = 0;
#endif
    flthread_info_t *info;

    qnode_t *succ = lock->qnext;
    if (NULL == succ) {
        if (__sync_bool_compare_and_swap(&lock->qtail, flqnode(lock), NULL)){
            goto accounting;

        }
#ifdef DEBUG
        succ_start = rdtsc();
#endif
        spin_then_yield(SPIN_LIMIT, NULL == (succ = readvol(lock->qnext)));
#ifdef DEBUG
        succ_end = rdtsc();
#endif
    }
    // if (NULL == succ && NULL != sub_succ){
    //     sub_succ->sub_state = RUNNABLE;
    // }

    succ->state = RUNNABLE;
    //futex(&succ->sub_state, FUTEX_WAKE_PRIVATE, 1, NULL);

accounting:
    // invariant: NULL == succ || succ->state = RUNNABLE
    info = (flthread_info_t *) pthread_getspecific(lock->flthread_info_key);
    now = rdtsc();
    cs = now - info->start_ticks;
    info->banned_until += cs * (__atomic_load_n(&lock->total_weight, __ATOMIC_RELAXED) / info->weight);
    info->next_acquire_time = now + cs * ((__atomic_load_n(&lock->total_weight, __ATOMIC_RELAXED) / (double)info->weight)) - cs;
    info->banned = now < info->banned_until;

    lock->sub_slice_holding = 0;
    futex(&lock->sub_slice_holding, FUTEX_WAKE_PRIVATE, 1, NULL);

    if (info->banned) {
        if (__sync_bool_compare_and_swap(&lock->slice_valid, 1, 0)) {
            futex(&lock->slice_valid, FUTEX_WAKE_PRIVATE, 1, NULL);
        }
    }
#ifdef DEBUG
    info->stat.release_succ_wait += succ_end - succ_start;
#endif
}

#endif // __FAIRLOCK_H__