/* Copyright (C) 2007-2022 Free Software Foundation, Inc.
   Contributed by Richard Henderson <rth@redhat.com>.

   This file is part of the GNU Offloading and Multi Processing Library
   (libgomp).

   Libgomp is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3, or (at your option)
   any later version.

   Libgomp is distributed in the hope that it will be useful, but WITHOUT ANY
   WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
   FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
   more details.

   Under Section 7 of GPL version 3, you are granted additional
   permissions described in the GCC Runtime Library Exception, version
   3.1, as published by the Free Software Foundation.

   You should have received a copy of the GNU General Public License and
   a copy of the GCC Runtime Library Exception along with this program;
   see the files COPYING3 and COPYING.RUNTIME respectively.  If not, see
   <http://www.gnu.org/licenses/>.  */

/* This file handles the maintenance of tasks in response to task
   creation and termination.  */

#include "libgomp.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "gomp-constants.h"

typedef struct gomp_task_depend_entry *hash_entry_type;

static inline void *
htab_alloc (size_t size)
{
  return gomp_malloc (size);
}

static inline void
htab_free (void *ptr)
{
  free (ptr);
}

#include "hashtab.h"

static inline hashval_t
htab_hash (hash_entry_type element)
{
  return hash_pointer (element->addr);
}

static inline bool
htab_eq (hash_entry_type x, hash_entry_type y)
{
  return x->addr == y->addr;
}

#ifdef GOMP_USE_XQUEUE
/**
 * Random generator from stackoverflow
*/
unsigned short lfsr = 0xACE1u;
unsigned bit;
unsigned myrand(){
    bit  = ((lfsr >> 0) ^ (lfsr >> 2) ^ (lfsr >> 3) ^ (lfsr >> 5) ) & 1;
    return lfsr =  (lfsr >> 1) | (bit << 15);
}

// declare the functions
void gomp_alloc_task_q(struct gomp_thread *);
static int gomp_push_task(struct gomp_task *);
static gomp_task_t* gomp_remove_my_task();
static gomp_task_t* gomp_remove_aux_task(unsigned long *);

#ifdef XTASK_ENABLE_STATS
#include <stdio.h>
void xstats_init(){
	struct gomp_thread *thr = gomp_thread();
	xstats_data_t *xd = &thr->xstats;
	xd->edix = 0;
	xd->sd = (xstats_data_cell_t *)gomp_malloc(sizeof(xstats_data_cell_t) * XSTATS_MAX_EVENTS);
	char *fpath = getenv("XSTATS_PATH");
	snprintf(xd->fname, 128, "%s/xstats_%d.csv", fpath, thr->ts.team_id);
}

void xstats_record(xstats_type_t event, unsigned long long v0, unsigned long long v1, unsigned long long v2){
	struct gomp_thread *thr = gomp_thread();
	if(thr->xstats.edix >= XSTATS_MAX_EVENTS){
		xtask_debug(0, 1, "xstats buffer is full.");
		return;
	}
	xstats_data_cell_t *sd = &thr->xstats.sd[thr->xstats.edix];
	unsigned int aux;

	sd->ts = __rdtscp(&aux);	
	sd->event = event;
	// different event has different meaning of v0, v1, v2
	sd->v0 = v0; // 
	sd->v1 = v1; // 
	sd->v2 = v2; // 
	thr->xstats.edix++;
}

void xstats_dump(struct gomp_thread *thr){
	xstats_data_t *xd = &thr->xstats;
	xstats_data_cell_t *sd = xd->sd;
	FILE *fp = fopen(xd->fname, "w");
	if(fp == NULL){
		xtask_debug(0, 1, "failed to open file %s", xd->fname);
		return;
	}
	fprintf(fp, "timestamp,event,v0,v1,v2");
	for(unsigned long long i = 0; i < xd->edix; i++){
		fprintf(fp, "\n%llu,%d,%llu,%llu,%llu", sd[i].ts, sd[i].event, sd[i].v0, sd[i].v1, sd[i].v2);
	}
	fclose(fp);
}
#endif


#ifdef XTASK_SWS // simple workstealing
// declarations;
static inline void xtask_ws_init();
static int xtask_steal_req();
static void xtask_handle_req();
static inline long long xtask_get_load(int tid);
static inline int xtask_ws_push_tasks(int n, int ttid, unsigned long *last_qid);


static inline void xtask_ws_init(){
	struct gomp_thread *thr = gomp_thread();
	wsi_t *wsi = &thr->wsi;
	wsi->info.high_load = thr->num_queues * HIGH_LOAD;
	wsi->info.low_load = thr->num_queues * LOW_LOAD;
	wsi->round = 1;
	wsi->req = 0;

	wsi->flag = WS_INITIAL;
	wsi->last_thr = 0;
	wsi->load = 0;

}

static inline long long xtask_get_load(int tid){
	struct gomp_thread *thr = gomp_thread()->thread_pool->threads[tid];
	struct gomp_taskq **taskq = thr->td_task_q;
	long long load = 0;
	for(int i = 0; i < thr->num_queues; i++){
		load += taskq[i]->nin - taskq[i]->nout;
	}
	return load;
}


static int xtask_steal_req(){
	struct gomp_thread *thr = gomp_thread();
	wsi_t *wsi = &thr->wsi;
	if(wsi->flag == WS_STEALING)
		return WS_REQ_PENDING;

	int last_thr = wsi->last_thr;
	for(int i = last_thr, j = 0; j < thr->num_queues; j++){
		int ttid = i < thr->num_queues ? i : 0; // target thid
		struct gomp_thread *vthr = thr->thread_pool->threads[ttid]; // victim thread
		long long load = xtask_get_load(ttid);
		vthr->wsi.load = load;

		if(load > wsi->info.high_load){
			// xtask_debug(0, 1, "highload: load=%lld, high_load=%lld", load, wsi->info.high_load);
			if(WS_REQ2ROUND(vthr->wsi.req) < vthr->wsi.round){
				// send req
				uint64_t round = vthr->wsi.round;
				vthr->wsi.req = WS_TID2REQ(thr->ts.team_id) | round;
				wsi->last_thr = i;

				// xtask_debug(0, 1, "send req to T#%d, load=%lld, round=%ld", ttid, load, round);
				wsi->flag = WS_STEALING;
				return WS_REQ_SENT;
			}
		}
		i++;
	}
	return WS_REQ_FAILED;
}

static inline int xtask_ws_push_tasks(int n, int ttid, unsigned long *last_qid){
	struct gomp_thread *thr = gomp_thread();
	// wsi_t *wsi = &thr->wsi;
	int tid = thr->ts.team_id;

	struct gomp_thread *thief_thr = thr->thread_pool->threads[ttid];
	
	
	// 
	//	    kmp_uint64 last_q = (stealer_id < gtid) ? task_team->tt.tt_nproc + stealer_id - gtid :
	//      stealer_id - gtid; //abs((kmp_int64)stealer_id - (kmp_int64)gtid);

	// this is the queue I from the thief where I can enqueue tasks
	int thief_qid_of_me = ttid < tid ? thr->num_queues + ttid - tid : ttid - tid;
	struct gomp_taskq *thief_task_q = thief_thr->td_task_q[thief_qid_of_me];
	


	gomp_task_t *task = NULL;
	int num_tries = 0;
	int npushed = 0;
	do{
		// first check if the thief's q of mine is full
		while(thief_task_q->td_deque[thief_task_q->td_deque_head] != NULL){
			num_tries++;
			if(num_tries < 25)
				continue;
			return npushed;
		}
		// there is an empty slot, we remove tasks from my q
		task = (gomp_task_t *) gomp_remove_aux_task(last_qid);
		if(task == NULL)
			task = (gomp_task_t *) gomp_remove_my_task();
		if(task == NULL)
			return npushed;

		thief_task_q->nin++;
		thief_task_q->td_deque[thief_task_q->td_deque_head] = task;
		thief_task_q->td_deque_head = (thief_task_q->td_deque_head + 1) & TASK_DEQUE_MASK(thr);
		thief_thr->last_q_accessed = thief_qid_of_me;

		npushed++;
	}while(task != NULL && npushed < n);

	return npushed;
}


static void xtask_handle_req(unsigned long *last_qid){
	struct gomp_thread *thr = gomp_thread();
	// int tid = thr->ts.team_id;
	wsi_t *wsi = &thr->wsi;
	wsi->flag = WS_INITIAL;

	// we don't need to update the round info as it is already updated by the thief
	// we only check if there is any request, if I got request, the thief think I have high load
	if(wsi->round == WS_REQ2ROUND(wsi->req)){
		// push tasks to thief
		int npushed = xtask_ws_push_tasks(2, WS_REQ2TID(wsi->req), last_qid);
		wsi->load -= npushed;
		wsi->round++;
		// xtask_debug(0, 1, "request found from T#%d, npushed=%d, round=%ld", WS_REQ2TID(wsi->req), npushed, wsi->round);
	}
}



#endif




#ifdef XTASK_LLWS
/**
 * XWS - XWorkStealing
*/

/**
 * XWS - Init
 * Called by alloc_task_q
*/
static inline void xws_init();


/**
 * Load Index related API
*/

/**
 * XWS - XWorkStealing
 * Find the victims
 * @param int n - the number of victims to find
 * @return true if the victims are found, false otherwise.
*/
bool xws_find_victims(int n);

/**
 * XWS - XWorkStealing
 * Reset the flags of current thread's load to inital state
*/
static void xws_reset();

/**
 * XWS - XWorkStealing
 * Update the loads of the threads
*/
static inline void xws_update_loads();

/**
 * Work-Stealing related API
*/

/**
 *  Called by the thief after it found nothing from q-ops
*/
bool xws_send_reqs();

/**
 * Called by the victim after it found tasks from q-ops
*/
bool xws_handle_reqs(unsigned long *last_qid);


static inline void xws_init(){
	struct gomp_thread *thr = gomp_thread();
	xws_t *xws = &thr->xws;

	xws->batch_size = XWS_BATCH_SIZE < thr->num_queues - 1 ? XWS_BATCH_SIZE : thr->num_queues - 1;
	xws->ld_states.very_low = (unsigned) XWS_VERY_LOW * thr->num_queues;
	xws->ld_states.low = (unsigned) XWS_LOW * thr->num_queues;
	xws->ld_states.high = (unsigned) XWS_HIGH * thr->num_queues;

	xws->flag = XWS_INIT_VAL;
	xws->nreqs = 0;
	xws->round = 1;
	xws->req = 0;

	xws->nops = 0;
	xws->ld_info.last_updated_tid = thr->ts.team_id;

	// allocate memory for sum and lds
	xws->ld_info.tsums = (long long *)gomp_malloc(sizeof(long long) * thr->num_queues);
	memset(xws->ld_info.tsums, 0, sizeof(long long) * thr->num_queues);
	xws->ld_info.lds = (volatile load_info_cell_t *)gomp_malloc(sizeof(load_info_cell_t) * thr->num_queues);
	for(int i = 0; i < thr->num_queues; i++){
		xws->ld_info.lds[i].ldi = 0;
		xws->ld_info.lds[i].visited = false;

	}
// 	memset(xws->ld_info.lds, 0, sizeof(load_info_cell_t) * thr->num_queues);
}


static inline void xws_printloads(int tid){
	struct gomp_thread *thr = gomp_thread();
	xws_t *xws = &thr->xws;

	if(thr->ts.team_id != tid)
		return;

	
	for(int i = 0; i < thr->num_queues; i++){
		long long load = 0;
		// my load and other's load
		struct gomp_thread *tthr = thr->thread_pool->threads[i];
		struct gomp_taskq **taskq = tthr->td_task_q;
		// freshly calcuated load
		for(int j = 0; j < thr->num_queues; j++){
			load += taskq[j]->nin - taskq[j]->nout;
		}
		if(tthr->xws.ld_info.lds[i].ldi > xws->ld_states.high)
			xtask_debug(0, 1, "ttid=%d, load=%lld/%d, my_ldi=%lld, visited=%d, their_ldi=%lld, nops=%llu,"
			" req=%lu, round=%lu",
			i, 
			load, 
			xws->ld_states.high,
			xws->ld_info.lds[i].ldi, 
			xws->ld_info.lds[i].visited,
			tthr->xws.ld_info.lds[i].ldi,
			tthr->xws.nops,
			tthr->xws.req,
			tthr->xws.round
			);
	
	}
}

static void xws_reset(){
	struct gomp_thread *thr = gomp_thread();
	xws_t *xws = &thr->xws;
	xws->nreqs = 0;
	for(int i = 0; i < thr->num_queues; i++){
		xws->ld_info.lds[i].visited = false;
	}
}

static inline void xws_update_loads(int qid){
	struct gomp_thread *thr = gomp_thread();
	xws_t *xws = &thr->xws;
	load_info_t *ld_info = &xws->ld_info;
	int tid = thr->ts.team_id;
	// xtask_debug(0, 1, "enters.");

	int next_qid;
	if(qid == -1){
		// default action
		// accumulated prefix sum, each iter, we update the load of one queue
		ld_info->tsum += thr->td_task_q[ld_info->qid]->nin - thr->td_task_q[ld_info->qid]->nout;
		// save this accumulated prefix sum for further load calculation
		// TODO: we can batch this if necessary
		ld_info->tsums[ld_info->qid] = ld_info->tsum;


		next_qid = ld_info->qid + 1 < thr->num_queues ? ld_info->qid + 1 : 0;
		
		// update load index
		ld_info->lds[tid].ldi = ld_info->tsums[ld_info->qid] - ld_info->tsums[next_qid];

		ld_info->qid = next_qid;
		// update others
		ld_info->last_updated_tid = ld_info->last_updated_tid + 1 < thr->num_queues ? ld_info->last_updated_tid + 1 : 0;
		struct gomp_thread *tthr = thr->thread_pool->threads[ld_info->last_updated_tid];
		tthr->xws.ld_info.lds[tid].ldi = xws->ld_info.lds[tid].ldi;

	}else{
		// xtask_debug(0, 1, "etners. qid=%d.", qid);
		// there is a specific qid we want to update
		while(qid != ld_info->qid){
			ld_info->tsum += thr->td_task_q[ld_info->qid]->nin - thr->td_task_q[ld_info->qid]->nout;
			ld_info->tsums[ld_info->qid] = ld_info->tsum;
			next_qid = ld_info->qid + 1 < thr->num_queues ? ld_info->qid + 1 : 0;
			ld_info->lds[tid].ldi = ld_info->tsums[ld_info->qid] - ld_info->tsums[next_qid];
			ld_info->qid = next_qid;
		}
	}
	xws->nops++;

}

static inline void xws_update_load(int ttid){
	struct gomp_thread *thr = gomp_thread();
	xws_t *xws = &thr->xws;
	thr->thread_pool->threads[ttid]->xws.ld_info.lds[thr->ts.team_id].ldi = xws->ld_info.lds[thr->ts.team_id].ldi;
}

bool xws_find_victims(int n){
	struct gomp_thread *thr = gomp_thread();
	xws_t *xws = &thr->xws;
	if(xws->flag != XWS_STEALING)
		xws_reset();

	int m = 0; // record number of victims that can send steal requests to
	for(int i = xws->last_req_qid, j = 0; j < thr->num_queues; j++){
		int qid = i < thr->num_queues ? i : 0;
		if(xws->ld_info.lds[qid].ldi > xws->ld_states.high && !xws->ld_info.lds[qid].visited){
			xws->ld_info.lds[qid].visited = true;
			xws->victims[m++] = qid;
			xws->nreqs++;
			if(m >= n){
				xws->last_req_qid = qid;
				return true;
			}
		}
		i++;
	}
	// xws_printloads(3);

	return false;
}

static int xws_push_tasks(int ttid, int n, unsigned long * last_qid){
	struct gomp_thread *thr = gomp_thread();
	struct gomp_thread *target_thr = thr->thread_pool->threads[ttid];
	int tid = thr->ts.team_id;
	int target_qid = ttid - tid < 0 ? ttid - tid + thr->num_queues : ttid - tid;


	// First remove aux queue - I think it is good for locality since my tasks are created localy
	gomp_task_t *task = NULL;
	struct gomp_taskq *task_q = NULL;
	int num_tries = 0, npushed = 0; // npushed is counter of successful pushes
	enum xws_flag flag = XWS_INIT_VAL;

	// starting from last q
	int qid = thr->last_q_accessed;
	int num_q_accessed = 0;

	while(num_q_accessed <= thr->num_queues){
		task_q = thr->td_task_q[qid];
		// Finding slots in thief's q
		while(target_thr->td_task_q[target_qid]->td_deque[target_thr->td_task_q[target_qid]->td_deque_head] != NULL){
			num_tries++;
			if(num_tries < 25)
				continue;
			flag = XWS_TASK_QUEUE_FULL; // target thr's q is full
			break;
		}

		if(flag == XWS_TASK_QUEUE_FULL)
			break;
		// target thr's q
		struct gomp_taskq *target_task_q = target_thr->td_task_q[target_qid];

		// Finding existing tasks in my q
		while((task = (gomp_task_t *) task_q->td_deque[task_q->td_deque_tail]) && npushed < n){
			task_q->td_deque[task_q->td_deque_tail] = NULL;
			task_q->td_deque_tail = (task_q->td_deque_tail + 1) & TASK_DEQUE_MASK(thr);
			target_task_q->nin++;
			target_task_q->td_deque[target_task_q->td_deque_head] = task;
			target_task_q->td_deque_head = (target_task_q->td_deque_head + 1) & TASK_DEQUE_MASK(thr);
			*last_qid = qid;
			task_q->nout++;
			
			if(npushed++ >= n)
				return npushed;
			
		}
		num_q_accessed++;
		qid = qid + 1 < thr->num_queues ? qid + 1 : 0;
	}

	return npushed;
}

/**
 * This is called only by the thief. A worker is a thief only when it found nothing after q-ops
*/
bool xws_send_reqs(){
	struct gomp_thread *thr = gomp_thread();
	xws_t *xws = &thr->xws;

	// for now, ignore the case when worker is already stealing
	if(xws->flag == XWS_STEALING)
		return false;


	bool find_victims = xws_find_victims(xws->batch_size);
	// if(find_victims)
	// 	xtask_debug(0, 1, "start stealing. nreqs=%d, find_victims=%d", xws->nreqs, find_victims);

	if(!find_victims)
		return false;

	int tid = thr->ts.team_id; // my tid
	uint64_t hb_tid = XWS_TID_TO_HIGH_BITS(tid);
	for(int i = 0; i < xws->batch_size; i++){
		xws_t *victim_xws = &thr->thread_pool->threads[xws->victims[i]]->xws;
		// send steal request to the victim
		victim_xws->req = victim_xws->round | hb_tid;
	}
	xws->flag = XWS_STEALING;
	return true;
}

bool xws_handle_reqs(unsigned long *last_qid){
	
	struct gomp_thread *thr = gomp_thread();
	xws_t *xws = &thr->xws;
	int tid = thr->ts.team_id;
	xws->flag = XWS_INIT_VAL; // I no longer steal, since this is called when tasks found
	
	// check load, and only respond when my load is high
	if(xws->ld_info.lds[tid].ldi >= xws->ld_states.high){
		// xtask_debug(0, 0, "enters. Load=%lld/%d, req=", xws->ld_info.lds[tid].ldi, xws->ld_states.high);
		uint64_t round = XWS_REQ_TO_ROUND(xws->req);
		int ttid = XWS_REQ_TO_TID(xws->req);
	
		if(round == xws->round){
			// xtask_debug(0, 1, "handle_req_from T#%d, load=%lld, round=%ld, req=%ld", ttid, xws->ld_info.lds[tid].ldi, round, xws->req);
			// remove tasks from my side and push to the thief
			int npushed __attribute__((unused));
			npushed = xws_push_tasks(ttid, 8, last_qid);
			// xtask_debug(0, 1, "task pushed=%d", npushed);
			// upload the load info
			xws_update_loads((int)(*last_qid));
			xws_update_load(ttid);

			
			xws->round++;
			return true;
		}
	}
	xws_update_loads(-1);
	return false;
}



#endif // XTASK_LLWS



#ifdef XTASK_RANDOM_WS


static inline void rws_init(){
	struct gomp_thread *thr = gomp_thread();
	rws_t *rws = &thr->rws;
	rws->round = 1;
	rws->req = 0;
	rws->flag = RWS_INIT_VAL;
	rws->batch_size = RWS_BATCH_SIZE < thr->num_queues - 1 ? RWS_BATCH_SIZE : thr->num_queues - 1;
	for(int i = 0; i < rws->batch_size; i++){
		rws->victims[i] = 0;
	}
}

static void find_victims(int n){
	struct gomp_thread *thr = gomp_thread();
	rws_t *rws = &thr->rws;
	for(int i = 0; i < n; i++){
		rws->victims[i] = abs(myrand() % thr->num_queues);
	}
}

static inline int steal_req(){
	struct gomp_thread *thr = gomp_thread();
	rws_t *rws = &thr->rws;
	if(rws->flag == RWS_STEALING)
		return RWS_STEAL_PENDING;
	find_victims(rws->batch_size);
	for(int i = 0; i < rws->batch_size; i++){
		int ttid = rws->victims[i];
		struct gomp_thread *vthr = thr->thread_pool->threads[ttid];
		if(WS_REQ2ROUND(vthr->rws.req) < vthr->rws.round){
			vthr->rws.req = (WS_TID2REQ(thr->ts.team_id) | vthr->rws.round);
			// xtask_debug(0, 0, "send req to T#%d, their_round=%ld, their_req=%ld", ttid, vthr->rws.round, vthr->rws.req);
			// vthr->rws.flag = RWS_REQ_RECEIVED;
			rws->flag = RWS_STEALING;
			return RWS_STEAL_SENT;
		}
	}
	return RWS_STEAL_FAILED;
}

// // called by push_tasks
// static inline int check_req(){
// 	struct gomp_thread *thr = gomp_thread();
// 	rws_t *rws = &thr->rws;
// 	if(rws->round == WS_REQ2ROUND(rws->req)){
// 		// push tasks to thief
// 		rws->round++;
// 		return WS_REQ2TID(rws->req);
// 	}
// 	return -1;
// }

#endif // XTASK_RANDOM_WS

#ifdef XTASK_RANDOM_BWS
#include <stdio.h>
void ws_get_env_vars(){
	struct gomp_thread *thr = gomp_thread();
	
	char *env;
	env = getenv("N_VICTIMS");
	if(env == NULL){
		thr->nvictims = N_VICTIMS;
		xtask_debug(0, 0, "WARNING: N_VICTIMS is not set, using default value %d", N_VICTIMS);
	}else{
		thr->nvictims = atoi(env);
		// xtask_debug(0, 0, "N_VICTIMS=%d", thr->nvictims);
	}

	env = getenv("N_REQ_CHECKS");
	if(env == NULL){
		thr->nreq_checks = N_REQ_CHECKS;
		xtask_debug(0, 0, "WARNING: N_REQ_CHECKS is not set, using default value %d", N_REQ_CHECKS);
	}else{
		thr->nreq_checks = atoi(env);
		// xtask_debug(0, 0, "N_REQ_CHECKS=%d", thr->nreq_checks);
	}

	env = getenv("STEAL_DIVIDER");
	if(env == NULL){
		thr->steal_divider = STEAL_DIVIDER;
		xtask_debug(0, 0, "WARNING: STEAL_DIVIDER is not set, using default value %d", STEAL_DIVIDER);
	}else{
		thr->steal_divider = atoi(env);
		// xtask_debug(0, 0, "STEAL_DIVIDER=%d", thr->steal_divider);
	}

	env = getenv("MAX_WAIT_COUNTDOWN");
	if(env == NULL){
		thr->max_wait_countdown = MAX_WAIT_COUNTDOWN;
		xtask_debug(0, 0, "WARNING: MAX_WAIT_COUNTDOWN is not set, using default value %d", MAX_WAIT_COUNTDOWN);
	}else{
		thr->max_wait_countdown = atoi(env);
		// xtask_debug(0, 0, "MAX_WAIT_COUNTDOWN=%d", thr->max_wait_countdown);
	}

}

static inline void send_reqs(){
	struct gomp_thread *thr = gomp_thread();
	int nthreads = thr->ts.team->nthreads;
	int nvictims = thr->nvictims;
	// unsigned vtid, vqid;
	unsigned vtid;
	#ifdef XTASK_ENABLE_STATS
	unsigned long long stats_nreqs_sent = 0;
	unsigned long long stats_nreqs = (unsigned long long) nvictims;
	#endif // XTASK_ENABLE_STATS

	for(int i = 0; i < nvictims; i++){
		while((vtid = myrand() % nthreads)== thr->ts.team_id);
		struct gomp_thread *vthr = thr->thread_pool->threads[vtid];
		struct rbws *rbws = vthr->rbws;
		rbws->req_q[rbws->req_head] = WS_TID2REQ(thr->ts.team_id) | rbws->round;
		// FIXME: This can be overwritten, but we don't know how much yet
		// new request can overwrite the old one, we don't care as we want to address the newest possible
		rbws->req_head = (rbws->req_head + 1) & REQ_Q_MASK(vthr);

		#ifdef XTASK_ENABLE_STATS
		stats_nreqs_sent++;
		#endif // XTASK_ENABLE_STATS
		
	}
	#ifdef XTASK_ENABLE_STATS
	xstats_record(XSTATS_REQ_SENT, stats_nreqs, stats_nreqs_sent, 0);
	#endif
}

static inline void handle_reqs(unsigned long *last_req_q){
	struct gomp_thread *thr = gomp_thread();
	// unsigned tid = thr->ts.team_id;
	struct rbws *rbws = thr->rbws;
	// directly check the tail of the req_q
	// if not -1, meaning we are in the process of redirecting tasks
	if((rbws->redirect_tid == -1) && (rbws->req_q[rbws->req_tail] != 0)){
		// this is a valid req, we will handle it
		/**
		 * strategy:
		 * we will just send tasks to the thief without knowing if I am high load or not
		*/
		unsigned ttid = WS_REQ2TID(rbws->req_q[rbws->req_tail]);
		rbws->redirect_tid = ttid;
	}
}

#endif // XTASK_RANDOM_BWS

void
gomp_alloc_task_q(struct gomp_thread *thr){
	if (thr->num_queues == 0)
		thr->num_queues = gomp_num_task_queues;
	
	thr->last_q = 0;
	thr->last_q_accessed = 0;
	#ifdef XTASK_ENABLE_STATS
	xstats_init(); // our stats is only useful after we use the q
	#endif
	
	#ifdef XTASK_RANDOM_BWS
	thr->last_req_q_accessed = 0;
	if(thr->nvictims <= 0 ){
		thr->nvictims = N_VICTIMS;
		xtask_debug(0, 0, "WARNING: nvictims is not set, using default value %d", N_VICTIMS);
	}
	if(thr->nreq_checks <=0 ){
		thr->nreq_checks = N_REQ_CHECKS;
		xtask_debug(0, 0, "WARNING: nreq_checks is not set, using default value %d", N_REQ_CHECKS);
	}
	if(thr->steal_divider <= 0){
		thr->steal_divider = STEAL_DIVIDER;
		xtask_debug(0, 0, "WARNING: steal_divider is not set, using default value %d", STEAL_DIVIDER);
	}
	if(thr->max_wait_countdown <= 0){
		thr->max_wait_countdown = MAX_WAIT_COUNTDOWN;
		xtask_debug(0, 0, "WARNING: max_wait_countdown is not set, using default value %d", MAX_WAIT_COUNTDOWN);
	}
	#endif
	
	// thr->td_task_q = (struct gomp_taskq **)gomp_malloc(sizeof(struct gomp_taskq *) * thr->num_queues);
	thr->td_task_q = (struct gomp_taskq **)gomp_malloc(sizeof(struct gomp_taskq *) * (thr->num_queues)); // with xws
	for (int queue_id = 0; queue_id < thr->num_queues; queue_id++){
			thr->td_task_q[queue_id] = (struct gomp_taskq *)gomp_malloc(sizeof(struct gomp_taskq));
			thr->td_task_q[queue_id]->td_deque = (volatile struct gomp_task **)gomp_malloc(sizeof(struct gomp_task *) * INITIAL_TASK_DEQUE_SIZE);
			thr->td_deque_size = INITIAL_TASK_DEQUE_SIZE;

			thr->td_task_q[queue_id]->td_deque_head = 0;
			thr->td_task_q[queue_id]->td_deque_tail = 0;


			thr->td_task_q[queue_id]->nin = 0;
			thr->td_task_q[queue_id]->nout = 0;

			for(int i = 0; i < thr->td_deque_size; i++){
				thr->td_task_q[queue_id]->td_deque[i] = NULL;
			}
	}
	#ifdef XTASK_RANDOM_BWS
	thr->rbws = (struct rbws *)gomp_malloc(sizeof(struct rbws));
	thr->rbws->round = 1;
	thr->rbws->req_q_size = INITIAL_TASK_DEQUE_SIZE;
	thr->rbws->req_head = 0;
	thr->rbws->req_tail = 0;
	thr->rbws->nre = 0;
	thr->rbws->redirect_tid = -1;
	thr->rbws->nredirects = INITIAL_TASK_DEQUE_SIZE >> thr->steal_divider;
	thr->rbws->req_q = (uint64_t *)gomp_malloc(sizeof(uint64_t) * INITIAL_TASK_DEQUE_SIZE); // just use same size as the task deque
	// memset(thr->rbws->req_q, 0, sizeof(uint64_t) * INITIAL_TASK_DEQUE_SIZE);
	for(int i = 0; i < thr->rbws->req_q_size; i++){
		thr->rbws->req_q[i] = 0;
	}
	#endif
	return;
};



static int
gomp_push_task(struct gomp_task *task){
	struct gomp_thread *thr = gomp_thread();
	struct gomp_team *team = thr->ts.team;
	unsigned long gtid = (unsigned long)omp_get_thread_num();

	// Check if deque is full
	int num_tries = 0;
	unsigned long last_q = thr->last_q;
	struct gomp_thread *target_thr;

	#ifdef XTASK_RANDOM_WS
	rws_t *rws = &thr->rws;
	// check requests
	if(rws->round == WS_REQ2ROUND(rws->req)){
		int ttid = WS_REQ2TID(rws->req);
		int qid = ttid < gtid ?thr->num_queues + ttid - gtid : ttid - gtid;
		// xtask_debug(0, 0, "handle_req from T#%d, qid=%d, round=%ld, req=%ld, batchsize=%d, num_queue=%d", ttid, qid, rws->round, rws->req, rws->batch_size, thr->num_queues);
		last_q = qid;
		rws->round++;
	}
	#endif

	unsigned long target_tid; // starting target tid

	#ifdef XTASK_RANDOM_BWS
	struct rbws *rbws = thr->rbws;
	if(rbws->redirect_tid == -1){
	#endif
		xtask_debug(0, 0, "normal push");

		target_tid = gtid + last_q;
		target_tid = (target_tid > team->nthreads - 1) ? (target_tid - team->nthreads) : target_tid;
		
		// TODO: this is other ways to make sure it is serial
		if (team->nthreads <= 1)
			target_thr = thr;
		else
			target_thr = thr->thread_pool->threads[target_tid]; //ww: does this pointer the same across threads? - seems so

		while (target_thr->td_task_q[last_q]->td_deque[target_thr->td_task_q[last_q]->td_deque_head] != NULL){
			num_tries++;
			if (num_tries < 25)
				continue;
			return TASK_NOT_PUSHED;
		}

	#ifdef XTASK_RANDOM_BWS
	}else{
		// redirect tasks to the target_tid
		bool target_full = false;
		target_tid =(unsigned long) rbws->redirect_tid;
		last_q = target_tid < gtid ? gtid - target_tid : target_tid - gtid;
		xtask_debug(0, 0, "Handle steal from T#%d, thr->last_q=%ld, mygtid=%ld, lastq#%ld, ttid=%ld", rbws->redirect_tid, thr->last_q, gtid, last_q, target_tid);
		rbws->nre++;

		// TODO: this is other ways to make sure it is serial
		if (team->nthreads <= 1)
			target_thr = thr;
		else
			target_thr = thr->thread_pool->threads[target_tid]; //ww: does this pointer the same across threads? - seems so

		while (target_thr->td_task_q[last_q]->td_deque[target_thr->td_task_q[last_q]->td_deque_head] != NULL){
			num_tries++;
			if (num_tries < 25)
				continue;
			target_full = true;
			xtask_debug(0, 0, "target full, nre=%d, nredirects=%d, redirect_tid=%d, target_tid=%ld, target_qid=%ld", rbws->nre, rbws->nredirects, rbws->redirect_tid, target_tid, last_q);
			// xtask_debug(0, 0, "target full, nre=%d, nredirects=%d, redirect_tid=%d", rbws->nre, rbws->nredirects, rbws->redirect_tid);
		}

		if(rbws->nre > rbws->nredirects || target_full){
			rbws->nre = 0;
			rbws->redirect_tid = -1;
			rbws->req_q[rbws->req_tail] = 0; // req is handled
			rbws->req_tail = (rbws->req_tail + 1) & REQ_Q_MASK(thr);

			// now everything is set to default mode, try again.
			return gomp_push_task(task);
		}

	}
	#endif

	struct gomp_taskq *task_q = target_thr->td_task_q[last_q];
	task_q->nin++; // Will it be reordered by compilers or CPU?. it doesn't matter
	task_q->td_deque[task_q->td_deque_head] = task;
	task_q->td_deque_head = (task_q->td_deque_head + 1) & TASK_DEQUE_MASK(thr);
	target_thr->last_q_accessed = last_q;

	#ifdef XTASK_RANDOM_BWS
	// only change the last_q when it is not in the process of redirecting tasks
	if(rbws->redirect_tid == -1){
	#endif

		if (thr->num_queues > 1){
			last_q++;
			if (last_q < thr->num_queues)
				thr->last_q = last_q;
			else
				thr->last_q = 0;
		}

	#ifdef XTASK_RANDOM_BWS
	}
	#endif

	return TASK_SUCCESSFULLY_PUSHED;
};

static gomp_task_t* 
gomp_remove_my_task(){

	gomp_task_t *task;
	struct gomp_thread *thr = gomp_thread();

	if (thr->td_task_q[0]->td_deque[thr->td_task_q[0]->td_deque_tail] == NULL)
		return NULL;
	task = (gomp_task_t *) thr->td_task_q[0]->td_deque[thr->td_task_q[0]->td_deque_tail];
	thr->td_task_q[0]->td_deque[thr->td_task_q[0]->td_deque_tail] = NULL;
	thr->td_task_q[0]->td_deque_tail = (thr->td_task_q[0]->td_deque_tail + 1) & TASK_DEQUE_MASK(thr);
	thr->td_task_q[0]->nout++;
	return task;
};

static gomp_task_t*
gomp_remove_aux_task(unsigned long *last_qid){
	
	gomp_task_t *task;
	struct gomp_thread *thr = gomp_thread();

	task = NULL;
	struct gomp_taskq *task_q= NULL;
	if(thr->last_q_accessed > 0){
		task_q = thr->td_task_q[thr->last_q_accessed];
		if (task_q->td_deque[task_q->td_deque_tail] != NULL){
			task = (gomp_task_t *) task_q->td_deque[task_q->td_deque_tail];
			task_q->td_deque[task_q->td_deque_tail] = NULL;
			task_q->td_deque_tail = (task_q->td_deque_tail + 1) & TASK_DEQUE_MASK(thr);
			*last_qid = thr->last_q_accessed;
		}
			
	}
	if(task == NULL){
		for(unsigned long queue_id = *last_qid; queue_id > 0; queue_id --){
			task_q = thr->td_task_q[queue_id];
			if (task_q->td_deque[task_q->td_deque_tail] != NULL){
				task = (gomp_task_t *) task_q->td_deque[task_q->td_deque_tail];
				task_q->td_deque[task_q->td_deque_tail] = NULL;
				task_q->td_deque_tail = (task_q->td_deque_tail + 1) & TASK_DEQUE_MASK(thr);
				*last_qid = queue_id;
				break;
			}
		}
	}

	if(task == NULL){
		for(unsigned long queue_id = thr->num_queues - 1; queue_id > *last_qid; queue_id --){
			task_q = thr->td_task_q[queue_id];
			if (task_q->td_deque[task_q->td_deque_tail] != NULL){
				task = (gomp_task_t *) task_q->td_deque[task_q->td_deque_tail];
				task_q->td_deque[task_q->td_deque_tail] = NULL;
				task_q->td_deque_tail = (task_q->td_deque_tail + 1) & TASK_DEQUE_MASK(thr);
				*last_qid = (queue_id);
				break;
			}
		}
	}

	if(__builtin_expect(task != NULL,1))
		task_q->nout++;
	
	return task;
};


/** author: ww
 * xflag_init has to be called after thread_dock, before the threads running actual tasks,
 * so the threads in thread_pool are in place.
 * and this should be called only once when thread starts, and for master thread after thread is created
 * FIXME: When entering the 2nd parallel region and so on, the xflag_init hasn't been called anywhere,
 * I would assume that xflag_init is called only once by each thread.
*/
void xflag_init(struct gomp_thread *thr){
	struct xflag *flag = &thr->xflag;
	flag->thr = thr;
	int tid = thr->ts.team_id;
	flag->state = XFLAG_STATE_RUNNING;
	flag->gathered = 0;
	flag->on_release = false;
	flag->parent = tid == 0 ? NULL : &thr->thread_pool->threads[(tid-1)/2]->xflag;
	flag->nchild = 0;
	for(int i = 0; i < XFLAG_TREE_DEGREE; i++){
		int child_tid = tid * 2 + i + 1;
		if(child_tid < thr->ts.team->nthreads){
			flag->child[i] = &thr->thread_pool->threads[child_tid]->xflag;		
			flag->nchild++;
		}else{
			flag->child[i] = NULL;
		}
		GOMP_ATOMIC_ST_REL(&flag->child_done[i], 0);
	}

	// xtask_debug(0, 0, "xflag_init, tid=%d, gathered=%d, nchild=%d", tid, flag->gathered, flag->nchild);
	flag->cidx = !(tid & 1);
}
/** author: ww
 * xflag_reinit is called when the thread is leaving the current barrier.
 * We pass gomp's barrier state so to align the state of the flag with the barrier's generation.
 * TODO: This is seems to be optimizable, instead of we store the state, we can just use the bar's state.
 * I think it is safer to reset the parents' child_done to 0, so that this var to other thread is ready-only
 * This can reduce complexity of thinking IMO.
 * 
*/

void xflag_reinit(struct gomp_thread *thr, gomp_barrier_state_t bs){
	// xtask_debug(0, 1, "xflag_reinit, tid=%d, bs=%d", thr->ts.team_id, bs);
	struct xflag *flag = &thr->xflag;
	flag->state = XFLAG_STATE_RUNNING;
	flag->gathered = bs;
	flag->on_release = false;
	if(flag->parent && flag->thr->ts.team_id != 0){
		GOMP_ATOMIC_ST_REL(&flag->parent->child_done[flag->cidx], 0);
	}
	// for(int i = 0; i < flag->nchild; i++){
	// 	GOMP_ATOMIC_ST_REL(&flag->child_done[i], 0);
	// }
}

/** author: ww
 * xflag_release can only be called by the root thread (thr->ts.team_id == 0).
 * We are modifying other threads' data structure, so we have to be careful.
 * We can modify other threads because this is only called once by the root, and after the threads are gathered.
 * We call xflag_release when the following conditions are met.
 * 1. xflag_gathered has been called once by the root <- this is to make sure all threads enters the current barrier
 * 2. xflag_done has been called by the root, and it finds that all the children are done.
 * 3. call xflag_release by the root.
*/
static void xflag_release(struct xflag *flag){
	flag->on_release = true;
	for(int i = 0; i < flag->nchild; i++){
		xflag_release(flag->child[i]);
	}
}

/** author: ww
 * xflag_gathered ensures that xflag_done can pass the first condition when the last barrier is reached,
 * this is to prevent xflag_done is called before some thread don't reach the barrier, and it is possible
 * that xflag_init hasn't been called by that thread, so we prevent thread from even accessing the data structure
 * of different thread.
 * FIXME: idk if this is really necessary, but I think it is better to be safe than sorry.
 * The last thread entering the bar do a broadcast to all the other threads, setting the flag gathered to true
 * the val root has to be carefully set by thread, in case it is the root, it has to be true
*/
static void xflag_gathered(struct xflag *flag, bool root, gomp_barrier_state_t bs){
	bs = bs & ~BAR_WAS_LAST;
	// xtask_debug(0, 0, "bs=%d, gathered=%d", bs, flag->gathered);
	if(root){
		// xtask_debug(0, 0, "tid: %d, nchild=%d, flag=%p, parent=(%d)%p, gathered=%d, on_release=%d", 
		// flag->thr->ts.team_id, 
		// flag->nchild,
		// flag,
		// flag->parent ? flag->parent->thr->ts.team_id : 0,
		// flag->parent,
		// flag->gathered,
		// flag->on_release
		// );
		flag->gathered = bs + BAR_INCR;
		for(int i = 0; i < flag->nchild; i++){
			xflag_gathered(flag->child[i], true, bs);
		}
	}else{
		xflag_gathered(flag->parent, flag->parent->thr->ts.team_id == 0, bs);
	}
}

/** author: ww
 * we need to first remove BAR_WAS_LAST since not sure how it can be used
 * TODO: so the barrier and flag don't handle cancel, future work is to handle cancel
 * gathered flag is only set when xflag_gathered is called,
 * and xflag_gathered is called only when the last thread enters the barrier with current bar's state
 * before gathered, the gathered should be state
 * after gathered, the gathered should be state + BAR_INCR, so we should wait for current generation
*/
static void xflag_done(struct xflag * flag, gomp_barrier_state_t bs){
	bs = bs & ~BAR_WAS_LAST;

	if(flag->gathered!= bs + BAR_INCR)
		return;
	if(flag->state == XFLAG_STATE_DONE)
		return;
	bool done = true;
	for(int i = 0; i < flag->nchild; i++)
		done &= GOMP_ATOMIC_LD_ACQ(&flag->child_done[i]);
	done &= !GOMP_ATOMIC_LD_ACQ(&flag->thr->task->td_incomplete_child_tasks);

	if(done){
		// this can only set once
		flag->state = XFLAG_STATE_DONE;
		// if we are the root, and all the children are done, release the children
		if(flag->thr->ts.team_id == 0){
			xflag_release(flag);
			return;
		}
		// if not, we set our bit at parent to 1 (done)
		GOMP_ATOMIC_ST_REL(&flag->parent->child_done[flag->cidx], 1);
		// xtask_debug(0, 0, "done, nchild=%d, child=%ld, %ld, cidx=%d, parent's child = %ld, %ld, parentflag=%p",
		// flag->nchild,
		// flag->child_done[0], flag->child_done[1], flag->cidx
		// , flag->parent->child_done[0], flag->parent->child_done[1],
		// flag->parent);
	}
}

/**
 * XPerflog - record timestamps and corresponding hfref number
*/
#ifdef GOMP_USE_XPERFLOG
#include <stdio.h>
#define XPERFLOG_PATH "/stor/auxiliary/wwang/xperlog/tmp/xperflog_%d_%d.csv"

static inline unsigned long long xperflog_get_new_fref(xperf_type_t event){
	struct gomp_thread *thr = gomp_thread();
	struct xperflog *perflog = &thr->xperflog;
	return ++perflog->frefc[event];
}

static inline unsigned long long xperflog_get_fref(xperf_type_t event){
	struct gomp_thread *thr = gomp_thread();
	struct xperflog *perflog = &thr->xperflog;
	return perflog->frefc[event];
}


void xperflog_init(){
	struct gomp_thread *thr = gomp_thread();
	struct xperflog *perflog = &thr->xperflog;
	perflog->xperflog_path = getenv("XPERFLOG_PATH");
	
	// team thread's init may be called multiple times, prevent this by checking generation
	if(perflog->generation){
		return;
	}
	perflog->fp = NULL;
	// append tid to the filename
	snprintf(perflog->filename, 64, "%s/xperflog_%d_%d.csv", perflog->xperflog_path, thr->ts.team_id, perflog->generation);
	perflog->log = (xperflog_cell_t *)gomp_malloc(sizeof(xperflog_cell_t) * XPERFLOG_MAX_EVENTS);
	perflog->eidx = 0;
	perflog->last_q = 0;
	perflog->is_stalling = false;

	
	// init frame reference counters array
	for(int i = 0; i < XPERF_N_EVENTS; i++)
		perflog->frefc[i] = 0;
	
	unsigned long long tref = xperflog_get_new_fref(XPERF_THREAD);
	xperflog_record(XPERF_THREAD, tref, tref);
	perflog->generation++;
}

/** 
 * xperflog_record - record timestamps and corresponding hfref(frame reference) number
 * @param event: xperf_type_t, the event type, can be combination of flags, e.g.
 * XPERF_TASK_END | XPERF_TASKWAIT means task end and then taskwait starts
 * @param hfref: unsigned long long, the frame reference number of event with higher bits as the event type
 * e.g. here hfref is XPERF_TASK_END's frame reference number
 * @param lfref: unsigned long long, the frame reference number of event with lower bits as the event type
*/
void xperflog_record(xperf_type_t event, unsigned long long hfref, unsigned long long lfref){
	struct gomp_thread *thr = gomp_thread();
	struct xperflog *perflog = &thr->xperflog;
	if(perflog->eidx >= XPERFLOG_MAX_EVENTS){
		xtask_debug(0, 0, "xperf - record: idx exceeds max events.");
		return;
	}

	if(LEVENT(event) == XPERF_STALL && perflog->is_stalling)
		return;

	if(HEVENT(event) == XPERF_STALL_END && !perflog->is_stalling)
		return;

	if(LEVENT(event) == XPERF_STALL)
		perflog->is_stalling = true;
	else
		perflog->is_stalling = false;


	// perflog->ts[perflog->eidx] = __rdtsc();
	unsigned int aux;
	perflog->log[perflog->eidx].ts = __rdtscp(&aux);	
	perflog->log[perflog->eidx].event = event;
	perflog->log[perflog->eidx].hfref = hfref;
	perflog->log[perflog->eidx].lfref = lfref;
	// record sample task count
	if(event > XPERF_THREAD){
		// for(int i = 0; i < 4; i++){
			// perflog->last_q = perflog->last_q + i < thr->num_queues ? perflog->last_q + i : (perflog->last_q + i) % thr->num_queues;
			// xtask_debug(0, 0, "xperf - record: last_q=%d, num_queues=%d", perflog->last_q, thr->num_queues);
			perflog->ssum +=(long long)(thr->td_task_q[perflog->last_q]->nin - thr->td_task_q[perflog->last_q]->nout);
			perflog->last_q = perflog->last_q + 1 >= thr->num_queues ? 0 : perflog->last_q + 1;
		// }
		perflog->log[perflog->eidx].sample = perflog->ssum;
	}

	perflog->eidx++;
}



void xperflog_wait(){
	struct gomp_thread *thr = gomp_thread();
	struct gomp_team *team = thr->ts.team;
	GOMP_ATOMIC_DEC(&team->xperflog_awaited);
	int zero = 0;
	int nthreads = team->nthreads;
	while(GOMP_ATOMIC_LD_ACQ(&team->xperflog_awaited)!=nthreads && GOMP_ATOMIC_CMPXCHG(&team->xperflog_awaited, &zero, nthreads) != 1){
		zero = 0;
	}
}
/**
 * Now this should be called by the users with the wrapper
 * And it has to be called under the omp parallel region
*/

void xperflog_dump(struct gomp_thread *thr){
	// xtask_debug(0, 0, "dumping");
	// the following is a bit anti-pattern.
	xperflog_record(XPERF_THREAD_END | XPERF_DUMP, xperflog_get_fref(XPERF_THREAD), xperflog_get_fref(XPERF_DUMP)); // record this in href

 	struct xperflog *perflog = &thr->xperflog;
	perflog->fp = (void *)fopen(perflog->filename, "w");
	if(perflog->fp == NULL){
		xtask_debug(0, 0, "xperf - dump: file pointer is null.");
		return;
	}

	// lets first do this using fprintf to output as csv file
	fprintf(perflog->fp, "timestamp,event,hfref,lfref,sample\n");
	for(unsigned long long i = 0; i < perflog->eidx; i++){
		fprintf(perflog->fp, "%llu,%d,%llu,%llu,%lld\n", 
		perflog->log[i].ts, 
		perflog->log[i].event, 
		perflog->log[i].hfref, 
		perflog->log[i].lfref,  
		perflog->log[i].sample);
	}

	fclose(perflog->fp);
	perflog->fp = NULL;

	// put a bar here
	// xperflog_wait();
}

/**
 * This can only be called after init, and the generation is gaurantted to be > 0
*/
void xperflog_reset(struct gomp_thread *thr){
	struct xperflog *perflog = &thr->xperflog;
	if(perflog->fp){
		fclose(perflog->fp);
		perflog->fp = NULL;
	}
	perflog->eidx = 0;
	perflog->last_q = 0;
	// init frefc
	for(int i = 0; i < XPERF_N_EVENTS; i++)
		perflog->frefc[i] = 0;
	
	// assign new filename
	snprintf(perflog->filename, 64, "%s/xperflog_%d_%d.csv", perflog->xperflog_path, thr->ts.team_id, perflog->generation);
	unsigned long long tref = xperflog_get_new_fref(XPERF_THREAD);
	xperflog_record(XPERF_THREAD, tref, tref); // record this in lfref
	perflog->generation++;
}

static inline void xperflog_done(struct gomp_thread *thr){
	if(thr->xperflog.fp){
		fclose(thr->xperflog.fp);
	}
}

void xperflog_dump_reset(){
	struct gomp_thread *thr = gomp_thread();
	xperflog_dump(thr);
	xperflog_reset(thr);
}
#endif // GOMP_USE_XPERFLOG

/**
 * Interfaces that can be called by the user
 * xomp_perflog_dump: dump the perflog to the file
 * xomp_perflog_reset: reset the perflog
*/

void xomp_perflog_dump(void){
	struct gomp_thread *thr = gomp_thread();
	#ifdef GOMP_USE_XPERFLOG
	xperflog_dump(thr);
	#endif
	// show some of the stats
	#ifdef XTASK_ENABLE_STATS
	xstats_dump(thr);
	#endif
	#ifndef GOMP_USE_XPERFLOG
	if(thr->ts.team_id == 0)
		xtask_debug(0, 0, "xperflog - dump: perflog is not enabled.");
	#endif // GOMP_USE_XPERFLOG

}


void xomp_perflog_info(void){
	#ifdef GOMP_USE_XPERFLOG
	struct gomp_thread *thr = gomp_thread();
	struct xperflog *perflog = &thr->xperflog;
	xtask_debug(0, 0, 
		"xperflog - info: tid=%d, eidx=%llu, last_q=%d, generation=%d, xperflog_path=%s, filename=%s",
		thr->ts.team_id, perflog->eidx, perflog->last_q, perflog->generation, perflog->xperflog_path, perflog->filename
	);
	#endif // GOMP_USE_XPERFLOG
}

#endif // GOMP_USE_XQUEUE

/* Create a new task data structure.  */

void
gomp_init_task (struct gomp_task *task, struct gomp_task *parent_task,
		struct gomp_task_icv *prev_icv)
{
  /* It would seem that using memset here would be a win, but it turns
     out that partially filling gomp_task allows us to keep the
     overhead of task creation low.  In the nqueens-1.c test, for a
     sufficiently large N, we drop the overhead from 5-6% to 1%.

     Note, the nqueens-1.c test in serial mode is a good test to
     benchmark the overhead of creating tasks as there are millions of
     tiny tasks created that all run undeferred.  */
  task->parent = parent_task;
  priority_queue_init (&task->children_queue);
  task->taskgroup = NULL;
  task->dependers = NULL;
  task->depend_hash = NULL;
  task->taskwait = NULL;
  task->depend_count = 0;
  task->completion_sem = NULL;
  task->deferred_p = false;
  task->icv = *prev_icv;
  task->kind = GOMP_TASK_IMPLICIT;
  task->in_tied_task = false;
  task->final_task = false;
  task->copy_ctors_done = false;
  task->parent_depends_on = false;
#ifdef GOMP_USE_XQUEUE
  GOMP_ATOMIC_ST_RLX(&task->td_incomplete_child_tasks, 0);
#endif
}

/* Clean up a task, after completing it.  */

void
gomp_end_task (void)
{
  struct gomp_thread *thr = gomp_thread ();
  struct gomp_task *task = thr->task;

  gomp_finish_task (task);
  thr->task = task->parent;
}

/* Clear the parent field of every task in LIST.  */

static inline void
gomp_clear_parent_in_list (struct priority_list *list)
{
  struct priority_node *p = list->tasks;
  if (p)
    do
      {
	priority_node_to_task (PQ_CHILDREN, p)->parent = NULL;
	p = p->next;
      }
    while (p != list->tasks);
}

/* Splay tree version of gomp_clear_parent_in_list.

   Clear the parent field of every task in NODE within SP, and free
   the node when done.  */

static void
gomp_clear_parent_in_tree (prio_splay_tree sp, prio_splay_tree_node node)
{
  if (!node)
    return;
  prio_splay_tree_node left = node->left, right = node->right;
  gomp_clear_parent_in_list (&node->key.l);
#if _LIBGOMP_CHECKING_
  memset (node, 0xaf, sizeof (*node));
#endif
  /* No need to remove the node from the tree.  We're nuking
     everything, so just free the nodes and our caller can clear the
     entire splay tree.  */
  free (node);
  gomp_clear_parent_in_tree (sp, left);
  gomp_clear_parent_in_tree (sp, right);
}

/* Clear the parent field of every task in Q and remove every task
   from Q.  */

static inline void
gomp_clear_parent (struct priority_queue *q)
{
  if (priority_queue_multi_p (q))
    {
      gomp_clear_parent_in_tree (&q->t, q->t.root);
      /* All the nodes have been cleared in gomp_clear_parent_in_tree.
	 No need to remove anything.  We can just nuke everything.  */
      q->t.root = NULL;
    }
  else
    gomp_clear_parent_in_list (&q->l);
}

/* Helper function for GOMP_task and gomp_create_target_task.

   For a TASK with in/out dependencies, fill in the various dependency
   queues.  PARENT is the parent of said task.  DEPEND is as in
   GOMP_task.  */

static void
gomp_task_handle_depend (struct gomp_task *task, struct gomp_task *parent,
			 void **depend)
{
  size_t ndepend = (uintptr_t) depend[0];
  size_t i;
  hash_entry_type ent;

  if (ndepend)
    {
      /* depend[0] is total # */
      size_t nout = (uintptr_t) depend[1]; /* # of out: and inout: */
      /* ndepend - nout is # of in: */
      for (i = 0; i < ndepend; i++)
	{
	  task->depend[i].addr = depend[2 + i];
	  task->depend[i].is_in = i >= nout;
	}
    }
  else
    {
      ndepend = (uintptr_t) depend[1]; /* total # */
      size_t nout = (uintptr_t) depend[2]; /* # of out: and inout: */
      size_t nmutexinoutset = (uintptr_t) depend[3]; /* # of mutexinoutset: */
      /* For now we treat mutexinoutset like out, which is compliant, but
	 inefficient.  */
      size_t nin = (uintptr_t) depend[4]; /* # of in: */
      /* ndepend - nout - nmutexinoutset - nin is # of depobjs */
      size_t normal = nout + nmutexinoutset + nin;
      size_t n = 0;
      for (i = normal; i < ndepend; i++)
	{
	  void **d = (void **) (uintptr_t) depend[5 + i];
	  switch ((uintptr_t) d[1])
	    {
	    case GOMP_DEPEND_OUT:
	    case GOMP_DEPEND_INOUT:
	    case GOMP_DEPEND_MUTEXINOUTSET:
	      break;
	    case GOMP_DEPEND_IN:
	      continue;
	    default:
	      gomp_fatal ("unknown omp_depend_t dependence type %d",
			  (int) (uintptr_t) d[1]);
	    }
	  task->depend[n].addr = d[0];
	  task->depend[n++].is_in = 0;
	}
      for (i = 0; i < normal; i++)
	{
	  task->depend[n].addr = depend[5 + i];
	  task->depend[n++].is_in = i >= nout + nmutexinoutset;
	}
      for (i = normal; i < ndepend; i++)
	{
	  void **d = (void **) (uintptr_t) depend[5 + i];
	  if ((uintptr_t) d[1] != GOMP_DEPEND_IN)
	    continue;
	  task->depend[n].addr = d[0];
	  task->depend[n++].is_in = 1;
	}
    }
  task->depend_count = ndepend;
  task->num_dependees = 0;
  if (parent->depend_hash == NULL)
    parent->depend_hash = htab_create (2 * ndepend > 12 ? 2 * ndepend : 12);
  for (i = 0; i < ndepend; i++)
    {
      task->depend[i].next = NULL;
      task->depend[i].prev = NULL;
      task->depend[i].task = task;
      task->depend[i].redundant = false;
      task->depend[i].redundant_out = false;

      hash_entry_type *slot = htab_find_slot (&parent->depend_hash,
					      &task->depend[i], INSERT);
      hash_entry_type out = NULL, last = NULL;
      if (*slot)
	{
	  /* If multiple depends on the same task are the same, all but the
	     first one are redundant.  As inout/out come first, if any of them
	     is inout/out, it will win, which is the right semantics.  */
	  if ((*slot)->task == task)
	    {
	      task->depend[i].redundant = true;
	      continue;
	    }
	  for (ent = *slot; ent; ent = ent->next)
	    {
	      if (ent->redundant_out)
		break;

	      last = ent;

	      /* depend(in:...) doesn't depend on earlier depend(in:...).  */
	      if (task->depend[i].is_in && ent->is_in)
		continue;

	      if (!ent->is_in)
		out = ent;

	      struct gomp_task *tsk = ent->task;
	      if (tsk->dependers == NULL)
		{
		  tsk->dependers
		    = gomp_malloc (sizeof (struct gomp_dependers_vec)
				   + 6 * sizeof (struct gomp_task *));
		  tsk->dependers->n_elem = 1;
		  tsk->dependers->allocated = 6;
		  tsk->dependers->elem[0] = task;
		  task->num_dependees++;
		  continue;
		}
	      /* We already have some other dependency on tsk from earlier
		 depend clause.  */
	      else if (tsk->dependers->n_elem
		       && (tsk->dependers->elem[tsk->dependers->n_elem - 1]
			   == task))
		continue;
	      else if (tsk->dependers->n_elem == tsk->dependers->allocated)
		{
		  tsk->dependers->allocated
		    = tsk->dependers->allocated * 2 + 2;
		  tsk->dependers
		    = gomp_realloc (tsk->dependers,
				    sizeof (struct gomp_dependers_vec)
				    + (tsk->dependers->allocated
				       * sizeof (struct gomp_task *)));
		}
	      tsk->dependers->elem[tsk->dependers->n_elem++] = task;
	      task->num_dependees++;
	    }
	  task->depend[i].next = *slot;
	  (*slot)->prev = &task->depend[i];
	}
      *slot = &task->depend[i];

      /* There is no need to store more than one depend({,in}out:) task per
	 address in the hash table chain for the purpose of creation of
	 deferred tasks, because each out depends on all earlier outs, thus it
	 is enough to record just the last depend({,in}out:).  For depend(in:),
	 we need to keep all of the previous ones not terminated yet, because
	 a later depend({,in}out:) might need to depend on all of them.  So, if
	 the new task's clause is depend({,in}out:), we know there is at most
	 one other depend({,in}out:) clause in the list (out).  For
	 non-deferred tasks we want to see all outs, so they are moved to the
	 end of the chain, after first redundant_out entry all following
	 entries should be redundant_out.  */
      if (!task->depend[i].is_in && out)
	{
	  if (out != last)
	    {
	      out->next->prev = out->prev;
	      out->prev->next = out->next;
	      out->next = last->next;
	      out->prev = last;
	      last->next = out;
	      if (out->next)
		out->next->prev = out;
	    }
	  out->redundant_out = true;
	}
    }
}

/* Called when encountering an explicit task directive.  If IF_CLAUSE is
   false, then we must not delay in executing the task.  If UNTIED is true,
   then the task may be executed by any member of the team.

   DEPEND is an array containing:
     if depend[0] is non-zero, then:
	depend[0]: number of depend elements.
	depend[1]: number of depend elements of type "out/inout".
	depend[2..N+1]: address of [1..N]th depend element.
     otherwise, when depend[0] is zero, then:
	depend[1]: number of depend elements.
	depend[2]: number of depend elements of type "out/inout".
	depend[3]: number of depend elements of type "mutexinoutset".
	depend[4]: number of depend elements of type "in".
	depend[5..4+depend[2]+depend[3]+depend[4]]: address of depend elements
	depend[5+depend[2]+depend[3]+depend[4]..4+depend[1]]: address of
		   omp_depend_t objects.  */

void
GOMP_task (void (*fn) (void *), void *data, void (*cpyfn) (void *, void *),
	   long arg_size, long arg_align, bool if_clause, unsigned flags,
	   void **depend, int priority_arg, void *detach)
{
#if defined(GOMP_USE_XPERFLOG) && defined(GOMP_USE_XQUEUE)
	unsigned long long gomp_task_fref = xperflog_get_new_fref(XPERF_THREAD);
	xperflog_record(XPERF_GOMP_TASK, gomp_task_fref, gomp_task_fref);
#endif // GOMP_USE_XPERFLOG
  struct gomp_thread *thr = gomp_thread ();
  struct gomp_team *team = thr->ts.team;
  int priority = 0;

	
	
#ifdef HAVE_BROKEN_POSIX_SEMAPHORES
  /* If pthread_mutex_* is used for omp_*lock*, then each task must be
     tied to one thread all the time.  This means UNTIED tasks must be
     tied and if CPYFN is non-NULL IF(0) must be forced, as CPYFN
     might be running on different thread than FN.  */
  if (cpyfn)
    if_clause = false;
  flags &= ~GOMP_TASK_FLAG_UNTIED;
#endif
  
#ifdef GOMP_USE_XQUEUE
	/*
	We are not going to use xq if there are incompat task clauses.
	FIXME: However, this is only a temporary solution and is not theoretically correct. I could break when user mix the usage
	of task clauses, when xtask can be enabled for some constructs while not for others - because race condition could occur.
	TODO: We need information regarding whether users use other clauses before runtime starts.
	flags & GOMP_TASK_FLAG_UNTIED: 1 - untied
	!(flags & ~GOMP_TASK_FLAG_UNTIED): 1 - only untied
	UPDATE: Now this is fixed by letting compiler pass flags to teams, if flags & 32, then we will use xq.
	thr->xq has been set when threads are started by the gomp_team_start
	*/
	// thr->use_xq = (flags & GOMP_TASK_FLAG_UNTIED) && !(flags & ~GOMP_TASK_FLAG_UNTIED) && (thr->use_xq);
	// // thr->use_xq = false;
	// // xtask_debug(0,0, "flags=%d", flags);
	bool use_xq = thr->use_xq;
	if(thr->td_task_q == NULL)
		gomp_alloc_task_q(thr);
#endif


  /* If parallel or taskgroup has been cancelled, don't start new tasks.  */
  if (__builtin_expect (gomp_cancel_var, 0) && team)
    {
      if (gomp_team_barrier_cancelled (&team->barrier))
	return;
      if (thr->task->taskgroup)
	{
	  if (thr->task->taskgroup->cancelled)
	    return;
	  if (thr->task->taskgroup->workshare
	      && thr->task->taskgroup->prev
	      && thr->task->taskgroup->prev->cancelled)
	    return;
	}
    }

  if (__builtin_expect ((flags & GOMP_TASK_FLAG_PRIORITY) != 0, 0))
    {
      priority = priority_arg;
      if (priority > gomp_max_task_priority_var)
	priority = gomp_max_task_priority_var;
    }

  if (!if_clause || team == NULL
      || (thr->task && thr->task->final_task)
      || team->task_count > 64 * team->nthreads)
    {
      struct gomp_task task;
      gomp_sem_t completion_sem;

      /* If there are depend clauses and earlier deferred sibling tasks
	 with depend clauses, check if there isn't a dependency.  If there
	 is, we need to wait for them.  There is no need to handle
	 depend clauses for non-deferred tasks other than this, because
	 the parent task is suspended until the child task finishes and thus
	 it can't start further child tasks.  */
      if ((flags & GOMP_TASK_FLAG_DEPEND)
	  && thr->task && thr->task->depend_hash)
	gomp_task_maybe_wait_for_dependencies (depend);

      gomp_init_task (&task, thr->task, gomp_icv (false));
      task.kind = GOMP_TASK_UNDEFERRED;
      task.final_task = (thr->task && thr->task->final_task)
			|| (flags & GOMP_TASK_FLAG_FINAL);
      task.priority = priority;

      if ((flags & GOMP_TASK_FLAG_DETACH) != 0)
	{
	  gomp_sem_init (&completion_sem, 0);
	  task.completion_sem = &completion_sem;
	  *(void **) detach = &task;
	  if (data)
	    *(void **) data = &task;

	  gomp_debug (0, "Thread %d: new event: %p\n",
		      thr->ts.team_id, &task);
	}

      if (thr->task)
	{
	  task.in_tied_task = thr->task->in_tied_task;
	  task.taskgroup = thr->task->taskgroup;
	}
      thr->task = &task;
      if (__builtin_expect (cpyfn != NULL, 0))
	{
	  char buf[arg_size + arg_align - 1];
	  char *arg = (char *) (((uintptr_t) buf + arg_align - 1)
				& ~(uintptr_t) (arg_align - 1));
	  cpyfn (arg, data);
	  fn (arg);
	}
      else
	fn (data);

      if ((flags & GOMP_TASK_FLAG_DETACH) != 0)
	{
	  gomp_sem_wait (&completion_sem);
	  gomp_sem_destroy (&completion_sem);
	}

      /* Access to "children" is normally done inside a task_lock
	 mutex region, but the only way this particular task.children
	 can be set is if this thread's task work function (fn)
	 creates children.  So since the setter is *this* thread, we
	 need no barriers here when testing for non-NULL.  We can have
	 task.children set by the current thread then changed by a
	 child thread, but seeing a stale non-NULL value is not a
	 problem.  Once past the task_lock acquisition, this thread
	 will see the real value of task.children.  */
	 #ifdef GOMP_USE_XQUEUE
	if(__builtin_expect(!use_xq, 0)){ // if not use xq - begin
	 #endif
      if (!priority_queue_empty_p (&task.children_queue, MEMMODEL_RELAXED))
	{
	  gomp_mutex_lock (&team->task_lock);
	  gomp_clear_parent (&task.children_queue);
	  gomp_mutex_unlock (&team->task_lock);
	}
	gomp_end_task ();
	#ifdef GOMP_USE_XQUEUE
	} // if not use xq - end
	#endif	
}
  else
    {
      struct gomp_task *task;
      struct gomp_task *parent = thr->task;
      struct gomp_taskgroup *taskgroup = parent->taskgroup;
      char *arg;
      bool do_wake;
      size_t depend_size = 0;

      if (flags & GOMP_TASK_FLAG_DEPEND)
	depend_size = ((uintptr_t) (depend[0] ? depend[0] : depend[1])
		       * sizeof (struct gomp_task_depend_entry));
      task = gomp_malloc (sizeof (*task) + depend_size
			  + arg_size + arg_align - 1);
      arg = (char *) (((uintptr_t) (task + 1) + depend_size + arg_align - 1)
		      & ~(uintptr_t) (arg_align - 1));
      gomp_init_task (task, parent, gomp_icv (false));
      task->priority = priority;
      task->kind = GOMP_TASK_UNDEFERRED;
      task->in_tied_task = parent->in_tied_task;
      task->taskgroup = taskgroup;
      task->deferred_p = true;
      if ((flags & GOMP_TASK_FLAG_DETACH) != 0)
	{
	  task->detach_team = team;

	  *(void **) detach = task;
	  if (data)
	    *(void **) data = task;

	  gomp_debug (0, "Thread %d: new event: %p\n", thr->ts.team_id, task);
	}
      thr->task = task;
      if (cpyfn)
	{
	  cpyfn (arg, data);
	  task->copy_ctors_done = true;
	}
      else
	memcpy (arg, data, arg_size);
      thr->task = parent;
      task->kind = GOMP_TASK_WAITING;
      task->fn = fn;
      task->fn_data = arg;
      task->final_task = (flags & GOMP_TASK_FLAG_FINAL) >> 1;
	  #ifdef GOMP_USE_XQUEUE
	  if(__builtin_expect(!use_xq, 0))
	  #endif
      gomp_mutex_lock (&team->task_lock);
      /* If parallel or taskgroup has been cancelled, don't start new
	 tasks.  */
      if (__builtin_expect (gomp_cancel_var, 0)
	  && !task->copy_ctors_done)
	{
	  if (gomp_team_barrier_cancelled (&team->barrier))
	    {
	    do_cancel:
		#ifdef GOMP_USE_XQUEUE
		if(__builtin_expect(!use_xq, 0))
		#endif
	      gomp_mutex_unlock (&team->task_lock);
	      gomp_finish_task (task);
	      free (task);
	      return;
	    }
	  if (taskgroup)
	    {
	      if (taskgroup->cancelled)
		goto do_cancel;
	      if (taskgroup->workshare
		  && taskgroup->prev
		  && taskgroup->prev->cancelled)
		goto do_cancel;
	    }
	}
      if (taskgroup)
	taskgroup->num_children++;
      if (depend_size)
	{
	  gomp_task_handle_depend (task, parent, depend);
	  if (task->num_dependees)
	    {
	      /* Tasks that depend on other tasks are not put into the
		 various waiting queues, so we are done for now.  Said
		 tasks are instead put into the queues via
		 gomp_task_run_post_handle_dependers() after their
		 dependencies have been satisfied.  After which, they
		 can be picked up by the various scheduling
		 points.  */
		 #ifdef GOMP_USE_XQUEUE
		if(__builtin_expect(!use_xq, 0))
		#endif
	      gomp_mutex_unlock (&team->task_lock);
	      return;
	    }
	}
#ifdef GOMP_USE_XQUEUE
	if(use_xq){
		
		GOMP_ATOMIC_INC(&task->parent->td_incomplete_child_tasks);

		if(gomp_push_task (task) == TASK_NOT_PUSHED){
		
			// execute it right away
			task->kind = GOMP_TASK_TIED;
			thr->task = task;
			#ifdef GOMP_USE_XPERFLOG
			unsigned long long task_fref = xperflog_get_new_fref(XPERF_TASK);
			xperflog_record(XPERF_GOMP_TASK_END | XPERF_TASK, gomp_task_fref,  task_fref);
			#endif // GOMP_USE_XPERFLOG
			task->fn(task->fn_data);
			#ifdef GOMP_USE_XPERFLOG
			xperflog_record(XPERF_TASK_END | XPERF_GOMP_TASK, task_fref, gomp_task_fref);
			#endif // GOMP_USE_XPERFLOG
			thr->task = parent;
			GOMP_ATOMIC_DEC(&task->parent->td_incomplete_child_tasks);
			gomp_finish_task(task);
			free(task);
			#ifdef GOMP_USE_XPERFLOG
			xperflog_record(XPERF_GOMP_TASK_END, gomp_task_fref, gomp_task_fref);
			#endif // GOMP_USE_XPERFLOG
			return;
		}
		#ifdef GOMP_USE_XPERFLOG
		xperflog_record(XPERF_GOMP_TASK_END, gomp_task_fref, gomp_task_fref);
		#endif // GOMP_USE_XPERFLOG
		return;
	}else{ //!xq - begin
#endif
      priority_queue_insert (PQ_CHILDREN, &parent->children_queue,
			     task, priority,
			     PRIORITY_INSERT_BEGIN,
			     /*adjust_parent_depends_on=*/false,
			     task->parent_depends_on);
      if (taskgroup)
	priority_queue_insert (PQ_TASKGROUP, &taskgroup->taskgroup_queue,
			       task, priority,
			       PRIORITY_INSERT_BEGIN,
			       /*adjust_parent_depends_on=*/false,
			       task->parent_depends_on);

      priority_queue_insert (PQ_TEAM, &team->task_queue,
			     task, priority,
			     PRIORITY_INSERT_END,
			     /*adjust_parent_depends_on=*/false,
			     task->parent_depends_on);

      ++team->task_count;
      ++team->task_queued_count;
      gomp_team_barrier_set_task_pending (&team->barrier);
      do_wake = team->task_running_count + !parent->in_tied_task
		< team->nthreads;
      gomp_mutex_unlock (&team->task_lock);
      if (do_wake)
	gomp_team_barrier_wake (&team->barrier, 1);
#ifdef GOMP_USE_XQUEUE
	} //!xq - end
#endif
    }
}

ialias (GOMP_taskgroup_start)
ialias (GOMP_taskgroup_end)
ialias (GOMP_taskgroup_reduction_register)

#define TYPE long
#define UTYPE unsigned long
#define TYPE_is_long 1
#include "taskloop.c"
#undef TYPE
#undef UTYPE
#undef TYPE_is_long

#define TYPE unsigned long long
#define UTYPE TYPE
#define GOMP_taskloop GOMP_taskloop_ull
#include "taskloop.c"
#undef TYPE
#undef UTYPE
#undef GOMP_taskloop

static void inline
priority_queue_move_task_first (enum priority_queue_type type,
				struct priority_queue *head,
				struct gomp_task *task)
{
#if _LIBGOMP_CHECKING_
  if (!priority_queue_task_in_queue_p (type, head, task))
    gomp_fatal ("Attempt to move first missing task %p", task);
#endif
  struct priority_list *list;
  if (priority_queue_multi_p (head))
    {
      list = priority_queue_lookup_priority (head, task->priority);
#if _LIBGOMP_CHECKING_
      if (!list)
	gomp_fatal ("Unable to find priority %d", task->priority);
#endif
    }
  else
    list = &head->l;
  priority_list_remove (list, task_to_priority_node (type, task), 0);
  priority_list_insert (type, list, task, task->priority,
			PRIORITY_INSERT_BEGIN, type == PQ_CHILDREN,
			task->parent_depends_on);
}

/* Actual body of GOMP_PLUGIN_target_task_completion that is executed
   with team->task_lock held, or is executed in the thread that called
   gomp_target_task_fn if GOMP_PLUGIN_target_task_completion has been
   run before it acquires team->task_lock.  */

static void
gomp_target_task_completion (struct gomp_team *team, struct gomp_task *task)
{
  struct gomp_task *parent = task->parent;
  if (parent)
    priority_queue_move_task_first (PQ_CHILDREN, &parent->children_queue,
				    task);

  struct gomp_taskgroup *taskgroup = task->taskgroup;
  if (taskgroup)
    priority_queue_move_task_first (PQ_TASKGROUP, &taskgroup->taskgroup_queue,
				    task);

  priority_queue_insert (PQ_TEAM, &team->task_queue, task, task->priority,
			 PRIORITY_INSERT_BEGIN, false,
			 task->parent_depends_on);
  task->kind = GOMP_TASK_WAITING;
  if (parent && parent->taskwait)
    {
      if (parent->taskwait->in_taskwait)
	{
	  /* One more task has had its dependencies met.
	     Inform any waiters.  */
	  parent->taskwait->in_taskwait = false;
	  gomp_sem_post (&parent->taskwait->taskwait_sem);
	}
      else if (parent->taskwait->in_depend_wait)
	{
	  /* One more task has had its dependencies met.
	     Inform any waiters.  */
	  parent->taskwait->in_depend_wait = false;
	  gomp_sem_post (&parent->taskwait->taskwait_sem);
	}
    }
  if (taskgroup && taskgroup->in_taskgroup_wait)
    {
      /* One more task has had its dependencies met.
	 Inform any waiters.  */
      taskgroup->in_taskgroup_wait = false;
      gomp_sem_post (&taskgroup->taskgroup_sem);
    }

  ++team->task_queued_count;
  gomp_team_barrier_set_task_pending (&team->barrier);
  /* I'm afraid this can't be done after releasing team->task_lock,
     as gomp_target_task_completion is run from unrelated thread and
     therefore in between gomp_mutex_unlock and gomp_team_barrier_wake
     the team could be gone already.  */
  if (team->nthreads > team->task_running_count)
    gomp_team_barrier_wake (&team->barrier, 1);
}

/* Signal that a target task TTASK has completed the asynchronously
   running phase and should be requeued as a task to handle the
   variable unmapping.  */

void
GOMP_PLUGIN_target_task_completion (void *data)
{
  struct gomp_target_task *ttask = (struct gomp_target_task *) data;
  struct gomp_task *task = ttask->task;
  struct gomp_team *team = ttask->team;

  gomp_mutex_lock (&team->task_lock);
  if (ttask->state == GOMP_TARGET_TASK_READY_TO_RUN)
    {
      ttask->state = GOMP_TARGET_TASK_FINISHED;
      gomp_mutex_unlock (&team->task_lock);
      return;
    }
  ttask->state = GOMP_TARGET_TASK_FINISHED;
  gomp_target_task_completion (team, task);
  gomp_mutex_unlock (&team->task_lock);
}

static void gomp_task_run_post_handle_depend_hash (struct gomp_task *);

/* Called for nowait target tasks.  */

bool
gomp_create_target_task (struct gomp_device_descr *devicep,
			 void (*fn) (void *), size_t mapnum, void **hostaddrs,
			 size_t *sizes, unsigned short *kinds,
			 unsigned int flags, void **depend, void **args,
			 enum gomp_target_task_state state)
{
  struct gomp_thread *thr = gomp_thread ();
  struct gomp_team *team = thr->ts.team;

  /* If parallel or taskgroup has been cancelled, don't start new tasks.  */
  if (__builtin_expect (gomp_cancel_var, 0) && team)
    {
      if (gomp_team_barrier_cancelled (&team->barrier))
	return true;
      if (thr->task->taskgroup)
	{
	  if (thr->task->taskgroup->cancelled)
	    return true;
	  if (thr->task->taskgroup->workshare
	      && thr->task->taskgroup->prev
	      && thr->task->taskgroup->prev->cancelled)
	    return true;
	}
    }

  struct gomp_target_task *ttask;
  struct gomp_task *task;
  struct gomp_task *parent = thr->task;
  struct gomp_taskgroup *taskgroup = parent->taskgroup;
  bool do_wake;
  size_t depend_size = 0;
  uintptr_t depend_cnt = 0;
  size_t tgt_align = 0, tgt_size = 0;
  uintptr_t args_cnt = 0;

  if (depend != NULL)
    {
      depend_cnt = (uintptr_t) (depend[0] ? depend[0] : depend[1]);
      depend_size = depend_cnt * sizeof (struct gomp_task_depend_entry);
    }
  if (fn)
    {
      /* GOMP_MAP_FIRSTPRIVATE need to be copied first, as they are
	 firstprivate on the target task.  */
      size_t i;
      for (i = 0; i < mapnum; i++)
	if ((kinds[i] & 0xff) == GOMP_MAP_FIRSTPRIVATE)
	  {
	    size_t align = (size_t) 1 << (kinds[i] >> 8);
	    if (tgt_align < align)
	      tgt_align = align;
	    tgt_size = (tgt_size + align - 1) & ~(align - 1);
	    tgt_size += sizes[i];
	  }
      if (tgt_align)
	tgt_size += tgt_align - 1;
      else
	tgt_size = 0;
      if (args)
	{
	  void **cargs = args;
	  while (*cargs)
	    {
	      intptr_t id = (intptr_t) *cargs++;
	      if (id & GOMP_TARGET_ARG_SUBSEQUENT_PARAM)
		cargs++;
	    }
	  args_cnt = cargs + 1 - args;
	}
    }

  task = gomp_malloc (sizeof (*task) + depend_size
		      + sizeof (*ttask)
		      + args_cnt * sizeof (void *)
		      + mapnum * (sizeof (void *) + sizeof (size_t)
				  + sizeof (unsigned short))
		      + tgt_size);
  gomp_init_task (task, parent, gomp_icv (false));
  task->priority = 0;
  task->kind = GOMP_TASK_WAITING;
  task->in_tied_task = parent->in_tied_task;
  task->taskgroup = taskgroup;
  ttask = (struct gomp_target_task *) &task->depend[depend_cnt];
  ttask->devicep = devicep;
  ttask->fn = fn;
  ttask->mapnum = mapnum;
  memcpy (ttask->hostaddrs, hostaddrs, mapnum * sizeof (void *));
  if (args_cnt)
    {
      ttask->args = (void **) &ttask->hostaddrs[mapnum];
      memcpy (ttask->args, args, args_cnt * sizeof (void *));
      ttask->sizes = (size_t *) &ttask->args[args_cnt];
    }
  else
    {
      ttask->args = args;
      ttask->sizes = (size_t *) &ttask->hostaddrs[mapnum];
    }
  memcpy (ttask->sizes, sizes, mapnum * sizeof (size_t));
  ttask->kinds = (unsigned short *) &ttask->sizes[mapnum];
  memcpy (ttask->kinds, kinds, mapnum * sizeof (unsigned short));
  if (tgt_align)
    {
      char *tgt = (char *) &ttask->kinds[mapnum];
      size_t i;
      uintptr_t al = (uintptr_t) tgt & (tgt_align - 1);
      if (al)
	tgt += tgt_align - al;
      tgt_size = 0;
      for (i = 0; i < mapnum; i++)
	if ((kinds[i] & 0xff) == GOMP_MAP_FIRSTPRIVATE)
	  {
	    size_t align = (size_t) 1 << (kinds[i] >> 8);
	    tgt_size = (tgt_size + align - 1) & ~(align - 1);
	    memcpy (tgt + tgt_size, hostaddrs[i], sizes[i]);
	    ttask->hostaddrs[i] = tgt + tgt_size;
	    tgt_size = tgt_size + sizes[i];
	  }
    }
  ttask->flags = flags;
  ttask->state = state;
  ttask->task = task;
  ttask->team = team;
  task->fn = NULL;
  task->fn_data = ttask;
  task->final_task = 0;
  gomp_mutex_lock (&team->task_lock);
  /* If parallel or taskgroup has been cancelled, don't start new tasks.  */
  if (__builtin_expect (gomp_cancel_var, 0))
    {
      if (gomp_team_barrier_cancelled (&team->barrier))
	{
	do_cancel:
	  gomp_mutex_unlock (&team->task_lock);
	  gomp_finish_task (task);
	  free (task);
	  return true;
	}
      if (taskgroup)
	{
	  if (taskgroup->cancelled)
	    goto do_cancel;
	  if (taskgroup->workshare
	      && taskgroup->prev
	      && taskgroup->prev->cancelled)
	    goto do_cancel;
	}
    }
  if (depend_size)
    {
      gomp_task_handle_depend (task, parent, depend);
      if (task->num_dependees)
	{
	  if (taskgroup)
	    taskgroup->num_children++;
	  gomp_mutex_unlock (&team->task_lock);
	  return true;
	}
    }
  if (state == GOMP_TARGET_TASK_DATA)
    {
      gomp_task_run_post_handle_depend_hash (task);
      gomp_mutex_unlock (&team->task_lock);
      gomp_finish_task (task);
      free (task);
      return false;
    }
  if (taskgroup)
    taskgroup->num_children++;
  /* For async offloading, if we don't need to wait for dependencies,
     run the gomp_target_task_fn right away, essentially schedule the
     mapping part of the task in the current thread.  */
  if (devicep != NULL
      && (devicep->capabilities & GOMP_OFFLOAD_CAP_OPENMP_400))
    {
      priority_queue_insert (PQ_CHILDREN, &parent->children_queue, task, 0,
			     PRIORITY_INSERT_END,
			     /*adjust_parent_depends_on=*/false,
			     task->parent_depends_on);
      if (taskgroup)
	priority_queue_insert (PQ_TASKGROUP, &taskgroup->taskgroup_queue,
			       task, 0, PRIORITY_INSERT_END,
			       /*adjust_parent_depends_on=*/false,
			       task->parent_depends_on);
      task->pnode[PQ_TEAM].next = NULL;
      task->pnode[PQ_TEAM].prev = NULL;
      task->kind = GOMP_TASK_TIED;
      ++team->task_count;
      gomp_mutex_unlock (&team->task_lock);

      thr->task = task;
      gomp_target_task_fn (task->fn_data);
      thr->task = parent;

      gomp_mutex_lock (&team->task_lock);
      task->kind = GOMP_TASK_ASYNC_RUNNING;
      /* If GOMP_PLUGIN_target_task_completion has run already
	 in between gomp_target_task_fn and the mutex lock,
	 perform the requeuing here.  */
      if (ttask->state == GOMP_TARGET_TASK_FINISHED)
	gomp_target_task_completion (team, task);
      else
	ttask->state = GOMP_TARGET_TASK_RUNNING;
      gomp_mutex_unlock (&team->task_lock);
      return true;
    }
  priority_queue_insert (PQ_CHILDREN, &parent->children_queue, task, 0,
			 PRIORITY_INSERT_BEGIN,
			 /*adjust_parent_depends_on=*/false,
			 task->parent_depends_on);
  if (taskgroup)
    priority_queue_insert (PQ_TASKGROUP, &taskgroup->taskgroup_queue, task, 0,
			   PRIORITY_INSERT_BEGIN,
			   /*adjust_parent_depends_on=*/false,
			   task->parent_depends_on);
  priority_queue_insert (PQ_TEAM, &team->task_queue, task, 0,
			 PRIORITY_INSERT_END,
			 /*adjust_parent_depends_on=*/false,
			 task->parent_depends_on);
  ++team->task_count;
  ++team->task_queued_count;
  gomp_team_barrier_set_task_pending (&team->barrier);
  do_wake = team->task_running_count + !parent->in_tied_task
	    < team->nthreads;
  gomp_mutex_unlock (&team->task_lock);
  if (do_wake)
    gomp_team_barrier_wake (&team->barrier, 1);
  return true;
}

/* Given a parent_depends_on task in LIST, move it to the front of its
   priority so it is run as soon as possible.

   Care is taken to update the list's LAST_PARENT_DEPENDS_ON field.

   We rearrange the queue such that all parent_depends_on tasks are
   first, and last_parent_depends_on points to the last such task we
   rearranged.  For example, given the following tasks in a queue
   where PD[123] are the parent_depends_on tasks:

	task->children
	|
	V
	C1 -> C2 -> C3 -> PD1 -> PD2 -> PD3 -> C4

	We rearrange such that:

	task->children
	|	       +--- last_parent_depends_on
	|	       |
	V	       V
	PD1 -> PD2 -> PD3 -> C1 -> C2 -> C3 -> C4.  */

static void inline
priority_list_upgrade_task (struct priority_list *list,
			    struct priority_node *node)
{
  struct priority_node *last_parent_depends_on
    = list->last_parent_depends_on;
  if (last_parent_depends_on)
    {
      node->prev->next = node->next;
      node->next->prev = node->prev;
      node->prev = last_parent_depends_on;
      node->next = last_parent_depends_on->next;
      node->prev->next = node;
      node->next->prev = node;
    }
  else if (node != list->tasks)
    {
      node->prev->next = node->next;
      node->next->prev = node->prev;
      node->prev = list->tasks->prev;
      node->next = list->tasks;
      list->tasks = node;
      node->prev->next = node;
      node->next->prev = node;
    }
  list->last_parent_depends_on = node;
}

/* Given a parent_depends_on TASK in its parent's children_queue, move
   it to the front of its priority so it is run as soon as possible.

   PARENT is passed as an optimization.

   (This function could be defined in priority_queue.c, but we want it
   inlined, and putting it in priority_queue.h is not an option, given
   that gomp_task has not been properly defined at that point).  */

static void inline
priority_queue_upgrade_task (struct gomp_task *task,
			     struct gomp_task *parent)
{
  struct priority_queue *head = &parent->children_queue;
  struct priority_node *node = &task->pnode[PQ_CHILDREN];
#if _LIBGOMP_CHECKING_
  if (!task->parent_depends_on)
    gomp_fatal ("priority_queue_upgrade_task: task must be a "
		"parent_depends_on task");
  if (!priority_queue_task_in_queue_p (PQ_CHILDREN, head, task))
    gomp_fatal ("priority_queue_upgrade_task: cannot find task=%p", task);
#endif
  if (priority_queue_multi_p (head))
    {
      struct priority_list *list
	= priority_queue_lookup_priority (head, task->priority);
      priority_list_upgrade_task (list, node);
    }
  else
    priority_list_upgrade_task (&head->l, node);
}

/* Given a CHILD_TASK in LIST that is about to be executed, move it out of
   the way in LIST so that other tasks can be considered for
   execution.  LIST contains tasks of type TYPE.

   Care is taken to update the queue's LAST_PARENT_DEPENDS_ON field
   if applicable.  */

static void inline
priority_list_downgrade_task (enum priority_queue_type type,
			      struct priority_list *list,
			      struct gomp_task *child_task)
{
  struct priority_node *node = task_to_priority_node (type, child_task);
  if (list->tasks == node)
    list->tasks = node->next;
  else if (node->next != list->tasks)
    {
      /* The task in NODE is about to become TIED and TIED tasks
	 cannot come before WAITING tasks.  If we're about to
	 leave the queue in such an indeterminate state, rewire
	 things appropriately.  However, a TIED task at the end is
	 perfectly fine.  */
      struct gomp_task *next_task = priority_node_to_task (type, node->next);
      if (next_task->kind == GOMP_TASK_WAITING)
	{
	  /* Remove from list.  */
	  node->prev->next = node->next;
	  node->next->prev = node->prev;
	  /* Rewire at the end.  */
	  node->next = list->tasks;
	  node->prev = list->tasks->prev;
	  list->tasks->prev->next = node;
	  list->tasks->prev = node;
	}
    }

  /* If the current task is the last_parent_depends_on for its
     priority, adjust last_parent_depends_on appropriately.  */
  if (__builtin_expect (child_task->parent_depends_on, 0)
      && list->last_parent_depends_on == node)
    {
      struct gomp_task *prev_child = priority_node_to_task (type, node->prev);
      if (node->prev != node
	  && prev_child->kind == GOMP_TASK_WAITING
	  && prev_child->parent_depends_on)
	list->last_parent_depends_on = node->prev;
      else
	{
	  /* There are no more parent_depends_on entries waiting
	     to run, clear the list.  */
	  list->last_parent_depends_on = NULL;
	}
    }
}

/* Given a TASK in HEAD that is about to be executed, move it out of
   the way so that other tasks can be considered for execution.  HEAD
   contains tasks of type TYPE.

   Care is taken to update the queue's LAST_PARENT_DEPENDS_ON field
   if applicable.

   (This function could be defined in priority_queue.c, but we want it
   inlined, and putting it in priority_queue.h is not an option, given
   that gomp_task has not been properly defined at that point).  */

static void inline
priority_queue_downgrade_task (enum priority_queue_type type,
			       struct priority_queue *head,
			       struct gomp_task *task)
{
#if _LIBGOMP_CHECKING_
  if (!priority_queue_task_in_queue_p (type, head, task))
    gomp_fatal ("Attempt to downgrade missing task %p", task);
#endif
  if (priority_queue_multi_p (head))
    {
      struct priority_list *list
	= priority_queue_lookup_priority (head, task->priority);
      priority_list_downgrade_task (type, list, task);
    }
  else
    priority_list_downgrade_task (type, &head->l, task);
}

/* Setup CHILD_TASK to execute.  This is done by setting the task to
   TIED, and updating all relevant queues so that CHILD_TASK is no
   longer chosen for scheduling.  Also, remove CHILD_TASK from the
   overall team task queue entirely.

   Return TRUE if task or its containing taskgroup has been
   cancelled.  */

static inline bool
gomp_task_run_pre (struct gomp_task *child_task, struct gomp_task *parent,
		   struct gomp_team *team)
{
#if _LIBGOMP_CHECKING_
  if (child_task->parent)
    priority_queue_verify (PQ_CHILDREN,
			   &child_task->parent->children_queue, true);
  if (child_task->taskgroup)
    priority_queue_verify (PQ_TASKGROUP,
			   &child_task->taskgroup->taskgroup_queue, false);
  priority_queue_verify (PQ_TEAM, &team->task_queue, false);
#endif

  /* Task is about to go tied, move it out of the way.  */
  if (parent)
    priority_queue_downgrade_task (PQ_CHILDREN, &parent->children_queue,
				   child_task);

  /* Task is about to go tied, move it out of the way.  */
  struct gomp_taskgroup *taskgroup = child_task->taskgroup;
  if (taskgroup)
    priority_queue_downgrade_task (PQ_TASKGROUP, &taskgroup->taskgroup_queue,
				   child_task);

  priority_queue_remove (PQ_TEAM, &team->task_queue, child_task,
			 MEMMODEL_RELAXED);
  child_task->pnode[PQ_TEAM].next = NULL;
  child_task->pnode[PQ_TEAM].prev = NULL;
  child_task->kind = GOMP_TASK_TIED;
  	#ifdef GOMP_USE_XQUEUE
	// wenyi: if no task in the queue, let the barrier know with clear_task_pending
	// this is protected by the team task lock
	// or we can use atomic operation to do similar thing but not using their barrier
	// the following call must be protected by the team task lock
	// gomp_team_barrier_clear_task_pending (&team->barrier);
	// gomp_team_barrier_cancelled (&team->barrier);
	#endif

  if (--team->task_queued_count == 0)
    gomp_team_barrier_clear_task_pending (&team->barrier);
  if (__builtin_expect (gomp_cancel_var, 0)
      && !child_task->copy_ctors_done)
    {
      if (gomp_team_barrier_cancelled (&team->barrier))
	return true;
      if (taskgroup)
	{
	  if (taskgroup->cancelled)
	    return true;
	  if (taskgroup->workshare
	      && taskgroup->prev
	      && taskgroup->prev->cancelled)
	    return true;
	}
    }
  return false;
}

static void
gomp_task_run_post_handle_depend_hash (struct gomp_task *child_task)
{
  struct gomp_task *parent = child_task->parent;
  size_t i;

  for (i = 0; i < child_task->depend_count; i++)
    if (!child_task->depend[i].redundant)
      {
	if (child_task->depend[i].next)
	  child_task->depend[i].next->prev = child_task->depend[i].prev;
	if (child_task->depend[i].prev)
	  child_task->depend[i].prev->next = child_task->depend[i].next;
	else
	  {
	    hash_entry_type *slot
	      = htab_find_slot (&parent->depend_hash, &child_task->depend[i],
				NO_INSERT);
	    if (*slot != &child_task->depend[i])
	      abort ();
	    if (child_task->depend[i].next)
	      *slot = child_task->depend[i].next;
	    else
	      htab_clear_slot (parent->depend_hash, slot);
	  }
      }
}

/* After a CHILD_TASK has been run, adjust the dependency queue for
   each task that depends on CHILD_TASK, to record the fact that there
   is one less dependency to worry about.  If a task that depended on
   CHILD_TASK now has no dependencies, place it in the various queues
   so it gets scheduled to run.

   TEAM is the team to which CHILD_TASK belongs to.  */

static size_t
gomp_task_run_post_handle_dependers (struct gomp_task *child_task,
				     struct gomp_team *team)
{
  struct gomp_task *parent = child_task->parent;
  size_t i, count = child_task->dependers->n_elem, ret = 0;
  for (i = 0; i < count; i++)
    {
      struct gomp_task *task = child_task->dependers->elem[i];

      /* CHILD_TASK satisfies a dependency for TASK.  Keep track of
	 TASK's remaining dependencies.  Once TASK has no other
	 dependencies, put it into the various queues so it will get
	 scheduled for execution.  */
      if (--task->num_dependees != 0)
	continue;

      struct gomp_taskgroup *taskgroup = task->taskgroup;
      if (parent)
	{
	  priority_queue_insert (PQ_CHILDREN, &parent->children_queue,
				 task, task->priority,
				 PRIORITY_INSERT_BEGIN,
				 /*adjust_parent_depends_on=*/true,
				 task->parent_depends_on);
	  if (parent->taskwait)
	    {
	      if (parent->taskwait->in_taskwait)
		{
		  /* One more task has had its dependencies met.
		     Inform any waiters.  */
		  parent->taskwait->in_taskwait = false;
		  gomp_sem_post (&parent->taskwait->taskwait_sem);
		}
	      else if (parent->taskwait->in_depend_wait)
		{
		  /* One more task has had its dependencies met.
		     Inform any waiters.  */
		  parent->taskwait->in_depend_wait = false;
		  gomp_sem_post (&parent->taskwait->taskwait_sem);
		}
	    }
	}
      else
	task->parent = NULL;
      if (taskgroup)
	{
	  priority_queue_insert (PQ_TASKGROUP, &taskgroup->taskgroup_queue,
				 task, task->priority,
				 PRIORITY_INSERT_BEGIN,
				 /*adjust_parent_depends_on=*/false,
				 task->parent_depends_on);
	  if (taskgroup->in_taskgroup_wait)
	    {
	      /* One more task has had its dependencies met.
		 Inform any waiters.  */
	      taskgroup->in_taskgroup_wait = false;
	      gomp_sem_post (&taskgroup->taskgroup_sem);
	    }
	}
      priority_queue_insert (PQ_TEAM, &team->task_queue,
			     task, task->priority,
			     PRIORITY_INSERT_END,
			     /*adjust_parent_depends_on=*/false,
			     task->parent_depends_on);
      ++team->task_count;
      ++team->task_queued_count;
      ++ret;
    }
  free (child_task->dependers);
  child_task->dependers = NULL;
  if (ret > 1)
    gomp_team_barrier_set_task_pending (&team->barrier);
  return ret;
}

static inline size_t
gomp_task_run_post_handle_depend (struct gomp_task *child_task,
				  struct gomp_team *team)
{
  if (child_task->depend_count == 0)
    return 0;

  /* If parent is gone already, the hash table is freed and nothing
     will use the hash table anymore, no need to remove anything from it.  */
  if (child_task->parent != NULL)
    gomp_task_run_post_handle_depend_hash (child_task);

  if (child_task->dependers == NULL)
    return 0;

  return gomp_task_run_post_handle_dependers (child_task, team);
}

/* Remove CHILD_TASK from its parent.  */

static inline void
gomp_task_run_post_remove_parent (struct gomp_task *child_task)
{
  struct gomp_task *parent = child_task->parent;
  if (parent == NULL)
    return;

  /* If this was the last task the parent was depending on,
     synchronize with gomp_task_maybe_wait_for_dependencies so it can
     clean up and return.  */
  if (__builtin_expect (child_task->parent_depends_on, 0)
      && --parent->taskwait->n_depend == 0
      && parent->taskwait->in_depend_wait)
    {
      parent->taskwait->in_depend_wait = false;
      gomp_sem_post (&parent->taskwait->taskwait_sem);
    }

  if (priority_queue_remove (PQ_CHILDREN, &parent->children_queue,
			     child_task, MEMMODEL_RELEASE)
      && parent->taskwait && parent->taskwait->in_taskwait)
    {
      parent->taskwait->in_taskwait = false;
      gomp_sem_post (&parent->taskwait->taskwait_sem);
    }
  child_task->pnode[PQ_CHILDREN].next = NULL;
  child_task->pnode[PQ_CHILDREN].prev = NULL;
}

/* Remove CHILD_TASK from its taskgroup.  */

static inline void
gomp_task_run_post_remove_taskgroup (struct gomp_task *child_task)
{
  struct gomp_taskgroup *taskgroup = child_task->taskgroup;
  if (taskgroup == NULL)
    return;
  bool empty = priority_queue_remove (PQ_TASKGROUP,
				      &taskgroup->taskgroup_queue,
				      child_task, MEMMODEL_RELAXED);
  child_task->pnode[PQ_TASKGROUP].next = NULL;
  child_task->pnode[PQ_TASKGROUP].prev = NULL;
  if (taskgroup->num_children > 1)
    --taskgroup->num_children;
  else
    {
      /* We access taskgroup->num_children in GOMP_taskgroup_end
	 outside of the task lock mutex region, so
	 need a release barrier here to ensure memory
	 written by child_task->fn above is flushed
	 before the NULL is written.  */
      __atomic_store_n (&taskgroup->num_children, 0, MEMMODEL_RELEASE);
    }
  if (empty && taskgroup->in_taskgroup_wait)
    {
      taskgroup->in_taskgroup_wait = false;
      gomp_sem_post (&taskgroup->taskgroup_sem);
    }
}


#ifdef GOMP_USE_XQUEUE
void xtask_barrier_handle_tasks(gomp_barrier_state_t state){
	#if defined(GOMP_USE_XQUEUE) && defined(GOMP_USE_XPERFLOG)
	unsigned long long bar_fref = xperflog_get_new_fref(XPERF_BAR);
	xperflog_record(XPERF_BAR, bar_fref, bar_fref);
	#endif // GOMP_USE_XQUEUE && GOMP_USE_XPERFLOG
	struct gomp_thread *thr = gomp_thread ();
	// struct gomp_team *team = thr->ts.team;
	struct gomp_task *task = thr->task;
	struct gomp_task *child_task = NULL;
	struct gomp_task *to_free = NULL;

	unsigned long gtid = (unsigned long)omp_get_thread_num();
	unsigned int use_own_tasks = 1, new_victim = 0;
	unsigned long last_qid = (thr->num_queues <= gtid) ? 1 : gtid;
	#ifdef XTASK_RANDOM_BWS
	unsigned long last_req_qid = last_qid;
	int wait_countdown = 0;
	int max_wait_countdown = thr->max_wait_countdown;
	#endif
	if(gomp_barrier_last_thread(state)){
			xflag_gathered(&thr->xflag, thr->ts.team_id == 0, state);
		}

bool cancelled = false;
while(1){
	while(1){
		cancelled = false;
		if(use_own_tasks){
			child_task = gomp_remove_my_task();
		}

		if((child_task == NULL) && (thr->num_queues > 1)){
			use_own_tasks = 0;
			child_task = gomp_remove_aux_task(&last_qid);
		}
		//bogus for victim
		if(new_victim)
			new_victim = new_victim;

		// Barrier Handle tasks
		if(child_task){
			#ifdef XTASK_LLWS
			xws_handle_reqs(&last_qid);
			#endif // XTASK_LLWS


			#ifdef XTASK_SWS
			xtask_handle_req(&last_qid);
			#endif // XTASK_SWS

			#ifdef XTASK_RANDOM_BWS
			handle_reqs(&last_req_qid);
			#endif // XTASK_RANDOM_BWS

			#ifdef GOMP_USE_XPERFLOG
			xperflog_record(XPERF_STALL_END | XPERF_BAR, bar_fref, bar_fref);
			#endif // GOMP_USE_XPERFLOG
			
		} else{
			#ifdef XTASK_LLWS
			xws_send_reqs();
			#endif

			#ifdef XTASK_RANDOM_WS
			steal_req();
			#endif

			#ifdef XTASK_SWS
			xtask_steal_req();
			#endif

			#ifdef XTASK_RANDOM_BWS
			if(wait_countdown > 0){
				wait_countdown--;
			}else{
				wait_countdown = max_wait_countdown;
				send_reqs();
			}
			#endif


			#ifdef GOMP_USE_XPERFLOG
			xperflog_record(XPERF_BAR_END| XPERF_STALL, bar_fref, bar_fref);
			#endif // GOMP_USE_XPERFLOG

		

			break;
		}

		if(child_task){
			//TODO: handle cancel
			child_task->kind = GOMP_TASK_TIED;
			child_task->in_tied_task = true;

			
			
			if (__builtin_expect (cancelled, 0)){
				if (to_free){
					gomp_finish_task (to_free);
					free (to_free);
					to_free = NULL;
				}
				goto finish_cancelled;
			}
			task = thr->task;
			thr->task = child_task;
			if (__builtin_expect (child_task->fn == NULL, 0)){
				if (gomp_target_task_fn (child_task->fn_data)){
				gomp_debug(0, "[tid=%d] wenyi(gomp_barrier_handle_tasks): unexpected.\n", omp_get_thread_num());
				break;
				}
				}
			else{
				#ifdef GOMP_USE_XPERFLOG
				unsigned long long task_fref = xperflog_get_new_fref(XPERF_TASK);
				xperflog_record(XPERF_BAR_END | XPERF_TASK, bar_fref, task_fref);
				#endif // GOMP_USE_XPERFLOG
				child_task->fn (child_task->fn_data);
				#ifdef GOMP_USE_XPERFLOG
				xperflog_record(XPERF_TASK_END | XPERF_BAR, task_fref, bar_fref); // task end can be used to encapsulate the task
				#endif // GOMP_USE_XPERFLOG
			}
				
			thr->task = task;
		}else
			break;
		
			
			

		if(!use_own_tasks && thr->td_task_q[0]->td_deque[thr->td_task_q[0]->td_deque_head] != NULL){
			use_own_tasks = 1;
			new_victim = 0;	
		}

		if (child_task)
		{
			GOMP_ATOMIC_DEC(&child_task->parent->td_incomplete_child_tasks);

			finish_cancelled:;
			to_free = child_task;
			child_task = NULL;	
		}

		if (to_free){
			gomp_finish_task (to_free);
			free (to_free);
			to_free = NULL;
		}
	}
	
	// task source has been depeleted, set this thread's done, let the parent thread know
	xflag_done(&thr->xflag, state);
	if(thr->xflag.on_release){
		#ifdef GOMP_USE_XPERFLOG
		xperflog_record(XPERF_BAR_END, bar_fref, bar_fref);
		#endif // GOMP_USE_XPERFLOG
		return;
	}
		
}
 // bar exits here
}
#endif

void
gomp_barrier_handle_tasks (gomp_barrier_state_t state)
{
  struct gomp_thread *thr = gomp_thread ();
  struct gomp_team *team = thr->ts.team;
  struct gomp_task *task = thr->task;
  struct gomp_task *child_task = NULL;
  struct gomp_task *to_free = NULL;
  int do_wake = 0;

  gomp_mutex_lock (&team->task_lock);
  if (gomp_barrier_last_thread (state))
    {
      if (team->task_count == 0)
	{
	  gomp_team_barrier_done (&team->barrier, state);
	  gomp_mutex_unlock (&team->task_lock);
	  gomp_team_barrier_wake (&team->barrier, 0);
	  return;
	}
      gomp_team_barrier_set_waiting_for_tasks (&team->barrier);
    }

  while (1)
    {
      bool cancelled = false;

      if (!priority_queue_empty_p (&team->task_queue, MEMMODEL_RELAXED))
	{
	  bool ignored;
	  child_task
	    = priority_queue_next_task (PQ_TEAM, &team->task_queue,
					PQ_IGNORED, NULL,
					&ignored);
	  cancelled = gomp_task_run_pre (child_task, child_task->parent,
					 team);
	  if (__builtin_expect (cancelled, 0))
	    {
	      if (to_free)
		{
		  gomp_finish_task (to_free);
		  free (to_free);
		  to_free = NULL;
		}
	      goto finish_cancelled;
	    }
	  team->task_running_count++;
	  child_task->in_tied_task = true;
	}
      else if (team->task_count == 0
	       && gomp_team_barrier_waiting_for_tasks (&team->barrier))
	{
	  gomp_team_barrier_done (&team->barrier, state);
	  gomp_mutex_unlock (&team->task_lock);
	  gomp_team_barrier_wake (&team->barrier, 0);
	  if (to_free)
	    {
	      gomp_finish_task (to_free);
	      free (to_free);
	    }
	  return;
	}
      gomp_mutex_unlock (&team->task_lock);
      if (do_wake)
	{
	  gomp_team_barrier_wake (&team->barrier, do_wake);
	  do_wake = 0;
	}
      if (to_free)
	{
	  gomp_finish_task (to_free);
	  free (to_free);
	  to_free = NULL;
	}
      if (child_task)
	{
	  thr->task = child_task;
	  if (__builtin_expect (child_task->fn == NULL, 0))
	    {
	      if (gomp_target_task_fn (child_task->fn_data))
		{
		  thr->task = task;
		  gomp_mutex_lock (&team->task_lock);
		  child_task->kind = GOMP_TASK_ASYNC_RUNNING;
		  team->task_running_count--;
		  struct gomp_target_task *ttask
		    = (struct gomp_target_task *) child_task->fn_data;
		  /* If GOMP_PLUGIN_target_task_completion has run already
		     in between gomp_target_task_fn and the mutex lock,
		     perform the requeuing here.  */
		  if (ttask->state == GOMP_TARGET_TASK_FINISHED)
		    gomp_target_task_completion (team, child_task);
		  else
		    ttask->state = GOMP_TARGET_TASK_RUNNING;
		  child_task = NULL;
		  continue;
		}
	    }
	  else
	    child_task->fn (child_task->fn_data);
	  thr->task = task;
	}
      else
	return;
      gomp_mutex_lock (&team->task_lock);
      if (child_task)
	{
	  if (child_task->detach_team)
	    {
	      assert (child_task->detach_team == team);
	      child_task->kind = GOMP_TASK_DETACHED;
	      ++team->task_detach_count;
	      --team->task_running_count;
	      gomp_debug (0,
			  "thread %d: task with event %p finished without "
			  "completion event fulfilled in team barrier\n",
			  thr->ts.team_id, child_task);
	      child_task = NULL;
	      continue;
	    }

	 finish_cancelled:;
	  size_t new_tasks
	    = gomp_task_run_post_handle_depend (child_task, team);
	  gomp_task_run_post_remove_parent (child_task);
	  gomp_clear_parent (&child_task->children_queue);
	  gomp_task_run_post_remove_taskgroup (child_task);
	  to_free = child_task;
	  if (!cancelled)
	    team->task_running_count--;
	  child_task = NULL;
	  if (new_tasks > 1)
	    {
	      do_wake = team->nthreads - team->task_running_count;
	      if (do_wake > new_tasks)
		do_wake = new_tasks;
	    }
	  --team->task_count;
	}
    }
}

/* Called when encountering a taskwait directive.

   Wait for all children of the current task.  */

void
GOMP_taskwait (void)
{
	#if defined(GOMP_USE_XQUEUE) && defined(GOMP_USE_XPERFLOG)
	unsigned long long taskwait_fref = xperflog_get_new_fref(XPERF_TASKWAIT);
	xperflog_record(XPERF_TASKWAIT, taskwait_fref, taskwait_fref);
	#endif // GOMP_USE_XQUEUE && GOMP_USE_XPERFLOG

  struct gomp_thread *thr = gomp_thread ();
  struct gomp_team *team = thr->ts.team;
  struct gomp_task *task = thr->task;
  struct gomp_task *child_task = NULL;
  struct gomp_task *to_free = NULL;
  struct gomp_taskwait taskwait;
  int do_wake = 0;

  /* The acquire barrier on load of task->children here synchronizes
     with the write of a NULL in gomp_task_run_post_remove_parent.  It is
     not necessary that we synchronize with other non-NULL writes at
     this point, but we must ensure that all writes to memory by a
     child thread task work function are seen before we exit from
     GOMP_taskwait.  */
#ifdef GOMP_USE_XQUEUE
	if(__builtin_expect(thr->use_xq, 1)){
		if (task == NULL){
			#ifdef GOMP_USE_XPERFLOG
			xperflog_record(XPERF_TASKWAIT_END, taskwait_fref, taskwait_fref);
			#endif // GOMP_USE_XPERFLOG
			return;
		}
			
	}
	else
#endif
  if (task == NULL
      || priority_queue_empty_p (&task->children_queue, MEMMODEL_ACQUIRE))
    return;


  memset (&taskwait, 0, sizeof (taskwait));
  bool child_q = false;

#ifdef GOMP_USE_XQUEUE
	unsigned long gtid = (unsigned long)omp_get_thread_num();
	unsigned int use_own_tasks = 1, new_victim = 0;
	unsigned long last_qid = (thr->num_queues <= gtid) ? 1 : gtid;
	#ifdef XTASK_RANDOM_BWS
	unsigned long last_req_qid = last_qid;
	int wait_countdown = 0;
	int max_wait_countdown = thr->max_wait_countdown;
	#endif
	gomp_task_t *next_task;
	if(__builtin_expect(thr->use_xq, 1)){
		// has to reimplement our own version of taskwait;
		while(GOMP_ATOMIC_LD_ACQ(&thr->task->td_incomplete_child_tasks)){
			// GOMP_taskwait	
			bool cancelled = false;
			if (to_free){
				gomp_finish_task (to_free);
				free (to_free);
				to_free = NULL;
			}

			next_task = NULL;
			if(use_own_tasks){
				next_task = gomp_remove_my_task();
			}

			if((next_task == NULL) && (thr->num_queues > 1)){
				use_own_tasks = 0;
			
				next_task = gomp_remove_aux_task(&last_qid);
			}
			// Taskwait
			if(next_task == NULL){
				#ifdef XTASK_LLWS
				xws_send_reqs();
				#endif
				#ifdef GOMP_USE_XPERFLOG
				xperflog_record(XPERF_TASKWAIT_END | XPERF_STALL, taskwait_fref, taskwait_fref);
				#endif
				#ifdef XTASK_SWS
				xtask_steal_req();
				#endif

				#ifdef XTASK_RANDOM_BWS
				if(wait_countdown > 0){
					wait_countdown--;
				}else{
					wait_countdown = max_wait_countdown;
					send_reqs();
				}
				#endif

				continue;
			}else{

				#ifdef XTASK_LLWS
				xws_handle_reqs(&last_qid);
				#endif

				#ifdef XTASK_SWS
				xtask_handle_req(&last_qid);
				#endif

				#ifdef XTASK_RANDOM_BWS
				handle_reqs(&last_req_qid);
				#endif
			}
			

			if (next_task->kind == GOMP_TASK_WAITING)
			{	
				#ifdef GOMP_USE_XPERFLOG
				xperflog_record(XPERF_STALL_END | XPERF_TASKWAIT, taskwait_fref, taskwait_fref);
				#endif
				child_task = next_task;
				child_task->kind = GOMP_TASK_TIED; // move this out of gomp_task_run_pre, so it only handles barriers
				child_task->in_tied_task = true;
				if (__builtin_expect (cancelled, 0)){
					if (to_free){
						gomp_finish_task (to_free);
						free (to_free);
						to_free = NULL;
					}
						goto xtask_finish_cancelled;
					}
			}
			else
			{

			// ww: TODO: this is also protected by task_lock, need to check if it is necessary
				gomp_debug(0, "[tid=%d] wenyi(GOMP_taskwait): This should never be reached!\n",omp_get_thread_num());
			}
			if (child_task){
				// task = thr->task;
				thr->task = child_task;
				if (__builtin_expect (child_task->fn == NULL, 0)){
					if (gomp_target_task_fn (child_task->fn_data)){
						thr->task = task;
						//gomp_mutex_lock (&team->task_lock);
						child_task->kind = GOMP_TASK_ASYNC_RUNNING;
						struct gomp_target_task *ttask
							= (struct gomp_target_task *) child_task->fn_data;
						/* If GOMP_PLUGIN_target_task_completion has run already
							in between gomp_target_task_fn and the mutex lock,
							perform the requeuing here.  */
						if (ttask->state == GOMP_TARGET_TASK_FINISHED)
							gomp_target_task_completion (team, child_task);
						else
							ttask->state = GOMP_TARGET_TASK_RUNNING;
						child_task = NULL;
						continue;
					}
				}
				else{
					#ifdef GOMP_USE_XPERFLOG
					unsigned long long task_fref = xperflog_get_new_fref(XPERF_TASK);
					xperflog_record(XPERF_TASKWAIT_END | XPERF_TASK, taskwait_fref, task_fref);
					#endif // GOMP_USE_XPERFLOG
					child_task->fn (child_task->fn_data);
					#ifdef GOMP_USE_XPERFLOG
					xperflog_record(XPERF_TASK_END | XPERF_TASKWAIT, task_fref, taskwait_fref); // task end can be used to encapsulate the task
					#endif // GOMP_USE_XPERFLOG
				}
					
				thr->task = task; // ww: task resumed
			}else continue;
			// gomp_sem_wait (&taskwait.taskwait_sem);
			// ww: wait in gnu's context indicates nothing in the queue, not suitable for our purpose. NOT SURE if it only waits when there is nothing in both of the queues or if there is nothing in either queues
			
			if(!use_own_tasks && thr->td_task_q[0]->td_deque[thr->td_task_q[0]->td_deque_head] != NULL){
				use_own_tasks = 1;
				new_victim = 0;
			}
			if(new_victim)
				new_victim = new_victim;
			
			// gomp_mutex_lock(&team->task_lock); was there
			if(child_task){
				// ww: this isn't likely to run
				if (child_task->detach_team){
				assert (child_task->detach_team == team);
				child_task->kind = GOMP_TASK_DETACHED;
				++team->task_detach_count;
				gomp_debug (0,
					"thread %d: task with event %p finished without "
					"completion event fulfilled in taskwait\n",
					thr->ts.team_id, child_task);
				child_task = NULL;
				continue;
				}

				GOMP_ATOMIC_DEC(&child_task->parent->td_incomplete_child_tasks);
				// GOMP_ATOMIC_DEC(&team->xtask_count);
		
				xtask_finish_cancelled:;
				to_free = child_task;
				child_task = NULL;
			}
		}

		bool destroy_taskwait = task->taskwait != NULL;
		task->taskwait = NULL;
		if (destroy_taskwait)
			gomp_sem_destroy(&taskwait.taskwait_sem);
	
		// record the exit of a taskwait
		#ifdef GOMP_USE_XPERFLOG
		xperflog_record(XPERF_TASKWAIT_END, taskwait_fref, taskwait_fref);
		#endif // GOMP_USE_XPERFLOG
		return;
	}else{ // taskwait - no-xq begin
#endif // GOMP_USE_XQUEUE

  gomp_mutex_lock (&team->task_lock);
  while (1)
    {
      bool cancelled = false;
      if (priority_queue_empty_p (&task->children_queue, MEMMODEL_RELAXED))
	{
	  bool destroy_taskwait = task->taskwait != NULL;
	  task->taskwait = NULL;
	  gomp_mutex_unlock (&team->task_lock);
	  if (to_free)
	    {
	      gomp_finish_task (to_free);
	      free (to_free);
	    }
	  if (destroy_taskwait)
	    gomp_sem_destroy (&taskwait.taskwait_sem);
	  return;
	}
      struct gomp_task *next_task
	= priority_queue_next_task (PQ_CHILDREN, &task->children_queue,
				    PQ_TEAM, &team->task_queue, &child_q);
      if (next_task->kind == GOMP_TASK_WAITING)
	{
	  child_task = next_task;
	  cancelled
	    = gomp_task_run_pre (child_task, task, team);
	  if (__builtin_expect (cancelled, 0))
	    {
	      if (to_free)
		{
		  gomp_finish_task (to_free);
		  free (to_free);
		  to_free = NULL;
		}
	      goto finish_cancelled;
	    }
	}
      else
	{
	/* All tasks we are waiting for are either running in other
	   threads, are detached and waiting for the completion event to be
	   fulfilled, or they are tasks that have not had their
	   dependencies met (so they're not even in the queue).  Wait
	   for them.  */
	  if (task->taskwait == NULL)
	    {
	      taskwait.in_depend_wait = false;
	      gomp_sem_init (&taskwait.taskwait_sem, 0);
	      task->taskwait = &taskwait;
	    }
	  taskwait.in_taskwait = true;
	}
      gomp_mutex_unlock (&team->task_lock);
      if (do_wake)
	{
	  gomp_team_barrier_wake (&team->barrier, do_wake);
	  do_wake = 0;
	}
      if (to_free)
	{
	  gomp_finish_task (to_free);
	  free (to_free);
	  to_free = NULL;
	}
      if (child_task)
	{
	  thr->task = child_task;
	  if (__builtin_expect (child_task->fn == NULL, 0))
	    {
	      if (gomp_target_task_fn (child_task->fn_data))
		{
		  thr->task = task;
		  gomp_mutex_lock (&team->task_lock);
		  child_task->kind = GOMP_TASK_ASYNC_RUNNING;
		  struct gomp_target_task *ttask
		    = (struct gomp_target_task *) child_task->fn_data;
		  /* If GOMP_PLUGIN_target_task_completion has run already
		     in between gomp_target_task_fn and the mutex lock,
		     perform the requeuing here.  */
		  if (ttask->state == GOMP_TARGET_TASK_FINISHED)
		    gomp_target_task_completion (team, child_task);
		  else
		    ttask->state = GOMP_TARGET_TASK_RUNNING;
		  child_task = NULL;
		  continue;
		}
	    }
	  else
	    child_task->fn (child_task->fn_data);
	  thr->task = task;
	}
      else
	gomp_sem_wait (&taskwait.taskwait_sem);
      gomp_mutex_lock (&team->task_lock);
      if (child_task)
	{
	  if (child_task->detach_team)
	    {
	      assert (child_task->detach_team == team);
	      child_task->kind = GOMP_TASK_DETACHED;
	      ++team->task_detach_count;
	      gomp_debug (0,
			  "thread %d: task with event %p finished without "
			  "completion event fulfilled in taskwait\n",
			  thr->ts.team_id, child_task);
	      child_task = NULL;
	      continue;
	    }

	 finish_cancelled:;
	  size_t new_tasks
	    = gomp_task_run_post_handle_depend (child_task, team);

	  if (child_q)
	    {
	      priority_queue_remove (PQ_CHILDREN, &task->children_queue,
				     child_task, MEMMODEL_RELAXED);
	      child_task->pnode[PQ_CHILDREN].next = NULL;
	      child_task->pnode[PQ_CHILDREN].prev = NULL;
	    }

	  gomp_clear_parent (&child_task->children_queue);

	  gomp_task_run_post_remove_taskgroup (child_task);

	  to_free = child_task;
	  child_task = NULL;
	  team->task_count--;
	  if (new_tasks > 1)
	    {
	      do_wake = team->nthreads - team->task_running_count
			- !task->in_tied_task;
	      if (do_wake > new_tasks)
		do_wake = new_tasks;
	    }
	}
    }
#ifdef GOMP_USE_XQUEUE
	} // taskwait - no-xq end
#endif
}

/* Called when encountering a taskwait directive with depend clause(s).
   Wait as if it was an mergeable included task construct with empty body.  */

void
GOMP_taskwait_depend (void **depend)
{
  struct gomp_thread *thr = gomp_thread ();
  struct gomp_team *team = thr->ts.team;

  /* If parallel or taskgroup has been cancelled, return early.  */
  if (__builtin_expect (gomp_cancel_var, 0) && team)
    {
      if (gomp_team_barrier_cancelled (&team->barrier))
	return;
      if (thr->task->taskgroup)
	{
	  if (thr->task->taskgroup->cancelled)
	    return;
	  if (thr->task->taskgroup->workshare
	      && thr->task->taskgroup->prev
	      && thr->task->taskgroup->prev->cancelled)
	    return;
	}
    }

  if (thr->task && thr->task->depend_hash)
    gomp_task_maybe_wait_for_dependencies (depend);
}

/* An undeferred task is about to run.  Wait for all tasks that this
   undeferred task depends on.

   This is done by first putting all known ready dependencies
   (dependencies that have their own dependencies met) at the top of
   the scheduling queues.  Then we iterate through these imminently
   ready tasks (and possibly other high priority tasks), and run them.
   If we run out of ready dependencies to execute, we either wait for
   the remaining dependencies to finish, or wait for them to get
   scheduled so we can run them.

   DEPEND is as in GOMP_task.  */

void
gomp_task_maybe_wait_for_dependencies (void **depend)
{
  struct gomp_thread *thr = gomp_thread ();
  struct gomp_task *task = thr->task;
  struct gomp_team *team = thr->ts.team;
  struct gomp_task_depend_entry elem, *ent = NULL;
  struct gomp_taskwait taskwait;
  size_t orig_ndepend = (uintptr_t) depend[0];
  size_t nout = (uintptr_t) depend[1];
  size_t ndepend = orig_ndepend;
  size_t normal = ndepend;
  size_t n = 2;
  size_t i;
  size_t num_awaited = 0;
  struct gomp_task *child_task = NULL;
  struct gomp_task *to_free = NULL;
  int do_wake = 0;

  if (ndepend == 0)
    {
      ndepend = nout;
      nout = (uintptr_t) depend[2] + (uintptr_t) depend[3];
      normal = nout + (uintptr_t) depend[4];
      n = 5;
    }
  gomp_mutex_lock (&team->task_lock);
  for (i = 0; i < ndepend; i++)
    {
      elem.addr = depend[i + n];
      elem.is_in = i >= nout;
      if (__builtin_expect (i >= normal, 0))
	{
	  void **d = (void **) elem.addr;
	  switch ((uintptr_t) d[1])
	    {
	    case GOMP_DEPEND_IN:
	      break;
	    case GOMP_DEPEND_OUT:
	    case GOMP_DEPEND_INOUT:
	    case GOMP_DEPEND_MUTEXINOUTSET:
	      elem.is_in = 0;
	      break;
	    default:
	      gomp_fatal ("unknown omp_depend_t dependence type %d",
			  (int) (uintptr_t) d[1]);
	    }
	  elem.addr = d[0];
	}
      ent = htab_find (task->depend_hash, &elem);
      for (; ent; ent = ent->next)
	if (elem.is_in && ent->is_in)
	  continue;
	else
	  {
	    struct gomp_task *tsk = ent->task;
	    if (!tsk->parent_depends_on)
	      {
		tsk->parent_depends_on = true;
		++num_awaited;
		/* If dependency TSK itself has no dependencies and is
		   ready to run, move it up front so that we run it as
		   soon as possible.  */
		if (tsk->num_dependees == 0 && tsk->kind == GOMP_TASK_WAITING)
		  priority_queue_upgrade_task (tsk, task);
	      }
	  }
    }
  if (num_awaited == 0)
    {
      gomp_mutex_unlock (&team->task_lock);
      return;
    }

  memset (&taskwait, 0, sizeof (taskwait));
  taskwait.n_depend = num_awaited;
  gomp_sem_init (&taskwait.taskwait_sem, 0);
  task->taskwait = &taskwait;

  while (1)
    {
      bool cancelled = false;
      if (taskwait.n_depend == 0)
	{
	  task->taskwait = NULL;
	  gomp_mutex_unlock (&team->task_lock);
	  if (to_free)
	    {
	      gomp_finish_task (to_free);
	      free (to_free);
	    }
	  gomp_sem_destroy (&taskwait.taskwait_sem);
	  return;
	}

      /* Theoretically when we have multiple priorities, we should
	 chose between the highest priority item in
	 task->children_queue and team->task_queue here, so we should
	 use priority_queue_next_task().  However, since we are
	 running an undeferred task, perhaps that makes all tasks it
	 depends on undeferred, thus a priority of INF?  This would
	 make it unnecessary to take anything into account here,
	 but the dependencies.

	 On the other hand, if we want to use priority_queue_next_task(),
	 care should be taken to only use priority_queue_remove()
	 below if the task was actually removed from the children
	 queue.  */
      bool ignored;
      struct gomp_task *next_task
	= priority_queue_next_task (PQ_CHILDREN, &task->children_queue,
				    PQ_IGNORED, NULL, &ignored);

      if (next_task->kind == GOMP_TASK_WAITING)
	{
	  child_task = next_task;
	  cancelled
	    = gomp_task_run_pre (child_task, task, team);
	  if (__builtin_expect (cancelled, 0))
	    {
	      if (to_free)
		{
		  gomp_finish_task (to_free);
		  free (to_free);
		  to_free = NULL;
		}
	      goto finish_cancelled;
	    }
	}
      else
	/* All tasks we are waiting for are either running in other
	   threads, or they are tasks that have not had their
	   dependencies met (so they're not even in the queue).  Wait
	   for them.  */
	taskwait.in_depend_wait = true;
      gomp_mutex_unlock (&team->task_lock);
      if (do_wake)
	{
	  gomp_team_barrier_wake (&team->barrier, do_wake);
	  do_wake = 0;
	}
      if (to_free)
	{
	  gomp_finish_task (to_free);
	  free (to_free);
	  to_free = NULL;
	}
      if (child_task)
	{
	  thr->task = child_task;
	  if (__builtin_expect (child_task->fn == NULL, 0))
	    {
	      if (gomp_target_task_fn (child_task->fn_data))
		{
		  thr->task = task;
		  gomp_mutex_lock (&team->task_lock);
		  child_task->kind = GOMP_TASK_ASYNC_RUNNING;
		  struct gomp_target_task *ttask
		    = (struct gomp_target_task *) child_task->fn_data;
		  /* If GOMP_PLUGIN_target_task_completion has run already
		     in between gomp_target_task_fn and the mutex lock,
		     perform the requeuing here.  */
		  if (ttask->state == GOMP_TARGET_TASK_FINISHED)
		    gomp_target_task_completion (team, child_task);
		  else
		    ttask->state = GOMP_TARGET_TASK_RUNNING;
		  child_task = NULL;
		  continue;
		}
	    }
	  else
	    child_task->fn (child_task->fn_data);
	  thr->task = task;
	}
      else
	gomp_sem_wait (&taskwait.taskwait_sem);
      gomp_mutex_lock (&team->task_lock);
      if (child_task)
	{
	 finish_cancelled:;
	  size_t new_tasks
	    = gomp_task_run_post_handle_depend (child_task, team);
	  if (child_task->parent_depends_on)
	    --taskwait.n_depend;

	  priority_queue_remove (PQ_CHILDREN, &task->children_queue,
				 child_task, MEMMODEL_RELAXED);
	  child_task->pnode[PQ_CHILDREN].next = NULL;
	  child_task->pnode[PQ_CHILDREN].prev = NULL;

	  gomp_clear_parent (&child_task->children_queue);
	  gomp_task_run_post_remove_taskgroup (child_task);
	  to_free = child_task;
	  child_task = NULL;
	  team->task_count--;
	  if (new_tasks > 1)
	    {
	      do_wake = team->nthreads - team->task_running_count
			- !task->in_tied_task;
	      if (do_wake > new_tasks)
		do_wake = new_tasks;
	    }
	}
    }
}

/* Called when encountering a taskyield directive.  */

void
GOMP_taskyield (void)
{
  /* Nothing at the moment.  */
}

static inline struct gomp_taskgroup *
gomp_taskgroup_init (struct gomp_taskgroup *prev)
{
  struct gomp_taskgroup *taskgroup
    = gomp_malloc (sizeof (struct gomp_taskgroup));
  taskgroup->prev = prev;
  priority_queue_init (&taskgroup->taskgroup_queue);
  taskgroup->reductions = prev ? prev->reductions : NULL;
  taskgroup->in_taskgroup_wait = false;
  taskgroup->cancelled = false;
  taskgroup->workshare = false;
  taskgroup->num_children = 0;
  gomp_sem_init (&taskgroup->taskgroup_sem, 0);
  return taskgroup;
}

void
GOMP_taskgroup_start (void)
{
  struct gomp_thread *thr = gomp_thread ();
  struct gomp_team *team = thr->ts.team;
  struct gomp_task *task = thr->task;

  /* If team is NULL, all tasks are executed as
     GOMP_TASK_UNDEFERRED tasks and thus all children tasks of
     taskgroup and their descendant tasks will be finished
     by the time GOMP_taskgroup_end is called.  */
  if (team == NULL)
    return;
  task->taskgroup = gomp_taskgroup_init (task->taskgroup);
}

void
GOMP_taskgroup_end (void)
{
  struct gomp_thread *thr = gomp_thread ();
  struct gomp_team *team = thr->ts.team;
  struct gomp_task *task = thr->task;
  struct gomp_taskgroup *taskgroup;
  struct gomp_task *child_task = NULL;
  struct gomp_task *to_free = NULL;
  int do_wake = 0;

  if (team == NULL)
    return;
  taskgroup = task->taskgroup;
  if (__builtin_expect (taskgroup == NULL, 0)
      && thr->ts.level == 0)
    {
      /* This can happen if GOMP_taskgroup_start is called when
	 thr->ts.team == NULL, but inside of the taskgroup there
	 is #pragma omp target nowait that creates an implicit
	 team with a single thread.  In this case, we want to wait
	 for all outstanding tasks in this team.  */
      gomp_team_barrier_wait (&team->barrier);
      return;
    }

  /* The acquire barrier on load of taskgroup->num_children here
     synchronizes with the write of 0 in gomp_task_run_post_remove_taskgroup.
     It is not necessary that we synchronize with other non-0 writes at
     this point, but we must ensure that all writes to memory by a
     child thread task work function are seen before we exit from
     GOMP_taskgroup_end.  */
  if (__atomic_load_n (&taskgroup->num_children, MEMMODEL_ACQUIRE) == 0)
    goto finish;

  bool unused;
  gomp_mutex_lock (&team->task_lock);
  while (1)
    {
      bool cancelled = false;
      if (priority_queue_empty_p (&taskgroup->taskgroup_queue,
				  MEMMODEL_RELAXED))
	{
	  if (taskgroup->num_children)
	    {
	      if (priority_queue_empty_p (&task->children_queue,
					  MEMMODEL_RELAXED))
		goto do_wait;
	      child_task
		= priority_queue_next_task (PQ_CHILDREN, &task->children_queue,
					    PQ_TEAM, &team->task_queue,
					    &unused);
	    }
	  else
	    {
	      gomp_mutex_unlock (&team->task_lock);
	      if (to_free)
		{
		  gomp_finish_task (to_free);
		  free (to_free);
		}
	      goto finish;
	    }
	}
      else
	child_task
	  = priority_queue_next_task (PQ_TASKGROUP, &taskgroup->taskgroup_queue,
				      PQ_TEAM, &team->task_queue, &unused);
      if (child_task->kind == GOMP_TASK_WAITING)
	{
	  cancelled
	    = gomp_task_run_pre (child_task, child_task->parent, team);
	  if (__builtin_expect (cancelled, 0))
	    {
	      if (to_free)
		{
		  gomp_finish_task (to_free);
		  free (to_free);
		  to_free = NULL;
		}
	      goto finish_cancelled;
	    }
	}
      else
	{
	  child_task = NULL;
	 do_wait:
	/* All tasks we are waiting for are either running in other
	   threads, or they are tasks that have not had their
	   dependencies met (so they're not even in the queue).  Wait
	   for them.  */
	  taskgroup->in_taskgroup_wait = true;
	}
      gomp_mutex_unlock (&team->task_lock);
      if (do_wake)
	{
	  gomp_team_barrier_wake (&team->barrier, do_wake);
	  do_wake = 0;
	}
      if (to_free)
	{
	  gomp_finish_task (to_free);
	  free (to_free);
	  to_free = NULL;
	}
      if (child_task)
	{
	  thr->task = child_task;
	  if (__builtin_expect (child_task->fn == NULL, 0))
	    {
	      if (gomp_target_task_fn (child_task->fn_data))
		{
		  thr->task = task;
		  gomp_mutex_lock (&team->task_lock);
		  child_task->kind = GOMP_TASK_ASYNC_RUNNING;
		  struct gomp_target_task *ttask
		    = (struct gomp_target_task *) child_task->fn_data;
		  /* If GOMP_PLUGIN_target_task_completion has run already
		     in between gomp_target_task_fn and the mutex lock,
		     perform the requeuing here.  */
		  if (ttask->state == GOMP_TARGET_TASK_FINISHED)
		    gomp_target_task_completion (team, child_task);
		  else
		    ttask->state = GOMP_TARGET_TASK_RUNNING;
		  child_task = NULL;
		  continue;
		}
	    }
	  else
	    child_task->fn (child_task->fn_data);
	  thr->task = task;
	}
      else
	gomp_sem_wait (&taskgroup->taskgroup_sem);
      gomp_mutex_lock (&team->task_lock);
      if (child_task)
	{
	  if (child_task->detach_team)
	    {
	      assert (child_task->detach_team == team);
	      child_task->kind = GOMP_TASK_DETACHED;
	      ++team->task_detach_count;
	      gomp_debug (0,
			  "thread %d: task with event %p finished without "
			  "completion event fulfilled in taskgroup\n",
			  thr->ts.team_id, child_task);
	      child_task = NULL;
	      continue;
	    }

	 finish_cancelled:;
	  size_t new_tasks
	    = gomp_task_run_post_handle_depend (child_task, team);
	  gomp_task_run_post_remove_parent (child_task);
	  gomp_clear_parent (&child_task->children_queue);
	  gomp_task_run_post_remove_taskgroup (child_task);
	  to_free = child_task;
	  child_task = NULL;
	  team->task_count--;
	  if (new_tasks > 1)
	    {
	      do_wake = team->nthreads - team->task_running_count
			- !task->in_tied_task;
	      if (do_wake > new_tasks)
		do_wake = new_tasks;
	    }
	}
    }

 finish:
  task->taskgroup = taskgroup->prev;
  gomp_sem_destroy (&taskgroup->taskgroup_sem);
  free (taskgroup);
}

static inline __attribute__((always_inline)) void
gomp_reduction_register (uintptr_t *data, uintptr_t *old, uintptr_t *orig,
			 unsigned nthreads)
{
  size_t total_cnt = 0;
  uintptr_t *d = data;
  struct htab *old_htab = NULL, *new_htab;
  do
    {
      if (__builtin_expect (orig != NULL, 0))
	{
	  /* For worksharing task reductions, memory has been allocated
	     already by some other thread that encountered the construct
	     earlier.  */
	  d[2] = orig[2];
	  d[6] = orig[6];
	  orig = (uintptr_t *) orig[4];
	}
      else
	{
	  size_t sz = d[1] * nthreads;
	  /* Should use omp_alloc if d[3] is not -1.  */
	  void *ptr = gomp_aligned_alloc (d[2], sz);
	  memset (ptr, '\0', sz);
	  d[2] = (uintptr_t) ptr;
	  d[6] = d[2] + sz;
	}
      d[5] = 0;
      total_cnt += d[0];
      if (d[4] == 0)
	{
	  d[4] = (uintptr_t) old;
	  break;
	}
      else
	d = (uintptr_t *) d[4];
    }
  while (1);
  if (old && old[5])
    {
      old_htab = (struct htab *) old[5];
      total_cnt += htab_elements (old_htab);
    }
  new_htab = htab_create (total_cnt);
  if (old_htab)
    {
      /* Copy old hash table, like in htab_expand.  */
      hash_entry_type *p, *olimit;
      new_htab->n_elements = htab_elements (old_htab);
      olimit = old_htab->entries + old_htab->size;
      p = old_htab->entries;
      do
	{
	  hash_entry_type x = *p;
	  if (x != HTAB_EMPTY_ENTRY && x != HTAB_DELETED_ENTRY)
	    *find_empty_slot_for_expand (new_htab, htab_hash (x)) = x;
	  p++;
	}
      while (p < olimit);
    }
  d = data;
  do
    {
      size_t j;
      for (j = 0; j < d[0]; ++j)
	{
	  uintptr_t *p = d + 7 + j * 3;
	  p[2] = (uintptr_t) d;
	  /* Ugly hack, hash_entry_type is defined for the task dependencies,
	     which hash on the first element which is a pointer.  We need
	     to hash also on the first sizeof (uintptr_t) bytes which contain
	     a pointer.  Hide the cast from the compiler.  */
	  hash_entry_type n;
	  __asm ("" : "=g" (n) : "0" (p));
	  *htab_find_slot (&new_htab, n, INSERT) = n;
	}
      if (d[4] == (uintptr_t) old)
	break;
      else
	d = (uintptr_t *) d[4];
    }
  while (1);
  d[5] = (uintptr_t) new_htab;
}

static void
gomp_create_artificial_team (void)
{
  struct gomp_thread *thr = gomp_thread ();
  struct gomp_task_icv *icv;
  struct gomp_team *team = gomp_new_team (1);
  struct gomp_task *task = thr->task;
  struct gomp_task **implicit_task = &task;
  icv = task ? &task->icv : &gomp_global_icv;
  team->prev_ts = thr->ts;
  thr->ts.team = team;
  thr->ts.team_id = 0;
  thr->ts.work_share = &team->work_shares[0];
  thr->ts.last_work_share = NULL;
#ifdef HAVE_SYNC_BUILTINS
  thr->ts.single_count = 0;
#endif
  thr->ts.static_trip = 0;
  thr->task = &team->implicit_task[0];
  gomp_init_task (thr->task, NULL, icv);
  while (*implicit_task
	 && (*implicit_task)->kind != GOMP_TASK_IMPLICIT)
    implicit_task = &(*implicit_task)->parent;
  if (*implicit_task)
    {
      thr->task = *implicit_task;
      gomp_end_task ();
      free (*implicit_task);
      thr->task = &team->implicit_task[0];
    }
#ifdef LIBGOMP_USE_PTHREADS
  else
    pthread_setspecific (gomp_thread_destructor, thr);
#endif
  if (implicit_task != &task)
    {
      *implicit_task = thr->task;
      thr->task = task;
    }
}

/* The format of data is:
   data[0]	cnt
   data[1]	size
   data[2]	alignment (on output array pointer)
   data[3]	allocator (-1 if malloc allocator)
   data[4]	next pointer
   data[5]	used internally (htab pointer)
   data[6]	used internally (end of array)
   cnt times
   ent[0]	address
   ent[1]	offset
   ent[2]	used internally (pointer to data[0])
   The entries are sorted by increasing offset, so that a binary
   search can be performed.  Normally, data[8] is 0, exception is
   for worksharing construct task reductions in cancellable parallel,
   where at offset 0 there should be space for a pointer and an integer
   which are used internally.  */

void
GOMP_taskgroup_reduction_register (uintptr_t *data)
{
  struct gomp_thread *thr = gomp_thread ();
  struct gomp_team *team = thr->ts.team;
  struct gomp_task *task;
  unsigned nthreads;
  if (__builtin_expect (team == NULL, 0))
    {
      /* The task reduction code needs a team and task, so for
	 orphaned taskgroups just create the implicit team.  */
      gomp_create_artificial_team ();
      ialias_call (GOMP_taskgroup_start) ();
      team = thr->ts.team;
    }
  nthreads = team->nthreads;
  task = thr->task;
  gomp_reduction_register (data, task->taskgroup->reductions, NULL, nthreads);
  task->taskgroup->reductions = data;
}

void
GOMP_taskgroup_reduction_unregister (uintptr_t *data)
{
  uintptr_t *d = data;
  htab_free ((struct htab *) data[5]);
  do
    {
      gomp_aligned_free ((void *) d[2]);
      d = (uintptr_t *) d[4];
    }
  while (d && !d[5]);
}
ialias (GOMP_taskgroup_reduction_unregister)

/* For i = 0 to cnt-1, remap ptrs[i] which is either address of the
   original list item or address of previously remapped original list
   item to address of the private copy, store that to ptrs[i].
   For i < cntorig, additionally set ptrs[cnt+i] to the address of
   the original list item.  */

void
GOMP_task_reduction_remap (size_t cnt, size_t cntorig, void **ptrs)
{
  struct gomp_thread *thr = gomp_thread ();
  struct gomp_task *task = thr->task;
  unsigned id = thr->ts.team_id;
  uintptr_t *data = task->taskgroup->reductions;
  uintptr_t *d;
  struct htab *reduction_htab = (struct htab *) data[5];
  size_t i;
  for (i = 0; i < cnt; ++i)
    {
      hash_entry_type ent, n;
      __asm ("" : "=g" (ent) : "0" (ptrs + i));
      n = htab_find (reduction_htab, ent);
      if (n)
	{
	  uintptr_t *p;
	  __asm ("" : "=g" (p) : "0" (n));
	  /* At this point, p[0] should be equal to (uintptr_t) ptrs[i],
	     p[1] is the offset within the allocated chunk for each
	     thread, p[2] is the array registered with
	     GOMP_taskgroup_reduction_register, d[2] is the base of the
	     allocated memory and d[1] is the size of the allocated chunk
	     for one thread.  */
	  d = (uintptr_t *) p[2];
	  ptrs[i] = (void *) (d[2] + id * d[1] + p[1]);
	  if (__builtin_expect (i < cntorig, 0))
	    ptrs[cnt + i] = (void *) p[0];
	  continue;
	}
      d = data;
      while (d != NULL)
	{
	  if ((uintptr_t) ptrs[i] >= d[2] && (uintptr_t) ptrs[i] < d[6])
	    break;
	  d = (uintptr_t *) d[4];
	}
      if (d == NULL)
	gomp_fatal ("couldn't find matching task_reduction or reduction with "
		    "task modifier for %p", ptrs[i]);
      uintptr_t off = ((uintptr_t) ptrs[i] - d[2]) % d[1];
      ptrs[i] = (void *) (d[2] + id * d[1] + off);
      if (__builtin_expect (i < cntorig, 0))
	{
	  size_t lo = 0, hi = d[0] - 1;
	  while (lo <= hi)
	    {
	      size_t m = (lo + hi) / 2;
	      if (d[7 + 3 * m + 1] < off)
		lo = m + 1;
	      else if (d[7 + 3 * m + 1] == off)
		{
		  ptrs[cnt + i] = (void *) d[7 + 3 * m];
		  break;
		}
	      else
		hi = m - 1;
	    }
	  if (lo > hi)
	    gomp_fatal ("couldn't find matching task_reduction or reduction "
			"with task modifier for %p", ptrs[i]);
	}
    }
}

struct gomp_taskgroup *
gomp_parallel_reduction_register (uintptr_t *data, unsigned nthreads)
{
  struct gomp_taskgroup *taskgroup = gomp_taskgroup_init (NULL);
  gomp_reduction_register (data, NULL, NULL, nthreads);
  taskgroup->reductions = data;
  return taskgroup;
}

void
gomp_workshare_task_reduction_register (uintptr_t *data, uintptr_t *orig)
{
  struct gomp_thread *thr = gomp_thread ();
  struct gomp_team *team = thr->ts.team;
  struct gomp_task *task = thr->task;
  unsigned nthreads = team->nthreads;
  gomp_reduction_register (data, task->taskgroup->reductions, orig, nthreads);
  task->taskgroup->reductions = data;
}

void
gomp_workshare_taskgroup_start (void)
{
  struct gomp_thread *thr = gomp_thread ();
  struct gomp_team *team = thr->ts.team;
  struct gomp_task *task;

  if (team == NULL)
    {
      gomp_create_artificial_team ();
      team = thr->ts.team;
    }
  task = thr->task;
  task->taskgroup = gomp_taskgroup_init (task->taskgroup);
  task->taskgroup->workshare = true;
}

void
GOMP_workshare_task_reduction_unregister (bool cancelled)
{
  struct gomp_thread *thr = gomp_thread ();
  struct gomp_task *task = thr->task;
  struct gomp_team *team = thr->ts.team;
  uintptr_t *data = task->taskgroup->reductions;
  ialias_call (GOMP_taskgroup_end) ();
  if (thr->ts.team_id == 0)
    ialias_call (GOMP_taskgroup_reduction_unregister) (data);
  else
    htab_free ((struct htab *) data[5]);

  if (!cancelled)
    gomp_team_barrier_wait (&team->barrier);
}

int
omp_in_final (void)
{
  struct gomp_thread *thr = gomp_thread ();
  return thr->task && thr->task->final_task;
}

ialias (omp_in_final)

void
omp_fulfill_event (omp_event_handle_t event)
{
  struct gomp_task *task = (struct gomp_task *) event;
  if (!task->deferred_p)
    {
      if (gomp_sem_getcount (task->completion_sem) > 0)
	gomp_fatal ("omp_fulfill_event: %p event already fulfilled!\n", task);

      gomp_debug (0, "omp_fulfill_event: %p event for undeferred task\n",
		  task);
      gomp_sem_post (task->completion_sem);
      return;
    }

  struct gomp_team *team = __atomic_load_n (&task->detach_team,
					    MEMMODEL_RELAXED);
  if (!team)
    gomp_fatal ("omp_fulfill_event: %p event is invalid or has already "
		"been fulfilled!\n", task);

  gomp_mutex_lock (&team->task_lock);
  if (task->kind != GOMP_TASK_DETACHED)
    {
      /* The task has not finished running yet.  */
      gomp_debug (0,
		  "omp_fulfill_event: %p event fulfilled for unfinished "
		  "task\n", task);
      __atomic_store_n (&task->detach_team, NULL, MEMMODEL_RELAXED);
      gomp_mutex_unlock (&team->task_lock);
      return;
    }

  gomp_debug (0, "omp_fulfill_event: %p event fulfilled for finished task\n",
	      task);
  size_t new_tasks = gomp_task_run_post_handle_depend (task, team);
  gomp_task_run_post_remove_parent (task);
  gomp_clear_parent (&task->children_queue);
  gomp_task_run_post_remove_taskgroup (task);
  team->task_count--;
  team->task_detach_count--;

  int do_wake = 0;
  bool shackled_thread_p = team == gomp_thread ()->ts.team;
  if (new_tasks > 0)
    {
      /* Wake up threads to run new tasks.  */
      gomp_team_barrier_set_task_pending (&team->barrier);
      do_wake = team->nthreads - team->task_running_count;
      if (do_wake > new_tasks)
	do_wake = new_tasks;
    }

  if (!shackled_thread_p
      && !do_wake
      && team->task_detach_count == 0
      && gomp_team_barrier_waiting_for_tasks (&team->barrier))
    /* Ensure that at least one thread is woken up to signal that the
       barrier can finish.  */
    do_wake = 1;

  /* If we are running in an unshackled thread, the team might vanish before
     gomp_team_barrier_wake is run if we release the lock first, so keep the
     lock for the call in that case.  */
  if (shackled_thread_p)
    gomp_mutex_unlock (&team->task_lock);
  if (do_wake)
    gomp_team_barrier_wake (&team->barrier, do_wake);
  if (!shackled_thread_p)
    gomp_mutex_unlock (&team->task_lock);

  gomp_finish_task (task);
  free (task);
}

ialias (omp_fulfill_event)
ialias (xomp_perflog_info)
