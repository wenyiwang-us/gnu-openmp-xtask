/* Copyright (C) 2005-2022 Free Software Foundation, Inc.
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

/* This is a Linux specific implementation of a barrier synchronization
   mechanism for libgomp.  This type is private to the library.  This 
   implementation uses atomic instructions and the futex syscall.  */

#include <limits.h>
#include "wait.h"


void
gomp_barrier_wait_end (gomp_barrier_t *bar, gomp_barrier_state_t state)
{
  // xtask_debug(0, 1, "state = %d.", state);
  if (__builtin_expect (state & BAR_WAS_LAST, 0))
    {
      /* Next time we'll be awaiting TOTAL threads again.  */
      bar->awaited = bar->total;
      __atomic_store_n (&bar->generation, bar->generation + BAR_INCR,
			MEMMODEL_RELEASE);
      futex_wake ((int *) &bar->generation, INT_MAX);
    }
  else
    {
      do
	do_wait ((int *) &bar->generation, state);
      while (__atomic_load_n (&bar->generation, MEMMODEL_ACQUIRE) == state);
    }
}

void
gomp_barrier_wait (gomp_barrier_t *bar)
{
  gomp_barrier_wait_end (bar, gomp_barrier_wait_start (bar));
}

/* Like gomp_barrier_wait, except that if the encountering thread
   is not the last one to hit the barrier, it returns immediately.
   The intended usage is that a thread which intends to gomp_barrier_destroy
   this barrier calls gomp_barrier_wait, while all other threads
   call gomp_barrier_wait_last.  When gomp_barrier_wait returns,
   the barrier can be safely destroyed.  */

void
gomp_barrier_wait_last (gomp_barrier_t *bar)
{
  gomp_barrier_state_t state = gomp_barrier_wait_start (bar);
  if (state & BAR_WAS_LAST)
    gomp_barrier_wait_end (bar, state);
}

void
gomp_team_barrier_wake (gomp_barrier_t *bar, int count)
{
  futex_wake ((int *) &bar->generation, count == 0 ? INT_MAX : count);
}

void
gomp_team_barrier_wait_end (gomp_barrier_t *bar, gomp_barrier_state_t state)
{
  // xtask_debug(0, 1, "state = %d.", state);
  unsigned int generation, gen;
  #ifdef GOMP_USE_XQUEUE
  #ifdef XTASK_ENABLE_PROF
  xstats_barrier_start();
  #endif
  /**
   * XTask: Design new centralized busy barrier that meets the following requirements:
   * 1. The barrier should be released only when:
   *  a. All threads have arrived at the barrier.
   *  b. There is no task in the task queue (aka. xtask_count == 0).
   *    1. If there is a task in the task queue, the barrier will keep running tasks until the task queue is empty.
   *    Warning: This design may not satisfy the OpenMP requirements, further check needed.
   *  c. generation +=1 by the last one exits the barrier.
   *  d. ? final_spin ?: when barrier is releasing, final_spin is 1
   * 
   * Barrier Generation in GOMP (should mean)/means all the threads living at the same time.
   * Barrier should only be put into sleep when:
   * 1. This thread reaches the barrier -> <atomic> awaited += 1 
   * 2. No tasks in task queue
   * 
  */
  struct gomp_thread *thr = gomp_thread();
  bool released = false;
  if(__builtin_expect(thr->use_xq, 1)){
    // xtask_debug(0, 2, "state=%d\n", state);
    /**
     * reset the awaited count to total, this is guaranteed to happen before last thread calling gathered.
     * last thread is gathered without the knowledge that the awaited count has been reset.
    */
    if(state & BAR_WAS_LAST){
      // xtask_debug(0, 0, "last bar gather.");
      __atomic_store_n(&bar->awaited, bar->total, MEMMODEL_RELEASE);
    }
    gomp_barrier_state_t t; 
    while(1){
      #ifdef XTASK_ENABLE_PROF
      xstats_barrier_end();
      #endif
      /**
       * xtask_barrier_handle_tasks returns only when all the condition meets:
       * 1. All threads have arrived at the barrier by calling gomp_team_barrier_wait, gomp_barrier_wait_start or gomp_team_barrier_wait_final once each
       * 2. Task queue is empty at the point of checking
       * 3. thr's task has no dependencies
       * 4. Then, the above condition will initiate the barrier releasing process.
       * Once thr detects it is released, it will return from this function.
      */

      xtask_barrier_handle_tasks(state); 
      /**
       * The threads has been released, and the first comer will see a full awaited count,
       * gathered and on_release has been set to true, reset them once
       * release flag make sure the state is only updated once inside the loop.
       * Note now the state is different than the one passed in
      */
     
     if(!released){
      state &= ~BAR_WAS_LAST;
      t = gomp_barrier_wait_start(bar);
      released = true;
     }

      gen = __atomic_load_n(&bar->generation, MEMMODEL_ACQUIRE);
      if(__builtin_expect(gen == state + BAR_INCR, 0))
        break;
      
      if(__builtin_expect(t & BAR_WAS_LAST, 0)){
        // or this atomic can be replaced with a simple assignmet?
        state &= ~BAR_CANCELLED;
        // state += BAR_INCR;
        bar->awaited = bar->total; // FIXME: maybe we can avoid using atomic like this?
        // __atomic_store_n(&bar->awaited, bar->total, MEMMODEL_RELEASE);
        __atomic_store_n(&bar->generation, state + BAR_INCR, MEMMODEL_RELEASE);
        // gen = __atomic_load_n(&bar->generation, MEMMODEL_ACQUIRE);
        // xtask_debug(0, 0, "last bar exits, state=%d, gen=%d, total=%d", state, gen, bar->total);
        break;
      }

      state &= ~BAR_CANCELLED;
    }
    // flag of current thread is reinit to the right state.
    xflag_reinit(thr, state);
   
  #ifdef XTASK_ENABLE_PROF
  xstats_barrier_end();
  #endif
  return;
  }else{ // if not using xq
  #endif

  if (__builtin_expect (state & BAR_WAS_LAST, 0))
    {
      /* Next time we'll be awaiting TOTAL threads again.  */
      struct gomp_thread *thr = gomp_thread ();
      struct gomp_team *team = thr->ts.team;

      bar->awaited = bar->total;
      team->work_share_cancelled = 0;
      if (__builtin_expect (team->task_count, 0))
	{
	  gomp_barrier_handle_tasks (state);
	  state &= ~BAR_WAS_LAST;
	}
      else
	{
	  state &= ~BAR_CANCELLED;
	  state += BAR_INCR - BAR_WAS_LAST;
	  __atomic_store_n (&bar->generation, state, MEMMODEL_RELEASE);
	  futex_wake ((int *) &bar->generation, INT_MAX);
	  return;
	}
    }

  generation = state;
  state &= ~BAR_CANCELLED;
  do
    {
      do_wait ((int *) &bar->generation, generation);
      gen = __atomic_load_n (&bar->generation, MEMMODEL_ACQUIRE);
      if (__builtin_expect (gen & BAR_TASK_PENDING, 0))
	{
	  gomp_barrier_handle_tasks (state);
	  gen = __atomic_load_n (&bar->generation, MEMMODEL_ACQUIRE);
	}
      generation |= gen & BAR_WAITING_FOR_TASK;
    }
  while (gen != state + BAR_INCR);
  #ifdef GOMP_USE_XQUEUE
  } // end of else
  #endif
}

void
gomp_team_barrier_wait (gomp_barrier_t *bar)
{
  gomp_team_barrier_wait_end (bar, gomp_barrier_wait_start (bar));
}

void
gomp_team_barrier_wait_final (gomp_barrier_t *bar)
{
  gomp_barrier_state_t state = gomp_barrier_wait_final_start (bar);
  if (__builtin_expect (state & BAR_WAS_LAST, 0))
    bar->awaited_final = bar->total;
  gomp_team_barrier_wait_end (bar, state);
}

bool
gomp_team_barrier_wait_cancel_end (gomp_barrier_t *bar,
				   gomp_barrier_state_t state)
{
  unsigned int generation, gen;

  if (__builtin_expect (state & BAR_WAS_LAST, 0))
    {
      /* Next time we'll be awaiting TOTAL threads again.  */
      /* BAR_CANCELLED should never be set in state here, because
	 cancellation means that at least one of the threads has been
	 cancelled, thus on a cancellable barrier we should never see
	 all threads to arrive.  */
      struct gomp_thread *thr = gomp_thread ();
      struct gomp_team *team = thr->ts.team;

      bar->awaited = bar->total;
      team->work_share_cancelled = 0;
      if (__builtin_expect (team->task_count, 0))
	{
	  gomp_barrier_handle_tasks (state);
	  state &= ~BAR_WAS_LAST;
	}
      else
	{
	  state += BAR_INCR - BAR_WAS_LAST;
	  __atomic_store_n (&bar->generation, state, MEMMODEL_RELEASE);
	  futex_wake ((int *) &bar->generation, INT_MAX);
	  return false;
	}
    }

  if (__builtin_expect (state & BAR_CANCELLED, 0))
    return true;

  generation = state;
  do
    {
      do_wait ((int *) &bar->generation, generation);
      gen = __atomic_load_n (&bar->generation, MEMMODEL_ACQUIRE);
      if (__builtin_expect (gen & BAR_CANCELLED, 0))
	return true;
      if (__builtin_expect (gen & BAR_TASK_PENDING, 0))
	{
	  gomp_barrier_handle_tasks (state);
	  gen = __atomic_load_n (&bar->generation, MEMMODEL_ACQUIRE);
	}
      generation |= gen & BAR_WAITING_FOR_TASK;
    }
  while (gen != state + BAR_INCR);

  return false;
}

bool
gomp_team_barrier_wait_cancel (gomp_barrier_t *bar)
{
  return gomp_team_barrier_wait_cancel_end (bar, gomp_barrier_wait_start (bar));
}

void
gomp_team_barrier_cancel (struct gomp_team *team)
{
  gomp_mutex_lock (&team->task_lock);
  if (team->barrier.generation & BAR_CANCELLED)
    {
      gomp_mutex_unlock (&team->task_lock);
      return;
    }
  team->barrier.generation |= BAR_CANCELLED;
  gomp_mutex_unlock (&team->task_lock);
  futex_wake ((int *) &team->barrier.generation, INT_MAX);
}
