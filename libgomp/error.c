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

/* This file contains routines used to signal errors.  Most places in the
   OpenMP API do not make any provision for failure, so we can't just
   defer the decision on reporting the problem to the user; we must do it
   ourselves or not at all.  */
/* ??? Is this about what other implementations do?  Assume stderr hasn't
   been pointed somewhere unsafe?  */

#include "libgomp.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>


#undef gomp_vdebug
void
gomp_vdebug (int kind __attribute__ ((unused)), const char *msg, va_list list)
{
#ifdef GOMP_USE_XQUEUE
  if(gomp_debug_var >= kind)
    vfprintf (stderr, msg, list);
#else

  if (gomp_debug_var)
    vfprintf (stderr, msg, list);
#endif
}

#undef gomp_debug
void
gomp_debug (int kind, const char *msg, ...)
{
  va_list list;

  va_start (list, msg);
  gomp_vdebug (kind, msg, list);
  va_end (list);
}

#ifdef GOMP_USE_XQUEUE
#undef xtask_debug
void concat(char *dest, const char *src1, const char *src2){
  while(*src1){
    *dest = *src1;
    src1++;
    dest++;
  }
  while(*src2){
    *dest = *src2;
    src2++;
    dest++;
  }
  *dest = '\0';
}
void
xtask_debug (int kind, int level, const char* func, const char *msg, ...)
{
  va_list list;

  if (gomp_debug_var >= kind)
    {
      va_start (list, msg);

      char tabs[64], prefix[128], buf[1024];
      // build tabs
      int i = 0;
      while(i < level){
        tabs[2 * i] = ' ';
        tabs[2 * i + 1] = ' ';
        i++;
      }
      tabs[2 * i] = '\0';

      sprintf(prefix, "[tid=%d]%s(%s): ", omp_get_thread_num(), tabs, func);
      sprintf(buf, "%s%s\n", prefix, msg);
      // print all at once so that the output is not interleaved
      vfprintf (stderr, buf, list);
      va_end (list);
    }
}

#ifdef XTASK_ENABLE_PROF
#undef xtask_prof
void xtask_prof (int kind, int level, const char* func, const char *msg, ...)
{
  va_list list;

  if (gomp_debug_var >= kind)
    {
      va_start (list, msg);

      char tabs[64], prefix[128], buf[1024];
      // build tabs
      int i = 0;
      while(i < level){
        tabs[2 * i] = ' ';
        tabs[2 * i + 1] = ' ';
        i++;
      }
      tabs[2 * i] = '\0';

      sprintf(prefix, "[XTASK-PROFLING][tid=%d]:%s ", omp_get_thread_num(), tabs);
      sprintf(buf, "%s%s\n", prefix, msg);
      // print all at once so that the output is not interleaved
      vfprintf (stderr, buf, list);
      va_end (list);
    }
}
#endif
#endif

void
gomp_verror (const char *fmt, va_list list)
{
  fputs ("\nlibgomp: ", stderr);
  vfprintf (stderr, fmt, list);
  fputc ('\n', stderr);
}

void
gomp_error (const char *fmt, ...)
{
  va_list list;

  va_start (list, fmt);
  gomp_verror (fmt, list);
  va_end (list);
}

void
gomp_vfatal (const char *fmt, va_list list)
{
  gomp_verror (fmt, list);
  exit (EXIT_FAILURE);
}

void
gomp_fatal (const char *fmt, ...)
{
  va_list list;

  va_start (list, fmt);
  gomp_vfatal (fmt, list);
  va_end (list);
}

void
GOMP_warning (const char *msg, size_t msglen)
{
  if (msg && msglen == (size_t) -1)
    gomp_error ("error directive encountered: %s", msg);
  else if (msg)
    {
      fputs ("\nlibgomp: error directive encountered: ", stderr);
      fwrite (msg, 1, msglen, stderr);
      fputc ('\n', stderr);
    }
  else
    gomp_error ("error directive encountered");
}

void
GOMP_error (const char *msg, size_t msglen)
{
  if (msg && msglen == (size_t) -1)
    gomp_fatal ("fatal error: error directive encountered: %s", msg);
  else if (msg)
    {
      fputs ("\nlibgomp: fatal error: error directive encountered: ", stderr);
      fwrite (msg, 1, msglen, stderr);
      fputc ('\n', stderr);
      exit (EXIT_FAILURE);
    }
  else
    gomp_fatal ("fatal error: error directive encountered");
}
