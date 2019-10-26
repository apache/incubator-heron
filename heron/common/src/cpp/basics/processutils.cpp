/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "basics/processutils.h"
#include <sys/resource.h>
#include <unistd.h>
#include <gperftools/malloc_extension.h>

pid_t ProcessUtils::getPid() { return ::getpid(); }

int ProcessUtils::getResourceUsage(struct rusage *usage) { return ::getrusage(RUSAGE_SELF, usage); }

/* ``generic.heap_size'' also includes unmapped pages in heap. These bytes
 * have been release back to OS. They always count towards virtual memory
 * memory usage, and depending on OS, typically do nto count towards physical
 * memory usage.
 *
 * See more at: https://gperftools.github.io/gperftools/tcmalloc.html
 *
 * Here is a sample output of heap usage statistic by
 * ``MallocExtension::instance()->GetStats''
 *
 *       484254200 (  461.8 MiB) Bytes in use by application
 *  +            0 (    0.0 MiB) Bytes in page heap freelist
 *  +     22847400 (   21.8 MiB) Bytes in central cache freelist
 *  +     11551232 (   11.0 MiB) Bytes in transfer cache freelist
 *  +      5528672 (    5.3 MiB) Bytes in thread cache freelists
 *  +      2617504 (    2.5 MiB) Bytes in malloc metadata
 *    ------------
 *  =    526799008 (  502.4 MiB) Actual memory used (physical + swap)
 *  +     41000960 (   39.1 MiB) Bytes released to OS (aka unmapped)
 *    ------------
 *  =    567799968 (  541.5 MiB) Virtual address space used
 *
 *           17441              Spans in use
 *               3              Thread heaps in use
 *            8192              Tcmalloc page size
 *
 * In this example, ``generic.heap_size'' is 541.5 MiB, and
 * ``tcmalloc.pageheap_unmapped_bytes'' is 39.1 MiB. Actual memory
 * usage would be 502.4 MiB.
 *
 */
size_t ProcessUtils::getTotalMemoryUsed() {
  size_t total = 0, unmapped = 0;
  MallocExtension::instance()->GetNumericProperty("generic.heap_size", &total);
  MallocExtension::instance()->GetNumericProperty(
      "tcmalloc.pageheap_unmapped_bytes", &unmapped);
  return total - unmapped;
}

sp_string ProcessUtils::getCurrentWorkingDirectory() {
  char buff[FILENAME_MAX];
  getcwd(buff, FILENAME_MAX);
  sp_string current_working_dir(buff);
  return current_working_dir;
}
