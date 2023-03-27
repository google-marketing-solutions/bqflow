###########################################################################
#
#  Copyright 2020 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
###########################################################################

import re
import psutil
import multiprocessing

"""Generic utilities that do not belong in any specific sub module.

Add general utility functions that are used across many modules.  Do
not add classes here.
"""

RE_ALPHA_NUMERIC = re.compile('([^\s\w]|_)+')
RE_URL = re.compile(r'https?://[^\s\'">]+')


def flag_last(o):
  """Flags the last loop of an iterator.

  Consumes an iterator, buffers one instance so it can look ahead.
  Returns True on last iteration.

  Args:
    * o: An iterator instance.

  Returns:
    * A tuple of (True/False, iteration). Returns True, next on StopIteration.

  """

  it = o.__iter__()

  try:
    e = next(it)
  except StopIteration:
    return

  while True:
    try:
      nxt = next(it)
      yield (False, e)
      e = nxt
    except StopIteration:
      yield (True, e)
      break


def has_values(o):
  """Converts iterator to a boolean.

  Destroys iterator but returns True if at least one value is present.

  Args:
    * o: An iterator instance.

  Returns:
    * True if at least one instance or False if none.

  """

  try:
    next(o)
    return True
  except StopIteration:
    return False


def memory_scale(maximum, multiple=1, single_cpu=False):
  """Returns amount of memory in bytes avaialbe up to maximum.

  Ensures memory is a multiple of provided numer.
  Divides memory by number of CPU.

  Args:
    * maximum: highest memory needed
    * multiple: ensures returned memory is multiple of this.
    * single_cpu: if False, divides memory by number of cpus.

  Returns:
    * Bytes of memory availabe.

  """

  memory = psutil.virtual_memory().total

  if not single_cpu:
    memory /= multiprocessing.cpu_count()

  if multiple and multiple != 1:
    memory = int(memory/multiple) * multiple

  return min(maximum, memory)


def date_to_str(value):
  return None if value is None else value.strftime('%Y-%m-%d')


def parse_url(text):
  return RE_URL.findall(text)


def parse_filename(text):
  return RE_ALPHA_NUMERIC.sub('', text).lower().replace(' ', '_')
