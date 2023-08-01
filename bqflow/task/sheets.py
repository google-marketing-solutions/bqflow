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

from bqflow.util.sheets_api import Sheets

def sheets(config, log, task):

  if config.verbose:
    print('SHEETS')

  # delete tab if specified, will delete sheet if no more tabs remain
  if task.get('delete', False):
    Sheets(config, task['auth']).tab_delete(
      task['sheet'],
      task['tab']
    )

  # create a sheet and tab if specified, if template
  if 'template' in task:
    Sheets(config, task['auth']).sheet_create(
      task['sheet'],
      task['tab'],
      task['template'].get('sheet'),
      task['template'].get('tab'),
    )

  # clear if specified
  if task.get('clear', False):
    Sheets(config, task['auth']).tab_clear(
      task['sheet'],
      task['tab'],
      task.get('range', 'A1')
    )

  # write data if specified
  if 'write' in task:
    Sheets(config, task['auth']).tab_write(
      sheet_url_or_name=task['sheet'],
      sheet_tab=task['tab'],
      sheet_range=task['range'],
      rows = get_rows(config, task['auth'], task['write']),
      append=task.get('append', False)
    )
