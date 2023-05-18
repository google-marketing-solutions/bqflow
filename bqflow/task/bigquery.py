###########################################################################
#
#  Copyright 2023 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  yoy may not use this file except in compliance with the License.
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

"""Handler for "bigquery" task in recipes.

One of the oldest tasks in BQFlow, due for a refactor to get_rows and
put_rows. Please test thouroughly when modifying this module.

Includes:
  bigquery_query - execute a query and write results to table ( future get_rows / put_rows )
   table - write query results to a table
   sheet - write query results to a sheet
   view - write query results to a view
  bigquery_run - execute a query without expected return results
  bigquery_storage - read from storage into a table
  bigquery_values - write explicit values to a table ( future get_rows )
"""

from util.bigquery_api import BigQuery
from util.bigquery_api import query_parameters
from util import data


def bigquery_run(config, task):
  """Execute a query without expected return results."""

  if config.verbose:
    print('RUN QUERY', task['run']['query'])

  BigQuery(config, task['auth']).query_run(
    project_id = config.project,
    query = query_parameters(
      task['run']['query'],
      task['run'].get('parameters')
    ),
    legacy = task['run'].get('legacy', False)
  )


def bigquery_values(config, task):
  """Write explicit values to a table."""

  if config.verbose:
    print('VALUES', task['from']['values'])

  BigQuery(config, task['auth']).rows_to_table(
    project_id = config.project,
    dataset_id = task['to']['dataset'],
    table_id = task['to']['table'],
    rows = data.get_rows(config, task['auth'], task['from']),
    schema = task.get('schema', []),
    skip_rows = 0
  )


def bigquery_query_to_table(config, task):
  """Execute a query and write results to table."""

  if config.verbose:
    print('QUERY TO TABLE', task['to']['table'])

  BigQuery(config, task['auth']).query_to_table(
    config.project,
    task['to']['dataset'],
    task['to']['table'],
    query_parameters(
      task['from']['query'],
      task['from'].get('parameters')
    ),
    disposition=task.get('write_disposition', 'WRITE_TRUNCATE'),
    legacy=task['from'].get('legacy', False)
  )


def bigquery_query_to_table(config, task):
  """Execute a query and write results to sheet."""

  if config.verbose:
    print('QUERY TO SHEET', task['to']['sheet'])

  rows = BigQuery(config, task['auth']).query_to_rows(
    config.project,
    task['from']['dataset'],
    query_parameters(
      task['from']['query'],
      task['from'].get('parameters')
    ),
    legacy=task['from'].get('legacy', False)
  )

  put_rows(
    config=config,
    auth=auth,
    destination = { 'sheets': {
      'auth':task['to'].get('auth', auth),
      'sheet':task['to']['sheet'],
      'tab':task['to']['tab'],
      'range':task['to'].get('range', 'A2'),
      'delete':task['to'].get('delete', False)
    }},
    rows = rows
  )


def bigquery_query_to_view(config, task):
  """Execute a query and write results to view."""

  if config.verbose:
    print('QUERY TO VIEW', task['to']['view'])

  BigQuery(config, task['auth']).query_to_view(
    config.project,
    task['to']['dataset'],
    task['to']['view'],
    query_parameters(
      task['from']['query'],
      task['from'].get('parameters')
    ),
    task['from'].get('legacy', False),
    task['to'].get('replace', False)
  )


def bigquery_storage(config, task):
  """Read from storage into a table."""

  if config.verbose:
    print('STORAGE TO TABLE', task['to']['table'])

  BigQuery(config, task['auth']).storage_to_table(
    config.project,
    task['to']['dataset'],
    task['to']['table'],
    task['from']['bucket'] + ':' + task['from']['path'],
    task.get('schema', []), task.get('skip_rows', 1),
    task.get('structure', 'CSV'),
    task.get('disposition', 'WRITE_TRUNCATE')
  )


def bigquery(config, log, task):

  if 'run' in task:
    bigquery_run(config, task)
  elif 'values' in task['from']:
    bigquery_values(config, task)
  elif 'query' in task['from']:
    if 'table' in task['to']:
      bigquery_query_to_table(config, task)
    elif 'view' in task['to']:
      bigquery_query_to_view(config, task)
    elif 'sheet' in task['to']:
      bigquery_query_to_sheet(config, task)
    else:
      raise NotImplementedError('The bigquery task has no such handler.')
  elif 'bucket' in task['from']:
    bigquery_query(config, task)
  else:
    raise NotImplementedError('The bigquery task has no such handler.')
