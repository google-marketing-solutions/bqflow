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
from util import csv
from util import data
from util.sheets_api import sheets_clear, sheets_write

def bigquery_run(config, task):
  """Execute a query without expected return results."""

  if config.verbose:
    print('RUN QUERY', task['run']['query'])

  BigQuery(config, task['auth']).run_query(
    config.project,
    query_parameters(
      task['run']['query'],
      task['run'].get('parameters')
    ),
    task['run'].get('legacy', False)
  )


def bigquery_values(config, task):
  """Write explicit values to a table.

  TODO: Replace with get_rows.
  """

  if config.verbose:
    print('VALUES', task['from']['values'])

  rows = data.get_rows(config, task['auth'], task['from'])
  BigQuery(config, task['auth']).rows_to_table(
    config.project,
    task['to']['dataset'],
    task['to']['table'],
    rows,
    task.get('schema', []),
    0
  )


def bigquery_query(config, task):
  """Execute a query and write results to table.

  TODO: Replace with get_rows and put_rows combination.
  """

  if 'table' in task['to']:
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
      disposition=task['write_disposition']
        if 'write_disposition' in task
        else 'WRITE_TRUNCATE',
      legacy=task['from'].get(
        'legacy',
        task['from'].get('useLegacySql', False)
      )  # DEPRECATED: useLegacySql
    )

  elif 'sheet' in task['to']:
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

    # makes sure types are correct in sheet
    rows = csv.rows_to_type(rows)

    sheets_clear(
      config,
      task['to'].get('auth', task['auth']),
      task['to']['sheet'],
      task['to']['tab'],
      task['to'].get('range', 'A2')
    )
    sheets_write(
      config,
      task['to'].get('auth', task['auth']),
      task['to']['sheet'],
      task['to']['tab'],
      task['to'].get('range', 'A2'),
      rows
    )

  elif 'sftp' in task['to']:
    if config.verbose:
      print('QUERY TO SFTP')

    rows = BigQuery(config, task['auth']).query_to_rows(
      config.project,
      task['from']['dataset'],
      query_parameters(
        task['from']['query'],
        task['from'].get('parameters')
      ),
      legacy=task['from'].get('use_legacy_sql', False)
    )

    if rows:
      data.put_rows(config, task['auth'], task['to'], rows)

  else:
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
      task['from'].get(
        'legacy',
        task['from'].get('useLegacySql', False)
      ),  # DEPRECATED: useLegacySql
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

  if 'run' in task and 'query' in task.get('run', {}):
    bigquery_run(config, task)
  elif 'values' in task['from']:
    bigquery_values(config, task)
  elif 'query' in task['from']:
    bigquery_query(config, task)
  elif 'bucket' in task['from']:
    bigquery_query(config, task)
  else:
    raise NotImplementedError('The bigquery task has no such handler.')