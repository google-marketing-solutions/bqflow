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

from bqflow.util.bigquery_api import BigQuery, query_parameters
from bqflow.util.drive import Drive
from bqflow.util.google_api import API_Drive
from bqflow.util.sheets_api import Sheets
from bqflow.util.csv import csv_to_rows, rows_header_trim
from bqflow.util import data


def bigquery_run(config, task):
  """Execute a query without expected return results."""

  if config.verbose:
    print('RUN QUERY')

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


def bigquery_query_to_sheet(config, task):
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

  data.put_rows(
    config=config,
    auth=task['auth'],
    destination = { 'sheets': {
      'auth':task['to'].get('auth', task['auth']),
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


def bigquery_storage_to_table(config, task):
  """Read from storage into a table."""

  if config.verbose:
    print('STORAGE TO TABLE', task['to']['table'])

  BigQuery(config, task['auth']).storage_to_table(
    project_id = config.project,
    dataset_id = task['to']['dataset'],
    table_id = task['to']['table'],
    path = task['from']['bucket'] + ':' + task['from']['path'],
    schema = task['to'].get('schema'),
    header = task.get('header', False),
    structure = task.get('structure', 'CSV'),
    disposition = task.get('disposition', 'WRITE_TRUNCATE')
  )


def bigquery_table_from_sheet(config, task):
  """Create a sheet linked table."""

  if config.verbose:
    print('TABLE FROM SHEET', task['from']['sheet'])

  BigQuery(config, task['auth']).table_from_sheet(
    project_id = config.project,
    dataset_id = task['to']['dataset'],
    table_id = task['to']['table'],
    sheet_url = Sheets(config, task['auth']).sheet_url(task['from']['sheet']),
    sheet_tab = task['from']['tab'],
    sheet_range = task['from'].get('range'),
    schema = task['to'].get('schema'),
    header = task['from'].get('header', False),
    overwrite = task.get('overwrite', False),
    expiration_days = task.get('expiration_days')
  )


def bigquery_table_from_drive(config, task):
  """Download Drive CSV files from file or folder path."""

  if config.verbose:
    print('TABLE FROM DRIVE', task['from']['drive'])

  def _fetch_rows():
    drive_or_folder = Drive(config, task['auth']).file_get(task['from']['drive'])
    if drive_or_folder['mimeType'] == 'application/vnd.google-apps.folder':
      file_ids = [f['id'] for f in API_Drive(config, task['auth'], iterate=True).files().list(
        q="'{}' in parents and (mimeType='text/csv' or mimeType='text/plain') and trashed=false".format(drive_or_folder['id']),
        fields='nextPageToken, files(id)'
      ).execute()]
    else:
      file_ids = [drive_or_folder['driveId']]

    for file_id in file_ids:
      if config.verbose:
        print('.', end='', flush=True)

      rows = csv_to_rows(API_Drive(config, task['auth']).files().get_media(fileId=file_id).execute().decode())
      if task['from']['header']:
        rows = rows_header_trim(rows)
      yield from rows

  BigQuery(config, task['auth']).rows_to_table(
    project_id = config.project,
    dataset_id = task['to']['dataset'],
    table_id = task['to']['table'],
    rows = _fetch_rows(),
    schema = task['to'].get('schema', [])
  )


def bigquery(config, log, task):

  if 'run' in task:
    bigquery_run(config, task)
  elif 'values' in task['from']:
    bigquery_values(config, task)
  elif 'sheet' in task['from']:
    bigquery_table_from_sheet(config, task)
  elif 'drive' in task['from']:
    bigquery_table_from_drive(config, task)
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
    bigquery_storage_to_table(config, task)
  else:
    raise NotImplementedError('The bigquery task has no such handler.')
