#!/usr/bin/env python3

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

"""BigQuery helper for performing developer tasks when using BQFlow."""

from __future__ import annotations
from typing import Any

import argparse
import json
import textwrap

from bqflow.util.bigquery_api import BigQuery, get_schema
from bqflow.util.configuration import Configuration
from bqflow.util.csv import csv_to_rows
from bqflow.util.google_api import API_BigQuery


def dashboard_template(schema: list[dict[str, Any]], level: int = 0) -> str:
  """Helper for creating null query used in Looker Studio.

  Generates a query string that when called generates the exact
  schema that is given as an argument.

  Args:
    schema: The JSON schema as used by BigQuery.
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema
    level: Used to track indentation, not passed by caller.

  Returns:
    String containing the query.
  """

  fields = []

  for field in schema:
    if field['type'] == 'RECORD':
      if field['mode'] == 'REPEATED':
        fields.append(
            'ARRAY (SELECT AS STRUCT'
            f' {dashboard_template(field["fields"], level + 2)}) AS'
            f' {field["name"]}'
        )
      else:
        fields.append(
            f'STRUCT ({dashboard_template(field["fields"], level + 2)}\n) AS'
            f' {field["name"]}'
        )
    else:
      fields.append('CAST(NULL AS {type}) AS {name}'.format(**field))

  return (
      ('' if level else 'SELECT ')
      + ('\n' + ' ' * level)
      + (',\n' + ' ' * level).join(fields)
  )


def task_template(auth: str, table: dict[Any]) -> dict[Any]:
  """Grabs view from BigQuery and embeds into a BQFlow task.

  Handles indentation and character escaping. Also replaces
  dataset and project with a parameter field for portability.
  Does not handle comments well, must be terminated by user.

  Args:
    auth: The auth type to code into the task.
    table: The view definition as returned by BigQuery API.
           https://cloud.google.com/bigquery/docs/reference/rest/v2/tables

  Returns:
    Dictionary containing the BQFlow task.
  """

  return {
      'bigquery': {
          'description': (
              f'Create the {table["tableReference"]["tableId"]} view.'
          ),
          'auth': auth,
          'from': {
              'query': table['view']['query'].replace(
                  table['tableReference']['projectId'] + '.', ''
              ),
          },
          'to': {
              'dataset': table['tableReference']['datasetId'],
              'view': table['tableReference']['tableId'],
          },
      }
  }


def tasks_template(auth: str, table: str) -> dict:
  """Creates a BQFlow wrapper around an individual task.

  Args:
    auth: The auth type to code into the task.
    table: The view definition as returned by BigQuery API.
           https://cloud.google.com/bigquery/docs/reference/rest/v2/tables

  Returns:
    Dictionary containing the BQFlow task.
  """

  return {
      'tasks': [
          {
              'dataset': {
                  'description': (
                      'Create the'
                      f' {table["tableReference"]["datasetId"]} dataset.'
                  ),
                  'auth': auth,
                  'dataset': table['tableReference']['datasetId'],
              }
          },
          task_template(auth, table),
      ]
  }


def main():
  # get parameters
  parser = argparse.ArgumentParser(
      formatter_class=argparse.RawDescriptionHelpFormatter,
      description=textwrap.dedent("""\
    Command line to get table schema from BigQuery.

    Helps developers upload data to BigQuery and pull schemas.  These are the
    most common BigQuery tasks when developing solutions.

    Examples:
      Display table schema: python bigquery.py -project [project_id] -s [credentials] -dataset [name] -table [name]
      Create view task: python bigquery.py -p [project_id] -s [credentials] -dataset [name] -task [name] -to_task
      Upload csv table: python bigquery.py -p [project_id] -s [credentials] -dataset [name] -table [name] -from_csv [file] -from_schema [file]
  """),
  )

  parser.add_argument(
      '-user', '-u', help='Path to USER credentials json file.', default=None
  )
  parser.add_argument(
      '-service',
      '-s',
      help='Path to SERVICE credentials json file.',
      default=None,
  )
  parser.add_argument(
      '-project', '-p', help='Name of cloud project.', default=None
  )

  parser.add_argument('-dataset', help='name of BigQuery dataset', default=None)
  parser.add_argument(
      '-table', '-view', help='name of BigQuery table or view', default=None
  )

  parser.add_argument(
      '-from_csv', help='upload to table from CSV file path', default=False
  )
  parser.add_argument(
      '-from_json', help='upload to table from JSON file path', default=False
  )
  parser.add_argument(
      '-from_schema', help='use SCHEMA file when uploading csv', default=False
  )

  parser.add_argument(
      '-to_task',
      action='store_true',
      help='print BQFlow task json',
      default=False,
  )
  parser.add_argument(
      '-to_tasks',
      action='store_true',
      help='print BQFlow workflow json',
      default=False,
  )
  parser.add_argument(
      '-to_dashboard',
      action='store_true',
      help='Generate a dashboard query that can be used as a table placeholder',
      default=False,
  )

  # initialize project

  args = parser.parse_args()
  config = Configuration(
      user=args.user, service=args.service, project=args.project
  )

  auth = 'service' if args.service else 'user'

  if args.to_task:
    table = (
        API_BigQuery(config, auth)
        .tables()
        .get(
            projectId=config.project, datasetId=args.dataset, tableId=args.table
        )
        .execute()
    )

    if table['type'] == 'VIEW':
      print(
          '   ',
          json.dumps(task_template(auth, table), indent=2)
          .replace('\\n', '\n')
          .replace('\n', '\n    '),
      )
    else:
      print(f'ERROR: {args.table} must be a view.')

  elif args.to_tasks:
    table = (
        API_BigQuery(config, auth)
        .tables()
        .get(
            projectId=config.project, datasetId=args.dataset, tableId=args.table
        )
        .execute()
    )
    if table['type'] == 'VIEW':
      print(
          json.dumps(tasks_template(auth, table), indent=2).replace('\\n', '\n')
      )
    else:
      print(f'ERROR: {args.table} must be a view.')

  elif args.from_csv:
    with open(args.from_csv, 'r', encoding='utf-8') as csv_file:
      rows = csv_to_rows(csv_file.read())

      if args.from_schema:
        with open(args.from_schema, 'r', encoding='utf-8') as schema_file:
          schema = json.load(schema_file)

      else:
        rows, schema = get_schema(rows)
        print('DETECTED SCHEMA', json.dumps(schema))
        print('Please run again with the above schema provided.')
        exit()

      BigQuery(config, auth).rows_to_table(
          config.project, args.dataset, args.table, rows, schema
      )

  elif args.from_json:
    with open(args.from_json, 'r', encoding='utf-8') as json_file:
      rows = json.load(json_file)

      if args.from_schema:
        with open(args.from_schema, 'r', encoding='utf-8') as schema_file:
          schema = json.load(schema_file)

      BigQuery(config, auth).json_to_table(
          config.project, args.dataset, args.table, rows, schema
      )

  else:
    schema = BigQuery(config, auth).table_to_schema(
        project_id=config.project, dataset_id=args.dataset, table_id=args.table
    )

    if args.to_dashboard:
      print()
      print(dashboard_template(schema))
      print()

    else:
      print(json.dumps(schema, indent=2))


if __name__ == '__main__':
  main()
