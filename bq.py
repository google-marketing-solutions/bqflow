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

import json
import textwrap
import argparse

from bqflow.util.bigquery_api import BigQuery, get_schema
from bqflow.util.configuration import Configuration
from bqflow.util.csv import csv_to_rows
from bqflow.util.google_api import API_BigQuery


def dashboard_template(schema, _level=0):
  """ Helper for creating null query used in Looker Studio.

    Generates a query string that when called generates the exact
    schema that is given as an argument.

    Args:
     - schema: (dict) The schema as returned by BigQuery.
     - _level: (int) Used to track indentation, not passed by caller.

    Returns:
      String containing the query.
  """

  fields = []

  for field in schema:
    if field['type'] == 'RECORD':
      if field['mode'] == 'REPEATED':
        fields.append('ARRAY (SELECT AS STRUCT {}) AS {}'.format(dashboard_template(field['fields'], _level + 2), field['name']))
      else:
        fields.append('STRUCT ({}\n) AS {}'.format(dashboard_template(field['fields'], _level + 2), field['name']))
    else:
      fields.append('CAST(NULL AS {type}) AS {name}'.format(**field))

  return ('' if _level else 'SELECT ') +  ('\n'+ ' ' * _level) + (',\n'+ ' ' * _level).join(fields)



def task_template(auth, table):
  """ Grabs view from BigQuery and embeds into a BQFlow task.

    Handles indentation and character escaping. Also replaces
    dataset and project with a paremeter field for portability.
    Does not handle comments well, must be terminated by user.

    Args:
     - table: (dict) The view definition as returned by BigQuery.

    Returns:
      Dictionary containing the BQFlow task.
  """

  task =  {
    "bigquery":{
      "auth":auth,
      "from":{
        "query":table['view']['query'].replace(table['tableReference']['projectId'] + '.', '').replace(table['tableReference']['datasetId'] + '.', '{dataset}.'),
        "legacy":table['view']['useLegacySql'],
        "parameters":{
          "dataset":table['tableReference']['datasetId']
        }
      },
      "to":{
        "dataset":table['tableReference']['datasetId'],
        "view":table['tableReference']['tableId']
      }
    }
  }
  return task


def main():
  # get parameters
  parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent("""\
    Command line to get table schema from BigQuery.

    Helps developers upload data to BigQuery and pull schemas.  These are the
    most common BigQuery tasks when developing solutions.

    Examples:
      Display table schema: `python bigquery.py --project [id] --dataset [name] --table [name] -s [credentials]`
      Create view task: `python bigquery.py --project [id] --dataset [name] --view [name] -s [credentials]`
      Upload csv table: `python bigquery.py --project [id] --dataset [name] --table [name] --csv [file] --schema [file] -s [credentials]`

  """))

  parser.add_argument('--user', '-u', help='Path to USER credentials json file.', default=None)
  parser.add_argument('--service', '-s', help='Path to SERVICE credentials json file.', default=None)
  parser.add_argument('--project', '-p', help='Name of cloud project.', default=None)

  parser.add_argument( '--dataset', help='name of BigQuery dataset', default=None)
  parser.add_argument( '--table', help='name of BigQuery table', default=None)
  parser.add_argument( '--view', help='name of view to turn into BQFlow task', default=None)
  parser.add_argument( '--csv', help='CSV file path', default=None)
  parser.add_argument( '--schema', help='SCHEMA file path', default=None)
  parser.add_argument( '--dashboard', help='Generate a dashboard query to mimic table schema.', default=None)

  # initialize project

  args = parser.parse_args()
  config = Configuration(
    user=args.user,
    service=args.service,
    project=args.project
  )

  auth = 'service' if args.service else 'user'

  schema = json.loads(args.schema) if args.schema else None

  if args.view:
    print(json.dumps(task_template(
     auth,
     API_BigQuery(config, auth).tables().get(projectId=config.project, datasetId=args.dataset, tableId=args.view).execute()
    ), indent=2).replace('\\n', '\n'))

  elif args.csv:

    with open(args.csv, 'r') as csv_file:
      rows = csv_to_rows(csv_file.read())

      if not schema:
        rows, schema = get_schema(rows)
        print('DETECETED SCHEMA', json.dumps(schema))
        print('Please run again with the above schema provided.')
        exit()

      BigQuery(config, auth).rows_to_table(
        config.project,
        args.dataset,
        args.table,
        rows,
        schema
      )

  else:
    schema = BigQuery(config, auth).table_to_schema(
      config.project,
      args.dataset,
      args.table or args.dashboard
    )

    if args.dashboard:
      print()
      print(dashboard_template(schema))
      print()

    else:
      print(json.dumps(schema, indent=2))

if __name__ == '__main__':
  main()
