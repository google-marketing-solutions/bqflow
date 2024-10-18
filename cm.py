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

import yaml
import argparse
import textwrap

from bqflow.util.cm_api import get_profile_for_api, report_to_rows, report_clean, report_file, report_schema
from bqflow.util.configuration import Configuration
from bqflow.util.csv import rows_to_type, rows_print
from bqflow.util.google_api import API_DCM


def task_template(auth, report):
  """Helper to create a BQFlow compatible task yaml from CM report."""

  task = {
    "cm_report":{
      "auth":auth,
      "report": {
        "name":report['name'],
        "account":report['accountId'],
        "body":report
      },
      "out":{
        "bigquery":{
          "auth":auth,
          "dataset":"CM360_Dataset",
          "table":"CM360_Report",
        }
      }
    }
  }

  try: del task['cm_report']['report']['body']['lastModifiedTime']
  except KeyError: pass
  try: del task['cm_report']['report']['body']['ownerProfileId']
  except KeyError: pass
  try: del task['cm_report']['report']['body']['accountId']
  except KeyError: pass
  try: del task['cm_report']['report']['body']['fileName']
  except KeyError: pass
  try: del task['cm_report']['report']['body']['name']
  except KeyError: pass
  try: del task['cm_report']['report']['body']['etag']
  except KeyError: pass
  try: del task['cm_report']['report']['body']['id']
  except KeyError: pass

  return task


def main():

  parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent("""\
      Command line to help debug CM reports and build reporting tools.

      Examples:
        To get list of reports: python cm.py --account [id] --list -u [user credentials path]
        To get report: python cm.py --account [id] --report [id] -u [user credentials path]
        To get report files: python cm.py --account [id] --files [id] -u [user credentials path]
        To get report sample: python cm.py --account [id] --sample [id] -u [user credentials path]
        To get report schema: python cm.py --account [id] --schema [id] -u [user credentials path]

    """
  ))

  parser.add_argument('--user', '-u', help='Path to USER credentials json file.', default=None)
  parser.add_argument('--service', '-s', help='Path to SERVICE credentials json file.', default=None)

  parser.add_argument('--account', help='Account ID to use to pull the report.', required=True)
  parser.add_argument('--report', help='Report ID to pull json definition.', default=None)
  parser.add_argument('--schema', help='Report ID to pull schema definition.', default=None)
  parser.add_argument('--sample', help='Report ID to pull sample data.', default=None)
  parser.add_argument('--files', help='Report ID to pull file list.', default=None)
  parser.add_argument('--list', help='List reports.', action='store_true')
  parser.add_argument('--task', help='Report ID to pull task definition.', default=None)

  # initialize project
  args = parser.parse_args()
  config = Configuration(
    user=args.user,
    service=args.service
  )

  auth = 'service' if args.service else 'user'

  profile = get_profile_for_api(config, auth, args.account)
  kwargs = { 'profileId': profile } 

  # get report yaml
  if args.report:
    kwargs['reportId'] = args.report
    report = API_DCM(config, auth).reports().get(**kwargs).execute()
    print(yaml.dump(report, indent=2, sort_keys=True))

  # get task yaml
  elif args.task:
    kwargs['reportId'] = args.task
    report = API_DCM(config, auth).reports().get(**kwargs).execute()
    print(yaml.dump(task_template(auth, report), indent=2, sort_keys=True))

  # get report files
  elif args.files:
    kwargs['reportId'] = args.files
    for rf in API_DCM(config,  auth, iterate=True).reports().files().list(**kwargs).execute():
      print(yaml.dump(rf, indent=2, sort_keys=True))

  # get schema
  elif args.schema:
    filename, report = report_file(config, auth, args.account,
                                   args.schema, None, 10)
    rows = report_to_rows(report)
    rows = report_clean(rows)
    print(yaml.dump(report_schema(next(rows)), indent=2, sort_keys=True))

  # get sample
  elif args.sample:
    filename, report = report_file(config, auth, args.account, args.sample, None, 10)
    rows = report_to_rows(report)
    rows = report_clean(rows)
    rows = rows_to_type(rows)
    for r in rows_print(rows, row_min=0, row_max=20):
      pass

  # get list
  else:
    for report in API_DCM( config, auth, iterate=True).reports().list(**kwargs).execute():
      print(yaml.dump(report, indent=2, sort_keys=True))


if __name__ == '__main__':
  main()
