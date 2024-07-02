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

import argparse
import textwrap

from bqflow.util.configuration import Configuration
from bqflow.util.drive import Drive
from bqflow.util.google_api import API_Drive
from bqflow.task.workflow import execute, get_workflow

GOOGLE_DRIVE_PREFIX = 'https://drive.google.com/'

def main():

  # load standard parameters
  parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent("""\
      Command line to execute all tasks in a workflow once.

      This script dispatches all the tasks in a JSON workflow to handlers in sequence.
      For each task, it calls a subprocess to execute the JSON instructions, waits
      for the process to complete and dispatches the next task, until all tasks are
      complete or a critical failure ( exception ) is raised.

      If an exception is raised in any task, all following tasks are not executed by design.

      Example: python run.py [path to workflow file]
      Caution: This script does NOT check if the last job finished, potentially causing overruns.
  """))

  parser.add_argument('workflow', help='Path, local or Google Drive link, to workflow json file to run.')
  parser.add_argument('--project', '-p', help='Cloud ID of Google Cloud Project.', default=None)
  parser.add_argument('--key', '-k', help='API Key of Google Cloud Project.', default=None)
  parser.add_argument('--service', '-s', help='Path to SERVICE credentials json file.', default=None)
  parser.add_argument('--client', '-c', help='Path to CLIENT credentials json file.', default=None)
  parser.add_argument('--user', '-u', help='Path to USER credentials json file.', default=None)
  parser.add_argument('--timezone', '-tz', help='Time zone to run schedules on.', default='America/Los_Angeles')
  parser.add_argument('--task', '-t', help='Task number of the task to run starting at 1.', default=None, type=int)
  parser.add_argument('--verbose', '-v', help='Print all the steps as they happen.', action='store_true')
  parser.add_argument('--force', '-force', help='Not used but included for compatiblity with another script.', action='store_true')

  args = parser.parse_args()

  config = Configuration(
    project=args.project,
    service=args.service,
    client=args.client,
    user=args.user,
    key=args.key,
    timezone=args.timezone,
    verbose=args.verbose
  )

  if args.workflow.startswith(GOOGLE_DRIVE_PREFIX):
    auth = 'user' if args.user else 'service'
    file_id = Drive(config, auth).file_id(args.workflow)
    if file_id is None:
      raise FileNotFoundError('Cound not parse Google Drive link, please use the link copy feature to get the URL.')
    file_content = API_Drive(config, auth).files().get_media(fileId=file_id).execute().decode()
    workflow = get_workflow(filecontent=file_content)
  else:
    workflow = get_workflow(filepath=args.workflow)

  execute(config, workflow, args.force, args.task)


if __name__ == '__main__':
  main()
