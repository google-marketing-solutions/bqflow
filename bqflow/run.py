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
import argparse
import textwrap
import importlib

from util.configuration import Configuration
from util.log import Log
from util.drive import Drive
from util.google_api import API_Drive

GOOGLE_DRIVE_PREFIX = 'https://drive.google.com/'


def get_workflow(filepath=None, filecontent=None):
  """Loads json for workflow, replaces newlines, and expands includes.

    Args:
     - filepath: (string) The local file path to the workflow json file to load.
     - filecontent: (string) The content of thw workflow to sanitize.

    Returns:
      Dictionary of workflow file.

  """

  try:
    if filecontent is None:
      with open(filepath) as workflow_file:
        filecontent = workflow_file.read()
    return json.loads(filecontent.replace('\n', ' '))
  except ValueError as e:
    pos = 0
    for count, line in enumerate(filecontent.splitlines(), 1):
      # do not add newlines, e.pos was run on a version where newlines were removed
      pos += len(line)
      if pos >= e.pos:
        e.lineno = count
        e.pos = pos
        e.args = (
          'JSON ERROR: %s LINE: %s CHARACTER: %s ERROR: %s LINE: %s' %
          (filepath, count, pos - e.pos, str(e.msg), line.strip()),
        )
        raise


def is_scheduled(configuration, task={}):
  """Wrapper for day_hour_scheduled that returns True if current time zone safe hour is in workflow schedule.

     Used as a helper for any cron job running projects.  Keeping this logic in
     project
     helps avoid time zone detection issues and scheduling discrepencies between
     machines.

    Args:
      * workflow: (Recipe JSON) The JSON of a workflow.
      * task: ( dictionary / JSON ) The specific task being considered for execution.

    Returns:
      - True if task is scheduled to run current hour, else False.
  """

  if configuration.days == [] or configuration.date.strftime('%a') in configuration.days:
    if configuration.hours == [] or configuration.hour in configuration.hours:
      return True

  return False


def execute(configuration, workflow, force=False, instance=None):
  """Run all the tasks in a project in one sequence.

  Imports and calls each task handler specified in the recpie.
  Passes the Configuration and task JSON to each handler.
  For a full list of tasks see: scripts/*.json

  Example:
  ```
    from util.configuration import Configuration

    if __name__ == "__main__":
      WORKFLOW = { "tasks":[
        { "hello":{
          "auth":"user",
          "say":"Hello World"
        }},
        { "dataset":{
          "auth":"service",
          "dataset":"Test_Dataset"
        }}
      ]}

      execute(
        config=Configuration(
          client='[CLIENT CREDENTIALS JSON STRING OR PATH]',
          user='[USER CREDENTIALS JSON STRING OR PATH]',
          service='[SERVICE CREDENTIALS JSON STRING OR PATH]',
          project='[GOOGLE CLOUD PROJECT ID]',
          verbose=True
        ),
        workflow=WORKFLOW,
        force=True
      )
  ```

  Args:
    * configuration: (class) Crednetials wrapper.
    * workflow: (dict) JSON definition of each handler and its parameters.
    * force: (bool) Ignore any schedule settings if true, false by default.
    * instance (int) Sequential index of task to execute (one based index).

  Returns:
    None

  Raises:
    All possible exceptions that may occur in a workflow.
  """

  log = Log(configuration, workflow.get('log'))

  for sequence, task in enumerate(workflow['tasks']):
    script, task = next(iter(task.items()))

    if instance and instance != sequence + 1:
      print('SKIPPING TASK #%d: %s - %s' % (sequence + 1, script, task.get('description', '')))
      continue
    else:
      print('RUNNING TASK #%d: %s - %s' % (sequence + 1, script, task.get('description', '')))

    if force or is_scheduled(configuration, task):
      python_callable = getattr(
        importlib.import_module('task.%s' % script),
        script
      )
      python_callable(configuration, log, task)
    else:
      print(
        'Schedule Skipping: add --force to ignore schedule'
      )


def main():

  # load standard parameters
  parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent("""\
      Command line to execute all tasks in a workflow once. ( Common Entry Point )

      This script dispatches all the tasks in a JSON workflow to handlers in sequence.
      For each task, it calls a subprocess to execute the JSON instructions, waits
      for the process to complete and dispatches the next task, until all tasks are
      complete or a critical failure ( exception ) is raised.

      If an exception is raised in any task, all following tasks are not executed by design.

      Example: python run.py [path to workflow file]
      Caution: This script does NOT check if the last job finished, potentially causing overruns.
      Notes:
        - To avoid running the entire script when debugging a single task, the command line
          can easily replace "all" with the name of any "task" in the json.  For example
          python run.py scripts/say_hello.json

        - Or specified further to run only the second hello task:
          python run.py scripts/say_hello.json -t 2

  """))

  parser.add_argument(
    'workflow',
     help='Path, local or Google Drive link, to workflow json file to run.'
  )

  parser.add_argument(
    '--project',
    '-p',
    help='Cloud ID of Google Cloud Project.',
    default=None
  )

  parser.add_argument(
    '--key',
    '-k',
    help='API Key of Google Cloud Project.',
    default=None
  )

  parser.add_argument(
   '--service',
    '-s',
    help='Path to SERVICE credentials json file.',
    default=None
  )

  parser.add_argument(
    '--client',
    '-c',
    help='Path to CLIENT credentials json file.',
    default=None
  )

  parser.add_argument(
    '--user',
    '-u',
     help='Path to USER credentials json file.',
    default=None
  )

  parser.add_argument(
    '--timezone',
    '-tz',
    help='Time zone to run schedules on.',
    default='America/Los_Angeles',
  )

  parser.add_argument(
    '--task',
    '-t',
    help='Task number of the task to run starting at 1.',
    default=None,
    type=int
  )

  parser.add_argument(
    '--verbose',
    '-v',
    help='Print all the steps as they happen.',
    action='store_true'
  )

  parser.add_argument(
    '--force',
    '-force',
    help='Not used but included for compatiblity with another script.',
    action='store_true'
  )

  args = parser.parse_args()

  configuration = Configuration(
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
    file_id = Drive(configuration, auth).file_id(args.workflow)
    if file_id is None:
      raise FileNotFoundError('Cound not parse Google Drive link, please use the link copy feature to get the URL.')
    file_content = API_Drive(configuration, auth).files().get_media(fileId=file_id).execute().decode()
    workflow = get_workflow(filecontent=file_content)
  else:
    workflow = get_workflow(filepath=args.workflow)

  execute(configuration, workflow, args.force, args.task)


if __name__ == '__main__':
  main()
