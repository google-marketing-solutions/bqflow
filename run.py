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

from bqflow.util.configuration import Configuration
from bqflow.util.log import Log
from bqflow.util.drive import Drive
from bqflow.util.google_api import API_Drive

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
      if pos + len(line) + 1 < e.pos:
        pos += len(line) + 1
      else:
        e.lineno = count
        e.args = (
          'JSON ERROR: %s LINE: %s CHARACTER: %s ERROR: %s LINE: %s' %
          (filepath, count, e.pos - pos - 1, str(e.msg), line.strip()),
        )
        raise


def auth_workflow(config, workflow):
  """Adjust the "auth":"user|service" parameter based on provided credentials.

     Ideally the provided credentials should match the workflow credentials,
     however, when they do not use whatever is provided and hope for the best.

     Time saver, prevents recoding the workflow when using only one credential.
     Also enables remote debugging recipes from drive using different credentials.

     If both or no credentials are provided the workflow is unmodified.

    Args:
      * config: (class) Credentials wrapper.
      * workflow: (Recipe JSON) The JSON of a workflow.

    Returns:
      Modified workflow with "auth" fields recursively updated.
  """

  def _auth_workflow(auth, workflow):
    """Recursively finds auth in workflow and sets them.

      Args:
        * auth: (string) Either 'service' or 'user'.
        * workflow: (Recipe JSON) The JSON of a workflow.

      Returns:
        Modified workflow with "auth" fields recursively updated.
    """

    if isinstance(workflow, dict):
      if 'auth' in workflow:
        workflow['auth'] = auth
      for key, value in workflow.items():
        _auth_workflow(auth, value)
    elif isinstance(workflow, (list, tuple)):
      for value in workflow:
        _auth_workflow(auth, value)

  if config.auth_options() == 'SERVICE':
    _auth_workflow('service', workflow)

  elif config.auth_options() == 'USER':
    _auth_workflow('user', workflow)


def is_scheduled(config, task={}):
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

  if config.days == [] or config.date.strftime('%a') in config.days:
    if config.hours == [] or config.hour in config.hours:
      return True

  return False


def execute(config, workflow, force=False, instance=None):
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
    * config: (class) Credentials wrapper.
    * workflow: (dict) JSON definition of each handler and its parameters.
    * force: (bool) Ignore any schedule settings if true, false by default.
    * instance (int) Sequential index of task to execute (one based index).

  Returns:
    None

  Raises:
    All possible exceptions that may occur in a workflow.
  """

  auth_workflow(config, workflow)

  log = Log(config, workflow.get('log'))

  for sequence, task in enumerate(workflow['tasks'], 1):
    script, task = next(iter(task.items()))

    if instance and instance != sequence:
      print('SKIPPING TASK #%d: %s - %s' % (sequence, script, task.get('description', '')))
      continue
    else:
      print('RUNNING TASK #%d: %s - %s' % (sequence, script, task.get('description', '')))

    if force or is_scheduled(config, task):
      python_callable = getattr(
        importlib.import_module('bqflow.task.%s' % script),
        script
      )
      task['sequence'] = sequence
      try:
        python_callable(config, log, task)
        log.write('OK', 'TASK #{} COMPLETE: {} - {}'.format(sequence, script, task.get('description', '')))
      except Exception as e:
        log.write('ERROR', 'TASK #{} FAILED: {} - {} WITH ERROR: {} {}'.format(sequence, script, task.get('description', ''), e.__class__.__name__, str(e)))
        raise

    else:
      print('Schedule Skipping: add --force to ignore schedule')


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
