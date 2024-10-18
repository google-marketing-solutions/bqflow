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
import importlib

from typing import Dict, Any

from bqflow.util.configuration import Configuration
from bqflow.util.log import Log
from jinja2 import Environment, FileSystemLoader, select_autoescape

env = Environment(
  loader=FileSystemLoader('.'),
    autoescape=select_autoescape()
)


def get_workflow(filepath:str=None, filecontent:str=None) -> Any:
  """Loads yaml for workflow, replaces newlines, and expands includes.

    Args:
     - filepath: (string) The local file path to the workflow json/yaml file to load.
     - filecontent: (string) The content of thw workflow to sanitize.

    Returns:
      Dictionary of workflow file.

    Raises:
      ValueError when there is a yaml parsing issue.
  """

  try:
    if filecontent is None:
      with open(filepath) as workflow_file:
        template = env.from_string(workflow_file.read())
    return yaml.safe_load(template.render())
  except ValueError as e:
    pos = 0
    for count, line in enumerate(filecontent.splitlines(), 1):
      if pos + len(line) + 1 < e.pos:
        pos += len(line) + 1
      else:
        e.lineno = count
        e.args = ('yaml ERROR: %s LINE: %s CHARACTER: %s ERROR: %s LINE: %s' %
          (filepath, count, e.pos - pos - 1, str(e.msg), line.strip())
        )
        raise


def auth_workflow(config:Configuration, workflow:Any) -> None:
  """Adjust the "auth":"user|service" parameter based on provided credentials.

     Ideally the provided credentials should match the workflow credentials,
     however, when they do not use whatever is provided and hope for the best.

     Time saver, prevents recoding the workflow when using only one credential.
     Also enables remote debugging recipes from drive using different credentials.

     If both or no credentials are provided the workflow is unmodified.

    Args:
      - config: (class) Credentials wrapper.
      - workflow: (Recipe JSON/YAML) The yaml of a workflow.

    Returns:
      Modified workflow with "auth" fields recursively updated.
  """

  def _auth_workflow(auth:str, workflow:Any) -> None:
    """Recursively finds auth in workflow and sets them.

      Args:
        - auth: (string) Either 'service' or 'user'.
        - workflow: (Recipe JSON/YAML) The yaml of a workflow.

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


def is_scheduled(config:Configuration, task:Dict = dict()) -> bool:
  """Wrapper for day_hour_scheduled that returns True if current time zone safe hour is in workflow schedule.

     Used as a helper for any cron job running projects.  Keeping this logic in
     project
     helps avoid time zone detection issues and scheduling discrepencies between
     machines.

    Args:
      * workflow: (Recipe JSON/YAML) The yaml of a workflow.
      * task: ( dictionary / yaml ) The specific task being considered for execution.

    Returns:
      - True if task is scheduled to run current hour, else False.
  """

  if config.days == [] or config.date.strftime('%a') in config.days:
    if config.hours == [] or config.hour in config.hours:
      return True

  return False


def execute(config:Configuration, workflow:Any, force:bool = False, instance:int = None) -> None:
  """Run all the tasks in a project in one sequence.

  Imports and calls each task handler specified in the recpie.
  Passes the Configuration and task yaml to each handler.
  For a full list of tasks see: scripts/*.yaml

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
        config = Configuration(
          client = '[CLIENT CREDENTIALS JSON STRING OR PATH]',
          user = '[USER CREDENTIALS JSON STRING OR PATH]',
          service = '[SERVICE CREDENTIALS JSON STRING OR PATH]',
          project = '[GOOGLE CLOUD PROJECT ID]',
          verbose = True
        ),
        workflow = WORKFLOW,
        force = True
      )
  ```

  Args:
    * config: (class) Credentials wrapper.
    * workflow: (dict) yaml definition of each handler and its parameters.
    * force: (bool) Ignore any schedule settings if true, false by default.
    * instance (int) Sequential index of task to execute (one based index).

  Returns:
    Log the log instance for testing

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
        importlib.import_module(script),
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

  return log