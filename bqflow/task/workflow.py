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

"""Handles workflow loading, execution, and scheduling."""

from __future__ import annotations
from typing import Any

import importlib
import json

from bqflow.util.configuration import Configuration
from bqflow.util.log import Log


def get_workflow(filepath: str = None, filecontent: str = None) -> dict[str, Any]:
  """Loads json for workflow, replaces newlines, and expands includes.

  Args:
    filepath: The local file path to the workflow JSON file.
    filecontent: The content of the workflow to sanitize.

  Returns:
    Dictionary of workflow file.
    https://github.com/google-marketing-solutions/bqflow/wiki/DV360-API-Example#workflow

  Raises:
    ValueError when there is a JSON parsing issue.
  """

  try:
    if filecontent is None:
      with open(filepath, 'r', encoding='UTF-8') as workflow_file:
        filecontent = workflow_file.read()
    return json.loads(filecontent.replace('\n', ' '))
  except ValueError as e:
    pos = 0
    for count, line in enumerate(filecontent.splitlines(), 1):
      if pos + len(line) + 1 < e.pos:
        pos += len(line) + 1
      else:
        e.lineno = count
        e.args = [(
            f'JSON ERROR: {filepath} LINE: {count} CHARACTER:'
            f' {e.pos - pos - 1} ERROR: {str(e.msg)} LINE: {line.strip()}'
        )]
        raise


def auth_workflow(config: Configuration, workflow: dict[str, Any]) -> None:
  """Adjust the "auth":"user|service" parameter based on provided credentials.

   Ideally the provided credentials should match the workflow credentials,
   however, when they do not use whatever is provided and hope for the best.

   Time saver, prevents recoding the workflow when using only one credential.
   Also enables remote debugging recipes from drive using different
   credentials.

   If both or no credentials are provided the workflow is unmodified.

  Args:
    config: Credentials wrapper.
    workflow: The JSON of a workflow.
    https://github.com/google-marketing-solutions/bqflow/wiki/DV360-API-Example#workflow

  Returns:
    Modified workflow with "auth" fields recursively updated.
  """

  def _auth_workflow(auth: str, workflow: dict[str, Any]) -> None:
    """Recursively finds auth in workflow and sets them.

    Args:
      auth: Either 'service' or 'user'.
      workflow: The JSON of a workflow.
      https://github.com/google-marketing-solutions/bqflow/wiki/DV360-API-Example#workflow

    Returns:
      None, modifies workflow in place with "auth" fields recursively updated.
    """

    if isinstance(workflow, dict):
      if 'auth' in workflow:
        workflow['auth'] = auth
      for value in workflow.values():
        _auth_workflow(auth, value)
    elif isinstance(workflow, (list, tuple)):
      for value in workflow:
        _auth_workflow(auth, value)

  if config.auth_options() == 'SERVICE':
    _auth_workflow('service', workflow)

  elif config.auth_options() == 'USER':
    _auth_workflow('user', workflow)


def is_scheduled(config: Configuration, task: dict[str, Any]) -> bool:
  """Check if workflow and task are scheduled to execute.

   Used as a helper for any cron job running projects.  Keeping this logic in
   project helps avoid time zone detection issues and scheduling discrepancies
   between machines.

  Args:
    config: The global parameters.
    task: The specific task being considered for execution from the workflow.
          https://github.com/google-marketing-solutions/bqflow/wiki/DV360-API-Example#workflow

  Returns:
    True if task is scheduled to run current hour, else False.
  """

  if config.days and config.date.strftime('%a') not in config.days:
    return False
  elif config.hours and config.hour not in config.hours:
    return False
  elif task.get('days') and config.date.strftime('%a') not in task['days']:
    return False
  elif task.get('hours') and config.hour not in task['hours']:
    return False
  else:
    return True


def execute(
    config: Configuration,
    workflow: dict[str, Any],
    force: bool = False,
    instance: int = None,
) -> None:
  """Run all the tasks in a project in one sequence.

  Imports and calls each task handler specified in the recpie.
  Passes the Configuration and task JSON to each handler.
  For a full list of tasks see: scripts/*.json

  Args:
    config: Credentials wrapper.
    workflow: JSON definition of each handler and its parameters.
              https://github.com/google-marketing-solutions/bqflow/wiki/DV360-API-Example#workflow
    force: Ignore any schedule settings if true, false by default.
    instance: Sequential index of task to execute (one based index).

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
  """

  auth_workflow(config, workflow)

  log = Log(config, workflow.get('log'))

  for sequence, task in enumerate(workflow['tasks'], 1):
    script, task = next(iter(task.items()))

    if instance and instance != sequence:
      print(
          f'SKIPPING TASK #{sequence}: {script} - {task.get("description", "")}'
      )
      continue
    else:
      print(
          f'RUNNING TASK #{sequence}: {script} - {task.get("description", "")}'
      )

    if force or is_scheduled(config, task):
      python_callable = getattr(
          importlib.import_module(f'bqflow.task.{script}'), script
      )
      task['sequence'] = sequence
      try:
        python_callable(config, log, task)
        log.write(
            'OK',
            'TASK #{} COMPLETE: {} - {}'.format(
                sequence, script, task.get('description', '')
            ),
        )
      except Exception as e:
        log.write(
            'ERROR',
            'TASK #{} FAILED: {} - {} WITH ERROR: {} {}'.format(
                sequence,
                script,
                task.get('description', ''),
                e.__class__.__name__,
                str(e),
            ),
        )
        raise

    else:
      print('Schedule Skipping: add --force to ignore schedule')

  return log
