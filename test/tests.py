#!/usr/bin/env python3

###########################################################################
#
#  Copyright 2024 Google LLC
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

"""Test harness for BQFlow.

Loads JSON workflows from the test folder, executes them and returns a log.
The decorator loads and executes the workflow and injects the log into the test.

When adding tests please follow the following rules:
  - Add JSON workflows to execute a task.
  - Create a test function that matches the JSON workflow file name.
  - Decorate it with IntegrationTests.execute_workflow.
  - For each function, add test logic to check data integrity.
  - Create test classes for each product.

To run this test harness the following ENV variables must be set:
  - BQFLOW_PROJECT - Google Cloud Project name.
  - BQFLOW_USER - Optional path to user credentials to run the test.
  - BQFLOW_SERVICE - Optional path to service credentials to run the test.
  - BQFLOW_KEY - Optional Google Cloud Project API key.
  - BQFLOW_TIMEZONE - Optional, defaults to 'America/Los_Angeles'.
  - BQFLOW_VERBOSE - Set to True or False, to toggle screen prints.

Example:
  export BQFLOW_PROJECT='gtech-kenjora'
  export BQFLOW_USER='/Users/kenjora/user.json'
  python -m unittest test.tests.CM360Tests.test_cm_reports
"""

from __future__ import annotations

import functools
import os
from typing import Any, Callable
import unittest

from bqflow.task.workflow import execute
from bqflow.task.workflow import get_workflow
from bqflow.util.bigquery_api import BigQuery
from bqflow.util.configuration import Configuration
from bqflow.util.log import Log


class IntegrationTests(unittest.TestCase):
  """Base integration test class that adds universal helpers and decorators."""

  def execute_workflow(
      func: Callable[..., Any], run: bool = True
  ) -> Callable[..., Any]:
    """Decorator that runs the workflow based on function name.

      Args:
        func - the test function to decorate.
        run - if False, skip execution of workflow and empty log.

      Returns:
        Executed test function.
    """

    @functools.wraps(func)
    def wrapper(self) -> Any:
      """Wrapper that gets the function name and calls the original function.

      Load the workflow from the JSON file which mimics the function name.
      Execute the workflow, forcing all tasks to run regardless of schedule.
      Loop through the log and ensure every task finished correctly.

      Returns:
        A call to the function with the workflow and log of the execution.
      """
      workflow = get_workflow(
          filepath=f'test/{func.__name__.replace("test_", "")}.json'
      )

      if run:
        log = execute(self.config, workflow, force=True)
      else:
        log = Log(self.config, self.auth)

      for log_entry in log.buffer:
        self.assertEqual(
            log_entry['Status'],
            'OK',
            f'{log_entry["Description"]} - {log_entry["Parameters"]}',
        )

      return func(self, workflow, log)

    return wrapper

  def setUp(self) -> None:
    """Initialize any credentials and time units for the test, read from ENV."""

    self.assertIsNotNone(
        os.environ.get('BQFLOW_PROJECT'),
        msg='No env variable, run: export BQFLOW_PROJECT="GCP PROJECT"',
    )

    self.assertIsNotNone(
        os.environ.get('BQFLOW_USER') or os.environ.get('BQFLOW_SERVICE'),
        msg='No env variable, run: export BQFLOW_USER="CREDENTIALS PATH"',
    )

    self.config = Configuration(
        project=os.environ.get('BQFLOW_PROJECT'),
        service=os.environ.get('BQFLOW_SERVICE'),
        user=os.environ.get('BQFLOW_USER'),
        key=os.environ.get('BQFLOW_KEY'),
        timezone=os.environ.get('BQFLOW_TIMEZONE', 'America/Los_Angeles'),
        verbose=os.environ.get('BQFLOW_VERBOSE', 'false').lower() == 'true',
    )

    self.auth = 'service' if os.environ.get('BQFLOW_KEY') else 'user'


class CM360Tests(IntegrationTests):
  """Class to group all the CM360 tests. Inherits setup and decorator."""

  @IntegrationTests.execute_workflow
  def test_cm_api(self, workflow, log: Log) -> None:
    """Run a test on the CM360 API."""
    # TODO(kenjora): Add test logic to check data integrity.
    pass

  @IntegrationTests.execute_workflow
  def test_cm_reports(self, workflow, log: Log) -> None:
    """Run a test on the CM360 Reports."""
    # TODO(kenjora): Add test logic to check data integrity.
    pass


class SA360Tests(IntegrationTests):
  """Class to group all the SA360 tests. Inherits setup and decorator."""

  @IntegrationTests.execute_workflow
  def test_sa_api(self, workflow, log: Log) -> None:
    """Run a test on the SA360 API."""
    # TODO(kenjora): Add test logic to check data integrity.
    pass


class DV360Tests(IntegrationTests):
  """Class to group all the DV360 tests. Inherits setup and decorator."""

  @IntegrationTests.execute_workflow
  def test_dv_api(self, workflow, log: Log) -> None:
    """Run a test on the DV360 API."""
    # TODO(kenjora): Add test logic to check data integrity.
    pass

  @IntegrationTests.execute_workflow
  def test_dv_report(self, workflow, log: Log) -> None:
    """Run a test on the DV360 Report (hard coded)."""

    # check expected report count
    reports_bq = len(
        list(
            BigQuery(self.config, self.auth).table_to_rows(
                project_id=self.config.project,
                dataset_id=workflow['tasks'][1]['bigquery']['to']['dataset'],
                table_id=workflow['tasks'][1]['bigquery']['to']['view'],
                as_object=True,
            )
        )
    )
    self.assertEqual(reports_bq, 3)

    # check expected advertisers
    advertisers_wf = workflow['tasks'][1]['bigquery']['from']['parameters'][
        'advertisers'
    ]
    advertisers_bq = [
        column['AdvertiserId']
        for column in BigQuery(self.config, self.auth).table_to_rows(
            project_id=self.config.project,
            dataset_id=workflow['tasks'][3]['dv_reports']['results'][
                'bigquery'
            ]['dataset'],
            table_id=workflow['tasks'][3]['dv_reports']['results']['bigquery'][
                'table'
            ],
            as_object=True,
        )
    ]
    self.assertCountEqual(advertisers_wf, advertisers_bq)

  @IntegrationTests.execute_workflow
  def test_dv_reports(self, workflow, log: Log) -> None:
    """Run a test on the DV360 Report (dynamic)."""
    # TODO(kenjora): Add test logic to check data integrity.
    pass


class GA4Tests(IntegrationTests):
  """Class to group all the GA4 tests. Inherits setup and decorator."""

  @IntegrationTests.execute_workflow
  def test_ga4_api(self, workflow, log: Log) -> None:
    """Run a test on the GA4 API."""
    # TODO(kenjora): Add test logic to check data integrity.
    pass

  @IntegrationTests.execute_workflow
  def test_ga4_reports(self, workflow, log: Log) -> None:
    """Run a test on the GA4 queries."""
    # TODO(kenjora): Add test logic to check data integrity.
    pass


class DriveTests(IntegrationTests):
  """Class to group all the Drive tests. Inherits setup and decorator."""

  @IntegrationTests.execute_workflow
  def test_drive_csv(self, workflow, log: Log) -> None:
    """Run a test on the Drive API."""
    # TODO(kenjora): Add test logic to check data integrity.
    pass


class GADSTests(IntegrationTests):
  """Class to group all the GADS tests. Inherits setup and decorator."""

  @IntegrationTests.execute_workflow
  def test_gads_reports(self, workflow, log: Log) -> None:
    """Run a test on the Google Ads API."""
    # TODO(kenjora): Add test logic to check data integrity.
    pass


class VertexTests(IntegrationTests):
  """Class to group all the Vertex tests. Inherits setup and decorator."""

  @IntegrationTests.execute_workflow
  def test_vertex(self, workflow, log: Log) -> None:
    """Run a test on the Vertex AI API."""
    # TODO(kenjora): Add test logic to check data integrity.
    pass


class GeneralTests(IntegrationTests):
  """Class to group all the General tests. Inherits setup and decorator."""

  @IntegrationTests.execute_workflow
  def test_merge(self, workflow, log: Log) -> None:
    """Run a test on the BigQuery merge data."""
    # TODO(kenjora): Add test logic to check data integrity.
    pass

  @IntegrationTests.execute_workflow
  def test_log(self, workflow, log: Log) -> None:
    """Run a test on the BQFlow BigQuery logging."""
    # TODO(kenjora): Add test logic to check data integrity.
    pass


if __name__ == '__main__':
  unittest.main()
