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

"""Handler that executes {"dv_reports": {...}} task in recipe JSON.

Allows entire reports to be generated in BigQuery SQL.  The reports
are then run and combined into a single table. In the example below
the reports are sharded across 9 reports in DV360.  This allows any
combination of splitting report data across multiple DV360 reports.

Example:
  { "dv_reports": {
    "description": "Create DV360 reports.",
    "auth": "user",
    "delete": false,
    "create": true,
    "run": false,
    "reports": {
      "bigquery": {
        "query": "
          SELECT
            STRUCT(
              STRUCT(
                'LAST_7_DAYS' AS `range`
              ) AS `dataRange`,
              'CSV' AS `format`,
              false AS `sendNotification`,
              FORMAT('YT Placements - %d', cohort) AS `title`
            ) AS `metadata`,
            STRUCT (
              ARRAY_CONCAT(
                [STRUCT(
                  'FILTER_ADVERTISER' AS `type`, advertiserId AS `value`
                )],
                ARRAY(
                  SELECT AS STRUCT
                    'FILTER_INSERTION_ORDER' AS `type`,
                    insertionOrderId AS `value`
                  FROM UNNEST(insertionOrderIds) AS insertionOrderId
                )
               ) AS `filters`,
              [ 'FILTER_DATE',
                'FILTER_TRUEVIEW_AD_GROUP',
                'FILTER_PLACEMENT_ALL_YOUTUBE_CHANNELS'
              ] AS `groupBys`,
              [ 'METRIC_REVENUE_USD',
                'METRIC_IMPRESSIONS'
              ] AS `metrics`,
              'YOUTUBE' AS `type`
            ) AS `params`,
            STRUCT(
              'ONE_TIME' AS `frequency`
            ) AS `schedule`
          FROM (
            SELECT
              cohort,
              advertiserId,
              ARRAY_AGG(insertionOrderId) AS insertionOrderIds
            FROM (
              SELECT
                advertiserId,
                insertionOrderId,
                MOD(
                  ROW_NUMBER() OVER(ORDER BY insertionOrderId ASC), 9
                ) + 1 AS cohort
              FROM `DV360_Youtube.DV360_InsertionOrders`
            )
            GROUP BY advertiserId, cohort
          )
          ORDER BY cohort
        ",
        "dataset": "DV360_Youtube"
      }
    }
    "results": {
      "bigquery": {
        "auth": "user",
        "dataset": "DV360_Youtube",
        "table": "DV360_Report_1",
        "header": true,
        "schema": [
          { "name": "Report_Day", "type": "DATE" },
          { "name": "AdGroup_Id", "type": "STRING" },
          { "name": "Revenue", "type": "FLOAT" },
          { "name": "Impressions", "type": "FLOAT" }
        ]
      }
    }
  }}
"""

from __future__ import annotations

from typing import Any

# TODO(kenjora): Replace multiple function imports with classes.
from bqflow.util.configuration import Configuration
from bqflow.util.data import get_rows
from bqflow.util.data import put_rows
from bqflow.util.dv_api import report_build
from bqflow.util.dv_api import report_clean
from bqflow.util.dv_api import report_delete
from bqflow.util.dv_api import report_file
from bqflow.util.dv_api import report_run
from bqflow.util.dv_api import report_to_rows
from bqflow.util.log import Log


def dv_reports_combine(
    config: Configuration, task: dict[str, Any], reports: list[dict[str, Any]]
) -> list[Any]:
  """Loop over the reports and return a single row generator for all.

  Args:
    config: All the parameters.
    task: The workflow JSON for this task for parameters.
    reports: The reports JSON resolved from the query.

  Yields:
    DV360 report rows.
  """

  for report in reports:
    filename, filedata = report_file(
        config,
        task['auth'],
        report.get('report_id', None),
        report.get('metadata', {}).get('title', None),
        task.get('timeout', 10),
    )

    # if a report exists
    if filedata:
      if config.verbose:
        print('DBM FILE', filename)

      # clean up the report
      rows = report_to_rows(filedata)
      rows = report_clean(rows, header=False)
      yield from rows


def dv_reports(
    config: Configuration, log: Log, task: dict[str, Any]
) -> None | list[Any]:
  """Handler for the dv_reports task as described in doc string.

  Handles DELETE, then CREATE, followed by RUN, and download.

  Args:
    config: All the parameters.
    log: Required interface logger, not necessarily used.
    task: The workflow JSON for this task for parameters.

  Returns:
    None or DV360 report rows as generator.
  """

  if config.verbose:
    print('DV Reports')

  reports = list(
      get_rows(config, task['auth'], task['reports'], as_object=True)
  )

  # check if report is to be deleted
  if task.get('delete', False):

    if config.verbose:
      print('DBM DELETE')

    for report in reports:
      report_delete(
          config,
          task['auth'],
          report.get('report_id', None),
          report.get('metadata', {}).get('title', None),
      )

  # check if report is to be created
  if task.get('create', False):

    for report in reports:
      if config.verbose:
        print('DBM BUILD', report['metadata']['title'])

      # create the report
      report_build(config, task['auth'], report)

  # check if report is to be run
  if task.get('run', False):

    if config.verbose:
      print('DBM RUN')

    for report in reports:
      # create the report
      report_run(
          config,
          task['auth'],
          report.get('report_id'),
          report.get('metadata', {}).get('title', None),
      )

  # check if report is to be downloaded
  if 'results' in task:
    return put_rows(
        config,
        task['auth'],
        task['results'],
        dv_reports_combine(config, task, reports),
    )
