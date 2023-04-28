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
import multiprocessing
from typing import List

from util.configuration import Configuration
from util.drive import Drive
from util.google_api import API_Drive

from run import get_workflow, execute


class DriveRunner():

  def __init__(self, config, auth):
    '''Construct a runner factory, providing project and authentication data.

    Args:
     config, required - see: util/configuration.py
     auth, required - either "user" or "service" used to create and/or read the report.

    Returns: None

    '''

    self.config = config
    self.auth = auth


  def execute_workflow(self, files: List) -> None:
    '''Executes workflows in the provided list in sequence.

    Args:
      - files: (list) A list of drive file ids to execute in sequence.

    '''

    for file in files:
      print('{} Starting: {}'.format(multiprocessing.current_process().name, file))
      workflow = get_workflow(filecontent=API_Drive(self.config, self.auth).files().get_media(fileId=file).execute().decode())
      execute(self.config, workflow['tasks'], force=False, instance=None)


  def execute_workflows(self, drive_path: str) -> None:
    folders = self.get_workflows(drive_path) 
    with multiprocessing.Pool(min(multiprocessing.cpu_count(), len(folders))) as executor:
      executor.map(self.execute_workflow, folders)


  def get_workflows(self, drive_path: str) -> List:
    folders = {}

    root = Drive(self.config, self.auth).file_id(args.drive)
    for file in API_Drive(self.config, self.auth, iterate=True).files().list(
      q='"{}" in parents and mimeType="application/json" and trashed=false'.format(root), fields='files(id,name,parents)'
    ).execute():
      key = ''.join(file['parents'])
      folders.setdefault(key, [])
      folders[key].append(file['id'])

    return folders.values()


if __name__ == '__main__':

  # load standard parameters
  parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent("""\
    Reads workflows from a folder in Google Drive and executes them.

    Folder Structure:

    Folder Link = dir, passed as parameter to this script
      - workflow_1 = dir, ran as a single sequence
        - workflow_a.json = file, the sequence of BQFlow steps to run
        - workflow_b.json = file, the sequence of BQFlow steps to run
        - ...
      - workflow_2 = dir, ran as a single sequence
        - workflow_a.json = file, the sequence of BQFlow steps to run
        - workflow_b.json = file, the sequence of BQFlow steps to run
        - ...

    Currently all the workflows will be executed using the credentials passed into this script.

  """))

  parser.add_argument(
    'drive',
     help='Drive folder URL or ID, only workflows from this folder will be read.'
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
    args.project,
    args.service,
    args.client,
    args.user,
    args.key,
    args.timezone,
    args.verbose
  )

  DriveRunner(
    configuration,
    'user' if args.user else 'service'
  ).execute_workflows(args.drive)
