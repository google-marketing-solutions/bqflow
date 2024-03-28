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

"""Helper class for dealing with drive API.

These functions help reduce code writing up stream. Ultimately the developer
should choose the restful API where possible. Only add functions here to reduce
code size upstream.

Sources:
  - https://developers.google.com/drive/api/v3/manage-uploads
  - https://developers.google.com/drive/api/v3/reference/about#methods
"""

from collections.abc import Mapping, Iterator
from io import BytesIO
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError
import mimetypes
import re
from typing import Any


from bqflow.util.google_api import API_Drive
from bqflow.util.configuration import Configuration
from bqflow.util import misc


DRIVE_CHUNKSIZE = misc.memory_scale(maximum = 200 * 1024**3, multiple = 256 * 1024)


class Drive():
  """Implement file handling helpers, mainly lookup by name instead of id.

  These are helpers to reduce error handling and file searching overhead.

  drive = Drive(config, task['auth'])
  file = drive.file_find(**task['kwargs'])

  Handles all serialization, pagination, and type convesion.
  """

  def __init__(self, config: Configuration, auth: str) -> None:
    """Construct a drive factory, providing project and authentication data.

    Args:
     config: see util/configuration.py
     auth: either 'user' or 'service' used to create and/or read the report.

    Returns: None
    """

    self.config = config
    self.auth = auth

  def about(self, fields: str = 'importFormats') -> Mapping[str, Any]:
    """Helper for determining mime type of upload."""
    return API_Drive(
      config = self.config,
      auth = self.auth
    ).about().get(fields = fields).execute()

  def file_id(self, url_or_name: str) -> str:
    """Returns the file id given a Name, URL, or file id.

    Used to make looking up documents simpler for the entire system.

    Args:
      url_or_name: the url, name, or ID of the sheet.

    Returns:
      id of the sheet if found otherwise None
    """

    if url_or_name.startswith('https://drive.google.com/open?id='):
      return url_or_name.split('?id=', 1)[-1]

    elif url_or_name.startswith('https://drive.google.com/'):
      m = re.search(
        '\/(?:drive\/folders|file\/d)\/([a-zA-Z0-9-_]+)(?:\/.*)?$',
        url_or_name
      )
      if m:
        return m.group(1)

    elif url_or_name.startswith('https://docs.google.com/'):
      m = re.search(
        '^(?:https:\/\/docs.google.com\/\w+\/d\/)([a-zA-Z0-9-_]+)(?:\/.*)?$',
        url_or_name
      )
      if m:
        return m.group(1)

    elif url_or_name.startswith('https://datastudio.google.com/'):
      m = re.search(
        '^(?:https:\/\/datastudio.google.com\/c\/\w+\/)([a-zA-Z0-9-_]+)(?:\/.*)?$',
        url_or_name
      )
      if m:
        return m.group(1)

    # check if name given convert to ID 'Some Document'
    else:
      document = self.file_find(url_or_name)
      if document:
        return document['id']

        # check if just ID given, '1uN9tnb-DZ9zZflZsoW4_34sf34tw3ff'
      else:
        m = re.search('^([a-zA-Z0-9-_]+)$', url_or_name)
        if m:
          return m.group(1)

    # probably a mangled id or name does not exist
    if self.config.verbose:
      print('DOCUMENT DOES NOT EXIST', url_or_name)
    return None

  def file_get(self, url_or_name: str) -> Mapping[str, Any]:
    """Helper for getting a file by url, name, or id."""
    drive_id = self.file_id(url_or_name)
    return API_Drive(
      config = self.config,
      auth = self.auth
    ).files().get(fileId = drive_id).execute()

  def file_exists(self, name_or_url: str) -> bool:
    """Helper for checking file exists by url, name, or id."""
    drive_id = self.file_id(name_or_url)
    if drive_id:
      try:
        API_Drive(
          config = self.config,
          auth = self.auth
        ).files().get(fileId = drive_id).execute()
        return True
      except HttpError:
        return False
    return False

  def file_list(
    self,
    parent: str = None
  ) -> Iterator[Mapping[str, Any]]:
    """Helper for listing existing files."""
    query = 'trashed = false'
    if parent:
      query = '%s and "%s" in parents' % (query, parent)
    yield from API_Drive(
      config = self.config,
      auth = self.auth,
      iterate = True
    ).files().list(q = query).execute()

  def file_find(
    self,
    name: str,
    parent: str = None
  ) -> Mapping[str, Any]:
    """Helper for finding existing file by name only."""
    query = 'trashed = false and name = "%s"' % name
    if parent:
      query = '%s and "%s" in parents' % (query, parent)

    try:
      return next(API_Drive(
        config = self.config,
        auth = self.auth,
        iterate = True
      ).files().list(q = query).execute())
    except StopIteration:
      return None

  def file_delete(self, name: str) -> bool:
    """Helper for deleting a file if it exists. Signals existence."""
    drive_id = self.file_id(name)

    if drive_id:
      API_Drive(
        config = self.config,
        auth = self.auth
      ).files().delete(fileId = drive_id).execute()
      return True

    return False

  def file_create(
    self,
    name: str,
    data: any,
    parent: str = None,
    mimetype: str = None,
    convert: bool = False,
    overwrite: bool = False
  ) -> Mapping[str, Any]:
    """Checks if file with name already exists ( outside of trash ) and

    if not, uploads the file.  Determines filetype based on filename extension
    and attempts to map to Google native such as Docs, Sheets, Slides, etc...

    For example:
      file_create('user','Sample Document','sample.txt',BytesIO('data'))
      Creates a Google Document object in the user's drive.

      file_Create('user','Sample Sheet','sample.csv',BytesIO('c1,c2\nr1,r1\n'))
      Creates a Google Sheet object in the user's drive.

    See: https://developers.google.com/drive/api/v3/manage-uploads

    Args:
      name: name of file to create, used as key to check if file exists
      data: any file like object that can be read from
      mimetype: explicitly specify the file type, auto detect if None.
      convert: attempt to convert to Google Drive Format like a doc.
      parent: the Google Drive ID to upload the file into
      overwrite: force the file writte even if it exists

    Returns:
      JSON specification of file created or existing.
  """

    if not overwrite:
      # attempt to find the file by name ( not in trash )
      drive_file = self.file_find(name, parent)

      # if file exists, return it, prevents obliterating user changes
      if drive_file:
        if self.config.verbose:
          print(f'Drive: File {name} exists.')
        return drive_file

    # if file does not exist, create it
    if self.config.verbose:
      print(f'Drive: Creating file: {name}')

    # determine type
    if not mimetype:
      mimetype = mimetypes.guess_type(name, strict = False)[0]
      if convert:
        # drive mime attempts to map to a native Google format
        mimetype = self.about(
         'importFormats'
        )['importFormats'].get(mimetype, mimetype)[0]

      if self.config.verbose:
        print('Detected Mime:', mimetype)

    # construct upload object, and stream upload in chunks
    body = {
      'name': name,
      'parents': [parent] if parent else [],
      'mimeType': mimetype
    }

    media = MediaIoBaseUpload(
      BytesIO(data or ' '),  # if data is empty BAD REQUEST error occurs
      mimetype = mimetype,
      chunksize = DRIVE_CHUNKSIZE,
      resumable = True
    )

    drive_file = API_Drive(
      config = self.config,
      auth = self.auth
    ).files().create(body = body, media_body = media, fields = 'id').execute()

    return drive_file

  def file_copy(
    self,
    source_name: str,
    destination_name: str
  ) -> Mapping[str, Any]:
    """Copies a file if it does not exists, otherwise returns existing."""

    destination_id = self.file_id(destination_name)

    if destination_id:
      if self.config.verbose:
        print('Drive: File exists.')
      return API_Drive(
        config = self.config,
        auth = self.auth
      ).files().get(fileId = destination_id).execute()

    else:
      source_id = self.file_id(source_name)

      if source_id:
        body = {'visibility': 'PRIVATE', 'name': destination_name}
        return API_Drive(
          config = self.config,
          auth = self.auth
        ).files().copy(fileId = source_id, body = body).execute()
      else:
        return None

  def folder_create(
    self,
    name: str,
    parent: str = None
  ) -> Mapping[str, Any]:
    """Helper for creating a folder."""

    body = {
      'name': name,
      'parents': [parent] if parent else [],
      'mimeType': 'application/vnd.google-apps.folder'
    }
    return API_Drive(
      config = self.config,
      auth = self.auth
    ).files().create(body = body, fields = 'id').execute()
