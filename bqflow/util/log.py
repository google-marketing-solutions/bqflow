import datetime

from util.bigquery_api import BigQuery
from util.data import put_rows

LOG_HEADER = '{Timing: <30} {Status: <10} {Description: <50} {Parameters: <50}'
LOG_SCHEMA = [
  { 'name': 'Timing', 'type': 'TIMESTAMP', 'mode': 'REQUIRED' },
  { 'name': 'Status', 'type': 'STRING', 'mode': 'REQUIRED' },
  { 'name': 'Description', 'type': 'STRING', 'mode': 'NULLABLE' },
  { 'name': 'Parameters', 'type': 'RECORD', 'mode': 'REPEATED', 'fields': [
    { 'name': 'Key', 'type': 'STRING', 'mode': 'NULLABLE' },
    { 'name': 'Value', 'type': 'STRING', 'mode': 'NULLABLE' },
  ]}
]

class Log():

  def __init__(
    self,
    config,
    destination
  ):
    """Used in BQFlow scripts to log application layer.

    Python logging is not used, it is purposefully left out to avoid
    using BQFlow logs for debugging, that should be done locally.

    Args:
      * config: (class) Credentials wrapper, see util/configuration.py.
      * destination: (dict) Currently only bigquery and stdout is supported if None.

    """

    self.config = config
    self.destination = destination or {}
    self.buffer = []

    if self.config.verbose:
      print('CREATING LOG')   

    if 'bigquery' in self.destination:
      self.destination['bigquery']['format'] = 'JSON'

      BigQuery(
        config,
        self.destination['bigquery']['auth'],
      ).datasets_create(
        project_id=config.project,
        dataset_id=self.destination['bigquery']['dataset'],
        expiration_days=self.destination.get('expiration_days')
      )

      BigQuery(
        self.config,
        self.destination['bigquery']['auth'],
      ).table_create(
        project_id=self.config.project,
        dataset_id=self.destination['bigquery']['dataset'],
        table_id=self.destination['bigquery']['table'],
        schema=LOG_SCHEMA,
        overwrite=False
      )


  def __del__(self):
    ''' Commit log buffer to destination as destructor.
    '''

    if self.config.verbose:
      print('WRITING LOG', self.buffer)   

    if 'bigquery' in self.destination:
      put_rows(
        config=self.config,
        auth=self.destination['bigquery']['auth'],
        destination=self.destination,
        rows=self.buffer
      )

    else:
      print()
      print('Log')
      print(LOG_HEADER.format(**{
        'Timing':'Timing',
        'Status':'Status',
        'Description':'Description',
        'Parameters':'Parameters',
      }))
      print(LOG_HEADER.format(**{
        'Timing':'-' * 30,
        'Status':'-' * 10,
        'Description':'-' * 50,
        'Parameters':'-' * 50
      }))
      for entry in self.buffer:
        entry['Parameters'] = ', '.join('{Key}:{Value}'.format(**p) for p in entry['Parameters'])
        print(LOG_HEADER.format(**entry))
      print()


  def write(self, status, description, parameters):
    """Writes to the local buffer, will be writen to destination in destructor.
  
    Args:
      status (string): typically 'OK' or 'ERROR'
      description (string): user defined context string
      parameters (dict): an object of { key:value } pairs.
  
    Returns:
      None
  
    Raises:
      Errors propegated from BigQuery.
  
    """

    self.buffer.append({
      'Timing':datetime.datetime.utcnow().isoformat(),
      'Status': status,
      'Description': description,
      'Parameters': parameters
    })
