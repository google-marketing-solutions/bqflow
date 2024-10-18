import sys

from behave import given

from bqflow.task.workflow import execute, get_workflow

sys.path.append('features/tasks')

@given('a config file "{config_file}"')
def step_impl(context, config_file):
  config = FakeConfiguration()
  workflow = get_workflow(filepath=f'features/{config_file}')
  context.log = execute(config, workflow, False, None)

@then('the output will include')
def step_impl(context):
  messages = [buf['Description'] for buf in context.log.buffer]
  for row in context.table:
    assert row['message'] in messages


class FakeConfiguration:
  def __init__(self):
    self.verbose = True
    self.days = []
    self.hours = []

  def auth_options(self):
    return ''