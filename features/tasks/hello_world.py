import logging

def hello_world(config, log, task):
  for kwargs in task['kwargs']:
    log.write('OK', f'{kwargs["greeting"]} world!')
