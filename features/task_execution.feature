Feature: Task execution in BQFlow

    Scenario: JSON is loaded into a Task
        Given a config file "config/tasks.json"
        Then the output will include
            | message                           |
            | hola world!                       |
            | ni hao world!                     |
            | namaste world!                    |
            | greetings! I mean... hello world! |
    
    Scenario: YAML is loaded into a Task
        Given a config file "config/tasks.yaml"
        Then the output will include
            | message                           |
            | hola world!                       |
            | ni hao world!                     |
            | namaste world!                    |
            | greetings! I mean... hello world! |
