## Alert Project

### Goal

To gain basic Pandas skills required for data manipulation and analysis, learn how to apply them in practice.

### Problem statement

There is a mobile application that sends real-time logs with logs in CSV file format.

1. Create a system that can process and analyze logs with logs and catch errors.
2. Create a mechanism to send alerts for the processed logs with two basic alerting rules:  
    2.1. Alert on more than 10 fatal errors in less than one minute  
    2.2. Alert on more than 10 fatal errors in less than one hour for a specific bundle_id.

### Requirements Fulfilled

- code works well, without any bugs/bugs or other failures
- well structured logic and clean code (adherence to formatting standards, project architecture organization, PEP8, etc.)
- solution is able to analyze 100 million records per day
- system is running by a single command via docker-compose
- proper git repository design
- the system constructed in a such way that new rules can be easily added in the future

### Project Structure

```
app/
    main.py              # Entry point
    src/
        alert.py         # Alert data model
        config.py        # Centralized configuration
        processor.py     # Chunk-based log processor
        rules.py         # Alerting rules

data/
    data.csv             # Input dataset

reports/
    report.log           # Output log file

.dockerignore
docker-compose.yaml
Dockerfile
README.md
requirements.txt
```

### Dataset

```alert_project_data```

#### Columns

```
error_code, error_message, severity, log_location, mode, model, graphics, session_id, sdkv, test_mode, flow_id, flow_type, sdk_date, publisher_id, game_id, bundle_id, appv, language, os, adv_id, gdpr, ccpa, country_code, date
```

### Architecture Overview

1. Alerting is implemented via **alert.py** module, which defines a unified **Alert** data class used across the entire system

2. All runtime configuration is centralized in **config.py**:
```
Config(
    CHUNK_SIZE=500000,
    DATE_COLUMN="date",
    ERROR_COLUMN="severity",
    ERROR_LEVEL="Error",
    THRESHOLD=10,
    TIME="1min",
    TIME_ATTR="1h",
    ATTRIBUTE="bundle_id",
    COLUMNS=[...]
)
```

This allows fast changing the wanted parameters without touching logic and reuse of config across all of the system

3. The **LogProcessor** in **processor.py** is responsible for parsing the data logs in an appropriate way:
    - reading files using chunk-based processing
    - normalizing date columns into a unified datetime format
    - passing each chunk to the rule engine for evaluation
    - flushing rule buffers at the end to correctly finalize time windows

This design ensures low memory usage and correctness across chunk boundaries

4. The rule engine in **rules.py** encapsulates all alert-detection logic:
    - **ErrorsRule** is an abstract base class that:
        + unifies check logic
        + delegates assembling part of checking to child rules
    - Concrete rules:
        + **ErrorsPerTimeRule** (default is *per minute*)
        + **ErrorsPerTimeAndAttributeRule** (default is *per hour* and via *bundle_id*)

Design of the engine allows adding new rules by simply subclassing **ErrorsRule**.  
And besides time windows are handled across chunks through an internal buffer that guarantees correct counts and alert generation

5. Main Controller (**main.py**) is the entry point of the application that:
    - Initializes *data path* and *configuration*
    - Initializes required *alerting rules*
    - Creates and runs the **LogProcessor**

### How to Run

1. Input dataset in as:  
```./data/data.csv```

2. Build and run via Docker Compose  
```docker-compose up --build```

### Alerts Output

Alerts are emitted via logging:
- written to ```./reports/report.log```
- printed to ```stdout```