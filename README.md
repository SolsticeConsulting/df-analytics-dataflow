# Meijer Digital Shopping - Conversational Analytics

The following library and accompanying documentation were created by the Kin + 
Carta delivery team as part of the delivery of the Meijer Digital Assistant.  

## Contents
This Pipeline and Library consist of the following:
- Pipeline Code
    - `dialogflow_analytic_batch_parser.py`
    - `dialogflow_analytic_stream_parser.py`
- `ConvoAnalytics` Library
    - `BeamFunctions.py`: Encapsulation Class that provides access to the 
    functions in this library within the pipeline code.  Contents are ApacheBeam
    DoFn subclassed. 
    - `DialogFlowTypes.py`: Dataclass and helper functions for converting raw 
    Dialogflow logs from Cloud Logging into a usable object.
    - `AnalyticRecord.py`: The library responsible for converting Dialogflow logs
    into final Analytic Records.
    
## Technology
This library and pipeline code utilizes Python 3.6 or newer with Apache Beam and 
the Google-Cloud-SDK.

## Architecture
This is a generic overview of the pipeline logic and how it flows into Azure. 
![Analytic Pipeline Chart]
(docs/images/streaming_pipeline.png)

The pipeline defined within compiles into this graph.
![Dataflow Pipeline Graph]
(docs/images/dataflow_pipeline.jpg)

## Pre-Requisites
In order to use this code you will need the following:
- Google Cloud SDK (Download Here)[https://cloud.google.com/sdk]
- Python 3 with the venv module
- Access to the GCP Console with permissions to access Storage and Dataflow
    - Note: These permissions are already setup on the `azure-pipeline-service-account` 
    service account.  See the authentication section below to understand how to use
    this account for your deployments.
    - todo: add detail for exactly which permissions are needed

## To get started with this code
1. Download this repository
2. From the repository root, setup your dev environment
    ```shell script
    python3 -m venv venv
    source venv/bin/activate
    pip3 install -r requirements.txt       
    ```
    a. This will create a virtual environment in the directory `venv`, activate it and 
    install the project dependencies into the environment
3. Create a `/secrets` directory in the repository root
4. Log onto the GCP console and download a JSON keyfile for your service account.  Save 
this file as `credentials-<env>.json` under the secrets folder.  (eg. `credentials-dev.json`)
5. Activate the credentials and authenticate to the GCP SDK
    ```shell script
    export GOOGLE_APPLICATION_CREDENTIALS=secrets/credentials-<env>.json
    gcloud auth activate-service-account --key-file=secrets/credentials-<env>.json
    ```

## The Batch Pipeline
The batch pipeline defined in `dialogflow_analytic_batch_parser.py` consumes all files from the upstream
cloud logging sink bucket and produces analytic records for all sessions found.  This process currently 
targets all available files though it can be tailored for particular windows of time.  Note:
If you do run any partial batch process, you will need to run it for all records after the 
desired window, this is due to the pipeline being able to detect the beginning of a conversation 
but not the end and therefore it will emit partial records at the end of the window.
(This can be corrected by considering the most recent timestamp on a windowed session
and rejecting it from the save process before committing to BQ/GCS)

## The Streaming Pipeline
The streaming pipeline defined in `dialogflow_analytic_stream_parser.py` executes a long 
running task in Google Cloud Platform that will read from the defined Pub/Sub topic and produce
analytic records for all conversations as soon as they are idle for 15 minutes.  It is standard
to start this pipeline before a batch run when changes are made to overall data structure. Doing
so will ensure that the pipeline captures all in flight conversations and then the batch pipeline
will backfill conversations that are complete.

## How to Run 
Apache Beam provides different Runner platforms to execute your pipeline on.  You can 
run the pipeline locally or on Google Cloud Platform as well as several other environments.

Our code is written to support both the `DirectRunner` and the `DataflowRunner`.  The direct pipeline
version emits files directly to the local machine into the `output` folder.  You will need to 
create a `output/analytic_records` and `output/conversation_records` directories. This is to prevent bottlenecks with 
upload speed.  Files may then be pushed to cloud storage manually.

Before executing these pipelines ensure that you've gone though virtualenv setup and 
authentication within the GCP CLI outlined above.  After these steps you're ready to launch
the pipelines.  

Each pipeline has a set of flags that are used to swap environment targets and enable logging
and/or profiling of the code. 

### Pipeline Deployment 
Deploying the pipeline is as simple as calling the definition script with a target flag. The 
direct flag is used to run the code on your local machine (useful for debugging).

#### For Streaming
```shell script
python dialogflow_analytic_stream_parser.py --target=<env>
```

#### For Batch
```shell script
python dialogflow_analytic_batch_parser.py --target=<env>
```

#### To run the Pipeline locally
```shell script
python dialogflow_analytic_batch_parser.py --target=<env> --direct
```

Optionally you can add verbosity to increase logging or enable profiling to log step 
execution parameters.
```shell script
python dialogflow_analytic_stream_parser.py --target=<env> --verbose --profile
```

## Updating this Code
Testing is performed using the [pytest library](https://docs.pytest.org/en/stable/). The 
established pattern for test structure is to co-locate tests with module code in a `tests` folder.

Test Coverage is tracked using the [coverage library](https://coverage.readthedocs.io/en/coverage-5.3/).
Both pytest and coverage are installed with the pip install command during setup.

### Running Tests
Running tests should be performed after all changes.  The pytest library will automatically 
discover all tests and report any failures. Running this with the coverage command will also
track coverage and prepare a report

```shell script
coverage run -m pytest
```

To view the reports you can either run 
```shell script
coverage report --include=ConvoAnalytics/*
```
or to generate an html report and open it
```shell script
coverage html --include=ConvoAnalytics/* && open htmlcov/index.html
```