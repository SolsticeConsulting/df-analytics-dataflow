import logging
import apache_beam as beam

from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import *
from ConvoAnalytics import BeamFunctions


# Utilize custom arg parser
options = BeamFunctions.DeploymentOptions()
target = options.target
direct = options.direct
profile = options.profile

# Setup Logging Configuration
logger = logging.getLogger("Meijer.Batch.Driver")

if options.verbose:
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
else:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

# Environment Values:  These follow a prescribed format and can be further parameterized.
logger.info(f'Batch Process targeting: {target}')
dataflow_project = f'meijer-dialogflow-chatbot-{target}'
dataflow_staging_bucket_name = f'meijer-dialogflow-dataflow-{target}'
output_bucket_name = f'meijer-dialogflow-analytics-{target}'
input_bucket_name = f'meijer-dialogflow-{target}'

# Standard Options Setup
if direct:
    options.view_as(StandardOptions).runner = 'DirectRunner'
else:
    options.view_as(StandardOptions).runner = 'DataflowRunner'

# Google Cloud Options setup
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = dataflow_project
google_cloud_options.job_name = 'dialogflow-analytics-batch-parser'
google_cloud_options.staging_location = f'gs://{dataflow_staging_bucket_name}/staging'
google_cloud_options.temp_location = f'gs://{dataflow_staging_bucket_name}/temp'
google_cloud_options.region = 'us-central1'

# Worker options
worker_options = options.view_as(WorkerOptions)
worker_options.num_workers = 4  # This raises the initial worker pool to ensure reading is fast
worker_options.disk_size_gb = 50

# Setup options for custom packages
setup_options = options.view_as(SetupOptions)
setup_options.setup_file = "./setup.py"

# Profiling Options for monitoring bottlenecks
if profile:
    profiling_options = options.view_as(ProfilingOptions)
    profiling_options.profile_cpu = True
    profiling_options.profile_memory = True
    profiling_options.profile_location = f'gs://{dataflow_staging_bucket_name}/profiles'

# Target GCS Bucket and file pattern
file_pattern = f'gs://{input_bucket_name}/dialogflow_agent/*/*/*/*.json'

# Initialize Pipeline Object
pipeline = beam.Pipeline(options=options)


# Pipeline Graph Below
# - beam.io.ReadFromText to load all log lines into a PCollection
# - beam.Reshuffle() to prevent fusion of steps (DataflowRunner over optimizes and bottlenecks this step)
# - beam.ParDo using custom parser LogParser() to convert from raw logs into DialogflowLog elements
# - Map each element into a K-V pair of (sessionId : DialogflowLog Element)
# - Group all by session key to create a <SessionID : List<DialogflowLog>> PCollection
# - beam.ParDo CreateAnalyticRecord to convert files into a PCollection of (AnalyticRecord, List<ConversationRecords>)
#   |- beam.ParDo Output the converted records to GCS bucket
#   |- beam.ParDo Output the converted records to BigQuery Table

converted_records = (
    pipeline
    | 'Read Log Lines' >> ReadFromText(file_pattern)
    | 'Shuffle Input Lines' >> beam.Reshuffle()
    | 'Convert Log Records' >> beam.ParDo(BeamFunctions.LogParser())
    | 'Set Session Key' >> beam.Map(lambda x: (x.session, x))
    | 'Group by Session' >> beam.GroupByKey()
    | 'Convert to Analytic Records' >> beam.ParDo(BeamFunctions.CreateAnalyticRecord())
)

# todo: Evaluate splitting input PColl into two to utilize standard IO sinks for BQ and GCS

# Output to Cloud Storage
if not direct:
    upload_output = (
            converted_records
            | 'Upload to GCS' >> beam.ParDo(BeamFunctions.GCSFileUpload(), bucket_name=output_bucket_name)
    )
    stream_output = (
            converted_records
            | 'Insert to BigQuery' >> beam.ParDo(BeamFunctions.BigQueryInsert())
    )
else:
    output = (
        converted_records
        | 'Output Sessions' >> beam.ParDo(BeamFunctions.LocalFileOutput())
    )


if __name__ == '__main__':
    result = pipeline.run()
