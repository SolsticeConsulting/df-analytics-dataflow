import apache_beam as beam
import logging
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import *
from apache_beam.utils.timestamp import Duration

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
logger.info(f'Stream Process targeting: {target}')
dataflow_project = f'meijer-dialogflow-chatbot-{target}'
dataflow_staging_bucket_name = f'meijer-dialogflow-dataflow-{target}'
input_topic_name = f'dialogflow-logs-{target}'
output_bucket = f'meijer-dialogflow-analytics-{target}'

# Standard Options Setup
if direct:
    options.view_as(StandardOptions).runner = 'DirectRunner'
else:
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    options.view_as(StandardOptions).streaming = True

# Google Cloud Options setup
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = dataflow_project
google_cloud_options.job_name = 'dialogflow-analytics-stream-parser'
google_cloud_options.staging_location = f'gs://{dataflow_staging_bucket_name}/staging'
google_cloud_options.temp_location = f'gs://{dataflow_staging_bucket_name}/temp'
google_cloud_options.region = 'us-central1'
google_cloud_options.update = True

# Worker options
worker_options = options.view_as(WorkerOptions)
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

# Create pipeline
pipeline = beam.Pipeline(options=options)

# Pipeline Graph Below
# - beam.io.ReadFromPubSub to read log lines into a PCollection
# - beam.ParDo using custom parser LogParser() to convert from raw logs into DialogflowLog elements
# - beam.ParDo using transforms that attach timestamp values to each Element
# - beam.WindowInto 15 min sessions with an allowed 5 min lateness
# - Map each element into a K-V pair of (sessionId : Timestamped DialogflowLog Element)
# - Group all by session key to create a <SessionID : List<DialogflowLog>> PCollection
# - Filter out Empty Session keys
# - beam.ParDo CreateAnalyticRecord to convert files into a PCollection of (AnalyticRecord, List<ConversationRecords>)
#   |- beam.ParDo Output the converted records to GCS bucket
#   |- beam.ParDo Output the converted records to BigQuery Table


converted_records = (
    pipeline
    | 'Read Pub Sub' >> ReadFromPubSub(topic=f'projects/{dataflow_project}/topics/{input_topic_name}')
    | 'Convert Log Records' >> beam.ParDo(BeamFunctions.LogParser())
    | 'Extract Timestamps' >> beam.ParDo(BeamFunctions.TimestampExtractor())
    | 'Window Records' >> beam.WindowInto(
        beam.window.Sessions(15*60),  # 15 min windows
        allowed_lateness=Duration(seconds=5 * 60)  # 5 mins
        )
    | 'Set Session Key' >> beam.Map(lambda x: (x.session, x))
    | 'Group by Session' >> beam.GroupByKey()
    | 'Filter Out Empty Sessions' >> beam.Filter(lambda x: len(x[1]) > 0)
    | 'Convert to Analytic Records' >> beam.ParDo(BeamFunctions.CreateAnalyticRecord())
)

# Parallel Output to GCS and BigQuery
# todo: Evaluate splitting input PColl into two to utilize standard IO sinks for BQ and GCS
gcs_output = converted_records | 'Upload to GCS' >> beam.ParDo(BeamFunctions.GCSFileUpload(), bucket_name=output_bucket)
bq_output = converted_records | 'Insert to BQ' >> beam.ParDo(BeamFunctions.BigQueryInsert())


if __name__ == '__main__':
    pipeline.run()
