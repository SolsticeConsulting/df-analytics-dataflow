import apache_beam as beam
import logging
import time

from apache_beam import DoFn
from apache_beam.options.pipeline_options import PipelineOptions
from pandas import DataFrame
from ConvoAnalytics.DialogFlowTypes import from_log_record
from ConvoAnalytics.AnalyticRecord import create_record_for_session
from ConvoAnalytics.AnalyticRecord import upload_to_cloud_storage
from ConvoAnalytics.AnalyticRecord import stream_to_big_query


logger = logging.getLogger("BeamFunctions")


class LogParser(DoFn):
    """Consumes a log text line and yields a DialogFlowLog Element"""
    def process(self, element, *args, **kwargs):
        yield from_log_record(element)


class CreateAnalyticRecord(DoFn):
    """Consumes an element as a DialogFlow element and transforms it into a <AnalyticHeader, <List<ConversationLine>>"""
    def process(self, element, *args, **kwargs):
        try:
            # Type cast is necessary as the _UnwindowedValues class doesn't implement __get_item__ and cant have .sort()
            # called directly on it. 
            log_list = list(element[1])
            session_analytic, session_conversation = create_record_for_session(log_list)
            yield session_analytic, session_conversation
        except:
            logger.error(f'Expected list, received : {type(element[1])}')


class LocalFileOutput(DoFn):
    """Consumes an <AnalyticHeader, <List<ConversationRow>> PCollection Element and saves to a local folder."""
    def process(self, element, *args, **kwargs):
        analytic_record = element[0]
        conversation_list = element[1]

        with open("output/analytic_records/analytic_records_"+analytic_record.record_key, "w+") as output:
            logging.debug("Output File: " + "output/analytic_records/analytic_records_"+analytic_record.record_key)
            analytic_record.export_to_data_frame().to_csv(output, index=False)

        with open("output/conversation_records/conversation_record_"+analytic_record.record_key, "w+") as output:
            logging.debug("Output File: " + "output/conversation_records/conversation_record_"+analytic_record.record_key)
            conversation_records_frame = DataFrame()
            for conversation_record in conversation_list:
                conversation_records_frame = conversation_records_frame.append(
                    other=conversation_record.export_to_data_frame(), ignore_index=True)

            conversation_records_frame.to_csv(output, index=False)


class GCSFileUpload(DoFn):
    """Consumes an <AnalyticHeader, <List<ConversationRow>> PCollection Element and uploads to a Cloud Storage Bucket"""
    def process(self, element, *args, **kwargs):
        analytic_record = element[0]
        conversation_list = element[1]

        analytic_filename = "analytic_records/analytic_record_"+analytic_record.record_key
        conversation_filename = "conversation_records/conversation_record_"+analytic_record.record_key

        upload_to_cloud_storage(
            analytic_record.export_to_data_frame(),
            kwargs["bucket_name"],
            analytic_filename
        )

        conversation_records_frame = DataFrame()
        for conversation_record in conversation_list:
            conversation_records_frame = conversation_records_frame.append(
                other=conversation_record.export_to_data_frame(), ignore_index=True)

        upload_to_cloud_storage(
            conversation_records_frame,
            kwargs["bucket_name"],
            conversation_filename
        )


class BigQueryInsert(DoFn):
    """Consumes an <AnalyticHeader, <List<ConversationRow>> PCollection Element and inserts into BigQuery"""
    def process(self, element, *args, **kwargs):
        analytic_record = element[0]
        conversation_list = element[1]
        stream_to_big_query(
            header_record=analytic_record,
            header_table_id="dialogflow_analytics.analytic_records",
            conversation=conversation_list,
            conversation_table_id="dialogflow_analytics.conversation_records"
        )


class TimestampExtractor(DoFn):
    """Consumes a DialogFlow log element and Emits a TimestampedValue with the log timestamp."""
    def process(self, element, *args, **kwargs):
        timestamp_str = element.timestamp
        try:
            time_tuple = time.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        except:
            pass
        try:
            time_tuple = time.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')
        except:
            pass
        unix_time = time.mktime(time_tuple)
        yield beam.window.TimestampedValue(element, unix_time)


class DeploymentOptions(PipelineOptions):
    """
    Custom Argument Parser Class to provide context specific parsing

    Arguments
    --target: One of ['dev', 'stg', 'prod']. Provides the target environment to use and is defaulted to dev.
    --direct: True if present.  Sets the pipeline to run on your local computer by using the DirectRunner option.
    --verbose: True if present. Sets the log level of the pipeline run to be DEBUG.  (Default is INFO)
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--target", choices=["dev", "stg", "prod"], default="dev")
        parser.add_argument("--verbose", action="store_true", required=False)
        parser.add_argument("--direct", action="store_true", required=False)
        parser.add_argument("--profile", action="store_true", required=False)
