from dataclasses import dataclass, astuple, asdict
from pandas import DataFrame
from ConvoAnalytics.DialogFlowTypes import DialogflowLog
from io import StringIO
from google.cloud.storage import Client
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from logging import getLogger
from typing import List


@dataclass
class AnalyticRecord:
    record_key: str = ''
    timestamp: str = ''
    session_id: str = ''
    num_fallback_triggered: int = 0
    escalated: bool = False
    origin: str = 'SITE'
    conversation_depth: int = 0
    csat: int = -1
    sentiment_score: float = 0.0
    ext_shopper_id: str = 'undefined'

    def generate_record_key(self):
        self.record_key = self.session_id + "_" + self.timestamp

    def export_to_data_frame(self) -> DataFrame:  # pragma: no cover
        return DataFrame.from_records(data=[astuple(self)], columns=asdict(self).keys())

    def get_schema(self) -> list:  # pragma: no cover
        return [
            SchemaField("record_key", "STRING"),
            SchemaField("timestamp", "TIMESTAMP"),
            SchemaField("session_id", "STRING"),
            SchemaField("num_fallback_triggered", "INTEGER"),
            SchemaField("escalated", "BOOLEAN"),
            SchemaField("origin", "STRING"),
            SchemaField("conversation_depth", "INTEGER"),
            SchemaField("csat", "INTEGER"),
            SchemaField("sentiment_score", "FLOAT"),
            SchemaField("ext_shopper_id", "STRING"),
        ]


@dataclass
class Conversation:
    record_lookup_key: str = ''
    timestamp: str = ''
    customer: str = ''
    agent: str = ''
    intent: str = ''
    event: str = ''
    sentiment_score: float = 0.0
    sentiment_magnitude: float = 0.0
    fallback_trigger: bool = False
    match_confidence: float = 0.0

    def export_to_data_frame(self) -> DataFrame:  # pragma: no cover
        return DataFrame.from_records(data=[astuple(self)], columns=asdict(self).keys())

    def get_schema(self) -> list:  # pragma: no cover
        return [
            SchemaField("record_lookup_key", "STRING"),
            SchemaField("timestamp", "TIMESTAMP"),
            SchemaField("customer", "STRING"),
            SchemaField("agent", "STRING"),
            SchemaField("intent", "STRING"),
            SchemaField("event", "STRING"),
            SchemaField("sentiment_score", "FLOAT"),
            SchemaField("sentiment_magnitude", "FLOAT"),
            SchemaField("fallback_trigger", "BOOLEAN"),
            SchemaField("match_confidence", "FLOAT"),
        ]


# Type Alias
ConversationList = List[Conversation]
DialogFlowLogs = List[DialogflowLog]

# Logging Configuration
logger = getLogger("AnalyticRecord")


# Parse the data frame for this session
def create_record_for_session(session_records: DialogFlowLogs) -> (AnalyticRecord, ConversationList):
    """Consumes a list of <DialogFlowLog> objects and emits a (<AnalyticHeader, ConversationList>) tuple."""
    analytic_header: AnalyticRecord = AnalyticRecord()
    conversation_list: ConversationList = list()

    # Sort the Conversation and Loop over records
    # Note: In place sort() is used vs deep copy sorted() as we don't need the original list ordering.
    session_records.sort(key=lambda record: record.timestamp)
    for session_record in session_records:

        # If this is the first record for this session, then we're going to set the meta data
        if analytic_header.record_key == "":
            analytic_header.timestamp = session_record.timestamp
            analytic_header.session_id = session_record.session
            analytic_header.generate_record_key()

        # Check if Business Messages, tattoo record if so
        if session_record.origin == 'BM':
            analytic_header.origin = session_record.origin

        # Check for escalation and tattoo record if so
        if session_record.intent == 'customer.support':
            analytic_header.escalated = True

        # Check for shopper id and tattoo record
        if session_record.ext_shopper_id != 'undefined':
            analytic_header.ext_shopper_id = session_record.ext_shopper_id

        # Increment the fallback counter if it's been triggered
        if session_record.intent.find('Fallback') > 0:
            analytic_header.num_fallback_triggered += 1

        # Set _either_ the customer or agent record based on requestType and update conversation depth accordingly
        if (
                session_record.log_type.find('request') > 0 and
                session_record.event != 'WELCOME' and
                session_record.message != 'hi'
        ):
            analytic_header.conversation_depth += 1
            customer_message = session_record.message
            agent_message = ''
        else:
            customer_message = ''
            agent_message = session_record.message

        if session_record.feedback_score > 0:
            analytic_header.csat = session_record.feedback_score

        conversation_row = Conversation(timestamp=session_record.timestamp,
                                        customer=customer_message,
                                        agent=agent_message,
                                        intent=session_record.intent,
                                        event=session_record.event,
                                        sentiment_score=session_record.sentiment_score,
                                        sentiment_magnitude=session_record.sentiment_magnitude,
                                        match_confidence=session_record.match_confidence,
                                        record_lookup_key=analytic_header.record_key)
        conversation_list.append(conversation_row)

    # Calculate Sentiment Average IF the conversation exists
    try:
        if analytic_header.conversation_depth > 0:
            avg_score = 0.0
            for line in conversation_list:
                avg_score += (line.sentiment_score * line.sentiment_magnitude)
            avg_score /= analytic_header.conversation_depth
            analytic_header.sentiment_score = round(avg_score, 3)
    except:  # pragma: no cover
        pass

    # Calculate fallback triggers
    for pos, conversation_line in enumerate(conversation_list):
        trigger_index = pos
        if str(conversation_line.intent).find("Fallback") > 0:
            conversation_list[trigger_index - 1].fallback_trigger = True

    return analytic_header, conversation_list


def upload_to_cloud_storage(data_frame: DataFrame, bucket_name: str, blob_name: str) -> None:  # pragma: no cover
    """Consumes a Pandas Dataframe and insert it into the appropriate Cloud Storage Bucket"""
    logger.debug("Uploading File to Storage")
    logger.debug("Target Bucket: " + bucket_name)
    logger.debug("Output Blob: " + blob_name)
    blob_to_upload = Client().bucket(bucket_name).blob(blob_name)
    with StringIO() as output_file:
        data_frame.to_csv(output_file, index=False)
        blob_to_upload.upload_from_string(output_file.getvalue())


def stream_to_big_query(
        header_record: AnalyticRecord,
        header_table_id: str,
        conversation: ConversationList,
        conversation_table_id: str
):  # pragma: no cover
    """Consumes a <AnalayticHeader> and a <ConversationList> to insert into the BigQuery Tables specified"""
    client = bigquery.Client()
    logger.debug(f"Inserting Analytic Record for session: {header_record.record_key} into {header_table_id}")
    client.insert_rows(table=header_table_id, rows=[asdict(header_record)], selected_fields=AnalyticRecord().get_schema())

    logger.debug(f"Inserting Conversation Data for session: {conversation[0].record_lookup_key} into {conversation_table_id}")
    client.insert_rows(table=conversation_table_id, rows=[asdict(conversation_row) for conversation_row in conversation], selected_fields=Conversation().get_schema())