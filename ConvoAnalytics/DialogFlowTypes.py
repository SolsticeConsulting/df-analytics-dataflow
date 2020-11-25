import json
import logging
from dataclasses import dataclass
from enum import Enum


class DialogflowLogType(str, Enum):
    DIALOGFLOW_REQUEST = "dialogflow_request",
    DIALOGFLOW_RESPONSE = "dialogflow_response",
    CONVERSATION_REQUEST = "conversation_request",
    CONVERSATION_RESPONSE = "conversation_response",
    FULFILLMENT_ERROR = "dialogflow_fulfillment_error_response"
    UNKNOWN = "unknown"


class InteractionOrigin(str, Enum):
    WEB_CHAT = "SITE",
    BUSINESS_MESSAGES = "BM"
    UNKNOWN = "UNKNOWN"


@dataclass
class DialogflowLog:
    session: str = ''
    message: str = ''
    intent: str = ''
    origin: str = InteractionOrigin.WEB_CHAT.value
    timestamp: str = ''
    event: str = ''
    log_type: str = DialogflowLogType.UNKNOWN.value
    request_id: str = ''
    sentiment_score: float = 0.0
    sentiment_magnitude: float = 0.0
    match_confidence: float = 0.0
    feedback_score: int = 0
    ext_shopper_id: str = 'undefined'
    raw_payload: str = ''

    # todo: Update this logic once DIALOGFLOW_RESPONSE payloads are valid JSON
    def parse_detailed_log(self):
        """Consume the raw_payload field and extract relevant attributes."""
        if self.log_type == DialogflowLogType.DIALOGFLOW_REQUEST:
            try:
                detailed_json = json.loads(self.raw_payload)
                query_input = json.loads(detailed_json["query_input"])
                self.message = query_input.get("text", {}).get("textInputs", [{}])[0].get("text", "")
                self.event = query_input.get("event", {}).get("name", "")
            except json.JSONDecodeError:
                print(f'Error while parsing detailed logs for Request : {self.request_id}')
        elif self.log_type == DialogflowLogType.CONVERSATION_REQUEST:
            try:
                detailed_json = json.loads(self.raw_payload)
                self.message = detailed_json["queryResult"].get("")
                self.event = detailed_json["queryResult"].get("intent", {}).get("displayName", "")
                self.sentiment_score = detailed_json["queryResult"].get("sentimentAnalysisResult", {}).get(
                    "queryTextSentiment", {}).get("score", "0")
                self.origin = detailed_json["originalDetectIntentRequest"]["payload"].get("origin",
                                                                                          InteractionOrigin.WEB_CHAT.value)
                self.ext_shopper_id = detailed_json["originalDetectIntentRequest"]["payload"].get("userId",
                                                                                                  "extShopperId=undefined&storeId=null")
                self.ext_shopper_id = self.ext_shopper_id.split("&")[0]
                self.ext_shopper_id = self.ext_shopper_id[13:]
                if self.ext_shopper_id == "null":
                    self.ext_shopper_id = "undefined"
            except json.JSONDecodeError:
                print(f'Error while parsing detailed logs for Request : {self.request_id}')
        elif self.log_type == DialogflowLogType.DIALOGFLOW_RESPONSE:
            self.intent = self.raw_payload[self.raw_payload.index("intent_name: \"") + 14: self.raw_payload.index('"',
                                                                                                                  self.raw_payload.index(
                                                                                                                      "intent_name: \"") + 14)]
            try:
                self.message = self.raw_payload[self.raw_payload.index("speech: \"") + 9: self.raw_payload.index('"',
                                                                                                                 self.raw_payload.index(
                                                                                                                     "speech: \"") + 9)]
            except ValueError:
                self.message = ""

            # Try to get the intent detection confidence
            try:
                detection_confidence_raw = self.raw_payload[self.raw_payload.index(
                    "score: ") + 7: self.raw_payload.index('\n', self.raw_payload.index(
                    "score: ") + 7)]
                self.match_confidence = float(detection_confidence_raw)
            except ValueError:
                self.match_confidence = 0.0

            # Try to grab the user feedback score
            if self.intent == "User Feedback":
                try:
                    self.feedback_score = int(self.raw_payload[
                                              self.raw_payload.index("user feedback ") + 14: self.raw_payload.index(
                                                  "user feedback ") + 15])
                except:
                    self.feedback_score = 0
        elif self.log_type == DialogflowLogType.CONVERSATION_RESPONSE:
            self.event = json.loads(self.raw_payload).get("followupEventInput", {}).get("name", "")
            self.message = \
                json.loads(self.raw_payload).get("fulfillmentMessages", [{}])[0].get("text", {}).get("text", [""])[0]
        else:
            pass


# todo: Evaluate moving this into the init function
# so that a valid log file can be created by calling the constructor directly with a raw log
def from_log_record(log_record: str) -> DialogflowLog:
    """Consumes a LogRecord string and converts it into a DialogflowLog object."""
    # Attempt to load the json record from the raw log
    try:
        log_json = json.loads(log_record)
    except json.JSONDecodeError:
        logging.error("Error while parsing raw log into json!")
        logging.debug("Raw Log:")
        logging.debug(log_record)
        raise

    parsed_log = DialogflowLog()

    # Attempt to gather meta-data attributes from top-level log
    try:
        parsed_log.timestamp = log_json["timestamp"]
        parsed_log.log_type = log_json["labels"]["type"]
        parsed_log.session = log_json["trace"]
        parsed_log.request_id = log_json["labels"]["request_id"]
        parsed_log.raw_payload = log_json["textPayload"][log_json["textPayload"].index("{"):]
    except ValueError:
        logging.warning("JSON Format missing required keys")
        logging.debug(log_record)
        pass
    parsed_log.parse_detailed_log()

    return parsed_log
