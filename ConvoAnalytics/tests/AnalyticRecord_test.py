from ConvoAnalytics.AnalyticRecord import create_record_for_session
from ConvoAnalytics.DialogFlowTypes import DialogflowLog, DialogflowLogType, InteractionOrigin
from typing import List


# Ensure that original function returns the correct analytic header and conversation
def test_create_record_for_session():
    # Type Alias
    DialogflowLogs = List[DialogflowLog]

    # Setup Test Log Collection
    session_logs: DialogflowLogs = [
        DialogflowLog(
            session="Test_Session",
            message="hi",
            intent="",
            origin=InteractionOrigin.WEB_CHAT.value,
            timestamp="2020-10-31T18:00:00.000Z",
            event="",
            log_type=DialogflowLogType.DIALOGFLOW_REQUEST.value,
            request_id="",
            sentiment_score=0.0,
            sentiment_magnitude=0.0,
            match_confidence=0.0,
            feedback_score=0,
            ext_shopper_id="",
            raw_payload=""
        ),
        DialogflowLog(
            session="Test_Session",
            message="Good Morning!",
            intent="Default Welcome Intent",
            origin=InteractionOrigin.WEB_CHAT.value,
            timestamp="2020-10-31T18:05:00.000Z",
            event="",
            log_type=DialogflowLogType.DIALOGFLOW_RESPONSE.value,
            request_id="",
            sentiment_score=0.0,
            sentiment_magnitude=0.0,
            match_confidence=1.0,
            feedback_score=0,
            ext_shopper_id="",
            raw_payload=""
        ),
        DialogflowLog(
            session="Test_Session",
            message="How are you?",
            intent="",
            origin=InteractionOrigin.WEB_CHAT.value,
            timestamp="2020-10-31T18:10:00.000Z",
            event="",
            log_type=DialogflowLogType.DIALOGFLOW_REQUEST.value,
            request_id="",
            sentiment_score=0.80,
            sentiment_magnitude=0.08,
            match_confidence=0.0,
            feedback_score=0,
            ext_shopper_id="",
            raw_payload=""
        ),
        DialogflowLog(
            session="Test_Session",
            message="I'm doing great!",
            intent="agent.howareyou",
            origin=InteractionOrigin.WEB_CHAT.value,
            timestamp="2020-10-31T18:15:00.000Z",
            event="",
            log_type=DialogflowLogType.DIALOGFLOW_RESPONSE.value,
            request_id="",
            sentiment_score=0.0,
            sentiment_magnitude=0.0,
            match_confidence=0.90,
            feedback_score=0,
            ext_shopper_id="",
            raw_payload=""
        ),
        DialogflowLog(
            session="Test_Session",
            message="user feedback 5",
            intent="",
            origin=InteractionOrigin.WEB_CHAT.value,
            timestamp="2020-10-31T18:15:00.000Z",
            event="",
            log_type=DialogflowLogType.DIALOGFLOW_REQUEST.value,
            request_id="",
            sentiment_score=0.0,
            sentiment_magnitude=0.0,
            match_confidence=0.90,
            feedback_score=5,
            ext_shopper_id="",
            raw_payload=""
        ),
        DialogflowLog(
            session="Test_Session",
            message="glyp glorp",
            intent="",
            origin=InteractionOrigin.WEB_CHAT.value,
            timestamp="2020-10-31T18:20:00.000Z",
            event="",
            log_type=DialogflowLogType.DIALOGFLOW_REQUEST.value,
            request_id="",
            sentiment_score=0.0,
            sentiment_magnitude=0.0,
            match_confidence=0.0,
            feedback_score=0,
            ext_shopper_id="",
            raw_payload=""
        ),
        DialogflowLog(
            session="Test_Session",
            message="I'm sorry, I can't understand that.",
            intent="Default Fallback Intent",
            origin=InteractionOrigin.WEB_CHAT.value,
            timestamp="2020-10-31T18:25:00.000Z",
            event="",
            log_type=DialogflowLogType.DIALOGFLOW_RESPONSE.value,
            request_id="",
            sentiment_score=0.0,
            sentiment_magnitude=0.0,
            match_confidence=0.3,
            feedback_score=0,
            ext_shopper_id="",
            raw_payload=""
        )
    ]

    created_header_record, created_conversation = create_record_for_session(session_logs)

    # Assertions
    assert created_header_record.record_key == "Test_Session_2020-10-31T18:00:00.000Z", \
        f'Format of record key incorrect. Expected: [session_id]_[timestamp], Was :{created_header_record.record_key}'
    assert created_header_record.conversation_depth == 3, \
        f'Conversation was expected to be 3. Was instead :{created_header_record.conversation_depth}'
    assert created_header_record.num_fallback_triggered == 1, \
        f'Conversation expected to have 1 fallback trigger. Was instead :{created_header_record.num_fallback_triggered}'
    assert created_header_record.origin == InteractionOrigin.WEB_CHAT.value, \
        f'Conversation expected to have WebChat origin. Was instead :{created_header_record.origin}'

# Ensure empty conversations are marked as such
def test_create_record_for_session_empty_convo():
    # Type Alias
    DialogflowLogs = List[DialogflowLog]

    # Setup Test Log List
    session_logs: DialogflowLogs = [
        DialogflowLog(
            session="Test_Session",
            message="hi",
            intent="",
            origin=InteractionOrigin.BUSINESS_MESSAGES.value,
            timestamp="2020-10-31T18:00:00.000Z",
            event="",
            log_type=DialogflowLogType.DIALOGFLOW_REQUEST.value,
            request_id="",
            sentiment_score=0.0,
            sentiment_magnitude=0.0,
            match_confidence=0.0,
            feedback_score=0,
            ext_shopper_id="",
            raw_payload=""
        ),
        DialogflowLog(
            session="Test_Session",
            message="Good Morning!",
            intent="Default Welcome Intent",
            origin=InteractionOrigin.BUSINESS_MESSAGES.value,
            timestamp="2020-10-31T18:05:00.000Z",
            event="",
            log_type=DialogflowLogType.DIALOGFLOW_RESPONSE.value,
            request_id="",
            sentiment_score=0.0,
            sentiment_magnitude=0.0,
            match_confidence=1.0,
            feedback_score=0,
            ext_shopper_id="",
            raw_payload=""
        )
    ]

    created_header_record, created_conversation = create_record_for_session(session_logs)

    # Assertions
    assert created_header_record.conversation_depth == 0, \
        f'Expected conversation depth 0. Was instead :{created_header_record.conversation_depth}'
    assert created_header_record.origin == InteractionOrigin.BUSINESS_MESSAGES.value, \
        f'Expected conversation origin of Business Messages. Was intstead :{created_header_record.origin}'

# Ensure empty conversations are marked as such
def test_create_record_for_session_customer_support():
    # Type Alias
    DialogflowLogs = List[DialogflowLog]

    # Setup Test Log List
    session_logs: DialogflowLogs = [
        DialogflowLog(
            session="Test_Session",
            message="hi",
            intent="",
            origin=InteractionOrigin.BUSINESS_MESSAGES.value,
            timestamp="2020-10-31T18:00:00.000Z",
            event="",
            log_type=DialogflowLogType.DIALOGFLOW_REQUEST.value,
            request_id="",
            sentiment_score=0.0,
            sentiment_magnitude=0.0,
            match_confidence=0.0,
            feedback_score=0,
            ext_shopper_id="",
            raw_payload=""
        ),
        DialogflowLog(
            session="Test_Session",
            message="Good Morning!",
            intent="Default Welcome Intent",
            origin=InteractionOrigin.BUSINESS_MESSAGES.value,
            timestamp="2020-10-31T18:05:00.000Z",
            event="",
            log_type=DialogflowLogType.DIALOGFLOW_RESPONSE.value,
            request_id="",
            sentiment_score=0.0,
            sentiment_magnitude=0.0,
            match_confidence=1.0,
            feedback_score=0,
            ext_shopper_id="",
            raw_payload=""
        ),
        DialogflowLog(
            session="Test_Session",
            message="Can I speak with someone",
            intent="",
            origin=InteractionOrigin.BUSINESS_MESSAGES.value,
            timestamp="2020-10-31T18:00:00.000Z",
            event="",
            log_type=DialogflowLogType.DIALOGFLOW_REQUEST.value,
            request_id="",
            sentiment_score=0.0,
            sentiment_magnitude=0.0,
            match_confidence=0.0,
            feedback_score=0,
            ext_shopper_id="",
            raw_payload=""
        ),
        DialogflowLog(
            session="Test_Session",
            message="Absolutely! Give us a call.",
            intent="customer.support",
            origin=InteractionOrigin.BUSINESS_MESSAGES.value,
            timestamp="2020-10-31T18:05:00.000Z",
            event="",
            log_type=DialogflowLogType.DIALOGFLOW_RESPONSE.value,
            request_id="",
            sentiment_score=0.0,
            sentiment_magnitude=0.0,
            match_confidence=1.0,
            feedback_score=0,
            ext_shopper_id="",
            raw_payload=""
        ),
    ]

    created_header_record, created_conversation = create_record_for_session(session_logs)

    # Assertions
    assert created_header_record.escalated, f'Expected escalated record, was not'
