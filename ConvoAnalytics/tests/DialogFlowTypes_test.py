from ConvoAnalytics.DialogFlowTypes import DialogflowLog
from dataclasses import asdict


def test_dialogflow_structure():
    assert len(asdict(DialogflowLog())) == 14, \
        f'Dictionary should be 14 keys, was instead {len(asdict(DialogflowLog()))}'
