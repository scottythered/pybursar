import requests as r
import json
from auth_puller import auth_puller


def slacker(desc, msg):
    slack_channel = auth_puller("auth.json", "slack")
    msg_header = "*Bursar " + desc + "*"
    formatted = [
        {"type": "context", "elements": [{"type": "mrkdwn", "text": msg_header}],},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": msg}},
    ]
    slackdata = {
        "text": "Bursar Integration Update",
        "blocks": json.dumps(formatted),
    }
    headers = {"Content-Type": "application/json"}
    r.post(slack_channel, json=slackdata, headers=headers)


def slackDebug(desc, msg):
    slack_channel = auth_puller("auth.json", "error_slack")
    msg_header = "*Bursar " + desc + "*"
    formatted = [
        {"type": "context", "elements": [{"type": "mrkdwn", "text": msg_header}],},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": msg}},
    ]
    slackdata = {
        "text": "Bursar Debug",
        "blocks": json.dumps(formatted),
    }
    headers = {"Content-Type": "application/json"}
    r.post(slack_channel, json=slackdata, headers=headers)
