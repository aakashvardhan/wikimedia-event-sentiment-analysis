#!/usr/bin/env python3
import os
import argparse, json, sys, time, re
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from requests_sse import EventSource


SSE_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


def ensure_topic(bootstrap, topic, partitions=3, rf=1):
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="wm-admin")
        existing = set(admin.list_topics())
        if topic not in existing:
            admin.create_topics(
                [NewTopic(name=topic, num_partitions=partitions, replication_factor=rf)]
            )
            print(f"[producer] created topic {topic}")
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        print(f"[producer] topic check warning: {e}", file=sys.stderr)


def clean_text(s: str) -> str:
    s = re.sub(r"http[s]?://\\S+", "", s or "")
    s = re.sub(r"\\s+", " ", s).strip()
    return s


def main():
    ap = argparse.ArgumentParser()
    default_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "broker:9092")
    ap.add_argument("--bootstrap", default=default_bootstrap)
    ap.add_argument("--topic", default="wm_rc_raw")
    ap.add_argument("--since", help="ISO8601, e.g. 2025-11-06T00:00:00Z")
    ap.add_argument("--al-wikis", action="store-true")
    args = ap.parse_args()
