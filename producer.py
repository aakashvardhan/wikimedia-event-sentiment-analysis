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
    ap.add_argument("--all-wikis", action="store_true")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--max-rows", type=int, default=1000, help="stop after N produced messages")
    args = ap.parse_args()
    
    app_name = os.getenv("APP_NAME", "AakashWikimediaSentiment")
    app_ver = os.getenv("APP_VERSION", "1.0")
    contact = os.getenv("APP_CONTACT", "mailto:aakashvardhan.madabhushi@sjsu.edu")
    headers = {"User-Agent": f"{app_name}/{app_ver} ({contact})"}

    
    url = SSE_URL + (f"?since={args.since}" if args.since else "")
    print(f"[producer] connecting to {url}", flush=True)
    
    ensure_topic(args.bootstrap, args.topic)
    
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        client_id="wm-eventstreams-producer",
        acks="all",
        linger_ms=20,
        retries=10,
        max_in_flight_requests_per_connection=1,
        compression_type="gzip",
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    
    
    produced = 0
    samples = 0
    try:
        with EventSource(url, headers=headers, timeout=30) as stream:
            for event in stream:
                raw = getattr(event, "data", None)
                if not raw:
                    continue
                try:
                    change = json.loads(event.data)
                except Exception:
                    continue
                
                #dropping canary event
                if change.get("meta", {}).get("domain") == "canary":
                    continue
            
                # english only by default
                if not args.all_wikis and change.get("wiki") != "enwiki":
                    continue
                
                rev_id = change.get("rev_id") or (change.get("revision") or {}).get("new")
                if not rev_id:
                    continue
                
                # skip bots, empty comments
                if change.get("bot"):
                    continue
                comment = clean_text(change.get("comment", ""))
                if not comment:
                    continue
                
                rec = {
                    "ts": change.get("meta", {}).get("dt"),
                    "wiki": change.get("wiki"),
                    "title": change.get("title"),
                    "rev_id": rev_id,
                    "user": change.get("user"),
                    "comment": comment
                }
                key = f"{rec['wiki']}|{rec['title']}"
                producer.send(args.topic, key=key, value=rec)
                produced += 1
                
                if args.debug and samples < 5:
                    print(f"[producer] sample {samples+1}: {rec['wiki']} | {rec['title']} | {rec['comment'][:80]}", flush=True)
                    samples +=1
                if produced % 50 == 0:
                    print(f"[producer] produced {produced}", flush=True)
                    
                if produced >= args.max_rows:
                    print(f"[producer] limit reached ({args.max_rows}); stopping", flush=True)
                    break
                    
    except KeyboardInterrupt:
        print("\n[producer] interrupted", flush=True)
    finally:
        producer.flush()
        print(f"[producer] flushed: total messages: {produced}", flush=True)
        
if __name__ == '__main__':
    main()
    

