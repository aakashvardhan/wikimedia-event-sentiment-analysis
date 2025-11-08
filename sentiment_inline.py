#!/usr/bin/env python3
import os, sys, re, json, time, signal, argparse, time
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:9092")

def ensure_topic(bootstrap, topic, partitions=3, rf=1):
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="wm-sentiment-admin")
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=rf)])
        print(f"[sentiment] created topic {topic}", flush=True)
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        print(f"[sentiment] topic create warning: {e}", file=sys.stderr, flush=True)

URL_RE = re.compile(r"http[s]?://\S+", re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")
WIKI_LINK_RE = re.compile(r"\[\[(?:[^|\]]+\|)?([^\]]+)\]\]")
TEMPLATE_RE = re.compile(r"\{\{[^}]+\}\}")
BRACKETS_RE = re.compile(r"[\[\]\(\)\{\}<>\|]")
WS_RE = re.compile(r"\s+")

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = URL_RE.sub("", s)
    s = TAG_RE.sub(" ", s)
    s = WIKI_LINK_RE.sub(r"\1", s)
    s = TEMPLATE_RE.sub(" ", s)
    s = BRACKETS_RE.sub(" ", s)
    s = WS_RE.sub(" ", s).strip()
    return s

def main():
    ap = argparse.ArgumentParser(description="Inline cleaner + English sentiment (VADER fallback for Alpine)")
    ap.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    ap.add_argument("--in-topic", default="wm_rc_raw")
    ap.add_argument("--out-topic", default="wm_sentiment_scored")
    ap.add_argument("--group", default=f"wm-sentiment-{int(time.time())}")
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--model", default="cardiffnlp/twitter-roberta-base-sentiment-latest")
    ap.add_argument("--engine", choices=["auto","hf","vader"], default="auto")
    ap.add_argument("--min-len", type=int, default=3)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--max-rows", type=int, default=0)
    args = ap.parse_args()

    # Decide engine
    engine = args.engine
    clf = None
    label_map = {"pos":"positive","neu":"neutral","neg":"negative"}

    if engine in ("auto","hf"):
        try:
            from transformers import pipeline 
            clf = pipeline("text-classification", model=args.model)
            engine = "hf"
            print("[sentiment] using HuggingFace transformers", flush=True)
        except Exception as e:
            if engine == "hf":
                print(f"[sentiment] HF init failed: {e}", file=sys.stderr, flush=True)
                sys.exit(1)
            print(f"[sentiment] HF unavailable, falling back to VADER: {e}", flush=True)
            engine = "vader"

    if engine == "vader":
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        vader = SentimentIntensityAnalyzer()
        def vader_label(text: str) -> str:
            s = vader.polarity_scores(text)
            # choose the max of pos/neu/neg
            key = max(("pos","neu","neg"), key=lambda k: s[k])
            return label_map[key]
        clf = vader_label
        print("[sentiment] using VADER (pure Python) on Alpine", flush=True)

    ensure_topic(args.bootstrap, args.out_topic)

    consumer = KafkaConsumer(
        args.in_topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
    )
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        acks="all",
        linger_ms=10,
        retries=5,
        compression_type="gzip",
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    print(f"[sentiment] consuming {args.in_topic} (group={args.group}), producing {args.out_topic}", flush=True)

    running = True
    def stop(*_):
        nonlocal running
        running = False
        print("\n[sentiment] stopping…", flush=True)
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    processed = 0
    shown = 0

    try:
        while running:
            batch = consumer.poll(timeout_ms=1000, max_records=256)
            if not batch:
                continue
            for _tp, records in batch.items():
                for m in records:
                    if not running:
                        break
                    rec = m.value
                    rev_id = rec.get("rev_id") or (rec.get("revision") or {}).get("new")
                    if not rev_id:
                        continue
                    text = clean_text(rec.get("comment", ""))
                    if len(text) < args.min_len:
                        continue

                    if engine == "hf":
                        pred = clf(text, truncation=True, max_length=96)
                        label = pred["label"] if isinstance(pred, dict) else pred[0]["label"]
                    else:
                        label = clf(text)

                    polarity = {"positive": 1.0, "neutral": 0.0, "negative": -1.0}[label.lower()]
                    out = {
                        "v": 1,
                        "ts": rec.get("ts"),
                        "wiki": rec.get("wiki"),
                        "title": rec.get("title"),
                        "rev_id": rev_id,
                        "user": rec.get("user"),
                        "text": text,
                        "label": label,
                        "polarity": polarity,
                        "engine": engine,
                        "model": args.model if engine=="hf" else "vaderSentiment",
                    }
                    key = f"{out['wiki']}|{out['title']}"
                    producer.send(args.out_topic, key=key, value=out)
                    processed += 1

                    if args.debug and shown < 5:
                        print(f"[sentiment] {out['title']} | {label} | {text[:80]}", flush=True)
                        shown += 1
                    
                    time.sleep(2)

                    if args.max_rows and processed >= args.max_rows:
                        print(f"[sentiment] limit reached ({args.max_rows}); stopping", flush=True)
                        running = False
                        break
    finally:
        try:
            producer.flush()
        finally:
            consumer.close()
        print(f"[sentiment] closed (total scored: {processed})", flush=True)

if __name__ == "__main__":
    main()
