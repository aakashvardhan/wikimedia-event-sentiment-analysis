#!/usr/bin/env python3
import argparse, json, sys, signal, os, time
from kafka import KafkaConsumer


def main():
    ap = argparse.ArgumentParser()
    default_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "broker:9092")
    ap.add_argument("--bootstrap", default=default_bootstrap)
    ap.add_argument("--topic", default="wm_rc_raw")
    ap.add_argument("--group", default="wm-test-consumer")
    ap.add_argument("--from-beginning", action="store-true")
    args = ap.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        enable_auto_commit=True,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        consumer_timeout_ms=0,
    )

    print(f"[consumer] subscribed to {args.topic}")
    running = True

    def handle_sig(*_):
        nonlocal running
        running = False
        print("\n[consumer] stopping...")

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    sleep_s = max(args.sleep_ms, 0) / 1000.0

    try:
        for msg in consumer:
            if not running:
                break
            rec = msg.value
            print(
                f"[{rec.get('ts')}] {rec.get('wiki')} | {rec.get('title')} | "
                f"rev={rec.get('rev_id')} | user={rec.get('user')} | "
                f"comment={rec.get('comment')[:120]}"
            )
            if sleep_s > 0:
                time.sleep(sleep_s)
    finally:
        consumer.close()
        print("[consumer] closed")


if __name__ == "__main__":
    main()
