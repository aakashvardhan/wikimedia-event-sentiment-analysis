#!/usr/bin/env python3
import argparse, json, sys, signal, os, time
from kafka import KafkaConsumer


def main():
    ap = argparse.ArgumentParser()
    default_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "broker:9092")
    ap.add_argument("--bootstrap", default=default_bootstrap)
    ap.add_argument("--topic", default="wm_rc_raw")
    ap.add_argument("--group", default=f"wm-test-{int(time.time())}")
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--sleep-ms", type=int, default=2)
    ap.add_argument("--max-rows", type=int, default=0, help="stop after N messages (0 = unlimited)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        enable_auto_commit=True,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None
    )

    print(f"[consumer] subscribed to {args.topic} (group={args.group})", flush=True)
    running = True
    
    def handle_sig(*_):
        nonlocal running
        running = False
        print("\n[consumer] stopping", flush=True)
        
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGINT, handle_sig)
    
    count = 0
    idle_ticks = 0
    sleep_s = args.sleep_ms
    print("[consumer] waiting for messages…", flush=True)
    try:
        while running:
            batch = consumer.poll(timeout_ms=1000, max_records=500)
            if not batch:
                idle_ticks += 1
                if idle_ticks % 5 == 0 and args.verbose:
                    print("[consumer] still waiting...", flush=True)
                continue
            
            idle_ticks = 0
            for _tp, records in batch.items():
                for msg in records:
                    if not running:
                        break
                    rec = msg.value
                    print(
                        f"[{rec.get('ts')}] {rec.get('wiki')} | {rec.get('title')} | "
                        f"rev={rec.get('rev_id')} | user={rec.get('user')} | "
                        f"comment={rec.get('comment')[:120]}"
                    )
                    count += 1
                    
                    if sleep_s:
                        time.sleep(sleep_s)
                    
                    if args.max_rows and count >= args.max_rows:
                        print(f"[consumer] limit reached ({args.max_rows}); stopping", flush=True)
                        running = False
                        break
            
    finally:
        consumer.close()
        print("[consumer] closed")
        
if __name__ == '__main__':
    main()
