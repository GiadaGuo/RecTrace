#!/usr/bin/env python3
"""
mock_producer.py
-----------------
Simulate real-time user behavior event stream -> Kafka topic: ods_user_behavior

Features:
  - Power-law distribution: 20% hot items get 80% traffic
  - Behavior funnel: pv 70%, cart 15%, fav 10%, buy 5%
  - Normal rate: ~100 events/s (Poisson distributed)
  - Burst mode: --burst flag triggers 500 events/s for 30s
  - Prints distribution stats on startup for validation

Usage:
    # Normal mode
    python scripts/mock_producer.py

    # Burst mode (backpressure test)
    python scripts/mock_producer.py --burst
"""

import argparse
import json
import random
import time
import math
import sys
from kafka import KafkaProducer

# ── Config ───────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = "localhost:9092"
TOPIC            = "ods_user_behavior"

NUM_USERS        = 1000
NUM_ITEMS        = 5000
NORMAL_RATE      = 100    # events/s
BURST_RATE       = 500    # events/s
BURST_DURATION   = 30     # seconds

BEHAVIOR_TYPES   = ["pv", "cart", "fav", "buy"]
BEHAVIOR_WEIGHTS = [70, 15, 10, 5]

random.seed(None)  # non-deterministic for true simulation

# ── Power-law item distribution (Zipf) ───────────────────────────────────────
def build_item_weights(n_items: int) -> list:
    """
    Zipf-like weights: item rank r gets weight 1/r.
    Top 20% (1000 items) will receive ~80% of traffic.
    """
    weights = [1.0 / math.pow(i, 1.2) for i in range(1, n_items + 1)]
    return weights


ITEM_IDS     = [f"I{i:07d}" for i in range(1, NUM_ITEMS + 1)]
ITEM_WEIGHTS = build_item_weights(NUM_ITEMS)

USER_IDS     = [f"U{i:06d}" for i in range(1, NUM_USERS + 1)]

# ── Startup distribution validation ──────────────────────────────────────────
def validate_distribution(n_samples: int = 10_000):
    print(f"\n[Validation] Sampling {n_samples:,} events to verify distributions ...\n")

    # Behavior distribution
    behaviors = random.choices(BEHAVIOR_TYPES, weights=BEHAVIOR_WEIGHTS, k=n_samples)
    beh_counts = {b: behaviors.count(b) for b in BEHAVIOR_TYPES}
    print("  Behavior type distribution:")
    for b, c in beh_counts.items():
        bar = "#" * int(c / n_samples * 100)
        print(f"    {b:4s}: {c:6d} ({c/n_samples*100:5.1f}%)  {bar}")

    # Hot item concentration: top 20% items
    items = random.choices(ITEM_IDS, weights=ITEM_WEIGHTS, k=n_samples)
    top_20_pct = NUM_ITEMS // 5
    top_items  = set(ITEM_IDS[:top_20_pct])
    top_traffic = sum(1 for x in items if x in top_items)
    print(f"\n  Item hotspot (top 20% = {top_20_pct} items):")
    print(f"    Traffic share: {top_traffic/n_samples*100:.1f}%  (target ~80%)")
    print()


# ── Event generator ───────────────────────────────────────────────────────────
def make_event() -> dict:
    user_id     = random.choice(USER_IDS)
    item_id     = random.choices(ITEM_IDS, weights=ITEM_WEIGHTS, k=1)[0]
    # category_id encoded in item_id position for simplicity; derive deterministically
    item_idx    = int(item_id[1:]) - 1
    category_id = (item_idx % 50) + 1
    behavior    = random.choices(BEHAVIOR_TYPES, weights=BEHAVIOR_WEIGHTS, k=1)[0]
    ts          = int(time.time() * 1000)   # milliseconds

    return {
        "user_id":     user_id,
        "item_id":     item_id,
        "category_id": category_id,
        "behavior":    behavior,
        "timestamp":   ts
    }


# ── Producer ──────────────────────────────────────────────────────────────────
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=1,
        linger_ms=5,
        batch_size=32768,
    )


def run(burst: bool):
    validate_distribution()

    producer = create_producer()
    rate     = BURST_RATE if burst else NORMAL_RATE
    mode     = "BURST" if burst else "NORMAL"

    print(f"[Producer] Starting in {mode} mode @ {rate} events/s")
    print(f"[Producer] Topic: {TOPIC}  |  Kafka: {KAFKA_BOOTSTRAP}")
    if burst:
        print(f"[Producer] Burst will last {BURST_DURATION}s then switch to normal rate")
    print("[Producer] Press Ctrl+C to stop\n")

    burst_start = time.time() if burst else None
    sent = 0
    interval = 1.0 / rate

    try:
        while True:
            # Switch out of burst after BURST_DURATION seconds
            if burst and time.time() - burst_start > BURST_DURATION:
                burst      = False
                rate       = NORMAL_RATE
                interval   = 1.0 / rate
                print(f"\n[Producer] Burst ended. Switched to NORMAL mode @ {rate} events/s")

            t0    = time.time()
            event = make_event()
            producer.send(TOPIC, key=event["user_id"], value=event)
            sent += 1

            if sent % 1000 == 0:
                print(f"\r[Producer] Sent {sent:,} events ...", end="", flush=True)

            # Rate limiting via sleep
            elapsed = time.time() - t0
            sleep   = interval - elapsed
            if sleep > 0:
                time.sleep(sleep)

    except KeyboardInterrupt:
        print(f"\n[Producer] Stopping. Total sent: {sent:,}")
    finally:
        producer.flush()
        producer.close()


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mock user behavior event producer")
    parser.add_argument(
        "--burst",
        action="store_true",
        help=f"Trigger burst traffic ({BURST_RATE} events/s for {BURST_DURATION}s)",
    )
    args = parser.parse_args()
    run(burst=args.burst)
