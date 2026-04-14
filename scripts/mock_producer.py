#!/usr/bin/env python3
"""
mock_producer.py
-----------------
Simulate real-time user behavior event stream -> Kafka topic: ods_user_behavior

Designed for recommendation system tracing platform.

Behavior funnel (recommendation scenario):
  show -> click -> cart/fav -> buy

Features:
  - Session model: each user visit generates a session with causally linked behaviors
  - Power-law user activity: top 10% users generate ~60% of traffic
  - Power-law item popularity: top 20% items get ~80% of traffic (Zipf)
  - Hourly traffic pattern: morning/noon/evening peaks
  - Recommendation tracing fields: session_id, req_id, rec_source, position
  - Normal rate: ~100 events/s (adjusted by hourly multiplier)
  - Burst mode: --burst flag triggers 5x rate for 30s

Usage:
    python scripts/mock_producer.py           # Normal mode
    python scripts/mock_producer.py --burst   # Backpressure test
"""

import argparse
import json
import math
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:19092"
TOPIC           = "ods_user_behavior"

NUM_USERS       = 1000
NUM_ITEMS       = 5000
NORMAL_RATE     = 100   # base events/s (before hourly multiplier)
BURST_RATE      = 500   # events/s in burst mode
BURST_DURATION  = 30    # seconds

# ── Behavior funnel ───────────────────────────────────────────────────────────
# CTR: show->click 10%, then click may lead to cart/fav/buy
CLICK_RATE       = 0.10   # show -> click
CART_RATE        = 0.15   # click -> cart
FAV_RATE         = 0.10   # click -> fav (independent of cart)
BUY_FROM_CART    = 0.20   # cart -> buy
BUY_FROM_CLICK   = 0.03   # click -> buy (without cart, impulse purchase)

# ── Recommendation sources ────────────────────────────────────────────────────
REC_SOURCES  = ["recommend", "search", "direct", "ad"]
REC_WEIGHTS  = [50, 25, 15, 10]

# ── Item position distribution (items ranked higher shown more) ───────────────
MAX_POSITION = 20

# ── ID lists ─────────────────────────────────────────────────────────────────
ITEM_IDS = [f"I{i:07d}" for i in range(1, NUM_ITEMS + 1)]
USER_IDS = [f"U{i:06d}" for i in range(1, NUM_USERS + 1)]

random.seed(None)  # non-deterministic

# ── Power-law distributions ───────────────────────────────────────────────────
def build_zipf_weights(n: int, exponent: float = 1.2) -> list:
    return [1.0 / math.pow(i, exponent) for i in range(1, n + 1)]

ITEM_WEIGHTS = build_zipf_weights(NUM_ITEMS, exponent=1.2)
# User activity: softer exponent so top users dominate less aggressively
USER_WEIGHTS = build_zipf_weights(NUM_USERS, exponent=0.8)


# ── Hourly traffic multiplier ─────────────────────────────────────────────────
def get_hourly_multiplier() -> float:
    """
    Model three daily peaks for e-commerce:
      - 08:00 morning (browse before commute)
      - 12:00 noon    (lunch break shopping)
      - 20:00 evening (main peak, post-work)
    Range: ~0.3x (3am low) to ~1.3x (8pm peak)
    """
    hour = datetime.now().hour
    base  = 0.3
    peak1 = 0.4 * math.exp(-((hour - 8)  ** 2) / 8)
    peak2 = 0.3 * math.exp(-((hour - 12) ** 2) / 6)
    peak3 = 1.0 * math.exp(-((hour - 20) ** 2) / 10)
    return base + peak1 + peak2 + peak3


# ── Session generator ─────────────────────────────────────────────────────────
def make_session(session_ts: int) -> list:
    """
    Generate a list of behavior events for one user session.

    A session models a single user visit:
      1. Pick a user (power-law activity distribution)
      2. Generate a recommendation request (req_id) for a list of items
      3. For each item in the list: emit show event
      4. Each show has CLICK_RATE chance -> emit click
      5. Each click may lead to cart, fav, buy (with configured rates)
      6. Timestamps within session are monotonically increasing

    Returns a list of event dicts ordered by timestamp.
    """
    user_id    = random.choices(USER_IDS, weights=USER_WEIGHTS, k=1)[0]
    rec_source = random.choices(REC_SOURCES, weights=REC_WEIGHTS, k=1)[0]
    req_id     = f"req_{uuid.uuid4().hex[:8]}"
    session_id = f"{user_id}_{session_ts}"

    # Number of items shown in this recommendation request (Poisson, mean=3)
    n_items = min(max(1, int(random.expovariate(1 / 3))), MAX_POSITION)
    shown_items = random.choices(ITEM_IDS, weights=ITEM_WEIGHTS, k=n_items)

    events = []
    ts = session_ts  # session-local timestamp cursor (ms)

    for position, item_id in enumerate(shown_items, start=1):
        item_idx    = int(item_id[1:]) - 1
        category_id = (item_idx % 50) + 1

        def make_ev(behavior: str, pos: int = 0) -> dict:
            nonlocal ts
            ts += random.randint(500, 8000)  # 0.5s ~ 8s between actions
            return {
                "user_id":     user_id,
                "item_id":     item_id,
                "category_id": category_id,
                "behavior":    behavior,
                "timestamp":   ts,
                "session_id":  session_id,
                "req_id":      req_id,
                "rec_source":  rec_source,
                "position":    pos,
            }

        # show event always emitted
        events.append(make_ev("show", position))

        # click with CTR probability
        if random.random() < CLICK_RATE:
            events.append(make_ev("click"))

            # cart after click
            if random.random() < CART_RATE:
                events.append(make_ev("cart"))
                # buy after cart (CVR)
                if random.random() < BUY_FROM_CART:
                    events.append(make_ev("buy"))

            # fav after click (independent of cart)
            if random.random() < FAV_RATE:
                events.append(make_ev("fav"))

            # impulse buy without cart
            if random.random() < BUY_FROM_CLICK:
                events.append(make_ev("buy"))

    return events


# ── Startup validation ────────────────────────────────────────────────────────
def validate_distribution(n_sessions: int = 500):
    print(f"\n[Validation] Simulating {n_sessions} sessions ...\n")

    all_events = []
    t0 = int(time.time() * 1000)
    for i in range(n_sessions):
        all_events.extend(make_session(t0 + i * 5000))

    total  = len(all_events)
    counts = {b: sum(1 for e in all_events if e["behavior"] == b)
              for b in ["show", "click", "cart", "fav", "buy"]}

    print("  Behavior distribution:")
    for b, c in counts.items():
        bar = "#" * int(c / total * 80)
        print(f"    {b:5s}: {c:6d} ({c/total*100:5.1f}%)  {bar}")

    shows  = counts["show"]
    clicks = counts["click"]
    buys   = counts["buy"]
    ctr    = clicks / shows if shows > 0 else 0
    cvr    = buys / clicks if clicks > 0 else 0
    print(f"\n  CTR (click/show):  {ctr*100:.1f}%  (target ~{CLICK_RATE*100:.0f}%)")
    print(f"  CVR (buy/click):   {cvr*100:.1f}%  (target ~{(BUY_FROM_CART*CART_RATE + BUY_FROM_CLICK)*100:.1f}%)")

    # User hotspot
    user_counts = {}
    for e in all_events:
        user_counts[e["user_id"]] = user_counts.get(e["user_id"], 0) + 1
    sorted_users = sorted(user_counts.values(), reverse=True)
    top10_traffic = sum(sorted_users[:NUM_USERS // 10])
    print(f"\n  User hotspot (top 10% = {NUM_USERS//10} users):")
    print(f"    Traffic share: {top10_traffic/total*100:.1f}%  (target ~60%)")

    # Item hotspot
    item_counts = {}
    for e in all_events:
        item_counts[e["item_id"]] = item_counts.get(e["item_id"], 0) + 1
    sorted_items = sorted(item_counts.values(), reverse=True)
    top20_traffic = sum(sorted_items[:NUM_ITEMS // 5])
    print(f"\n  Item hotspot (top 20% = {NUM_ITEMS//5} items):")
    print(f"    Traffic share: {top20_traffic/total*100:.1f}%  (target ~80%)")
    print()


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

    producer    = create_producer()
    mode        = "BURST" if burst else "NORMAL"
    burst_start = time.time() if burst else None
    sent        = 0

    print(f"[Producer] Starting in {mode} mode")
    print(f"[Producer] Topic: {TOPIC}  |  Kafka: {KAFKA_BOOTSTRAP}")
    if burst:
        print(f"[Producer] Burst: {BURST_RATE} events/s for {BURST_DURATION}s then normal")
    print("[Producer] Press Ctrl+C to stop\n")

    # Session-based event buffer: generate sessions, drain events at target rate
    event_buffer = []
    session_ts   = int(time.time() * 1000)

    try:
        while True:
            # Switch out of burst mode after duration
            if burst and time.time() - burst_start > BURST_DURATION:
                burst = False
                print(f"\n[Producer] Burst ended. Switched to NORMAL mode")

            # Refill buffer with new sessions when running low
            if len(event_buffer) < 50:
                session_ts += random.randint(1000, 5000)
                event_buffer.extend(make_session(session_ts))

            # Determine target rate
            if burst:
                target_rate = BURST_RATE
            else:
                multiplier  = get_hourly_multiplier()
                target_rate = max(10, int(NORMAL_RATE * multiplier))

            interval = 1.0 / target_rate

            t0    = time.time()
            event = event_buffer.pop(0)
            # Use current wall-clock time for the event timestamp to keep it fresh
            event["timestamp"] = int(time.time() * 1000)
            producer.send(TOPIC, key=event["user_id"], value=event)
            sent += 1

            if sent % 1000 == 0:
                hour = datetime.now().hour
                mult = get_hourly_multiplier()
                print(f"\r[Producer] Sent {sent:,} events | rate ~{int(NORMAL_RATE*mult)}/s | hour={hour:02d}h | buffer={len(event_buffer)}",
                      end="", flush=True)

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
    parser = argparse.ArgumentParser(description="Mock recommendation behavior event producer")
    parser.add_argument(
        "--burst",
        action="store_true",
        help=f"Trigger burst traffic ({BURST_RATE} events/s for {BURST_DURATION}s)",
    )
    args = parser.parse_args()
    run(burst=args.burst)
