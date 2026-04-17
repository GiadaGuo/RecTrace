#!/usr/bin/env python3
"""
mock_producer.py
-----------------
Simulate real-time user behavior event stream -> Kafka topic: ods_user_behavior

Generates 8 event types matching Section 2.2 of ARCHITECTURE.md:

  bhv_id=1001  home   show   – homepage recommendation exposure (items list)
  bhv_id=2001  home   click  – homepage item click
  bhv_id=1001  search show   – search result exposure (items list + query)
  bhv_id=2001  search click  – search result click (query + item_id)
  bhv_id=2001  pdp    click  – product detail page view
  bhv_id=3001  pdp    click  fav  – favourite from PDP
  bhv_id=3001  pdp    click  cart – add-to-cart from PDP
  bhv_id=3001  pdp    click  buy  – purchase from PDP

Routing design (single Kafka topic):
  Downstream Jobs first split by bhv_id (1001/2001/3001), then by bhv_page
  (home/search/pdp) to handle page-specific logic.

Features:
  - Causally linked events within a session (show -> click -> pdp -> fav/cart/buy)
  - Power-law item popularity (Zipf exponent=1.2: top 20% items ~80% traffic)
  - Power-law user activity  (Zipf exponent=0.8: top 10% users ~60% traffic)
  - Hourly traffic multiplier: morning/noon/evening peaks
  - req_id transparently passed through the entire session funnel
  - Normal rate: ~100 events/s, Burst mode: --burst flag (500 events/s, 30s)

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

# ── Behavior funnel probabilities ─────────────────────────────────────────────
# Home recommendation funnel
HOME_CTR           = 0.10   # show -> click (homepage)
SEARCH_CTR         = 0.18   # show -> click (search, higher intent)
PDP_FROM_HOME      = 0.70   # home click -> enter PDP
PDP_FROM_SEARCH    = 0.65   # search click -> enter PDP
CART_RATE          = 0.15   # PDP -> cart
FAV_RATE           = 0.10   # PDP -> fav (independent of cart)
BUY_FROM_CART      = 0.20   # cart -> buy
BUY_FROM_PDP       = 0.03   # PDP -> buy directly (impulse)

# ── Scene mix ─────────────────────────────────────────────────────────────────
# Probability that a session is a home-recommendation session vs search session
HOME_SESSION_RATE   = 0.65
SEARCH_SESSION_RATE = 0.35

# ── Search query pool (representative e-commerce queries) ─────────────────────
QUERY_POOL = [
    "耐克跑鞋", "Nike跑步鞋 男款", "夏季T恤", "连衣裙", "运动裤",
    "无线耳机", "蓝牙耳机", "苹果数据线", "手机壳", "充电宝",
    "口红", "护肤套装", "面膜", "洗面奶", "防晒霜",
    "电饭锅", "空气炸锅", "咖啡机", "榨汁机", "微波炉",
    "瑜伽垫", "哑铃", "跳绳", "健身手套", "运动水壶",
    "背包 男", "钱包 女", "手表", "太阳镜", "帽子",
    "小米手机", "华为手机壳", "iPad 保护套", "键盘 机械", "鼠标 无线",
    "儿童玩具", "积木", "绘本", "书包 小学生",
]
# Zipf weights for queries: some queries are searched much more often
QUERY_WEIGHTS = [1.0 / math.pow(i, 0.9) for i in range(1, len(QUERY_POOL) + 1)]

# ── ID lists ─────────────────────────────────────────────────────────────────
ITEM_IDS = [f"I{i:07d}" for i in range(1, NUM_ITEMS + 1)]
USER_IDS = [f"U{i:06d}" for i in range(1, NUM_USERS + 1)]

random.seed(None)

# ── Power-law distributions ───────────────────────────────────────────────────
def build_zipf_weights(n: int, exponent: float) -> list:
    return [1.0 / math.pow(i, exponent) for i in range(1, n + 1)]

ITEM_WEIGHTS = build_zipf_weights(NUM_ITEMS, exponent=1.2)
USER_WEIGHTS = build_zipf_weights(NUM_USERS, exponent=0.8)


# ── Hourly traffic multiplier ─────────────────────────────────────────────────
def get_hourly_multiplier() -> float:
    """
    Three daily peaks for e-commerce:
      08:00 morning  (browse before commute)
      12:00 noon     (lunch break)
      20:00 evening  (main peak, post-work)
    Range: ~0.3x (3am low) to ~1.3x (8pm peak)
    """
    hour  = datetime.now().hour
    base  = 0.3
    peak1 = 0.4 * math.exp(-((hour - 8)  ** 2) / 8)
    peak2 = 0.3 * math.exp(-((hour - 12) ** 2) / 6)
    peak3 = 1.0 * math.exp(-((hour - 20) ** 2) / 10)
    return base + peak1 + peak2 + peak3


# ── Helpers ───────────────────────────────────────────────────────────────────
def new_req_id() -> str:
    return f"req_{uuid.uuid4().hex[:8]}"

def new_log_id() -> str:
    return f"log_{uuid.uuid4().hex[:12]}"

def pick_items(n: int) -> list:
    """Pick n items following power-law popularity."""
    return random.choices(ITEM_IDS, weights=ITEM_WEIGHTS, k=n)

def pick_query() -> str:
    return random.choices(QUERY_POOL, weights=QUERY_WEIGHTS, k=1)[0]

def make_ev(uid: str, bhv_id: str, bhv_page: str,
            bhv_src: str, bhv_type: str, bhv_value: str, bhv_ext: dict,
            ts: int, req_id: str = None) -> dict:
    return {
        "uid":       uid,
        "logid":     new_log_id(),
        "req_id":    req_id,
        "bhv_id":    bhv_id,
        "bhv_page":  bhv_page,
        "bhv_src":   bhv_src,
        "bhv_type":  bhv_type,
        "bhv_value": bhv_value,
        "ts":        ts,
        "bhv_ext":   bhv_ext,
    }


# ── Session generators ────────────────────────────────────────────────────────

def make_home_session(session_ts: int) -> list:
    """
    Homepage recommendation session.

    Flow:
      1. Recommend a list of items -> home show (bhv_id=1001)
      2. Each item has HOME_CTR chance -> home click (bhv_id=2001)
      3. Each click has PDP_FROM_HOME chance -> pdp view (bhv_id=2001)
      4. PDP may lead to fav / cart / buy (bhv_id=3001)
    """
    uid        = random.choices(USER_IDS, weights=USER_WEIGHTS, k=1)[0]
    req_id     = new_req_id()
    ts         = session_ts

    # Number of items in recommendation list (Poisson-ish, capped)
    n_items     = min(max(1, int(random.expovariate(1 / 4))), 20)
    shown_items = pick_items(n_items)

    events = []

    def tick() -> int:
        nonlocal ts
        ts += random.randint(300, 6000)
        return ts

    # ── Home exposure (1 event, items list) ──────────────────────────────────
    items_ext = [{"item_id": iid, "position": pos}
                 for pos, iid in enumerate(shown_items, start=1)]
    events.append(make_ev(
        uid,
        bhv_id="1001", bhv_page="home", bhv_src="direct",
        bhv_type="show", bhv_value=None,
        bhv_ext={"items": items_ext},
        ts=tick(), req_id=req_id,
    ))

    # ── Click funnel for each shown item ─────────────────────────────────────
    for pos, item_id in enumerate(shown_items, start=1):
        if random.random() >= HOME_CTR:
            continue

        # Home click → directly eligible for PDP actions (no separate PDP view event)
        events.append(make_ev(
            uid,
            bhv_id="2001", bhv_page="home", bhv_src="direct",
            bhv_type="click", bhv_value=None,
            bhv_ext={"item_id": item_id, "position": pos},
            ts=tick(), req_id=req_id,
        ))

        if random.random() >= PDP_FROM_HOME:
            continue

        _append_pdp_actions(events, uid, item_id, req_id, tick, bhv_src="home")

    return events


def make_search_session(session_ts: int) -> list:
    """
    Search session.

    Flow:
      1. User searches a query -> search exposure (bhv_id=1001)
      2. Each item has SEARCH_CTR chance -> search click (bhv_id=2001)
      3. Each click has PDP_FROM_SEARCH chance -> pdp view (bhv_id=2001)
      4. PDP may lead to fav / cart / buy (bhv_id=3001)
    """
    uid        = random.choices(USER_IDS, weights=USER_WEIGHTS, k=1)[0]
    req_id     = new_req_id()
    query      = pick_query()
    ts         = session_ts

    n_items     = min(max(1, int(random.expovariate(1 / 5))), 30)
    shown_items = pick_items(n_items)

    events = []

    def tick() -> int:
        nonlocal ts
        ts += random.randint(300, 5000)
        return ts

    # ── Search exposure (1 event, items list + query) ─────────────────────────
    items_ext = [{"item_id": iid, "position": pos}
                 for pos, iid in enumerate(shown_items, start=1)]
    events.append(make_ev(
        uid,
        bhv_id="1001", bhv_page="search", bhv_src="direct",
        bhv_type="show", bhv_value=None,
        bhv_ext={"query": query, "items": items_ext},
        ts=tick(), req_id=req_id,
    ))

    # ── Click funnel ──────────────────────────────────────────────────────────
    for pos, item_id in enumerate(shown_items, start=1):
        if random.random() >= SEARCH_CTR:
            continue

        # Search click → directly eligible for PDP actions (no separate PDP view event)
        events.append(make_ev(
            uid,
            bhv_id="2001", bhv_page="search", bhv_src="direct",
            bhv_type="click", bhv_value=None,
            bhv_ext={"query": query, "item_id": item_id, "position": pos},
            ts=tick(), req_id=req_id,
        ))

        if random.random() >= PDP_FROM_SEARCH:
            continue

        _append_pdp_actions(events, uid, item_id, req_id, tick, bhv_src="search")

    return events


def _append_pdp_actions(events: list, uid: str,
                         item_id: str, req_id: str, tick, bhv_src: str = "home") -> None:
    """Append fav / cart / buy events for a PDP visit."""

    if random.random() < CART_RATE:
        events.append(make_ev(
            uid,
            bhv_id="3001", bhv_page="pdp", bhv_src=bhv_src,
            bhv_type="click", bhv_value="cart",
            bhv_ext={"item_id": item_id},
            ts=tick(), req_id=req_id,
        ))
        if random.random() < BUY_FROM_CART:
            events.append(make_ev(
                uid,
                bhv_id="3001", bhv_page="pdp", bhv_src=bhv_src,
                bhv_type="click", bhv_value="buy",
                bhv_ext={"item_id": item_id},
                ts=tick(), req_id=req_id,
            ))

    if random.random() < FAV_RATE:
        events.append(make_ev(
            uid,
            bhv_id="3001", bhv_page="pdp", bhv_src=bhv_src,
            bhv_type="click", bhv_value="fav",
            bhv_ext={"item_id": item_id},
            ts=tick(), req_id=req_id,
        ))

    if random.random() < BUY_FROM_PDP:
        events.append(make_ev(
            uid,
            bhv_id="3001", bhv_page="pdp", bhv_src=bhv_src,
            bhv_type="click", bhv_value="buy",
            bhv_ext={"item_id": item_id},
            ts=tick(), req_id=req_id,
        ))


def make_session(session_ts: int) -> list:
    """Randomly choose home or search session based on configured mix."""
    if random.random() < HOME_SESSION_RATE:
        return make_home_session(session_ts)
    return make_search_session(session_ts)


# ── Startup validation ────────────────────────────────────────────────────────
def validate_distribution(n_sessions: int = 500):
    print(f"\n[Validation] Simulating {n_sessions} sessions ...\n")

    all_events = []
    t0 = int(time.time() * 1000)
    for i in range(n_sessions):
        all_events.extend(make_session(t0 + i * 5000))

    total = len(all_events)

    # bhv_value distribution
    value_counts = {}
    for e in all_events:
        key = f"{e['bhv_page']}/{e['bhv_type']}/{e['bhv_value'] or '-'}"
        value_counts[key] = value_counts.get(key, 0) + 1

    print("  Event type distribution:")
    for k, c in sorted(value_counts.items(), key=lambda x: -x[1]):
        bar = "#" * int(c / total * 60)
        print(f"    {k:30s}: {c:6d} ({c/total*100:5.1f}%)  {bar}")

    # Funnel rates
    home_show   = sum(1 for e in all_events if e["bhv_page"] == "home"   and e["bhv_type"] == "show")
    home_click  = sum(1 for e in all_events if e["bhv_page"] == "home"   and e["bhv_type"] == "click")
    srch_show   = sum(1 for e in all_events if e["bhv_page"] == "search" and e["bhv_type"] == "show")
    srch_click  = sum(1 for e in all_events if e["bhv_page"] == "search" and e["bhv_type"] == "click")
    cart_count  = sum(1 for e in all_events if e["bhv_value"] == "cart")
    buy_count   = sum(1 for e in all_events if e["bhv_value"] == "buy")

    # home CTR is per-item, not per-show-event (show event has multiple items)
    home_items  = sum(
        len(e["bhv_ext"].get("items", []))
        for e in all_events if e["bhv_page"] == "home" and e["bhv_type"] == "show"
    )
    srch_items  = sum(
        len(e["bhv_ext"].get("items", []))
        for e in all_events if e["bhv_page"] == "search" and e["bhv_type"] == "show"
    )

    print(f"\n  Home  CTR  (click/item shown): {home_click/home_items*100:.1f}%  (target ~{HOME_CTR*100:.0f}%)"  if home_items > 0 else "")
    print(f"  Search CTR (click/item shown): {srch_click/srch_items*100:.1f}%  (target ~{SEARCH_CTR*100:.0f}%)" if srch_items > 0 else "")
    print(f"  Cart/buy ratio: {buy_count/cart_count*100:.1f}%  (target ~{BUY_FROM_CART*100:.0f}%)" if cart_count > 0 else "")

    # User hotspot
    user_counts = {}
    for e in all_events:
        user_counts[e["uid"]] = user_counts.get(e["uid"], 0) + 1
    sorted_users = sorted(user_counts.values(), reverse=True)
    top10 = sum(sorted_users[:NUM_USERS // 10])
    print(f"\n  User hotspot  (top 10%): {top10/total*100:.1f}%  (target ~60%)")

    # Item hotspot (across bhv_ext items lists and single item_id fields)
    item_counts = {}
    for e in all_events:
        ext = e.get("bhv_ext") or {}
        for it in ext.get("items", []):
            item_counts[it["item_id"]] = item_counts.get(it["item_id"], 0) + 1
        if ext.get("item_id"):
            item_counts[ext["item_id"]] = item_counts.get(ext["item_id"], 0) + 1
    sorted_items = sorted(item_counts.values(), reverse=True)
    top20 = sum(sorted_items[:NUM_ITEMS // 5])
    total_item_refs = sum(item_counts.values())
    print(f"  Item  hotspot (top 20%): {top20/total_item_refs*100:.1f}%  (target ~80%)\n")


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

    event_buffer = []
    session_ts   = int(time.time() * 1000)

    try:
        while True:
            if burst and time.time() - burst_start > BURST_DURATION:
                burst = False
                print(f"\n[Producer] Burst ended. Switched to NORMAL mode")

            if len(event_buffer) < 50:
                session_ts += random.randint(1000, 5000)
                event_buffer.extend(make_session(session_ts))

            if burst:
                target_rate = BURST_RATE
            else:
                multiplier  = get_hourly_multiplier()
                target_rate = max(10, int(NORMAL_RATE * multiplier))

            interval = 1.0 / target_rate

            t0    = time.time()
            event = event_buffer.pop(0)
            # Refresh timestamp to wall-clock time
            event["ts"] = int(time.time() * 1000)
            producer.send(TOPIC, key=event["uid"], value=event)
            sent += 1

            if sent % 1000 == 0:
                hour = datetime.now().hour
                mult = get_hourly_multiplier()
                print(f"\r[Producer] Sent {sent:,} events | rate ~{int(NORMAL_RATE*mult)}/s"
                      f" | hour={hour:02d}h | buffer={len(event_buffer)}",
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
