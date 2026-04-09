#!/usr/bin/env python3
"""
init_dim_data.py
-----------------
Generate dimension data for 1000 users and 5000 items.
Writes to:
  - SQLite: data/dim.db  (tables: dim_user, dim_item)
  - Redis:  dim:user:{user_id} -> Hash
            dim:item:{item_id} -> Hash

Usage:
    python scripts/init_dim_data.py
"""

import os
import sqlite3
import random
import redis
from faker import Faker

# ── Config ──────────────────────────────────────────────────────────────────
SQLITE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "dim.db")
REDIS_HOST  = "localhost"
REDIS_PORT  = 6379
KAFKA_BOOTSTRAP = "localhost:19092"  # 宿主机访问 Kafka 用此地址

NUM_USERS   = 1000
NUM_ITEMS   = 5000

# Dimension enumerations
CITIES      = ["北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "西安", "南京", "重庆"]
USER_LEVELS = [1, 2, 3, 4, 5]   # 1=普通, 5=VIP
BRANDS      = ["Apple", "Samsung", "Huawei", "Xiaomi", "OPPO", "Nike", "Adidas", "Uniqlo", "H&M", "ZARA"]
CATEGORIES  = list(range(1, 51))  # 50 categories

fake = Faker("zh_CN")
random.seed(42)

# ── SQLite ───────────────────────────────────────────────────────────────────
def init_sqlite(users: list, items: list):
    os.makedirs(os.path.dirname(SQLITE_PATH), exist_ok=True)
    conn = sqlite3.connect(SQLITE_PATH)
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_user (
            user_id   TEXT PRIMARY KEY,
            age       INTEGER,
            city      TEXT,
            level     INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_item (
            item_id     TEXT PRIMARY KEY,
            category_id INTEGER,
            brand       TEXT,
            price       REAL
        )
    """)

    cur.execute("DELETE FROM dim_user")
    cur.execute("DELETE FROM dim_item")

    cur.executemany("INSERT INTO dim_user VALUES (?,?,?,?)", users)
    cur.executemany("INSERT INTO dim_item VALUES (?,?,?,?)", items)

    conn.commit()
    conn.close()
    print(f"[SQLite] Written {len(users)} users and {len(items)} items -> {SQLITE_PATH}")


# ── Redis ────────────────────────────────────────────────────────────────────
def init_redis(users: list, items: list):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pipe = r.pipeline(transaction=False)

    for user_id, age, city, level in users:
        key = f"dim:user:{user_id}"
        pipe.hset(key, mapping={"age": age, "city": city, "level": level})

    for item_id, category_id, brand, price in items:
        key = f"dim:item:{item_id}"
        pipe.hset(key, mapping={"category_id": category_id, "brand": brand, "price": price})

    pipe.execute()
    print(f"[Redis]  Written {len(users)} users and {len(items)} items")


# ── Data generation ──────────────────────────────────────────────────────────
def generate_users() -> list:
    users = []
    for i in range(1, NUM_USERS + 1):
        user_id = f"U{i:06d}"
        age     = random.randint(18, 65)
        city    = random.choice(CITIES)
        level   = random.choices(USER_LEVELS, weights=[40, 30, 15, 10, 5])[0]
        users.append((user_id, age, city, level))
    return users


def generate_items() -> list:
    items = []
    for i in range(1, NUM_ITEMS + 1):
        item_id     = f"I{i:07d}"
        category_id = random.choice(CATEGORIES)
        brand       = random.choice(BRANDS)
        price       = round(random.uniform(9.9, 9999.0), 2)
        items.append((item_id, category_id, brand, price))
    return items


# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Generating dimension data ...")
    users = generate_users()
    items = generate_items()

    init_sqlite(users, items)

    try:
        init_redis(users, items)
    except redis.exceptions.ConnectionError as e:
        print(f"[Redis]  WARNING: Could not connect to Redis ({e}). Skipping Redis write.")
        print("[Redis]  Make sure 'docker compose up' is running before this step.")

    print("\nDone. Dimension data is ready.")
    print(f"  Users: {len(users)}  (IDs: U000001 ~ U{NUM_USERS:06d})")
    print(f"  Items: {len(items)}  (IDs: I0000001 ~ I{NUM_ITEMS:07d})")
