# recommend-service/tests/test_stream_debug.py
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import fakeredis
from langchain_core.messages import HumanMessage
from agent.orchestrator import stream, run
from dotenv import load_dotenv
load_dotenv()  # 加载 .env 文件

# 使用 fakeredis，不需要启动真实 Redis
rc = fakeredis.FakeRedis(decode_responses=True)

# 手动往 fakeredis 塞一点测试数据
rc.hset("user:1001:features", mapping={
    "age": "25",
    "gender": "M",
    "city": "Beijing"
})

messages = [HumanMessage(content="请帮我查一下用户1001的特征")]

# 测试 run 函数
print("=== 测试 run ===")
result = run(messages, rc)
print(f"结果: {result}")

# 测试 stream 函数
print("\n=== 测试 stream ===")
for chunk in stream(messages, rc):
    print(f"Chunk: {chunk[:300] if len(chunk) > 300 else chunk}")