from pathlib import Path
import os

from external.prompt import USER_PROMPT
from prompt import SYSTEM_PROMPT
from dotenv import load_dotenv
from openai import OpenAI


ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"

load_dotenv(ENV_PATH, override=True)

# 直接在这里改参数
MODEL = "deepseek-v4-pro"



client=OpenAI(
    api_key=os.getenv('DEEPSEEK_API_KEY'),
    base_url="https://api.deepseek.com",
)  # 直接使用环境变量中的 API Key 和 Base URL 初始化客户端
response = client.chat.completions.create(
    model=MODEL,
    messages=[
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": USER_PROMPT},
    ],
)

print(response.choices[0].message.content)