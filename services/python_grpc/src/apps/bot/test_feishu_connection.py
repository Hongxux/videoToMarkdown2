
import os
import requests
import json

APP_ID = os.getenv("FEISHU_APP_ID")
APP_SECRET = os.getenv("FEISHU_APP_SECRET")

if not APP_ID or not APP_SECRET:
    print("❌ 错误: 请设置环境变量 FEISHU_APP_ID 和 FEISHU_APP_SECRET")
    exit(1)

def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers = {
        "Content-Type": "application/json; charset=utf-8"
    }
    payload = {
        "app_id": APP_ID,
        "app_secret": APP_SECRET
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        
        if data.get("code") == 0:
            print(f"✅ 成功获取 Tenant Access Token! App ID 和 Secret 有效。")
            print(f"Tenant Access Token: {data.get('tenant_access_token')[:10]}...")
            return data.get("tenant_access_token")
        else:
            print(f"❌ 获取 Token 失败: {data.get('msg')}")
            return None
    except Exception as e:
        print(f"❌ 请求异常: {e}")
        return None

if __name__ == "__main__":
    print(f"正在测试 App ID: {APP_ID}")
    get_tenant_access_token()
