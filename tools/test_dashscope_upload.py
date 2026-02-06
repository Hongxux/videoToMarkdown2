"""
测试 DashScope File.upload 功能

验证大视频文件是否能通过 DashScope SDK 上传并获取临时 URL
"""

import asyncio
import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))


async def test_dashscope_upload(video_path: str, api_key: str):
    """测试 DashScope File.upload"""
    
    print(f"\n{'='*80}")
    print(f"测试 DashScope File.upload")
    print(f"{'='*80}\n")
    
    # 检查文件
    if not Path(video_path).exists():
        print(f"❌ 文件不存在: {video_path}")
        return
    
    file_size = Path(video_path).stat().st_size
    file_size_mb = file_size / (1024 * 1024)
    print(f"📦 文件信息:")
    print(f"   - 路径: {video_path}")
    print(f"   - 大小: {file_size_mb:.2f} MB ({file_size:,} bytes)")
    
    # 导入 dashscope
    try:
        import dashscope
        print(f"\n✅ dashscope SDK 已安装")
        print(f"   - 版本: {dashscope.__version__ if hasattr(dashscope, '__version__') else 'unknown'}")
    except ImportError as e:
        print(f"\n❌ dashscope SDK 未安装: {e}")
        return
    
    # 设置 API Key
    dashscope.api_key = api_key
    print(f"\n🔑 API Key: {api_key[:20]}...{api_key[-10:]}")
    
    # 测试上传
    print(f"\n🚀 开始上传...")
    
    def _upload():
        """同步上传函数"""
        with open(video_path, "rb") as f:
            resp = dashscope.File.upload(
                file=f,
                model_name="qwen3-vl-plus"
            )
        return resp
    
    try:
        # 异步执行上传
        resp = await asyncio.to_thread(_upload)
        
        print(f"\n📊 上传响应:")
        print(f"   - 类型: {type(resp)}")
        
        # 解析响应
        status_code = getattr(resp, "status_code", None)
        output = getattr(resp, "output", None)
        message = getattr(resp, "message", None)
        request_id = getattr(resp, "request_id", None)
        
        print(f"   - status_code: {status_code}")
        print(f"   - request_id: {request_id}")
        
        if status_code == 200:
            print(f"\n✅ 上传成功!")
            
            if output and isinstance(output, dict):
                url = output.get("url")
                file_id = output.get("file_id")
                
                print(f"\n📎 文件信息:")
                print(f"   - file_id: {file_id}")
                print(f"   - URL: {url}")
                
                if url:
                    print(f"\n✅ 获取到临时 URL,可以用于 VL API 调用")
                    return url
            else:
                print(f"\n⚠️  output 格式异常: {output}")
        else:
            print(f"\n❌ 上传失败!")
            print(f"   - message: {message}")
            print(f"   - 完整响应: {resp}")
        
        # 兼容 dict 形式返回
        if isinstance(resp, dict):
            print(f"\n📋 Dict 形式响应:")
            print(f"   - status_code: {resp.get('status_code')}")
            if resp.get('status_code') == 200:
                output_dict = resp.get('output', {})
                url = output_dict.get('url')
                print(f"   - URL: {url}")
                if url:
                    return url
        
    except Exception as e:
        print(f"\n❌ 上传异常: {e}")
        import traceback
        traceback.print_exc()
    
    return None


async def test_with_vl_analyzer(video_path: str):
    """使用 VL Analyzer 的上传方法测试"""
    
    print(f"\n{'='*80}")
    print(f"测试 VL Analyzer 的 DashScope 上传集成")
    print(f"{'='*80}\n")
    
    from MVP_Module2_HEANCING.module2_content_enhancement.vl_video_analyzer import VLVideoAnalyzer
    from MVP_Module2_HEANCING.module2_content_enhancement.config_loader import load_module2_config
    
    # 加载配置
    config = load_module2_config()
    vl_config = config.get("vl_material_generation", {})
    
    # 初始化分析器
    analyzer = VLVideoAnalyzer(vl_config)
    
    # 测试上传
    print(f"🚀 调用 _try_get_dashscope_temp_url...")
    try:
        temp_url = await analyzer._try_get_dashscope_temp_url(video_path)
        
        if temp_url:
            print(f"\n✅ 上传成功!")
            print(f"   - 临时 URL: {temp_url}")
            
            # 测试完整的消息构建
            print(f"\n🔍 测试完整消息构建...")
            messages = await analyzer._build_messages(video_path)
            
            content = messages[0]["content"]
            video_urls = [item for item in content if item.get("type") == "video_url"]
            image_urls = [item for item in content if item.get("type") == "image_url"]
            
            print(f"\n📊 消息分析:")
            print(f"   - 视频 URL: {len(video_urls)} 个")
            print(f"   - 图片 URL: {len(image_urls)} 个")
            
            if video_urls:
                url = video_urls[0]["video_url"]["url"]
                if url.startswith("http"):
                    print(f"\n✅ 使用 DashScope 临时 URL 策略")
                    print(f"   - URL: {url[:100]}...")
                else:
                    print(f"\n⚠️  使用其他策略: {url[:50]}...")
            
            if image_urls:
                print(f"\n⚠️  降级到关键帧模式 ({len(image_urls)} 帧)")
        else:
            print(f"\n⚠️  上传失败或不可用,系统会降级到关键帧")
            
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


async def main():
    """主函数"""
    
    # 获取配置
    from MVP_Module2_HEANCING.module2_content_enhancement.config_loader import load_module2_config
    config = load_module2_config()
    vl_config = config.get("vl_material_generation", {})
    api_key = vl_config.get("api", {}).get("api_key", "")
    
    if not api_key:
        print(f"❌ 未配置 API Key")
        return
    
    # 查找测试视频
    video_path = "storage/20225626c2a19253c4121f684ecdff12/video.mp4"
    
    if len(sys.argv) > 1:
        video_path = sys.argv[1]
    
    if not Path(video_path).exists():
        print(f"❌ 视频文件不存在: {video_path}")
        return
    
    # 测试1: 直接使用 dashscope SDK
    print(f"\n{'#'*80}")
    print(f"# 测试 1: 直接使用 dashscope.File.upload")
    print(f"{'#'*80}")
    
    temp_url = await test_dashscope_upload(video_path, api_key)
    
    # 测试2: 使用 VL Analyzer 集成
    print(f"\n\n{'#'*80}")
    print(f"# 测试 2: VL Analyzer 集成测试")
    print(f"{'#'*80}")
    
    await test_with_vl_analyzer(video_path)
    
    print(f"\n\n{'='*80}")
    print(f"测试完成")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    asyncio.run(main())
