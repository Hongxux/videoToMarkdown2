"""
测试 VL 大文件上传功能

验证三级降级策略:
1. data-uri (< 6.75MB)
2. DashScope File.upload (需要 dashscope SDK)
3. 关键帧抽取 (降级方案)
"""

import asyncio
import sys
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from MVP_Module2_HEANCING.module2_content_enhancement.vl_video_analyzer import VLVideoAnalyzer
from MVP_Module2_HEANCING.module2_content_enhancement.config_loader import load_module2_config


async def test_video_upload(video_path: str):
    """测试视频上传功能"""
    
    print(f"\n{'='*80}")
    print(f"测试视频: {video_path}")
    print(f"{'='*80}\n")
    
    # 检查文件是否存在
    if not Path(video_path).exists():
        print(f"❌ 文件不存在: {video_path}")
        return
    
    # 获取文件大小
    file_size = Path(video_path).stat().st_size
    file_size_mb = file_size / (1024 * 1024)
    print(f"📦 文件大小: {file_size_mb:.2f} MB ({file_size:,} bytes)")
    
    # 判断预期策略
    max_data_uri_bytes = 6.75 * 1024 * 1024  # 约 6.75MB (base64后约9MB)
    if file_size <= max_data_uri_bytes:
        print(f"✅ 预期策略: data-uri (文件 < 6.75MB)")
    else:
        print(f"⚠️  预期策略: DashScope上传 或 关键帧 (文件 > 6.75MB)")
    
    # 加载配置
    print(f"\n📋 加载配置...")
    config = load_module2_config()
    vl_config = config.get("vl_material_generation", {})
    
    # 显示配置
    api_config = vl_config.get("api", {})
    print(f"   - video_input_mode: {api_config.get('video_input_mode', 'auto')}")
    print(f"   - max_input_frames: {api_config.get('max_input_frames', 6)}")
    print(f"   - max_image_dim: {api_config.get('max_image_dim', 1280)}")
    
    # 初始化分析器
    print(f"\n🚀 初始化 VL 分析器...")
    analyzer = VLVideoAnalyzer(vl_config)
    
    # 测试消息构建 (不实际调用API)
    print(f"\n🔍 测试消息构建...")
    try:
        messages = await analyzer._build_messages(video_path)
        
        # 分析消息类型
        content = messages[0]["content"]
        
        # 统计内容类型
        video_urls = [item for item in content if item.get("type") == "video_url"]
        image_urls = [item for item in content if item.get("type") == "image_url"]
        texts = [item for item in content if item.get("type") == "text"]
        
        print(f"\n📊 消息内容分析:")
        print(f"   - 视频 URL: {len(video_urls)} 个")
        print(f"   - 图片 URL: {len(image_urls)} 个")
        print(f"   - 文本: {len(texts)} 个")
        
        if video_urls:
            video_url = video_urls[0]["video_url"]["url"]
            if video_url.startswith("data:video"):
                print(f"\n✅ 使用策略: data-uri")
                print(f"   - Base64 长度: {len(video_url):,} 字符")
                base64_size_mb = len(video_url) / (1024 * 1024)
                print(f"   - Base64 大小: {base64_size_mb:.2f} MB")
                if base64_size_mb > 10:
                    print(f"   ⚠️  警告: 超过 10MB 限制!")
            elif video_url.startswith("http"):
                print(f"\n✅ 使用策略: DashScope 临时 URL")
                print(f"   - URL: {video_url[:100]}...")
        
        if image_urls:
            print(f"\n✅ 使用策略: 关键帧抽取")
            print(f"   - 关键帧数量: {len(image_urls)}")
            
            # 检查每帧大小
            for idx, img_item in enumerate(image_urls[:3]):  # 只检查前3帧
                data_uri = img_item["image_url"]["url"]
                if data_uri.startswith("data:image"):
                    frame_size_mb = len(data_uri) / (1024 * 1024)
                    print(f"   - Frame {idx+1}: {frame_size_mb:.2f} MB")
        
        print(f"\n✅ 消息构建成功!")
        
    except Exception as e:
        print(f"\n❌ 消息构建失败: {e}")
        import traceback
        traceback.print_exc()


async def main():
    """主函数"""
    
    print(f"\n{'='*80}")
    print(f"VL 大文件上传功能测试")
    print(f"{'='*80}")
    
    # 查找测试视频
    test_dirs = [
        Path("d:/videoToMarkdownTest2/test_videos"),
        Path("d:/videoToMarkdownTest2/semantic_unit_clips"),
        Path("d:/videoToMarkdownTest2/output/semantic_unit_clips"),
    ]
    
    test_videos = []
    for test_dir in test_dirs:
        if test_dir.exists():
            test_videos.extend(list(test_dir.glob("*.mp4")))
    
    if not test_videos:
        print(f"\n⚠️  未找到测试视频文件")
        print(f"\n请提供视频路径进行测试:")
        print(f"   python test_vl_upload.py <video_path>")
        return
    
    # 按文件大小排序
    test_videos.sort(key=lambda p: p.stat().st_size)
    
    # 选择不同大小的视频进行测试
    selected_videos = []
    
    # 小文件 (< 5MB)
    small = [v for v in test_videos if v.stat().st_size < 5 * 1024 * 1024]
    if small:
        selected_videos.append(small[0])
    
    # 中等文件 (5-15MB)
    medium = [v for v in test_videos if 5 * 1024 * 1024 <= v.stat().st_size < 15 * 1024 * 1024]
    if medium:
        selected_videos.append(medium[0])
    
    # 大文件 (> 15MB)
    large = [v for v in test_videos if v.stat().st_size >= 15 * 1024 * 1024]
    if large:
        selected_videos.append(large[0])
    
    # 如果没有选中任何视频,使用前3个
    if not selected_videos:
        selected_videos = test_videos[:3]
    
    # 测试每个视频
    for video_path in selected_videos:
        await test_video_upload(str(video_path))
        print(f"\n")
    
    print(f"\n{'='*80}")
    print(f"测试完成")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    # 支持命令行参数
    if len(sys.argv) > 1:
        video_path = sys.argv[1]
        asyncio.run(test_video_upload(video_path))
    else:
        asyncio.run(main())
