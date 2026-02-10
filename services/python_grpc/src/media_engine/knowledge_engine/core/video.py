"""
模块说明：视频转Markdown流程中的 video 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import os
import yt_dlp
from pathlib import Path
from .processing import BaseProcessor

class VideoProcessor(BaseProcessor):
    """类说明：VideoProcessor 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(self, on_progress=None, proxy=None, disable_ssl_verify=False):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - on_progress: 函数入参（类型：未标注）。
        - proxy: 函数入参（类型：未标注）。
        - disable_ssl_verify: 函数入参（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        super().__init__(on_progress=on_progress)
        self.proxy = proxy
        self.disable_ssl_verify = disable_ssl_verify

    def download(self, url: str, output_dir: str, filename: str = "video") -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self.proxy
        - 条件：ffmpeg_path
        - 条件：p == 'ffmpeg' or (os.path.exists(p) and os.path.isfile(p))
        依据来源（证据链）：
        - 输入参数：filename。
        - 对象内部状态：self.proxy。
        输入参数：
        - url: 函数入参（类型：str）。
        - output_dir: 目录路径（类型：str）。
        - filename: 函数入参（类型：str）。
        输出参数：
        - 字符串结果。"""
        os.makedirs(output_dir, exist_ok=True)
        # yt-dlp 的 template 不包含扩展名，它会自动添加
        output_template = os.path.join(output_dir, f"{filename}.%(ext)s")
        
        self.emit_progress("download", 0.1, f"准备下载: {url}")
        
        import sys
        
        # 尝试定位 ffmpeg
        ffmpeg_path = None
        possible_paths = [
            os.path.join(sys.prefix, 'Library', 'bin', 'ffmpeg.exe'), # Windows Conda
            os.path.join(sys.prefix, 'bin', 'ffmpeg'), # Linux/Mac Conda
            'ffmpeg' # System PATH
        ]
        
        for p in possible_paths:
            if p == 'ffmpeg' or (os.path.exists(p) and os.path.isfile(p)):
                ffmpeg_path = p
                break

        # 配置选项
        ydl_opts = {
            'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
            'outtmpl': output_template,
            'merge_output_format': 'mp4',
            'noplaylist': True,
            'progress_hooks': [self._progress_hook],
            'quiet': True,
            'no_warnings': True,
            # 性能优化与稳定性配置
            'concurrent_fragment_downloads': 8,    # 并发下载分片 (对B站Dash流有效)
            'http_chunk_size': 10 * 1024 * 1024,   # 10MB chunk
            'socket_timeout': 30,                  # 增加超时时间 (单位: 秒)
            'retries': 10,                         # 增加重试次数
            'fragment_retries': 10,                # 分片下载重试次数
            'nocheckcertificate': self.disable_ssl_verify,  # 可配置的 SSL 验证
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        }
        
        if self.proxy:
            ydl_opts['proxy'] = self.proxy
        
        if ffmpeg_path:
            ydl_opts['ffmpeg_location'] = ffmpeg_path
            self.emit_progress("download", 0.15, f"使用FFmpeg: {ffmpeg_path}")
        else:
             self.emit_progress("download", 0.15, "警告: 未找到FFmpeg，可能无法合并高清视频")
        
        self.emit_progress("download", 0.2, "初始化下载引擎...")
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                self.emit_progress("download", 0.3, "开始下载流...")
                ydl.download([url])
            
            self.emit_progress("download", 0.9, "下载完成，确认文件...")
            
            # 查找下载的文件 (不固定扩展名，支持 mp4, mkv, webm 等)
            valid_exts = {'.mp4', '.mkv', '.webm', '.mov', '.avi'}
            for file in os.listdir(output_dir):
                f_path = Path(output_dir) / file
                if file.startswith(filename) and f_path.suffix.lower() in valid_exts:
                    abs_path = str(f_path.absolute())
                    self.emit_progress("download", 1.0, f"视频就绪: {file}", data={"path": abs_path})
                    return abs_path
            
            raise FileNotFoundError(f"未在 {output_dir} 找到以 {filename} 开头的有效视频文件")
            
        except Exception as e:
            self.emit_progress("download", 0.0, f"下载失败: {str(e)}")
            raise RuntimeError(f"yt-dlp 执行失败: {str(e)}")

    def _progress_hook(self, d):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：d['status'] == 'downloading'
        - 条件：d['status'] == 'finished'
        依据来源（证据链）：
        - 输入参数：d。
        - 配置字段：status。
        输入参数：
        - d: 函数入参（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if d['status'] == 'downloading':
            # 计算百分比
            try:
                p = d.get('_percent_str', '0%').replace('%','')
                progress = float(p) / 100.0
                # 映射到 0.3 - 0.9 范围
                final_progress = 0.3 + (progress * 0.6)
                self.emit_progress("download", final_progress, f"下载中: {d.get('_percent_str')}")
            except:
                pass
        elif d['status'] == 'finished':
            self.emit_progress("download", 0.9, "下载完成，正在合并...")
    
    def detect_playlist(self, url: str) -> bool:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - url: 函数入参（类型：str）。
        输出参数：
        - 布尔判断结果。"""
        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': True,  # 只提取信息，不下载
                'socket_timeout': 30,
                'retries': 5,
                'proxy': self.proxy
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                # 检查是否有 entries（播放列表标志）
                return 'entries' in info and info['entries'] is not None
        except Exception as e:
            self.emit_progress("download", 0.0, f"检测播放列表失败: {str(e)}")
            return False
    
    def get_playlist_info(self, url: str) -> dict:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：'entries' not in info
        - 条件：entry
        依据来源（证据链）：
        输入参数：
        - url: 函数入参（类型：str）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': True,
                'socket_timeout': 30,
                'retries': 5,
                'proxy': self.proxy
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                
                if 'entries' not in info:
                    return None
                
                episodes = []
                for i, entry in enumerate(info['entries'], 1):
                    if entry:  # 有时 entries 可能包含 None
                        episodes.append({
                            "index": i,
                            "title": entry.get('title', f'Episode {i}'),
                            "url": entry.get('url', entry.get('webpage_url', ''))
                        })
                
                return {
                    "title": info.get('title', 'Unknown Playlist'),
                    "total_episodes": len(episodes),
                    "episodes": episodes
                }
        except Exception as e:
            self.emit_progress("download", 0.0, f"获取播放列表信息失败: {str(e)}")
            return None
    
    @staticmethod
    def parse_episode_range(range_str: str, total_episodes: int) -> list:
        """
        执行逻辑：
        1) 接收原始输入。
        2) 按规则解析为内部结构。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：规范化输入结构，提升下游稳定性。
        决策逻辑：
        - 条件：range_str.lower() == 'all'
        - 条件：'-' in part
        依据来源（证据链）：
        - 输入参数：range_str。
        输入参数：
        - range_str: 函数入参（类型：str）。
        - total_episodes: 函数入参（类型：int）。
        输出参数：
        - 列表结果（与输入或处理结果一一对应）。"""
        if range_str.lower() == 'all':
            return list(range(1, total_episodes + 1))
        
        episodes = set()
        parts = range_str.split(',')
        
        for part in parts:
            part = part.strip()
            if '-' in part:
                # 范围，例如 "1-5"
                try:
                    start, end = part.split('-')
                    start, end = int(start.strip()), int(end.strip())
                    episodes.update(range(start, end + 1))
                except:
                    pass
            else:
                # 单个数字
                try:
                    episodes.add(int(part))
                except:
                    pass
        
        # 过滤超出范围的集数
        valid_episodes = [e for e in sorted(episodes) if 1 <= e <= total_episodes]
        return valid_episodes
    
    def download_playlist(self, url: str, output_base_dir: str, episode_range: list = None) -> list:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not playlist_info
        - 条件：episode_range
        依据来源（证据链）：
        - 输入参数：episode_range。
        输入参数：
        - url: 函数入参（类型：str）。
        - output_base_dir: 目录路径（类型：str）。
        - episode_range: 函数入参（类型：list）。
        输出参数：
        - 列表结果（与输入或处理结果一一对应）。"""
        playlist_info = self.get_playlist_info(url)
        if not playlist_info:
            raise RuntimeError("无法获取播放列表信息")
        
        total = playlist_info['total_episodes']
        episodes_to_download = episode_range if episode_range else list(range(1, total + 1))
        
        self.emit_progress("download", 0.0, f"准备下载 {len(episodes_to_download)} 集（共 {total} 集）")
        
        downloaded_videos = []
        
        for i, episode_index in enumerate(episodes_to_download, 1):
            episode = playlist_info['episodes'][episode_index - 1]
            
            # 为每一集创建独立目录
            episode_dir = os.path.join(output_base_dir, f"episode_{episode_index:02d}", "downloads")
            
            self.emit_progress("download", i / len(episodes_to_download), 
                             f"下载第 {episode_index} 集: {episode['title'][:30]}...")
            
            try:
                video_path = self.download(episode['url'], episode_dir, filename="video")
                downloaded_videos.append({
                    "index": episode_index,
                    "title": episode['title'],
                    "path": video_path,
                    "output_dir": os.path.dirname(episode_dir)  # episode_XX 目录
                })
            except Exception as e:
                self.emit_progress("download", -1, f"第 {episode_index} 集下载失败: {str(e)}")
        
        self.emit_progress("download", 1.0, f"完成！成功下载 {len(downloaded_videos)} 集")
        return downloaded_videos
