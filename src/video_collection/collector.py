"""
TikTok Video Collector Module

Handles downloading videos from TikTok using TikTokApi and yt-dlp for downloading.
"""

import asyncio
import os
import re
import httpx
import yt_dlp
import tempfile
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from loguru import logger
from TikTokApi import TikTokApi
from TikTokApi.exceptions import TikTokException

from src.utils.file_manager import FileManager


@dataclass
class VideoMetadata:
    """Metadata for a TikTok video."""
    
    id: str
    author: str
    desc: str
    create_time: int
    duration: float
    height: int
    width: int
    cover: str
    download_url: str
    play_url: str
    music_author: str
    music_title: str
    likes: int = 0
    shares: int = 0
    comments: int = 0
    views: int = 0
    local_path: Optional[str] = None
    url: Optional[str] = None  # Original URL
    
    def to_dict(self) -> Dict:
        """
        Convert metadata to dictionary.
        
        Returns:
            Dictionary representation of metadata
        """
        return {
            "id": self.id,
            "author": self.author,
            "desc": self.desc,
            "create_time": self.create_time,
            "duration": self.duration,
            "height": self.height,
            "width": self.width,
            "cover": self.cover,
            "download_url": self.download_url,
            "play_url": self.play_url,
            "music_author": self.music_author,
            "music_title": self.music_title,
            "likes": self.likes,
            "shares": self.shares,
            "comments": self.comments,
            "views": self.views,
            "local_path": self.local_path,
            "url": self.url
        }


class TikTokCollector:
    """Collects and downloads TikTok videos."""
    
    def __init__(self, config, file_manager: Optional[FileManager] = None):
        """
        Initialize the TikTok collector.
        
        Args:
            config: Application configuration
            file_manager: Optional file manager instance
        """
        self.config = config
        self.tiktok_config = config.tiktok
        self.file_manager = file_manager or FileManager()
        self.api = None
        self.initialized = False
        self.logger = logger
        self.video_id_patterns = [
            r'https?://(?:www\.)?tiktok\.com/@[\w.-]+/video/(\d+)',
            r'https?://(?:m\.)?tiktok\.com/v/(\d+)',
            r'https?://(?:vm|vt)\.tiktok\.com/(\w+)'
        ]
    
    async def _initialize_api(self) -> None:
        """Initialize the TikTok API."""
        try:
            if not self.initialized:
                # For TikTokApi v7.1.0
                logger.info("Initializing TikTokApi v7.1.0")
                
                # Create a temporary data directory if it doesn't exist
                data_dir = Path("./data/tiktok_api")
                data_dir.mkdir(parents=True, exist_ok=True)
                
                # Initialize the API
                self.api = TikTokApi()
                
                # Create sessions with proper browser emulation
                await self.api.create_sessions(
                    num_sessions=1,
                    headless=False,
                    ms_tokens=[self.tiktok_config.ms_token] if hasattr(self.tiktok_config, 'ms_token') else None,
                    cookies=[{
                        'name': 'sessionid',
                        'value': self.tiktok_config.session_id if hasattr(self.tiktok_config, 'session_id') else None
                    }] if hasattr(self.tiktok_config, 'session_id') else None,
                    browser="chromium",
                    sleep_after=3
                )
                
                self.initialized = True
                logger.debug("TikTok API initialized successfully")
        
        except Exception as e:
            logger.error(f"Error initializing TikTok API: {str(e)}")
            if self.api:
                await self.cleanup()
            raise

    async def cleanup(self) -> None:
        """Cleanup TikTok API resources."""
        try:
            if self.api:
                await self.api.close_sessions()
                self.api = None
                self.initialized = False
                self.logger.debug("TikTok API cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Error during TikTok API cleanup: {str(e)}")

    def _extract_video_id(self, url: str) -> Optional[str]:
        """
        Extract video ID from a TikTok URL.
        
        Args:
            url: TikTok video URL
            
        Returns:
            Video ID if found, None otherwise
        """
        for pattern in self.video_id_patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        logger.warning(f"Could not extract video ID from URL: {url}")
        return None

    def _construct_video_url(self, video_id: str) -> str:
        """
        Construct a TikTok video URL from an ID.
        
        Args:
            video_id: TikTok video ID
            
        Returns:
            TikTok video URL
        """
        return f"https://www.tiktok.com/@placeholder/video/{video_id}"
    
    async def _get_video_info(self, video_id: str, original_url: str = None) -> VideoMetadata:
        """Get video information from TikTok API."""
        if not self.api:
            raise Exception("TikTok API not initialized")

        try:
            # Construct the video URL if not provided
            if not original_url:
                original_url = f"https://www.tiktok.com/video/{video_id}"

            # Get video data using TikTokApi v7.1.0 method
            video = self.api.video(url=original_url)
            video_data = await video.info()
            
            if not video_data:
                raise Exception(f"Failed to get video data for ID: {video_id}")

            # Extract video information from the response
            author = video_data.get('author', {}).get('uniqueId', '')
            desc = video_data.get('desc', '')
            create_time = video_data.get('createTime', 0)
            duration = video_data.get('video', {}).get('duration', 0)
            height = video_data.get('video', {}).get('height', 0)
            width = video_data.get('video', {}).get('width', 0)
            cover = video_data.get('video', {}).get('cover', '')
            download_url = video_data.get('video', {}).get('downloadAddr', '')
            play_url = video_data.get('video', {}).get('playAddr', '')
            
            # Music information
            music_author = video_data.get('music', {}).get('authorName', '')
            music_title = video_data.get('music', {}).get('title', '')
            
            # Statistics
            stats = video_data.get('stats', {})
            likes = stats.get('diggCount', 0)
            shares = stats.get('shareCount', 0)
            comments = stats.get('commentCount', 0)
            views = stats.get('playCount', 0)

            return VideoMetadata(
                id=video_id,
                author=author,
                desc=desc,
                create_time=create_time,
                duration=duration,
                height=height,
                width=width,
                cover=cover,
                download_url=download_url,
                play_url=play_url,
                music_author=music_author,
                music_title=music_title,
                likes=likes,
                shares=shares,
                comments=comments,
                views=views,
                url=original_url
            )

        except Exception as e:
            self.logger.error(f"Failed to get video info for {video_id}: {str(e)}")
            raise
    
    def _download_with_ytdlp(self, url: str, output_path: str) -> bool:
        """
        Download a video using yt-dlp.
        
        Args:
            url: Video URL to download
            output_path: Path to save the video
            
        Returns:
            True if download was successful, False otherwise
        """
        temp_dir = tempfile.mkdtemp()
        temp_output = os.path.join(temp_dir, 'video.mp4')
        
        try:
            # Configure yt-dlp options
            ydl_opts = {
                'format': 'best',
                'outtmpl': temp_output,
                'quiet': True,
                'no_warnings': True,
                'ignoreerrors': False,
                'noplaylist': True,
                'cookiesfrombrowser': ('chrome',),  # Try to use browser cookies
                'noprogress': True
            }
            
            # Download the video
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
            
            # Check if the file was downloaded
            if os.path.exists(temp_output):
                # Move to the final destination
                shutil.copy2(temp_output, output_path)
                return True
            else:
                return False
                
        except Exception as e:
            logger.error(f"yt-dlp download error: {str(e)}")
            return False
        finally:
            # Clean up temporary directory
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                logger.warning(f"Error removing temporary directory: {str(e)}")
    
    async def _download_video(self, video_metadata: VideoMetadata) -> Optional[str]:
        """
        Download a TikTok video using provided metadata.
        
        Args:
            video_metadata: Video metadata including download URL
            
        Returns:
            Path to downloaded video if successful, None otherwise
        """
        try:
            video_id = video_metadata.id
            logger.info(f"Downloading video {video_id} by @{video_metadata.author}")
            
            # Generate output path
            output_path = self.file_manager.get_download_path(video_id)
            
            # Use original URL if available
            video_url = video_metadata.url
            
            # Download using yt-dlp (synchronously)
            loop = asyncio.get_event_loop()
            success = await loop.run_in_executor(
                None, 
                self._download_with_ytdlp, 
                video_url, 
                output_path
            )
            
            if not success:
                logger.error(f"Failed to download video {video_id} with yt-dlp")
                return None
            
            # Update metadata with local path
            video_metadata.local_path = output_path
            logger.success(f"Video {video_id} downloaded to {output_path}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Error downloading video {video_metadata.id}: {str(e)}")
            return None
    
    async def download_videos(self, urls: List[str]) -> List[VideoMetadata]:
        """
        Download videos from a list of TikTok URLs.
        
        Args:
            urls: List of TikTok video URLs
            
        Returns:
            List of VideoMetadata objects for successfully downloaded videos
        """
        try:
            # Ensure API is initialized
            await self._initialize_api()
            
            # Map URLs to video IDs
            url_to_id = {}
            for url in urls:
                url = url.strip()
                video_id = self._extract_video_id(url)
                if video_id:
                    url_to_id[url] = video_id
            
            if not url_to_id:
                self.logger.error("No valid TikTok URLs provided")
                return []
            
            self.logger.info(f"Extracted {len(url_to_id)} video IDs from URLs")
            
            # Get metadata and download videos
            results = []
            for url, video_id in url_to_id.items():
                # Get video metadata
                metadata = await self._get_video_info(video_id, url)
                if metadata:
                    # Download the video
                    download_path = await self._download_video(metadata)
                    if download_path:
                        results.append(metadata)
            
            self.logger.info(f"Successfully downloaded {len(results)} videos")
            return results
            
        except Exception as e:
            self.logger.error(f"Error downloading videos: {str(e)}")
            return []
        finally:
            # Clean up the API resources
            await self.cleanup()


# Example usage
if __name__ == "__main__":
    import asyncio
    from src.utils.config_loader import ConfigLoader
    from src.utils.logger_config import setup_logger
    
    # Setup logger
    setup_logger("DEBUG")
    
    # Load configuration
    config = ConfigLoader().get_config()
    
    # Create TikTok collector
    collector = TikTokCollector(config)
    
    # Example TikTok URLs
    urls = [
        "https://www.tiktok.com/@username/video/1234567890123456789",
        "https://vm.tiktok.com/abcdefg/"
    ]
    
    # Download videos
    async def main():
        results = await collector.download_videos(urls)
        print(f"Downloaded {len(results)} videos")
    
    asyncio.run(main()) 