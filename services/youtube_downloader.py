import yt_dlp
import os
import aiohttp
from typing import Dict, List, Optional
from services.base_downloader import BaseDownloader

class YouTubeDownloader(BaseDownloader):
    """Загрузчик для YouTube с поддержкой резервных методов"""
    
    def __init__(self, downloads_dir="downloads"):
        super().__init__(downloads_dir)
        self.cobalt_downloader = None  # Ленивая инициализация
    
    async def extract_video_info(self, url: str) -> Dict:
        """Извлекает информацию о видео с YouTube"""
        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': False,
                'skipdownload': True,
                'no_color': True,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                
                return {
                    'title': info.get('title', ''),
                    'duration': info.get('duration', 0),
                    'thumbnail': info.get('thumbnail', ''),
                    'uploader': info.get('uploader', ''),
                    'view_count': info.get('view_count', 0),
                    'formats': info.get('formats', []),
                    'is_live': info.get('is_live', False),
                }
        except Exception as e:
            self.logger.error(f"Ошибка при получении информации о видео: {e}")
            return {}
    
    async def download_video(self, url: str, output_path: str = None) -> Optional[str]:
        """Загружает видео с YouTube с поддержкой резервных методов"""
        if not output_path:
            output_path = self.generate_output_filename("youtube")
        
        temp_path = output_path + ".temp"
        
        # Метод 1: yt-dlp
        try:
            self.logger.info(f"Попытка загрузки через yt-dlp: {url}")
            
            ydl_opts = {
                'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                'outtmpl': temp_path,
                'quiet': True,
                'no_warnings': True,
                'extract_flat': False,
                'no_color': True,
                'merge_output_format': 'mp4',
                'prefer_ffmpeg': True,
                'retries': 3,
                'fragment_retries': 3,
                'skip_unavailable_fragments': True,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
                
                if os.path.exists(temp_path):
                    os.replace(temp_path, output_path)
                    self.logger.info(f"Успешная загрузка через yt-dlp: {output_path}")
                    return output_path
        except Exception as e:
            self.logger.warning(f"Ошибка загрузки через yt-dlp: {e}")
            # Метод 1 не сработал, пробуем Метод 2
        
        # Метод 2: Cobalt API
        try:
            self.logger.info(f"Попытка загрузки через Cobalt API: {url}")
            
            # Ленивая инициализация Cobalt загрузчика
            if not self.cobalt_downloader:
                from services.cobalt import CobaltDownloader
                self.cobalt_downloader = CobaltDownloader()
            
            # Проверяем, что Cobalt загрузчик инициализирован
            if self.cobalt_downloader:
                # Пробуем загрузить через Cobalt
                cobalt_path = await self.cobalt_downloader.download_video(url)
                
                if cobalt_path and os.path.exists(cobalt_path):
                    # Перемещаем в нужное место
                    os.replace(cobalt_path, output_path)
                    self.logger.info(f"Успешная загрузка через Cobalt API: {output_path}")
                    return output_path
            else:
                self.logger.warning("Cobalt загрузчик не инициализирован")
        except Exception as e:
            self.logger.warning(f"Ошибка загрузки через Cobalt API: {e}")
            # Метод 2 не сработал, пробуем Метод 3
        
        # Метод 3: Прямое скачивание ссылки
        try:
            self.logger.info(f"Попытка прямой загрузки видео: {url}")
            
            # Получаем прямую ссылку на видео
            direct_urls = await self._extract_direct_urls(url)
            
            if not direct_urls:
                raise ValueError("Не удалось получить прямые ссылки на видео")
            
            # Пробуем каждую ссылку
            for direct_url in direct_urls:
                success = await self._download_with_session(direct_url, output_path)
                if success:
                    self.logger.info(f"Успешная прямая загрузка: {output_path}")
                    return output_path
        except Exception as e:
            self.logger.error(f"Ошибка при прямой загрузке: {e}")
        
        # Если все методы не сработали
        self.logger.error(f"Все методы загрузки не удались для URL: {url}")
        return None
    
    async def _extract_direct_urls(self, url: str) -> List[str]:
        """Извлекает прямые ссылки на видео из YouTube URL"""
        try:
            info = await self.extract_video_info(url)
            formats = info.get('formats', [])
            
            # Фильтруем форматы с прямыми URL
            direct_urls = []
            
            for format_info in formats:
                if format_info.get('url') and format_info.get('ext') == 'mp4':
                    direct_urls.append(format_info['url'])
            
            # Сортируем по качеству
            return direct_urls
        except Exception as e:
            self.logger.error(f"Ошибка при извлечении прямых ссылок: {e}")
            return []