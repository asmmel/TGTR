import re
import json
import time
import requests
import logging
import os
import asyncio
from typing import Optional, Dict, Tuple, Any
from datetime import datetime
from config.config import setup_logging
from services.base_downloader import BaseDownloader

logger = setup_logging(__name__)

class InstagramDownloader(BaseDownloader):
    """Загрузчик для Instagram с улучшенной архитектурой и fallback методами"""
    
    def __init__(self, downloads_dir="downloads"):
        super().__init__(downloads_dir)
        
        # ИСПРАВЛЕНО: Инициализируем все атрибуты в правильном порядке
        self.current_proxy_index = 0
        
        # Реальные рабочие прокси (замените на свои)
        self.working_proxies = [
            "posledtp52:TiCBNGs8sq@63.125.90.106:50100",
            "posledtp52:TiCBNGs8sq@72.9.186.194:50100", 
            "posledtp52:TiCBNGs8sq@5.133.163.38:50100"
        ]
        
        # Сессия с прокси для API запросов
        self.api_session = requests.Session()
        
        # Сессия без прокси для скачивания файлов
        self.download_session = requests.Session()
        self.download_session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "video/webm,video/ogg,video/*;q=0.9,application/ogg;q=0.7,audio/*;q=0.6,*/*;q=0.5",
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
        })
        
        # Инициализируем прокси
        self.setup_api_session_with_proxy()
        
        logger.info("InstagramDownloader инициализирован с улучшенной архитектурой")
    
    def setup_api_session_with_proxy(self):
        """Настройка сессии с прокси для API запросов"""
        try:
            if self.working_proxies:
                proxy_string = self.working_proxies[self.current_proxy_index]
                proxy_url = f"http://{proxy_string}"
                
                proxy_config = {
                    'http': proxy_url,
                    'https': proxy_url
                }
                
                self.api_session.proxies.update(proxy_config)
                self.api_session.headers.update({
                    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
                    "Accept": "*/*",
                    "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
                })
                
                logger.info(f"API сессия настроена с прокси: {proxy_string}")
            else:
                logger.warning("Прокси не настроены, используется прямое соединение для API")
                
        except Exception as e:
            logger.error(f"Ошибка настройки прокси: {e}")
            logger.info("Переключаемся на прямое соединение для API")
    
    def rotate_proxy(self):
        """Переключение на следующий прокси"""
        if self.working_proxies:
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.working_proxies)
            self.setup_api_session_with_proxy()
            logger.info(f"Переключились на прокси #{self.current_proxy_index + 1}")
    
    def extract_shortcode(self, url: str) -> Optional[str]:
        """Извлечение shortcode из URL Instagram"""
        patterns = [
            r'/p/([A-Za-z0-9_-]+)/',     # Posts
            r'/reel/([A-Za-z0-9_-]+)/',  # Reels
            r'/tv/([A-Za-z0-9_-]+)/',    # IGTV
            r'/([A-Za-z0-9_-]+)/?$'      # Direct shortcode
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        return None
    
    async def fallback_to_ytdlp(self, url: str, output_path: str) -> bool:
        """Резервный метод через yt-dlp (проверенно работает!)"""
        try:
            logger.info(f"🔄 Fallback: загрузка через yt-dlp: {url}")
            
            import yt_dlp
            
            # Оптимизированные настройки для Instagram
            ydl_opts = {
                'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                'outtmpl': output_path,
                'quiet': True,
                'no_warnings': True,
                'extract_flat': False,
                'no_color': True,
                'merge_output_format': 'mp4',
                'prefer_ffmpeg': True,
                'retries': 5,  # Увеличено количество попыток
                'fragment_retries': 5,
                'skip_unavailable_fragments': True,
                'http_headers': {
                    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15'
                }
            }
            
            # Запускаем в отдельном потоке
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: yt_dlp.YoutubeDL(ydl_opts).download([url])
            )
            
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                logger.info(f"✅ Успешная загрузка через yt-dlp: {output_path}")
                return True
            else:
                logger.error("❌ yt-dlp не создал файл или файл пустой")
                return False
                
        except Exception as e:
            logger.error(f"❌ Ошибка yt-dlp: {e}")
            return False
    
    async def try_instagram_api_method(self, url: str, output_path: str) -> bool:
        """Попытка через Instagram API (может не работать из-за блокировок)"""
        try:
            logger.info(f"🔄 Попытка через Instagram API: {url}")
            
            shortcode = self.extract_shortcode(url)
            if not shortcode:
                return False
            
            # Пробуем получить данные поста
            for attempt in range(3):
                try:
                    # Базовый запрос к странице
                    post_url = f"https://www.instagram.com/reel/{shortcode}/"
                    response = self.api_session.get(post_url, timeout=15)
                    
                    if response.status_code == 200:
                        # Ищем video_url в HTML
                        content = response.text
                        video_pattern = r'"video_url":"([^"]+)"'
                        match = re.search(video_pattern, content)
                        
                        if match:
                            video_url = match.group(1).replace('\\u0026', '&')
                            logger.info(f"🎯 Найден video_url в HTML: {video_url[:100]}...")
                            
                            # Скачиваем без прокси
                            success = await self.download_video_direct(video_url, output_path)
                            if success:
                                logger.info("✅ Успешная загрузка через Instagram API")
                                return True
                    
                    elif response.status_code == 403:
                        logger.warning(f"403 ошибка, переключаем прокси (попытка {attempt + 1})")
                        self.rotate_proxy()
                        await asyncio.sleep(2)
                        continue
                    
                except Exception as e:
                    logger.warning(f"Ошибка в попытке {attempt + 1}: {e}")
                    if attempt < 2:
                        self.rotate_proxy()
                        await asyncio.sleep(2)
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка Instagram API метода: {e}")
            return False
    
    async def download_video_direct(self, url: str, output_path: str) -> bool:
        """Загрузка видео БЕЗ прокси (прямое соединение)"""
        try:
            logger.info(f"📥 Прямая загрузка видео: {url[:100]}...")
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "video/webm,video/ogg,video/*;q=0.9,*/*;q=0.5",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.instagram.com/",
                "Origin": "https://www.instagram.com"
            }
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Выполняем запрос БЕЗ прокси
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.download_session.get(url, headers=headers, stream=True, timeout=60)
            )
            
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            logger.info(f"💾 Сохранение в: {output_path}")
            if total_size > 0:
                logger.info(f"📊 Размер файла: {total_size / (1024*1024):.2f} MB")
            
            with open(output_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
                        downloaded_size += len(chunk)
            
            logger.info(f"✅ Видео загружено: {downloaded_size / (1024*1024):.2f} MB")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка прямой загрузки: {e}")
            return False
    
    async def extract_video_info(self, url: str) -> Dict:
        """Извлечение информации о видео"""
        try:
            shortcode = self.extract_shortcode(url)
            return {
                'title': f'Instagram Video {shortcode}',
                'duration': 0,
                'thumbnail': '',
                'uploader': 'Instagram User',
                'formats': [],
                'is_live': False
            }
        except Exception as e:
            logger.error(f"Ошибка при извлечении информации: {e}")
            return {}
    
    async def download_video(self, url: str, output_path: str = None) -> Optional[str]:
        """Главный метод загрузки с приоритетом на рабочие методы"""
        if not output_path:
            output_path = self.generate_output_filename("instagram")
        
        try:
            logger.info(f"🚀 Начинаем загрузку Instagram видео: {url}")
            
            # ПРИОРИТЕТ 1: yt-dlp (проверенно работает по логам!)
            success = await self.fallback_to_ytdlp(url, output_path)
            if success and os.path.exists(output_path):
                logger.info(f"✅ Успешная загрузка через yt-dlp: {output_path}")
                return output_path
            
            logger.warning("yt-dlp не сработал, пробуем Instagram API...")
            
            # ПРИОРИТЕТ 2: Instagram API (может не работать)
            success = await self.try_instagram_api_method(url, output_path)
            if success and os.path.exists(output_path):
                logger.info(f"✅ Успешная загрузка через Instagram API: {output_path}")
                return output_path
            
            # Если всё не удалось
            logger.error(f"❌ Все методы загрузки провалились для: {url}")
            
            # Удаляем частично загруженный файл
            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except:
                    pass
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при загрузке: {e}")
            
            # Удаляем частично загруженный файл
            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except:
                    pass
            
            return None
    
    def __del__(self):
        """Очистка ресурсов"""
        try:
            if hasattr(self, 'api_session'):
                self.api_session.close()
            if hasattr(self, 'download_session'):
                self.download_session.close()
        except:
            pass