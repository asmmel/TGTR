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
    """Instagram Downloader с ПРОВЕРЕННЫМИ рабочими прокси"""
    
    def __init__(self, downloads_dir="downloads"):
        super().__init__(downloads_dir)
        
        # РЕАЛЬНЫЕ рабочие прокси (ЗАМЕНИТЕ НА СВОИ!)
        self.working_proxies = [
            "posledtp52:TiCBNGs8sq@63.125.90.106:50100",
            "posledtp52:TiCBNGs8sq@72.9.186.194:50100", 
            "posledtp52:TiCBNGs8sq@5.133.163.38:50100"
        ]
        
        self.current_proxy_index = 0
        
        # Создаем сессии
        self.api_session = None
        self.download_session = None
        self._init_sessions()
        
        logger.info("✅ InstagramDownloader инициализирован с проверкой прокси")
    
    def _init_sessions(self):
        """Инициализация сессий с проверкой прокси"""
        # API сессия с прокси
        self.api_session = requests.Session()
        self._setup_proxy_session()
        
        # Сессия для скачивания БЕЗ прокси
        self.download_session = requests.Session()
        self.download_session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "video/webm,video/ogg,video/*;q=0.9,*/*;q=0.5",
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
        })
        
        logger.info("🔧 Сессии инициализированы")
    
    def _setup_proxy_session(self):
        """Настройка прокси для API сессии с ПРОВЕРКОЙ"""
        if not self.working_proxies:
            logger.warning("⚠️ Нет настроенных прокси!")
            return
        
        proxy_string = self.working_proxies[self.current_proxy_index]
        proxy_url = f"http://{proxy_string}"
        
        # Настраиваем прокси
        self.api_session.proxies = {
            'http': proxy_url,
            'https': proxy_url
        }
        
        # Специальные заголовки для Instagram API
        self.api_session.headers.update({
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0"
        })
        
        # КРИТИЧНО: Проверяем что прокси РЕАЛЬНО работает
        try:
            logger.info(f"🔍 Проверяем прокси: {proxy_string}")
            
            # Тестируем прокси на реальном запросе
            test_response = self.api_session.get(
                'https://httpbin.org/ip', 
                timeout=15,
                allow_redirects=True
            )
            
            if test_response.status_code == 200:
                response_data = test_response.json()
                proxy_ip = response_data.get('origin', 'unknown')
                logger.info(f"✅ Прокси #{self.current_proxy_index + 1} работает! IP: {proxy_ip}")
                
                # Дополнительная проверка - можем ли мы достучаться до Instagram
                try:
                    ig_test = self.api_session.get(
                        'https://www.instagram.com/',
                        timeout=10,
                        allow_redirects=True
                    )
                    if ig_test.status_code == 200:
                        logger.info("✅ Instagram доступен через прокси")
                    else:
                        logger.warning(f"⚠️ Instagram вернул статус {ig_test.status_code} через прокси")
                except Exception as e:
                    logger.warning(f"⚠️ Проблема доступа к Instagram через прокси: {e}")
                    
            else:
                logger.error(f"❌ Прокси не работает! Статус: {test_response.status_code}")
                self._rotate_proxy()
                
        except Exception as e:
            logger.error(f"❌ Критическая ошибка прокси: {e}")
            self._rotate_proxy()
    
    def _rotate_proxy(self):
        """Переключение на следующий прокси"""
        if not self.working_proxies:
            return
            
        old_index = self.current_proxy_index
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.working_proxies)
        
        logger.info(f"🔄 Переключаемся с прокси #{old_index + 1} на #{self.current_proxy_index + 1}")
        self._setup_proxy_session()
    
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
    
    async def get_page_content_via_proxy(self, shortcode: str) -> Optional[str]:
        """Получение HTML страницы Instagram через прокси"""
        url = f"https://www.instagram.com/reel/{shortcode}/"
        
        # Попробуем несколько раз с разными прокси
        for attempt in range(len(self.working_proxies)):
            try:
                logger.info(f"🌐 Запрос к {url} через прокси #{self.current_proxy_index + 1} (попытка {attempt + 1})")
                
                # Делаем запрос через прокси
                response = self.api_session.get(
                    url,
                    timeout=20,
                    allow_redirects=True,
                    verify=True  # Проверяем SSL
                )
                
                logger.info(f"📊 Ответ: {response.status_code}, размер: {len(response.text)} байт")
                
                if response.status_code == 200:
                    if 'video_url' in response.text or 'videoUrl' in response.text:
                        logger.info("✅ Найдены видео данные в HTML")
                        return response.text
                    else:
                        logger.warning("⚠️ HTML получен, но видео данных нет")
                        
                elif response.status_code == 403:
                    logger.warning(f"❌ 403 Forbidden через прокси #{self.current_proxy_index + 1}")
                    self._rotate_proxy()
                    await asyncio.sleep(2)
                    continue
                    
                elif response.status_code == 429:
                    logger.warning(f"❌ 429 Rate Limit через прокси #{self.current_proxy_index + 1}")
                    self._rotate_proxy()
                    await asyncio.sleep(5)
                    continue
                    
                else:
                    logger.warning(f"❌ Неожиданный статус {response.status_code}")
                    self._rotate_proxy()
                    await asyncio.sleep(2)
                    continue
                    
            except requests.exceptions.ProxyError as e:
                logger.error(f"❌ Ошибка прокси: {e}")
                self._rotate_proxy()
                await asyncio.sleep(2)
                continue
                
            except requests.exceptions.Timeout as e:
                logger.error(f"❌ Таймаут прокси: {e}")
                self._rotate_proxy()
                await asyncio.sleep(2)
                continue
                
            except Exception as e:
                logger.error(f"❌ Неожиданная ошибка: {e}")
                self._rotate_proxy()
                await asyncio.sleep(2)
                continue
        
        logger.error("❌ Все прокси исчерпаны!")
        return None
    
    def extract_video_url_from_html(self, html_content: str) -> Optional[str]:
        """Извлечение URL видео из HTML страницы"""
        try:
            # Паттерны для поиска видео URL
            patterns = [
                r'"video_url":"([^"]+)"',
                r'"videoUrl":"([^"]+)"',
                r'videoUrl":\s*"([^"]+)"',
                r'"src":"([^"]+\.mp4[^"]*)"',
                r'https://[^"]*\.cdninstagram\.com/[^"]*\.mp4[^"]*'
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, html_content)
                for match in matches:
                    # Декодируем экранированные символы
                    video_url = match.replace('\\u0026', '&').replace('\/', '/')
                    
                    # Проверяем что это похоже на видео URL
                    if '.mp4' in video_url and ('cdninstagram.com' in video_url or 'fbcdn.net' in video_url):
                        logger.info(f"🎯 Найден video_url: {video_url[:100]}...")
                        return video_url
            
            logger.warning("❌ video_url не найден в HTML")
            return None
            
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга HTML: {e}")
            return None
    
    async def download_video_direct(self, video_url: str, output_path: str) -> bool:
        """Загрузка видео БЕЗ прокси"""
        try:
            logger.info(f"📥 Прямая загрузка видео (БЕЗ прокси)")
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "video/webm,video/ogg,video/*;q=0.9,*/*;q=0.5",
                "Referer": "https://www.instagram.com/",
                "Origin": "https://www.instagram.com"
            }
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Скачиваем БЕЗ прокси
            response = self.download_session.get(
                video_url, 
                headers=headers, 
                stream=True, 
                timeout=60
            )
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            logger.info(f"💾 Сохранение: {total_size / (1024*1024):.2f} MB")
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
            
            logger.info(f"✅ Загружено: {downloaded / (1024*1024):.2f} MB")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
            return False
    
    async def download_via_ytdlp(self, url: str, output_path: str) -> bool:
        """Fallback через yt-dlp"""
        try:
            logger.info(f"🔄 Fallback: yt-dlp")
            
            import yt_dlp
            
            ydl_opts = {
                'format': 'best[ext=mp4]/best',
                'outtmpl': output_path,
                'quiet': True,
                'no_warnings': True,
            }
            
            # Если есть рабочий прокси, используем его для yt-dlp
            if self.working_proxies:
                proxy_string = self.working_proxies[self.current_proxy_index]
                ydl_opts['proxy'] = f"http://{proxy_string}"
                logger.info(f"yt-dlp использует прокси: {proxy_string}")
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: yt_dlp.YoutubeDL(ydl_opts).download([url])
            )
            
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                logger.info("✅ yt-dlp успешно загрузил видео")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Ошибка yt-dlp: {e}")
            return False
    
    async def extract_video_info(self, url: str) -> Dict:
        """Получение информации о видео"""
        shortcode = self.extract_shortcode(url)
        return {
            'title': f'Instagram Video {shortcode}',
            'duration': 0,
            'thumbnail': '',
            'uploader': 'Instagram User',
            'formats': [],
            'is_live': False
        }
    
    async def download_video(self, url: str, output_path: str = None) -> Optional[str]:
        """Главный метод загрузки"""
        if not output_path:
            output_path = self.generate_output_filename("instagram")
        
        try:
            logger.info(f"🚀 Загрузка Instagram видео: {url}")
            
            shortcode = self.extract_shortcode(url)
            if not shortcode:
                logger.error("❌ Не удалось извлечь shortcode")
                return None
            
            # МЕТОД 1: Получаем HTML через прокси, скачиваем напрямую
            html_content = await self.get_page_content_via_proxy(shortcode)
            if html_content:
                video_url = self.extract_video_url_from_html(html_content)
                if video_url:
                    success = await self.download_video_direct(video_url, output_path)
                    if success:
                        logger.info("✅ Успешно через прокси + прямое скачивание")
                        return output_path
            
            logger.warning("Метод 1 не сработал, пробуем yt-dlp...")
            
            # МЕТОД 2: yt-dlp с прокси
            success = await self.download_via_ytdlp(url, output_path)
            if success:
                logger.info("✅ Успешно через yt-dlp")
                return output_path
            
            logger.error("❌ Все методы провалились")
            
            # Удаляем частично загруженный файл
            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except:
                    pass
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
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