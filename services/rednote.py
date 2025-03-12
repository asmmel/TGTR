import requests
import json
import asyncio
import re
import os
import logging
from urllib.parse import urlparse
from random import choice
from typing import Optional, Dict, Tuple
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class XHSDownloader:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 YaBrowser/25.2.0.0 Safari/537.36",
            "Referer": "https://www.xiaohongshu.com/",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Accept-Language": "ru,en;q=0.9,de;q=0.8,pt;q=0.7",
            "Origin": "https://www.xiaohongshu.com",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "cross-site",
            "Priority": "u=1, i",
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def get_video_info(self, url: str) -> Tuple[bool, str, Optional[Dict]]:
        try:
            item_id = url.split('/')[-1].split('?')[0] if '/' in url else url
            if not item_id:
                return False, "Неверный URL: ID не найден", None

            response = self.session.get(url, allow_redirects=True)
            if response.status_code != 200:
                return False, f"Ошибка HTTP {response.status_code}", None

            response.encoding = 'utf-8'
            final_url = response.url
            logger.info(f"Финальный URL после редиректов: {final_url}")

            soup = BeautifulSoup(response.text, 'html.parser')
            mp4_pattern = re.compile(r'https?://sns-video-[a-z-]+\.xhscdn\.com/stream/[^"]+\.mp4')
            html_text = response.text
            
            mp4_matches = mp4_pattern.findall(html_text)
            if mp4_matches:
                video_url = mp4_matches[0]
                title = soup.title.string if soup.title else "Untitled"
                logger.info(f"Найден URL видео через regex: {video_url}")
                return True, "Успешно получена информация", {"video_url": video_url, "title": title}

            for script in soup.find_all('script'):
                if script.string:
                    mp4_in_script = mp4_pattern.search(script.string)
                    if mp4_in_script:
                        video_url = mp4_in_script.group(0)
                        title = soup.title.string if soup.title else "Untitled"
                        logger.info(f"Найден URL видео в скрипте: {video_url}")
                        return True, "Успешно получена информация", {"video_url": video_url, "title": title}

            return False, "URL видео не найден в HTML или скриптах", None

        except Exception as e:
            logger.error(f"Ошибка при получении информации о видео (XHSDownloader): {e}")
            return False, f"Ошибка: {str(e)}", None

    def download_video(self, video_url: str, output_path: str) -> bool:
        try:
            headers = self.headers.copy()
            headers["Range"] = "bytes=0-"
            response = self.session.get(video_url, headers=headers, stream=True)
            if response.status_code not in (200, 206):
                logger.error(f"Ошибка HTTP: {response.status_code}")
                return False

            with open(output_path, 'wb') as f:
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                last_percentage = -1  # Используем -1 как начальное значение, чтобы гарантировать вывод 0%
                
                # Целевые проценты для логирования
                target_percentages = [0, 25, 50, 75, 100]
                
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size:
                            current_percentage = int((downloaded / total_size) * 100)
                            
                            # Проверяем, перешли ли мы через точку целевого процента
                            for target in target_percentages:
                                if last_percentage < target <= current_percentage:
                                    logger.info(f"Скачивание (XHSDownloader): {target}%")
                                    last_percentage = current_percentage
                                    break
                
                # Убедимся, что финальный 100% был выведен
                if last_percentage < 100 and downloaded >= total_size:
                    logger.info(f"Скачивание (XHSDownloader): 100%")
                    
            return os.path.exists(output_path)

        except Exception as e:
            logger.error(f"Ошибка при скачивании видео (XHSDownloader): {e}")
            return False

    def close(self):
        self.session.close()

class RedNoteDownloader:
    def __init__(self):
        self.session = requests.Session()
        self.last_video_title = None
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]
        self.accept_languages = ["zh-CN,zh;q=0.9,en;q=0.8", "en-US,en;q=0.9"]
        self.update_headers()
        self.xhs = XHSDownloader()  # Добавляем новый загрузчик

    def update_headers(self):
        """Обновление заголовков с случайными значениями"""
        self.session.headers.update({
            "accept": "*/*",
            "accept-language": choice(self.accept_languages),
            "content-type": "application/json",
            "user-agent": choice(self.user_agents),
            "sec-ch-ua-platform": choice(["Windows", "macOS"]),
        })

    def extract_video_id(self, url: str) -> Optional[str]:
        """Извлечение ID видео из разных форматов ссылок"""
        try:
            if 'xhslink.com' in url:
                pattern = r'http[s]?://xhslink\.com/\w+/([a-zA-Z0-9]+)'
                match = re.search(pattern, url)
                if match:
                    return match.group(1)
            elif 'xiaohongshu.com' in url:
                pattern = r'item/([a-zA-Z0-9]+)'
                match = re.search(pattern, url)
                if match:
                    return match.group(1)
            return None
        except Exception as e:
            logger.error(f"Ошибка при извлечении ID видео: {e}")
            return None

    async def get_video_url(self, url: str, max_retries: int = 3) -> Tuple[bool, str, Optional[Dict]]:
        """Получение URL видео с двумя этапами: сначала XHSDownloader, затем API"""
        # Этап 1: Пробуем через XHSDownloader
        logger.info("Попытка получить видео через XHSDownloader")
        success, message, video_info = self.xhs.get_video_info(url)
        if success:
            return True, "Успешно получено через XHSDownloader", video_info
        
        logger.info(f"XHSDownloader не сработал: {message}. Переходим к API.")

        # Этап 2: Пробуем через API rndownloader.app
        for attempt in range(max_retries):
            try:
                video_id = self.extract_video_id(url)
                if not video_id:
                    return False, "Не удалось извлечь ID видео из ссылки", None

                api_url = "https://rndownloader.app/api/watermark"
                payload = {"url": url}
                
                self.update_headers()
                timeout = 30 * (attempt + 1)
                
                response = self.session.post(api_url, json=payload, timeout=timeout)
                
                if response.status_code == 504:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 5
                        logger.info(f"Получен статус 504, ожидание {wait_time} секунд перед повторной попыткой...")
                        await asyncio.sleep(wait_time)
                        continue
                        
                response.raise_for_status()
                
                data = response.json()
                if data.get("success"):
                    self.last_video_title = data.get("title", "")
                    return True, "Успешно получено через API", {
                        "video_url": data["video_url"],
                        "title": self.last_video_title,
                        "image_url": data.get("image_url", "")
                    }
                    
                if attempt < max_retries - 1:
                    await asyncio.sleep(3)
                    continue
                    
                return False, "Не удалось получить информацию о видео через API", None

            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    logger.warning(f"Таймаут запроса (попытка {attempt + 1}/{max_retries}). Ожидание {wait_time} секунд...")
                    await asyncio.sleep(wait_time)
                    continue
                return False, "Превышено время ожидания ответа от сервера", None
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    logger.error(f"Ошибка запроса (попытка {attempt + 1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(wait_time)
                    continue
                return False, f"Ошибка при выполнении запроса: {str(e)}", None
                
            except Exception as e:
                logger.error(f"Неожиданная ошибка: {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                    continue
                return False, f"Неожиданная ошибка: {str(e)}", None

        return False, "Превышено количество попыток получения информации о видео", None

    async def download_video(self, video_url: str, output_path: str) -> bool:
        """Скачивание видео с использованием XHSDownloader как основного метода"""
        # Сначала пробуем через XHSDownloader
        if self.xhs.download_video(video_url, output_path):
            return True
        
        # Если не получилось, пробуем старый метод
        try:
            self.update_headers()
            response = self.session.get(video_url, stream=True, timeout=30)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            block_size = 1024 * 1024  # 1MB
            
            with open(output_path, 'wb') as file:
                for chunk in response.iter_content(block_size):
                    if chunk:
                        file.write(chunk)
            
            return True

        except Exception as e:
            logger.error(f"Ошибка при скачивании видео (старый метод): {e}")
            return False

    def close(self):
        """Закрытие сессий"""
        self.session.close()
        self.xhs.close()