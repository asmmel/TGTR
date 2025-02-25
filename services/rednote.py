import requests
import json
import asyncio
import re
import logging
from urllib.parse import urlparse
from random import choice
from typing import Optional, Dict, Tuple

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
            # Для коротких ссылок xhslink.com
            if 'xhslink.com' in url:
                pattern = r'http[s]?://xhslink\.com/\w+/([a-zA-Z0-9]+)'
                match = re.search(pattern, url)
                if match:
                    return match.group(1)

            # Для полных ссылок xiaohongshu.com
            elif 'xiaohongshu.com' in url:
                pattern = r'item/([a-zA-Z0-9]+)'
                match = re.search(pattern, url)
                if match:
                    return match.group(1)

            return None
        except Exception as e:
            logging.error(f"Ошибка при извлечении ID видео: {e}")
            return None

    async def get_video_url(self, url: str, max_retries: int = 3) -> Tuple[bool, str, Optional[Dict]]:
        """Получение URL видео с повторными попытками"""
        for attempt in range(max_retries):
            try:
                video_id = self.extract_video_id(url)
                if not video_id:
                    return False, "Не удалось извлечь ID видео из ссылки", None

                api_url = "https://rndownloader.app/api/watermark"
                payload = {"url": url}
                
                self.update_headers()
                
                # Увеличиваем таймаут для каждой следующей попытки
                timeout = 30 * (attempt + 1)
                
                response = self.session.post(api_url, json=payload, timeout=timeout)
                
                # Проверяем статус ответа
                if response.status_code == 504:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 5  # Увеличиваем время ожидания с каждой попыткой
                        logging.info(f"Получен статус 504, ожидание {wait_time} секунд перед повторной попыткой...")
                        await asyncio.sleep(wait_time)
                        continue
                        
                response.raise_for_status()
                
                data = response.json()
                if data.get("success"):
                    # Сохраняем название видео
                    self.last_video_title = data.get("title", "")
                    return True, "success", {
                        "video_url": data["video_url"],
                        "title": self.last_video_title,
                        "image_url": data.get("image_url", "")
                    }
                    
                # Если нет успеха, но и нет ошибки
                if attempt < max_retries - 1:
                    await asyncio.sleep(3)
                    continue
                    
                return False, "Не удалось получить информацию о видео", None

            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    logging.warning(f"Таймаут запроса (попытка {attempt + 1}/{max_retries}). Ожидание {wait_time} секунд...")
                    await asyncio.sleep(wait_time)
                    continue
                return False, "Превышено время ожидания ответа от сервера", None
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    logging.error(f"Ошибка запроса (попытка {attempt + 1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(wait_time)
                    continue
                return False, f"Ошибка при выполнении запроса: {str(e)}", None
                
            except Exception as e:
                logging.error(f"Неожиданная ошибка: {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)
                    continue
                return False, f"Неожиданная ошибка: {str(e)}", None

        return False, "Превышено количество попыток получения информации о видео", None

    async def download_video(self, video_url: str, output_path: str) -> bool:
        """Скачивание видео"""
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
            logging.error(f"Ошибка при скачивании видео: {e}")
            return False