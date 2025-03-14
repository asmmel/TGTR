import requests
import json
import os
import random
import time
import base64
import uuid
import logging
from typing import Optional, Dict, List
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from fake_useragent import UserAgent
from services.monitoring import MonitoringService
import asyncio
import re
from dotenv import load_dotenv

class KuaishouDownloader:
    def __init__(self):
        self.ua = UserAgent()
        self.base_url = "https://www.kuaishou.com"
        self.api_url = f"{self.base_url}/graphql"
        self.monitoring = MonitoringService()
        self.max_attempts = 5
        self.current_video_url = None  # Переменная для хранения текущего URL видео
        
        # Загружаем настройки из .env
        load_dotenv()
        self.use_proxy = os.getenv('USE_PROXY', 'False').lower() == 'true'  # По умолчанию False
        self.proxy_pool = self._load_proxies_from_env() if self.use_proxy else []
        
        # Остальные настройки остаются без изменений
        self.cookies_pool = [
            {
                'did': 'web_a0f68f4df26043f29e59d325bda759fd',
                'didv': '1729873260000',
                'kpf': 'PC_WEB',
                'clientid': '3',
                '_bl_uid': 'k1mv42nFs8pzgkm1ekwd0vR8v7tL',
                'userId': '4447174443',
                'kuaishou.server.webday7_st': 'ChprdWFpc2hvdS5zZXJ2ZXIud2ViZGF5Ny5zdBKwAbGxGCRMipMI3d_Z2QCzd_MxlgxwsuFMlGFlokfmo0HJiDa0QUuT6rhNN_JPJ_hdifgzpwDzsktSAsOJNU-nqEQ7NOu0eeHGmUFuuQ91D4lpGiU9xpBjKgmw6x3gqLLYfRKX6qtmb89OGT34ng6jEAToCaVKsFLKvtuJYRRJMVXOVnYWeyUJgsaQx9LGQFe9qwrVYAIAXlUDRzvfYzoXeqd2ywb-__XYaKNvqLwFI5AZGhLiAcjqdxyK2yTb3QWg1bwhL5siIMG4vY4eeZ7TuCLVAxkv-hEq-wwq93WwD8I7HxLuqnLcKAUwAQ',
                'kuaishou.server.webday7_ph': '53048c6032795b200cb10eb57b98ad0821ae',
                'kpn': 'KUAISHOU_VISION'
            },
            {
                'did': f'web_{uuid.uuid4().hex[:32]}',
                'didv': str(int(time.time() * 1000)),
                'kpf': 'PC_WEB',
                'clientid': '3',
                '_bl_uid': f'k1mv42nFs8pzgkm1ekwd0vR8v7tL',
                'userId': '4447174444',
                'kuaishou.server.webday7_st': 'ChprdWFpc2hvdS5zZXJ2ZXIud2ViZGF5Ny5zdBKwAbGxGCRMipMI3d_Z2QCzd_MxlgxwsuFMlGFlokfmo0HJiDa0QUuT6rhNN_JPJ_hdifgzpwDzsktSAsOJNU-nqEQ7NOu0eeHGmUFuuQ91D4lpGiU9xpBjKgmw6x3gqLLYfRKX6qtmb89OGT34ng6jEAToCaVKsFLKvtuJYRRJMVXOVnYWeyUJgsaQx9LGQFe9qwrVYAIAXlUDRzvfYzoXeqd2ywb-__XYaKNvqLwFI5AZGhLiAcjqdxyK2yTb3QWg1bwhL5siIMG4vY4eeZ7TuCLVAxkv-hEq-wwq93WwD8I7HxLuqnLcKAUwAQ',
                'kuaishou.server.webday7_ph': '53048c6032795b200cb10eb57b98ad0821ae',
                'kpn': 'KUAISHOU_VISION'
            }
        ]

    def _load_proxies_from_env(self) -> List[Dict[str, str]]:
        """Загрузка прокси из .env файла"""
        try:
            proxy_pool = []
            proxy_count = 1
            while True:
                proxy_env = os.getenv(f'PROXY_{proxy_count}')
                if not proxy_env:
                    break
                if '@' in proxy_env:
                    proxy_url = f"http://{proxy_env}"
                    proxy_pool.append({"http": proxy_url, "https": proxy_url})
                    logging.info(f"Загружен прокси #{proxy_count}")
                else:
                    logging.warning(f"Неверный формат прокси #{proxy_count} в .env файле")
                proxy_count += 1
            if not proxy_pool:
                logging.error("Не найдены настройки прокси в .env файле")
                raise ValueError("Прокси не настроены")
            logging.info(f"Успешно загружено {len(proxy_pool)} прокси")
            return proxy_pool
        except Exception as e:
            logging.error(f"Ошибка при загрузке прокси из .env: {str(e)}")
            raise

    def _get_random_user_agent(self) -> str:
        return self.ua.random
    
    def _generate_webday7_st(self):
        random_bytes = os.urandom(64)
        token_base = base64.b64encode(random_bytes).decode('utf-8')
        return f"ChprdWFpc2hvdS5zZXJ2ZXIud2ViZGF5Ny5zdBK{token_base[:100]}KAUwAQ"

    def _generate_webday7_ph(self):
        return ''.join(random.choice('0123456789abcdef') for _ in range(32))

    def _get_random_proxy(self) -> Optional[dict]:
        if not self.use_proxy or not self.proxy_pool:
            return None
        return random.choice(self.proxy_pool)

    def _create_session(self, proxy: Optional[dict] = None) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        if proxy:
            session.proxies.update(proxy)
            logging.info(f"Используется прокси: {proxy}")
        else:
            logging.info("Прокси не используется")
        return session

    async def _extract_video_id(self, url: str) -> str:
        try:
            if 'v.kuaishou.com' in url:
                logging.info(f"Обнаружена короткая ссылка: {url}")
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1'
                }
                max_attempts = 3
                for attempt in range(max_attempts):
                    try:
                        proxy = self._get_random_proxy() if self.use_proxy else None
                        logging.info(f"Используем прокси (попытка {attempt + 1}): {proxy if proxy else 'Нет'}")
                        session = self._create_session(proxy)
                        response = session.get(url, headers=headers, allow_redirects=True, timeout=30)
                        logging.info(f"Финальный URL: {response.url}")
                        logging.info(f"История редиректов: {[r.url for r in response.history]}")
                        logging.info(f"Статус-код: {response.status_code}")

                        video_id = None
                        if '/short-video/' in response.url:
                            video_id = response.url.split('/short-video/')[-1].split('?')[0]
                        if not video_id:
                            match = re.search(r'photoId["\']:\s*["\']([^"\']+)["\']', response.text)
                            if match:
                                video_id = match.group(1)
                        if not video_id and 'Location' in response.headers:
                            location = response.headers['Location']
                            if '/short-video/' in location:
                                video_id = location.split('/short-video/')[-1].split('?')[0]
                        if video_id:
                            logging.info(f"Извлечен ID видео: {video_id}")
                            return video_id
                        logging.error(f"Текст ответа (первые 1000 символов): {response.text[:1000]}")
                        raise Exception("Не удалось извлечь ID видео")
                    except requests.exceptions.ProxyError as proxy_error:
                        logging.error(f"Ошибка прокси {proxy}: {proxy_error}")
                        if attempt < max_attempts - 1:
                            logging.info(f"Повторная попытка {attempt + 2} из {max_attempts}")
                            await asyncio.sleep(2)
                        else:
                            raise
                    except requests.exceptions.RequestException as e:
                        logging.error(f"Ошибка сетевого запроса: {e}")
                        raise
                raise Exception(f"Не удалось извлечь ID видео после {max_attempts} попыток")
            return url.split('/short-video/')[-1].split('?')[0]
        except Exception as e:
            logging.error(f"Критическая ошибка при извлечении video ID: {e}")
            raise

    async def _get_video_info(self, video_id: str) -> Optional[Dict]:
        start_time = time.time()
        logging.info(f"Получаем информацию о видео ID: {video_id}")

        for cookie_index, cookies in enumerate(self.cookies_pool):
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Origin': 'https://www.kuaishou.com',
                    'Referer': f'https://www.kuaishou.com/short-video/{video_id}'
                }

                logging.info(f"Пробуем cookies set #{cookie_index + 1}")
                proxy = self._get_random_proxy() if self.use_proxy else None
                logging.info(f"Используем прокси: {proxy if proxy else 'Нет'}")

                payload = {
                    "operationName": "visionVideoDetail",
                    "variables": {"photoId": video_id, "page": "detail"},
                    "query": """
                    query visionVideoDetail($photoId: String, $page: String) {
                        visionVideoDetail(photoId: $photoId, page: $page) {
                            photo {
                                id
                                duration
                                caption
                                photoUrl
                                photoH265Url
                            }
                        }
                    }"""
                }

                session = self._create_session(proxy)
                for k, v in cookies.items():
                    session.cookies.set(k, v, domain='.kuaishou.com')

                response = session.post(self.api_url, headers=headers, json=payload, timeout=10)
                data = response.json()

                if 'data' in data and 'visionVideoDetail' in data['data']:
                    photo_data = data['data']['visionVideoDetail']['photo']
                    if photo_data:
                        video_url = photo_data.get('photoUrl') or photo_data.get('photoH265Url')
                        if video_url:
                            check_session = self._create_session(proxy)
                            check_response = check_session.head(video_url, headers=headers, timeout=5)
                            if check_response.status_code == 200:
                                duration = time.time() - start_time
                                self.monitoring.log_api_call('kuaishou', 'get_video_info', True)
                                self.monitoring.log_download_time('kuaishou', duration)
                                return photo_data
                            else:
                                logging.warning(f"URL видео недоступен: {video_url}, код: {check_response.status_code}")
                                continue

                logging.warning(f"Неудачный ответ API: {json.dumps(data, indent=2)}")
                await asyncio.sleep(1)

            except Exception as e:
                logging.error(f"Ошибка в попытке {cookie_index + 1}: {str(e)}")
                await asyncio.sleep(1)
                continue

        self.monitoring.log_api_call('kuaishou', 'get_video_info', False, "Все попытки не удались")
        return None

    async def download_video(self, url: str, output_path: str) -> Optional[str]:
        """Скачивание видео с сохранением URL в переменную"""
        self.current_video_url = None  # Сбрасываем URL перед началом новой загрузки

        for attempt in range(self.max_attempts):
            try:
                logging.info(f"Попытка {attempt + 1} из {self.max_attempts}")

                # Если URL еще не извлечен, получаем его
                if not self.current_video_url:
                    video_id = await self._extract_video_id(url)
                    logging.info(f"Извлечен ID видео: {video_id}")

                    current_cookies = self.cookies_pool[attempt % len(self.cookies_pool)]
                    headers = {
                        'User-Agent': self._get_random_user_agent(),
                        'Accept': 'application/json, text/plain, */*',
                        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                        'Cookie': '; '.join([f'{k}={v}' for k, v in current_cookies.items()]),
                        'Host': 'www.kuaishou.com',
                        'Origin': 'https://www.kuaishou.com',
                        'Referer': f'https://www.kuaishou.com/short-video/{video_id}'
                    }

                    video_info = await self._get_video_info(video_id)
                    if not video_info:
                        raise Exception("Не удалось получить информацию о видео")

                    self.current_video_url = video_info.get('photoUrl') or video_info.get('photoH265Url')
                    if not self.current_video_url:
                        raise Exception("URL видео не найден в ответе API")
                    logging.info(f"Найден и сохранен URL видео: {self.current_video_url}")

                # Используем сохраненный URL для загрузки
                headers = {
                    'User-Agent': self._get_random_user_agent(),
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Cookie': '; '.join([f'{k}={v}' for k, v in self.cookies_pool[attempt % len(self.cookies_pool)].items()]),
                    'Host': 'www.kuaishou.com',
                    'Origin': 'https://www.kuaishou.com',
                    'Referer': f'https://www.kuaishou.com/short-video/{video_id}'  # Оставляем video_id из последней попытки
                }

                success = await self._download_with_headers(self.current_video_url, output_path, headers)
                if success:
                    self.monitoring.log_api_call('kuaishou', 'download', True)
                    self.current_video_url = None  # Обнуляем URL после успешной загрузки
                    return output_path

            except Exception as e:
                logging.error(f'Попытка {attempt + 1} не удалась: {str(e)}')
                if os.path.exists(output_path):
                    os.remove(output_path)
                await asyncio.sleep(2)
                continue

        self.monitoring.log_api_call('kuaishou', 'download', False, "Превышено максимальное количество попыток")
        self.current_video_url = None  # Обнуляем URL после всех попыток
        return None

    async def _download_with_headers(self, url: str, output_path: str, headers: dict) -> bool:
        try:
            download_headers = headers.copy()
            download_headers.update({
                'Host': 'v2.kwaicdn.com',
                'Accept': 'video/webm,video/ogg,video/*;q=0.9,application/ogg;q=0.7,audio/*;q=0.6,*/*;q=0.5',
                'Accept-Encoding': 'identity;q=1, *;q=0',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Range': 'bytes=0-',
                'Sec-Fetch-Dest': 'video',
                'Sec-Fetch-Mode': 'no-cors',
                'Sec-Fetch-Site': 'cross-site',
            })

            proxy = self._get_random_proxy() if self.use_proxy else None
            session = self._create_session(proxy)
            logging.info(f"Начинаем загрузку видео через прокси: {proxy if proxy else 'Нет'}")

            response = session.get(url, headers=download_headers, stream=True, timeout=60)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            logging.info(f"Размер файла: {total_size // (1024*1024)} MB")

            block_size = 1024 * 1024
            downloaded = 0

            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(block_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        progress = int((downloaded / total_size) * 100)
                        logging.info(f"Прогресс: {progress}% [{downloaded} / {total_size}]")

            logging.info("Загрузка завершена!")
            return True

        except Exception as e:
            logging.error(f"Ошибка при загрузке: {str(e)}")
            return False
