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
    """Загрузчик для Instagram с прокси для API запросов и прямым соединением для скачивания"""
    
    def __init__(self, downloads_dir="downloads"):
        super().__init__(downloads_dir)
        
        # Настройка прокси для API запросов (тестовые прокси)
        self.test_proxies = [
            "posledtp52:TiCBNGs8sq@5.133.163.38:50100",
            "posledtp52:TiCBNGs8sq@63.125.90.106:50100", 
            "posledtp52:TiCBNGs8sq@72.9.186.194:50100"
        ]
        
        # Сессия с прокси для API запросов
        self.api_session = requests.Session()
        self.setup_api_session_with_proxy()
        
        # Сессия без прокси для скачивания файлов
        self.download_session = requests.Session()
        self.download_session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "video/webm,video/ogg,video/*;q=0.9,application/ogg;q=0.7,audio/*;q=0.6,*/*;q=0.5",
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
        })
        
        self.current_proxy_index = 0
        logger.info("InstagramDownloader инициализирован с прокси поддержкой")
    
    def setup_api_session_with_proxy(self):
        """Настройка сессии с прокси для API запросов"""
        try:
            if self.test_proxies:
                proxy_string = self.test_proxies[self.current_proxy_index]
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
        if self.test_proxies:
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.test_proxies)
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
    
    async def get_instagram_params(self, shortcode: str) -> Dict[str, Any]:
        """Получение параметров через прокси"""
        try:
            post_url = f"https://www.instagram.com/reel/{shortcode}/"
            logger.debug(f"Получение параметров через прокси с: {post_url}")
            
            headers = {
                "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1"
            }
            
            # Используем сессию с прокси для получения параметров
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.api_session.get(post_url, headers=headers, timeout=30)
            )
            
            if response.status_code != 200:
                logger.warning(f"Статус страницы через прокси: {response.status_code}")
                return {}
            
            content = response.text
            params = {}
            
            # Извлекаем различные токены
            token_patterns = {
                "lsd": r'"LSD",\[\],\{"token":"([^"]+)"',
                "jazoest": r'"jazoest":"([^"]+)"',
                "csrf_token": r'"csrf_token":"([^"]+)"',
                "__spin_r": r'"__spin_r":(\d+)',
                "__hsi": r'"hsi":"(\d+)"',
            }
            
            for key, pattern in token_patterns.items():
                match = re.search(pattern, content)
                if match:
                    params[key] = match.group(1)
                    logger.debug(f"Найден {key} через прокси: {match.group(1)[:20]}...")
            
            logger.info(f"Параметры получены через прокси: {list(params.keys())}")
            return params
            
        except Exception as e:
            logger.error(f"Ошибка при получении параметров через прокси: {e}")
            # Пробуем переключиться на другой прокси
            self.rotate_proxy()
            return {}
    
    async def fetch_instagram_post(self, instagram_url: str, max_retries: int = 3) -> Tuple[Optional[Dict], Optional[str]]:
        """Получение данных поста через прокси с ротацией прокси при ошибках"""
        shortcode = self.extract_shortcode(instagram_url)
        
        if not shortcode:
            logger.error("Неверный URL Instagram или shortcode не найден")
            return None, None
        
        logger.info(f"Извлечен shortcode: {shortcode}")
        
        for attempt in range(max_retries):
            try:
                # Минимальная задержка
                await asyncio.sleep(1)
                
                # Получаем динамические параметры через прокси
                try:
                    dynamic_params = await self.get_instagram_params(shortcode)
                    logger.info(f"Получены динамические параметры через прокси (попытка {attempt + 1}): {list(dynamic_params.keys())}")
                except Exception as e:
                    logger.warning(f"Ошибка получения параметров через прокси (попытка {attempt + 1}): {e}")
                    dynamic_params = {}
                
                # URL для GraphQL
                url = "https://www.instagram.com/graphql/query"
                
                # Параметры запроса
                import random
                params = {
                    "av": "0",
                    "__d": "www",
                    "__user": "0", 
                    "__a": "1",
                    "__req": str(random.randint(1, 50)),
                    "dpr": "2",
                    "__ccg": "UNKNOWN",
                    "__comet_req": "7",
                    "__spin_b": "trunk",
                    "fb_api_caller_class": "RelayModern",
                    "fb_api_req_friendly_name": "PolarisPostActionLoadPostQueryQuery",
                    "variables": json.dumps({
                        "shortcode": shortcode,
                        "fetch_tagged_user_count": None,
                        "hoisted_comment_id": None,
                        "hoisted_reply_id": None
                    }),
                    "server_timestamps": "true",
                    "doc_id": "8845758582119845",
                }
                
                # Добавляем динамические параметры если есть
                if dynamic_params:
                    params.update(dynamic_params)
                
                # Заголовки для API запроса
                headers = {
                    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
                    "Accept": "*/*",
                    "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": "https://www.instagram.com",
                    "Referer": f"https://www.instagram.com/reel/{shortcode}/",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                    "X-Instagram-AJAX": "1",
                    "X-Requested-With": "XMLHttpRequest",
                }
                
                # Добавляем CSRF токен если есть
                if 'lsd' in dynamic_params:
                    headers["X-CSRFToken"] = dynamic_params['lsd']
                elif 'csrf_token' in dynamic_params:
                    headers["X-CSRFToken"] = dynamic_params['csrf_token']
                
                logger.info(f"Выполнение запроса к Instagram GraphQL API через прокси (попытка {attempt + 1})...")
                
                try:
                    # Выполняем POST-запрос через прокси
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(
                        None,
                        lambda: self.api_session.post(url, data=params, headers=headers, timeout=30)
                    )
                    
                    logger.info(f"Ответ сервера через прокси: {response.status_code}")
                    
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            
                            # Проверяем наличие данных
                            if 'data' in data and data['data']:
                                logger.info("JSON успешно получен через прокси и содержит данные")
                                
                                # Сохраняем для отладки
                                if os.environ.get('DEBUG_INSTAGRAM', '').lower() == 'true':
                                    filename = f"debug_instagram_{shortcode}_{int(time.time())}.json"
                                    with open(filename, "w", encoding="utf-8") as f:
                                        json.dump(data, f, indent=2, ensure_ascii=False)
                                    logger.info(f"Debug данные сохранены в: {filename}")
                                
                                return data, shortcode
                                
                            elif 'errors' in data:
                                logger.error(f"GraphQL ошибки через прокси: {data['errors']}")
                                # Переключаемся на следующий прокси
                                self.rotate_proxy()
                                continue
                                
                            else:
                                logger.warning("Ответ через прокси не содержит данных или ошибок")
                                logger.warning(f"Ключи ответа: {list(data.keys())}")
                                # Переключаемся на следующий прокси
                                self.rotate_proxy()
                                continue
                                
                        except json.JSONDecodeError as e:
                            logger.error(f"Ошибка парсинга JSON через прокси: {e}")
                            logger.error(f"Ответ сервера: {response.text[:200]}...")
                            # Переключаемся на следующий прокси
                            self.rotate_proxy()
                            continue
                    
                    elif response.status_code == 429:
                        logger.warning("Rate limit обнаружен через прокси")
                        # Переключаемся на следующий прокси и добавляем задержку
                        self.rotate_proxy()
                        await asyncio.sleep(5)
                        continue
                        
                    elif response.status_code == 403:
                        logger.warning("Доступ запрещен (403) через прокси")
                        # Переключаемся на следующий прокси
                        self.rotate_proxy()
                        continue
                        
                    else:
                        logger.error(f"HTTP ошибка через прокси: {response.status_code}")
                        logger.error(f"Ответ: {response.text[:100]}...")
                        # Переключаемся на следующий прокси
                        self.rotate_proxy()
                        continue
                        
                except Exception as request_error:
                    logger.error(f"Ошибка выполнения запроса через прокси (попытка {attempt + 1}): {request_error}")
                    # Переключаемся на следующий прокси
                    self.rotate_proxy()
                    continue
                    
            except Exception as e:
                logger.error(f"Критическая ошибка в fetch_instagram_post через прокси (попытка {attempt + 1}): {e}")
                # Переключаемся на следующий прокси
                self.rotate_proxy()
                continue
        
        logger.error("Все попытки получения данных через прокси исчерпаны")
        return None, None
    
    def extract_video_url(self, json_data: dict) -> Optional[str]:
        """Извлечение URL видео из JSON данных"""
        try:
            logger.debug(f"Анализ JSON структуры: {list(json_data.keys())}")
            
            media = None
            
            if 'data' in json_data:
                data_keys = list(json_data['data'].keys())
                logger.debug(f"Ключи в data: {data_keys}")
                
                # Проверяем новый формат xdt_shortcode_media
                if 'xdt_shortcode_media' in json_data['data']:
                    media = json_data['data']['xdt_shortcode_media']
                    logger.info("✅ Найден новый формат: xdt_shortcode_media")
                    
                # Потом старый формат для совместимости
                elif 'shortcode_media' in json_data['data']:
                    media = json_data['data']['shortcode_media']
                    logger.info("✅ Найден старый формат: shortcode_media")
                
                else:
                    logger.error(f"❌ Медиа данные не найдены. Доступные ключи: {data_keys}")
                    return None
            else:
                logger.error(f"❌ Ключ 'data' не найден. Доступные ключи: {list(json_data.keys())}")
                return None
            
            if not media:
                logger.error("❌ Объект media пустой")
                return None
            
            # Логируем информацию о медиа
            media_type = media.get('__typename', 'Unknown')
            is_video = media.get('is_video', False)
            logger.info(f"📱 Тип медиа: {media_type}, Это видео: {is_video}")
            
            # Проверяем что это видео (поддержка новых типов)
            video_types = ['GraphVideo', 'XDTGraphVideo']
            
            if media_type in video_types or is_video:
                logger.info("✅ Подтвержден тип видео")
                
                # Поиск прямого video_url
                if 'video_url' in media:
                    video_url = media['video_url']
                    if video_url and isinstance(video_url, str):
                        logger.info(f"🎯 Найден прямой video_url: {video_url[:100]}...")
                        return video_url
                    else:
                        logger.warning("⚠️ video_url найден, но пустой или неверного типа")
                
                # Поиск в video_resources (если прямого нет)
                if 'video_resources' in media and media['video_resources']:
                    logger.info(f"🔍 Поиск в video_resources ({len(media['video_resources'])} элементов)")
                    video_resources = media['video_resources']
                    
                    # Берем ресурс с максимальным разрешением
                    highest_quality = max(
                        video_resources, 
                        key=lambda x: x.get('config_width', 0) * x.get('config_height', 0)
                    )
                    
                    video_url = highest_quality.get('src')
                    if video_url:
                        logger.info(f"🎯 Найден video_url в video_resources: {video_url[:100]}...")
                        return video_url
                
                logger.warning("⚠️ video_url не найден в стандартных местах")
            
            # Обработка каруселей (для постов с несколькими видео)
            elif media_type in ['GraphSidecar', 'XDTGraphSidecar']:
                logger.info("🎠 Обработка карусели")
                edges = media.get('edge_sidecar_to_children', {}).get('edges', [])
                
                for i, edge in enumerate(edges):
                    node = edge.get('node', {})
                    logger.debug(f"Элемент карусели {i+1}: {node.get('__typename', 'Unknown')}, is_video: {node.get('is_video', False)}")
                    
                    if node.get('is_video', False):
                        video_url = node.get('video_url')
                        if video_url:
                            logger.info(f"🎯 Найден video_url в карусели: {video_url[:100]}...")
                            return video_url
            
            # Рекурсивный поиск по всему JSON
            logger.warning("🔍 Выполняется рекурсивный поиск video_url...")
            
            def find_video_url_recursive(obj, path=""):
                if isinstance(obj, dict):
                    # Прямой поиск video_url
                    if "video_url" in obj and isinstance(obj["video_url"], str):
                        logger.info(f"🎯 Рекурсивно найден video_url в {path}")
                        return obj["video_url"]
                    
                    # Рекурсивный поиск в подобъектах
                    for key, value in obj.items():
                        result = find_video_url_recursive(value, f"{path}.{key}" if path else key)
                        if result:
                            return result
                            
                elif isinstance(obj, list):
                    for i, item in enumerate(obj):
                        result = find_video_url_recursive(item, f"{path}[{i}]")
                        if result:
                            return result
                
                return None
            
            video_url = find_video_url_recursive(json_data)
            if video_url:
                logger.info(f"🎯 Найден video_url рекурсивным поиском: {video_url[:100]}...")
                return video_url
            
            # Если ничего не найдено - детальное логирование для отладки
            logger.error("❌ video_url не найден нигде!")
            logger.error(f"Доступные ключи в media: {list(media.keys())[:20]}")
            
            return None
                    
        except Exception as e:
            logger.error(f"💥 Критическая ошибка в extract_video_url: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    async def download_video_direct(self, url: str, output_path: str) -> bool:
        """Загрузка видео БЕЗ прокси (прямое соединение)"""
        try:
            logger.info(f"Загрузка видео БЕЗ прокси с: {url}")
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                "Accept": "video/webm,video/ogg,video/*;q=0.9,application/ogg;q=0.7,audio/*;q=0.6,*/*;q=0.5",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "identity",
                "Range": "bytes=0-",
                "Referer": "https://www.instagram.com/",
                "Origin": "https://www.instagram.com"
            }
            
            # Создаем директорию, если не существует
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Выполняем запрос БЕЗ прокси (используем download_session)
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.download_session.get(url, headers=headers, stream=True, timeout=60)
            )
            
            response.raise_for_status()
            
            # Получаем размер файла
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            logger.info(f"Сохранение в: {output_path}")
            if total_size > 0:
                logger.info(f"Размер файла: {total_size / (1024*1024):.2f} MB")
            
            logger.info("🚀 Скачивание БЕЗ прокси началось...")
            
            # Загружаем файл по частям
            with open(output_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Показываем прогресс
                        if total_size > 0:
                            progress = (downloaded_size / total_size) * 100
                            if downloaded_size % (1024*1024) == 0:  # Каждый MB
                                logger.debug(f"Прогресс скачивания БЕЗ прокси: {progress:.1f}% ({downloaded_size / (1024*1024):.2f} MB)")
            
            logger.info(f"✅ Видео загружено успешно БЕЗ прокси: {output_path}")
            logger.info(f"Итоговый размер: {downloaded_size / (1024*1024):.2f} MB")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке видео БЕЗ прокси: {e}")
            return False
    
    async def extract_video_info(self, url: str) -> Dict:
        """Извлечение информации о видео из URL"""
        try:
            shortcode = self.extract_shortcode(url)
            
            # Попытка получить информацию через GraphQL API (с прокси)
            data, _ = await self.fetch_instagram_post(url)
            
            if data:
                media = None
                if 'data' in data:
                    if 'xdt_shortcode_media' in data['data']:
                        media = data['data']['xdt_shortcode_media']
                    elif 'shortcode_media' in data['data']:
                        media = data['data']['shortcode_media']
                
                if media:
                    return {
                        'title': f'Instagram Video {shortcode}',
                        'duration': media.get('video_duration', 0),
                        'thumbnail': media.get('display_url', ''),
                        'uploader': media.get('owner', {}).get('username', 'Instagram User'),
                        'formats': [],
                        'is_live': False,
                        'view_count': media.get('video_view_count', 0),
                        'like_count': media.get('edge_media_preview_like', {}).get('count', 0)
                    }
            
            # Fallback к базовой информации
            return {
                'title': f'Instagram Video {shortcode}',
                'duration': 0,
                'thumbnail': '',
                'uploader': 'Instagram User',
                'formats': [],
                'is_live': False
            }
        except Exception as e:
            logger.error(f"Ошибка при извлечении информации о видео: {e}")
            return {}
    
    async def download_video(self, url: str, output_path: str = None) -> Optional[str]:
        """Главный метод загрузки видео из Instagram"""
        if not output_path:
            output_path = self.generate_output_filename("instagram")
        
        try:
            # Метод 1: GraphQL API с прокси для получения ссылки + прямое скачивание
            try:
                logger.info(f"🔄 Попытка загрузки через GraphQL API с прокси: {url}")
                
                # Получаем данные поста ЧЕРЕЗ ПРОКСИ
                data, shortcode = await self.fetch_instagram_post(url)
                
                if data and shortcode:
                    # Извлекаем URL видео
                    video_url = self.extract_video_url(data)
                    
                    if video_url:
                        logger.info("🎯 URL видео получен через прокси, начинаем скачивание БЕЗ прокси")
                        # Загружаем видео БЕЗ ПРОКСИ
                        success = await self.download_video_direct(video_url, output_path)
                        
                        if success and os.path.exists(output_path):
                            logger.info(f"✅ Успешная загрузка через GraphQL API (прокси) + прямое скачивание: {output_path}")
                            return output_path
                        else:
                            logger.warning("❌ Скачивание БЕЗ прокси не удалось")
                    else:
                        logger.warning("❌ video_url не найден в ответе GraphQL API")
                else:
                    logger.warning("❌ Не удалось получить данные через GraphQL API с прокси")
                    
            except Exception as e:
                logger.warning(f"❌ Ошибка GraphQL API метода с прокси: {e}")
            
            # Метод 2: Резервный метод через yt-dlp (БЕЗ прокси)
            try:
                logger.info(f"🔄 Попытка загрузки через yt-dlp БЕЗ прокси: {url}")
                
                import yt_dlp
                
                ydl_opts = {
                    'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                    'outtmpl': output_path,
                    'quiet': True,
                    'no_warnings': True,
                    'extract_flat': False,
                    'no_color': True,
                    'merge_output_format': 'mp4',
                    'prefer_ffmpeg': True,
                    'retries': 3,
                    'fragment_retries': 3,
                    'skip_unavailable_fragments': True,
                    # БЕЗ прокси для yt-dlp
                }
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([url])
                    
                    if os.path.exists(output_path):
                        logger.info(f"✅ Успешная загрузка через yt-dlp БЕЗ прокси: {output_path}")
                        return output_path
                        
            except Exception as e:
                logger.warning(f"❌ Ошибка yt-dlp метода БЕЗ прокси: {e}")
            
            # Если все методы провалились
            logger.error(f"❌ Все методы загрузки провалились для URL: {url}")
            
            # Удаляем частично загруженный файл, если он существует
            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except:
                    pass
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при загрузке видео: {e}")
            
            # Удаляем частично загруженный файл
            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except:
                    pass
            
            return None
    
    def __del__(self):
        """Очистка ресурсов при удалении объекта"""
        try:
            if hasattr(self, 'api_session'):
                self.api_session.close()
            if hasattr(self, 'download_session'):
                self.download_session.close()
        except:
            pass