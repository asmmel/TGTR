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
    """Загрузчик для Instagram с улучшенным методом и резервным вариантом"""
    
    def __init__(self, downloads_dir="downloads"):
        super().__init__(downloads_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
        })
        logger.info("InstagramDownloader инициализирован")
    
    def extract_shortcode(self, url: str) -> Optional[str]:
        """Извлечение shortcode из URL Instagram"""
        # Обновленные паттерны для различных форматов URL Instagram
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
        """
        УЛУЧШЕННАЯ версия get_instagram_params
        """
        try:
            post_url = f"https://www.instagram.com/reel/{shortcode}/"
            logger.debug(f"Получение параметров с: {post_url}")
            
            headers = {
                "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1"
            }
            
            # Используем синхронный запрос через executor для стабильности
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.session.get(post_url, headers=headers, timeout=30)
            )
            
            if response.status_code != 200:
                logger.warning(f"Статус страницы: {response.status_code}")
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
                    logger.debug(f"Найден {key}: {match.group(1)[:20]}...")
            
            return params
            
        except Exception as e:
            logger.error(f"Ошибка при получении параметров: {e}")
            return {}
    
    async def _make_request_async(self, method: str, url: str, **kwargs) -> Optional[str]:
        """Асинхронная обертка для запросов"""
        try:
            loop = asyncio.get_event_loop()
            if method.lower() == "get":
                response = await loop.run_in_executor(
                    None, 
                    lambda: self.session.get(url, **kwargs)
                )
            else:
                response = await loop.run_in_executor(
                    None, 
                    lambda: self.session.post(url, **kwargs)
                )
            
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса: {e}")
            return None
    
    async def fetch_instagram_post(self, instagram_url: str) -> Tuple[Optional[Dict], Optional[str]]:
        """
        ИСПРАВЛЕННАЯ версия fetch_instagram_post
        """
        try:
            shortcode = self.extract_shortcode(instagram_url)
            
            if not shortcode:
                logger.error("Неверный URL Instagram или shortcode не найден")
                return None, None
            
            logger.info(f"Извлечен shortcode: {shortcode}")
            
            # Минимальная задержка
            await asyncio.sleep(1)
            
            # Получаем динамические параметры
            try:
                dynamic_params = await self.get_instagram_params(shortcode)
                logger.info(f"Получены динамические параметры: {list(dynamic_params.keys())}")
            except Exception as e:
                logger.warning(f"Ошибка получения параметров: {e}. Используем базовые.")
                dynamic_params = {}
            
            # URL для GraphQL
            url = "https://www.instagram.com/graphql/query"
            
            # Параметры запроса (проверенные рабочие)
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
                "doc_id": "8845758582119845",  # Рабочий doc_id
            }
            
            # Добавляем динамические параметры если есть
            if dynamic_params:
                params.update(dynamic_params)
            
            # Заголовки
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
            
            logger.info("Выполнение запроса к Instagram GraphQL API...")
            
            try:
                # Выполняем POST-запрос
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.session.post(url, data=params, headers=headers, timeout=30)
                )
                
                logger.info(f"Ответ сервера: {response.status_code}")
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        
                        # Проверяем наличие данных
                        if 'data' in data and data['data']:
                            logger.info("JSON успешно получен и содержит данные")
                            
                            # Сохраняем для отладки
                            if os.environ.get('DEBUG_INSTAGRAM', '').lower() == 'true':
                                import time
                                filename = f"debug_instagram_{shortcode}_{int(time.time())}.json"
                                with open(filename, "w", encoding="utf-8") as f:
                                    json.dump(data, f, indent=2, ensure_ascii=False)
                                logger.info(f"Debug данные сохранены в: {filename}")
                            
                            return data, shortcode
                            
                        elif 'errors' in data:
                            logger.error(f"GraphQL ошибки: {data['errors']}")
                            return None, None
                            
                        else:
                            logger.warning("Ответ не содержит данных или ошибок")
                            logger.warning(f"Ключи ответа: {list(data.keys())}")
                            return None, None
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"Ошибка парсинга JSON: {e}")
                        logger.error(f"Ответ сервера: {response.text[:200]}...")
                        return None, None
                
                elif response.status_code == 429:
                    logger.warning("Rate limit обнаружен")
                    return None, None
                    
                elif response.status_code == 403:
                    logger.warning("Доступ запрещен (403)")
                    return None, None
                    
                else:
                    logger.error(f"HTTP ошибка: {response.status_code}")
                    logger.error(f"Ответ: {response.text[:100]}...")
                    return None, None
                    
            except Exception as request_error:
                logger.error(f"Ошибка выполнения запроса: {request_error}")
                return None, None
                
        except Exception as e:
            logger.error(f"Критическая ошибка в fetch_instagram_post: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None, None





    
    def extract_video_url(self, json_data: dict) -> Optional[str]:
        """
        ИСПРАВЛЕННАЯ версия extract_video_url
        Правильно обрабатывает формат xdt_shortcode_media
        """
        try:
            # Логируем для отладки
            logger.debug(f"Анализ JSON структуры: {list(json_data.keys())}")
            
            media = None
            
            if 'data' in json_data:
                data_keys = list(json_data['data'].keys())
                logger.debug(f"Ключи в data: {data_keys}")
                
                # ВАЖНО: Сначала проверяем НОВЫЙ формат xdt_shortcode_media
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
            video_types = ['GraphVideo', 'XDTGraphVideo']  # XDTGraphVideo - новый тип!
            
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
            
            # ПОСЛЕДНИЙ ШАНС: Рекурсивный поиск по всему JSON
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
            
            # Ищем любые ключи со словом 'video'
            video_keys = [k for k in media.keys() if 'video' in k.lower()]
            if video_keys:
                logger.error(f"Найдены ключи с 'video': {video_keys}")
            
            # Ищем любые ключи со словом 'url'
            url_keys = [k for k in media.keys() if 'url' in k.lower()]
            if url_keys:
                logger.error(f"Найдены ключи с 'url': {url_keys}")
            
            return None
                    
        except Exception as e:
            logger.error(f"💥 Критическая ошибка в extract_video_url: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    async def download_video_new_method(self, url: str, output_path: str) -> bool:
        """Загрузка видео с URL и сохранение в файл"""
        try:
            logger.info(f"Загрузка видео с: {url}")
            
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
            
            # Выполняем запрос
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.session.get(url, headers=headers, stream=True, timeout=60)
            )
            
            response.raise_for_status()
            
            # Получаем размер файла
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            logger.info(f"Сохранение в: {output_path}")
            if total_size > 0:
                logger.info(f"Размер файла: {total_size / (1024*1024):.2f} MB")
            
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
                                logger.debug(f"Прогресс: {progress:.1f}% ({downloaded_size / (1024*1024):.2f} MB)")
            
            logger.info(f"Видео загружено успешно: {output_path}")
            logger.info(f"Итоговый размер: {downloaded_size / (1024*1024):.2f} MB")
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке видео: {e}")
            return False
    
    async def extract_video_info(self, url: str) -> Dict:
        """Извлечение информации о видео из URL"""
        try:
            shortcode = self.extract_shortcode(url)
            
            # Попытка получить информацию через GraphQL API
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
            # Метод 1: Новый GraphQL API метод
            try:
                logger.info(f"Попытка загрузки через GraphQL API: {url}")
                
                # Получаем данные поста
                data, shortcode = await self.fetch_instagram_post(url)
                
                if data and shortcode:
                    # Извлекаем URL видео
                    video_url = self.extract_video_url(data)
                    
                    if video_url:
                        # Загружаем видео
                        success = await self.download_video_new_method(video_url, output_path)
                        
                        if success and os.path.exists(output_path):
                            logger.info(f"Успешная загрузка через GraphQL API: {output_path}")
                            return output_path
                        else:
                            logger.warning("Загрузка через GraphQL API не удалась")
                    else:
                        logger.warning("video_url не найден в ответе GraphQL API")
                else:
                    logger.warning("Не удалось получить данные через GraphQL API")
                    
            except Exception as e:
                logger.warning(f"Ошибка GraphQL API метода: {e}")
            
            # Метод 2: Резервный метод через yt-dlp
            try:
                logger.info(f"Попытка загрузки через yt-dlp: {url}")
                
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
                }
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([url])
                    
                    if os.path.exists(output_path):
                        logger.info(f"Успешная загрузка через yt-dlp: {output_path}")
                        return output_path
                        
            except Exception as e:
                logger.warning(f"Ошибка yt-dlp метода: {e}")
            
            # Если все методы провалились
            logger.error(f"Все методы загрузки провалились для URL: {url}")
            
            # Удаляем частично загруженный файл, если он существует
            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except:
                    pass
            
            return None
            
        except Exception as e:
            logger.error(f"Критическая ошибка при загрузке видео: {e}")
            
            # Удаляем частично загруженный файл
            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except:
                    pass
            
            return None