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
        """Получение динамических параметров со страницы Instagram"""
        try:
            post_url = f"https://www.instagram.com/reel/{shortcode}/"
            logger.info(f"Получение параметров с: {post_url}")
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1"
            }
            
            response = await self._make_request_async("get", post_url, headers=headers)
            
            if not response:
                logger.error("Ошибка при получении HTML-страницы")
                return {}
            
            params = {}
            
            # Извлекаем токен LSD
            lsd_match = re.search(r'"LSD",\[\],\{"token":"([^"]+)"', response)
            if lsd_match:
                params["lsd"] = lsd_match.group(1)
                logger.debug(f"Найден токен LSD: {params['lsd']}")
            
            # Извлекаем jazoest
            jazoest_match = re.search(r'"jazoest":"([^"]+)"', response)
            if jazoest_match:
                params["jazoest"] = jazoest_match.group(1)
                logger.debug(f"Найден jazoest: {params['jazoest']}")
            
            # Извлекаем __spin_r (revision)
            spin_r_match = re.search(r'"__spin_r":(\d+)', response)
            if spin_r_match:
                params["__spin_r"] = spin_r_match.group(1)
                logger.debug(f"Найден __spin_r: {params['__spin_r']}")
            
            # Извлекаем __hsi
            hsi_match = re.search(r'"hsi":"(\d+)"', response)
            if hsi_match:
                params["__hsi"] = hsi_match.group(1)
                logger.debug(f"Найден __hsi: {params['__hsi']}")
            
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
        """Получение данных поста из Instagram GraphQL API"""
        try:
            shortcode = self.extract_shortcode(instagram_url)
            
            if not shortcode:
                logger.error("Неверный URL Instagram или shortcode не найден")
                return None, None
            
            logger.info(f"Извлечен shortcode: {shortcode}")
            
            current_timestamp = int(time.time())
            dynamic_params = await self.get_instagram_params(shortcode)
            
            url = "https://www.instagram.com/graphql/query"
            
            # Базовые параметры
            params = {
                "av": "0",
                "__d": "www",
                "__user": "0",
                "__a": "1",
                "__req": "b",
                "dpr": "1",
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
                "doc_id": "8845758582119845"
            }
            
            # Динамические параметры
            time_sensitive_params = {
                "__hs": "20158.HYP:instagram_web_pkg.2.1...0",
                "__rev": "1020782089",
                "__s": "3q1x7r:b7bkvx:frscxl",
                "__hsi": "7480634687841513346",
                "__dyn": "7xeUjG1mxu1syUbFp41twpUnwgU7SbzEdF8aUco2qwJw5ux609vCwjE1EE2Cw8G11wBz81s8hwGxu786a3a1YwBgao6C0Mo2swtUd8-U2zxe2GewGw9a361qw8Xxm16wa-0raazo7u3C2u2J0bS1LwTwKG1pg2fwxyo6O1FwlEcUed6goK2O4UrAwHxW1oxe17wciubBKu9w",
                "__csr": "g9i2cnbVbXlkBcHyVd9QVb-hQACDXGA_le4-haGA_UZ3XAGm8IyKXLAFXhlEyxaRydqSuBz8HAV4ay95RAxmppfz9lKZ2V9o-eGFohyryK9yUB9KEGpacKq8nx2XzHpoG49ERzoK5orx66U8E01fAo9ERwq8Ehob8dU4y4QcgoEJ09qui0IoVwyGE5G1IwVw8u0gi0q-058o0Gx1C488C0gm0luhUr5BgCl0aOmfw4Dxf84o15Fx832zFqg46lo07wG0tO06fU",
                "__hsdp": "",
                "__hblp": "",
                "lsd": "AVoXppBilIg",
                "jazoest": "21029",
                "__spin_r": "1020782089",
                "__spin_t": str(current_timestamp),
            }
            
            # Объединяем параметры
            all_params = {**params, **time_sensitive_params, **dynamic_params}
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://www.instagram.com",
                "Referer": f"https://www.instagram.com/reel/{shortcode}/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "X-CSRFToken": dynamic_params.get('lsd', ''),
                "X-Instagram-AJAX": "1",
                "X-Requested-With": "XMLHttpRequest"
            }
            
            logger.info("Выполнение запроса к Instagram GraphQL API...")
            
            # Выполняем POST-запрос
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.session.post(url, data=all_params, headers=headers, timeout=30)
            )
            
            response.raise_for_status()
            data = response.json()
            
            # Сохраняем для отладки если включен DEBUG режим
            if os.environ.get('DEBUG_INSTAGRAM', '').lower() == 'true':
                filename = f"instagram_post_{shortcode}.json"
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                logger.debug(f"Ответ сохранен в {filename}")
            
            return data, shortcode
            
        except Exception as e:
            logger.error(f"Ошибка при получении данных поста: {e}")
            return None, None
    
    def extract_video_url(self, json_data: Dict) -> Optional[str]:
        """Извлечение URL видео из ответа Instagram API с поддержкой новых форматов"""
        try:
            # Поддержка как нового, так и старого формата API
            media = None
            
            if 'data' in json_data:
                # Новый формат: xdt_shortcode_media
                if 'xdt_shortcode_media' in json_data['data']:
                    media = json_data['data']['xdt_shortcode_media']
                    logger.debug("Найден формат xdt_shortcode_media")
                # Старый формат: shortcode_media
                elif 'shortcode_media' in json_data['data']:
                    media = json_data['data']['shortcode_media']
                    logger.debug("Найден формат shortcode_media")
            
            if not media:
                logger.error("Медиа данные не найдены в JSON")
                return None
            
            logger.debug(f"Тип медиа: {media.get('__typename', 'Unknown')}")
            logger.debug(f"Это видео: {media.get('is_video', False)}")
            
            # Проверяем, что это видео (поддержка обоих форматов)
            video_types = ['GraphVideo', 'XDTGraphVideo']
            if media.get('__typename') in video_types or media.get('is_video', False):
                # Прямой поиск video_url
                video_url = media.get('video_url')
                if video_url:
                    logger.info(f"Найден video_url: {video_url[:100]}...")
                    return video_url
                
                # Альтернативный поиск в video_resources
                if 'video_resources' in media and len(media['video_resources']) > 0:
                    logger.debug(f"Найдено video_resources с {len(media['video_resources'])} вариантами")
                    video_resources = media['video_resources']
                    highest_quality = max(video_resources, key=lambda x: x.get('config_width', 0) * x.get('config_height', 0))
                    video_url = highest_quality.get('src')
                    if video_url:
                        logger.info(f"Найден video_url в video_resources: {video_url[:100]}...")
                        return video_url
            
            # Обработка каруселей (поддержка обоих форматов)
            elif media.get('__typename') in ['GraphSidecar', 'XDTGraphSidecar']:
                logger.debug("Обработка карусели")
                edges = media.get('edge_sidecar_to_children', {}).get('edges', [])
                for i, edge in enumerate(edges):
                    node = edge.get('node', {})
                    logger.debug(f"Элемент карусели {i+1}: {node.get('__typename', 'Unknown')}, is_video: {node.get('is_video', False)}")
                    if node.get('is_video', False):
                        video_url = node.get('video_url')
                        if video_url:
                            logger.info(f"Найден video_url в карусели: {video_url[:100]}...")
                            return video_url
            
            # Рекурсивный поиск video_url в JSON (последняя попытка)
            def find_video_url_recursive(obj):
                if isinstance(obj, dict):
                    if "video_url" in obj and isinstance(obj["video_url"], str):
                        return obj["video_url"]
                    for key, value in obj.items():
                        result = find_video_url_recursive(value)
                        if result:
                            return result
                elif isinstance(obj, list):
                    for item in obj:
                        result = find_video_url_recursive(item)
                        if result:
                            return result
                return None
            
            video_url = find_video_url_recursive(json_data)
            if video_url:
                logger.info(f"Найден video_url рекурсивным поиском: {video_url[:100]}...")
                return video_url
            
            logger.error("video_url не найден во всей структуре JSON")
            logger.debug(f"Доступные ключи в media: {list(media.keys())[:10]}...")
            return None
                
        except Exception as e:
            logger.error(f"Ошибка при извлечении URL видео: {e}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
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