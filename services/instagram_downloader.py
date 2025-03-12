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
        regex = r"instagram\.com\/(?:p|reel)\/([A-Za-z0-9_-]+)"
        match = re.search(regex, url)
        return match.group(1) if match else None
    
    async def get_instagram_params(self, shortcode: str) -> Dict[str, Any]:
        """Получение динамических параметров со страницы Instagram"""
        try:
            # Формируем URL для поста Instagram
            post_url = f"https://www.instagram.com/reel/{shortcode}/"
            
            logger.info(f"Получение параметров с: {post_url}")
            
            # Выполняем GET-запрос на страницу поста
            response = await self._make_request_async("get", post_url)
            
            if not response:
                logger.error("Ошибка при получении HTML-страницы")
                return {}
            
            # Извлекаем токены из HTML-ответа
            html_content = response
            
            # Инициализируем словарь параметров
            params = {}
            
            # Извлекаем токен LSD
            lsd_match = re.search(r'"LSD"[^{]*{"token":"([^"]+)"', html_content)
            if lsd_match:
                params["lsd"] = lsd_match.group(1)
                logger.info(f"Найден токен LSD: {params['lsd']}")
            
            # Извлекаем jazoest
            jazoest_match = re.search(r'jazoest=(\d+)', html_content)
            if jazoest_match:
                params["jazoest"] = jazoest_match.group(1)
                logger.info(f"Найден jazoest: {params['jazoest']}")
            
            # Извлекаем __spin_r (revision)
            spin_r_match = re.search(r'"__spin_r":(\d+)', html_content)
            if spin_r_match:
                params["__spin_r"] = spin_r_match.group(1)
                logger.info(f"Найден __spin_r: {params['__spin_r']}")
            
            # Извлекаем __hsi
            hsi_match = re.search(r'"hsi":"(\d+)"', html_content)
            if hsi_match:
                params["__hsi"] = hsi_match.group(1)
                logger.info(f"Найден __hsi: {params['__hsi']}")
            
            # Извлекаем __hs (haste_session)
            hs_match = re.search(r'"haste_session":"([^"]+)"', html_content)
            if hs_match:
                params["__hs"] = hs_match.group(1)
                logger.info(f"Найден __hs: {params['__hs']}")
            
            # Извлекаем __rev
            rev_match = re.search(r'"server_revision":(\d+)', html_content)
            if rev_match:
                params["__rev"] = rev_match.group(1)
                logger.info(f"Найден __rev: {params['__rev']}")
            
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
            # Извлекаем shortcode из URL
            shortcode = self.extract_shortcode(instagram_url)
            
            if not shortcode:
                logger.error("Неверный URL Instagram или shortcode не найден")
                return None, None
            
            logger.info(f"Извлечен shortcode: {shortcode}")
            
            # Получаем текущую временную метку в секундах (UTC)
            current_timestamp = int(time.time())
            logger.info(f"Текущая временная метка: {current_timestamp}")
            
            # Получаем динамические параметры со страницы Instagram
            dynamic_params = await self.get_instagram_params(shortcode)
            
            # Конечная точка API
            url = "https://www.instagram.com/graphql/query"
            
            # Базовые параметры, которые с меньшей вероятностью изменяются
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
            
            # Динамические параметры, зависящие от времени
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
                "__spin_t": str(current_timestamp),  # Динамически обновляем временную метку
            }
            
            # Объединяем базовые и зависящие от времени параметры
            # Переопределяем динамически полученными параметрами
            all_params = {**params, **time_sensitive_params, **dynamic_params}
            
            # Настройка заголовков запроса
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://www.instagram.com",
                "Referer": f"https://www.instagram.com/reel/{shortcode}/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin"
            }
            
            # Выполняем запрос
            logger.info("Выполнение запроса к Instagram GraphQL API...")
            logger.info(f"Используемый URL: {instagram_url}")
            logger.info(f"Время запроса: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Выполняем POST-запрос
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.session.post(url, data=all_params, headers=headers)
            )
            
            # Проверяем успешность запроса
            response.raise_for_status()
            
            # Разбираем JSON-ответ
            data = response.json()
            
            # Отладочное логирование с сохранением в файл
            if os.environ.get('DEBUG_INSTAGRAM', '').lower() == 'true':
                filename = f"instagram_post_{shortcode}.json"
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                logger.debug(f"Ответ сохранен в {filename}")
                
                params_filename = f"request_params_{shortcode}.json"
                with open(params_filename, "w", encoding="utf-8") as f:
                    json.dump(all_params, f, indent=2, ensure_ascii=False)
                logger.debug(f"Параметры запроса сохранены в {params_filename}")
            
            return data, shortcode
            
        except Exception as e:
            logger.error(f"Ошибка при получении данных поста: {e}")
            return None, None
    
    def extract_video_url(self, json_data: Dict) -> Optional[str]:
        """Извлечение URL видео из ответа Instagram API"""
        try:
            # Путь к URL видео может варьироваться в зависимости от структуры ответа API
            video_url = None
            
            # Проходим по структуре JSON
            # Пробуем несколько путей, потому что структура API Instagram может меняться
            try:
                # Путь 1: Стандартный путь для основных данных поста
                shortcode_media = json_data["data"]["xdt_api__v1__media__shortcode__web_info"]["data"]["shortcode_media"]
                
                # Проверяем наличие video_url в основных video_versions
                if "video_url" in shortcode_media:
                    video_url = shortcode_media["video_url"]
                
                # Пытаемся найти видео в video_versions, если оно существует
                elif "video_versions" in shortcode_media and len(shortcode_media["video_versions"]) > 0:
                    # Получаем версию с наивысшим качеством (обычно первая в списке)
                    video_url = shortcode_media["video_versions"][0]["url"]
                
                # Ищем URL видео в карусели, если это пост-карусель
                elif "edge_sidecar_to_children" in shortcode_media:
                    edges = shortcode_media["edge_sidecar_to_children"]["edges"]
                    for edge in edges:
                        node = edge["node"]
                        if node.get("is_video", False) and "video_url" in node:
                            video_url = node["video_url"]
                            break
            except (KeyError, TypeError):
                # Если первый путь не удался, пробуем альтернативные пути
                pass
            
            if not video_url:
                try:
                    # Путь 2: Альтернативный путь, иногда используемый
                    media = json_data["data"]["media"]
                    if "video_url" in media:
                        video_url = media["video_url"]
                except (KeyError, TypeError):
                    pass
                    
            if not video_url:
                # Рекурсивно ищем video_url в данных JSON
                def find_video_url(obj):
                    if isinstance(obj, dict):
                        # Проверяем, содержит ли этот словарь video_url
                        if "video_url" in obj and isinstance(obj["video_url"], str):
                            return obj["video_url"]
                        
                        # В противном случае ищем во всех значениях этого словаря
                        for key, value in obj.items():
                            result = find_video_url(value)
                            if result:
                                return result
                    
                    elif isinstance(obj, list):
                        # Ищем во всех элементах этого списка
                        for item in obj:
                            result = find_video_url(item)
                            if result:
                                return result
                    
                    return None
                
                video_url = find_video_url(json_data)
            
            if video_url:
                logger.info(f"Найден URL видео: {video_url}")
                return video_url
            else:
                logger.error("Не удалось найти URL видео в ответе JSON")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка при извлечении URL видео: {e}")
            return None
    
    async def download_video_new_method(self, url: str, output_path: str) -> bool:
        """Загрузка видео с URL и сохранение в файл"""
        try:
            logger.info(f"Загрузка видео с: {url}")
            
            # Настройка заголовков запроса
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.instagram.com/",
                "Sec-Fetch-Dest": "video",
                "Sec-Fetch-Mode": "no-cors",
                "Sec-Fetch-Site": "same-site"
            }
            
            # Выполняем запрос для загрузки видео
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.session.get(url, headers=headers, stream=True)
            )
            
            response.raise_for_status()
            
            # Создаем директорию downloads, если она не существует
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Получаем общий размер файла для отображения прогресса
            total_size = int(response.headers.get('content-length', 0))
            
            # Загружаем с индикатором прогресса
            with open(output_path, "wb") as f:
                if total_size == 0:
                    # Если размер неизвестен, просто загружаем
                    f.write(response.content)
                else:
                    # Показываем прогресс
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB фрагменты
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            progress = int(100 * downloaded / total_size)
                            if progress % 10 == 0:  # Логируем каждые 10%
                                logger.info(f"Прогресс: {progress}% ({downloaded/1024/1024:.1f}MB/{total_size/1024/1024:.1f}MB)")
            
            logger.info(f"Видео сохранено в: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке видео: {e}")
            return False
    
    async def extract_video_info(self, url: str) -> Dict:
        """Извлечение информации о видео из URL"""
        # Имитация работы метода для совместимости с BaseDownloader
        # В действительности мы получаем информацию через другие методы
        try:
            shortcode = self.extract_shortcode(url)
            return {
                'title': f'Instagram Video {shortcode}',
                'duration': 0,  # Длительность неизвестна
                'thumbnail': '',  # Миниатюра неизвестна
                'uploader': 'Instagram User',
                'formats': [],
                'is_live': False
            }
        except Exception as e:
            logger.error(f"Ошибка при извлечении информации о видео: {e}")
            return {}
    
    async def download_video(self, url: str, output_path: str = None) -> Optional[str]:
        """Загрузка видео из Instagram с использованием новых и резервных методов"""
        if not output_path:
            output_path = self.generate_output_filename("instagram")
        
        try:
            # Метод 1: Используем новый метод через GraphQL API
            try:
                logger.info(f"Попытка загрузки через новый метод GraphQL API: {url}")
                
                # Получаем данные поста
                data, shortcode = await self.fetch_instagram_post(url)
                
                if data and shortcode:
                    # Извлекаем URL видео из ответа
                    video_url = self.extract_video_url(data)
                    
                    if video_url:
                        # Загружаем видео
                        success = await self.download_video_new_method(video_url, output_path)
                        
                        if success and os.path.exists(output_path):
                            logger.info(f"Успешная загрузка через новый метод GraphQL API: {output_path}")
                            return output_path
            except Exception as e:
                logger.warning(f"Ошибка загрузки через новый метод GraphQL API: {e}")
                # Продолжаем со следующим методом
                
            # Метод 2: Используем yt-dlp (стандартный метод из BaseDownloader)
            try:
                logger.info(f"Попытка загрузки через yt-dlp: {url}")
                
                # Создаем временный путь для yt-dlp
                temp_path = f"{output_path}.ytdlp"
                
                import yt_dlp
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
                        logger.info(f"Успешная загрузка через yt-dlp: {output_path}")
                        return output_path
            except Exception as e:
                logger.warning(f"Ошибка загрузки через yt-dlp: {e}")
            
            # Если все методы не сработали
            logger.error(f"Все методы загрузки не удались для URL: {url}")
            
            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except:
                    pass
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке видео: {e}")
            
            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except:
                    pass
            
            return None