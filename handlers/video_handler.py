import time
import logging
import aiohttp
import uuid
from datetime import datetime
import asyncio
import aiofiles
from typing import Optional, List
import yt_dlp
from moviepy.editor import VideoFileClip
from pyrogram import Client
import os
from os import path
import math
import re

from aiogram import Bot, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramRetryAfter
from config.config import BOT_TOKEN



from services.kuaishou import KuaishouDownloader
from services.transcriber import VideoTranscriber
from services.rednote import RedNoteDownloader
from services.database import Database
from services.tts_service import TTSService
from services.audio_handler import AudioHandler
from states.states import VideoProcessing
from services.cobalt import CobaltDownloader
from services.instagram_downloader import InstagramDownloader
from services.connection_manager import ConnectionManager
from services.video_streaming import VideoStreamingService
from services.chunk_uploader import ChunkUploader
from services.video_speed import VideoSpeedService


from pyrogram import Client
import os
from os import path
import math

import weakref
from typing import Dict, Set, Any
import gc

from config.config import setup_logging
from config.config import ELEVENLABS_VOICES, API_ID, API_HASH
# Настройка логирования
logger = setup_logging(__name__)

class VideoHandler:
    def __init__(self):
        """Инициализация обработчика видео"""
        self.kuaishou = KuaishouDownloader()
        self.rednote = RedNoteDownloader()
        self.transcriber = VideoTranscriber()
        self.tts_service = TTSService()
        self.connection_manager = ConnectionManager("telegram_client")
        self.chunk_uploader = ChunkUploader()
        self.db = Database()
        self.audio_handler = AudioHandler()
        
        self.downloads_dir = "downloads"  # Для скачанных видео
        self.video_speed_service = VideoSpeedService(self.downloads_dir)
        
        self.file_registry = {}
        self.bot = None  # Будет установлен позже
        self.session = None
        self.connector = None
        self.bot_files_base_dir = None
        self.bot_api_dir = "telegram-bot-api-data/telegram-bot-api-data"  # Базовая директория локального сервера
        self.DOWNLOAD_TIMEOUT = 60  # таймаут для скачивания в секундах
        self.local_api_url = "http://localhost:8081"  # URL локального сервера
        self.api_endpoint = f"{self.local_api_url}/bot{BOT_TOKEN}"
        self.session = None


        # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Замена на обычный set с таймаутом
        self.active_users: Set[int] = set()
        self.user_timeouts: Dict[int, float] = {}  # user_id -> timestamp
        self.user_timeout_duration = 300  # 5 минут таймаут
        
        # Кэш загрузчиков с автоочисткой
        self._downloader_cache: Dict[str, Any] = {}
        self._cache_cleanup_interval = 300  # 5 минут
        
        # Семафор для ограничения одновременных загрузок
        self._download_semaphore = asyncio.Semaphore(3)
        
        # Запускаем фоновую очистку
        asyncio.create_task(self._background_cleanup())
        
        # Настройки клиента
        self.app = None
        self.api_id = API_ID
        self.api_hash = API_HASH
        self.bot_token = BOT_TOKEN
        
        # Создаем только основную директорию
        os.makedirs(self.downloads_dir, exist_ok=True)
        

    async def init_session(self):
        """Инициализация сессии"""
        if not self.session:
            self.connector = aiohttp.TCPConnector(force_close=True)
            self.session = aiohttp.ClientSession(
                base_url=self.local_api_url,
                connector=self.connector
            )

    async def close_session(self):
        """Закрытие сессии"""
        if self.session:
            await self.session.close()
            self.session = None
        if self.connector:
            await self.connector.close()
            self.connector = None

    def add_active_user(self, user_id: int) -> bool:
        """Добавление пользователя в активные с проверкой таймаута"""
        current_time = time.time()
        
        # Очищаем устаревшие записи
        self._cleanup_expired_users(current_time)
        
        if user_id in self.active_users:
            # Проверяем, не истек ли таймаут
            if user_id in self.user_timeouts:
                if current_time - self.user_timeouts[user_id] < self.user_timeout_duration:
                    return False  # Пользователь все еще активен
                else:
                    # Таймаут истек, удаляем и добавляем заново
                    self.remove_active_user(user_id)
        
        self.active_users.add(user_id)
        self.user_timeouts[user_id] = current_time
        return True
    
    def remove_active_user(self, user_id: int):
        """Удаление пользователя из активных"""
        self.active_users.discard(user_id)
        self.user_timeouts.pop(user_id, None)
    
    def _cleanup_expired_users(self, current_time: float):
        """Очистка пользователей с истекшим таймаутом"""
        expired_users = [
            user_id for user_id, timestamp in self.user_timeouts.items()
            if current_time - timestamp > self.user_timeout_duration
        ]
        
        for user_id in expired_users:
            self.remove_active_user(user_id)
    
    async def _background_cleanup(self):
        """Фоновая очистка ресурсов каждые 5 минут"""
        while True:
            try:
                await asyncio.sleep(self._cache_cleanup_interval)
                current_time = time.time()
                self._cleanup_expired_users(current_time)
                await self._cleanup_downloaders()
                gc.collect()  # Принудительная сборка мусора
                logger.debug(f"Выполнена фоновая очистка. Активных пользователей: {len(self.active_users)}")
            except Exception as e:
                logger.error(f"Ошибка в фоновой очистке: {e}")
    
    async def _cleanup_downloaders(self):
        """Очистка кэша загрузчиков"""
        try:
            for service_type, downloader in list(self._downloader_cache.items()):
                if hasattr(downloader, 'cleanup'):
                    await downloader.cleanup()
                del self._downloader_cache[service_type]
            logger.debug("Кэш загрузчиков очищен")
        except Exception as e:
            logger.error(f"Ошибка при очистке загрузчиков: {e}")

    def _get_or_create_downloader(self, service_type: str) -> Any:
        """Получение или создание загрузчика с кэшированием"""
        if service_type not in self._downloader_cache:
            if service_type == 'instagram':
                from services.instagram_downloader import InstagramDownloader
                self._downloader_cache[service_type] = InstagramDownloader(self.downloads_dir)
            elif service_type == 'kuaishou':
                from services.kuaishou import KuaishouDownloader
                self._downloader_cache[service_type] = KuaishouDownloader()
            elif service_type == 'rednote':
                from services.rednote import RedNoteDownloader
                self._downloader_cache[service_type] = RedNoteDownloader()
            else:
                # Fallback загрузчик
                from services.youtube_downloader import YouTubeDownloader
                self._downloader_cache[service_type] = YouTubeDownloader(self.downloads_dir)
        
        return self._downloader_cache[service_type]
    
    async def download_video(self, url: str, service_type: str) -> str:
        """ИСПРАВЛЕННЫЙ метод загрузки видео с семафором и правильным управлением ресурсами"""
        async with self._download_semaphore:  # Ограничиваем количество одновременных загрузок
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            temp_name = f"temp_{service_type}_{timestamp}"
            final_name = f"{service_type}_{timestamp}"
            
            temp_path = os.path.join(self.downloads_dir, f"{temp_name}.mp4")
            final_path = os.path.join(self.downloads_dir, f"{final_name}.mp4")
            
            download_success = False
            error_messages = []
            downloader = None
            
            try:
                logger.info(f"Начинаем загрузку {service_type} видео: {url}")
                
                if service_type == 'instagram':
                    # Метод 1: Через специализированный загрузчик
                    try:
                        logger.info("Попытка загрузки Instagram видео через специализированный API...")
                        downloader = self._get_or_create_downloader('instagram')
                        result_path = await downloader.download_video(url, temp_path)
                        
                        if result_path and os.path.exists(result_path):
                            if result_path != temp_path and os.path.exists(result_path):
                                import shutil
                                shutil.copy2(result_path, temp_path)
                                # Удаляем оригинал если он отличается
                                try:
                                    if result_path != temp_path:
                                        os.remove(result_path)
                                except:
                                    pass
                            logger.info(f"✅ Успешная загрузка Instagram видео через специализированный API")
                            download_success = True
                        else:
                            error_messages.append("Специализированный Instagram API не смог загрузить видео")
                            
                    except Exception as e:
                        logger.warning(f"❌ Ошибка специализированного Instagram API: {e}")
                        error_messages.append(f"Ошибка Instagram API: {str(e)}")
                    finally:
                        # Очищаем ресурсы загрузчика
                        if downloader and hasattr(downloader, 'cleanup'):
                            try:
                                await downloader.cleanup()
                            except:
                                pass
                
                # Метод 2: Через Cobalt (резервный для всех сервисов)
                if not download_success:
                    try:
                        logger.info(f"Попытка загрузки {service_type} видео через Cobalt API...")
                        from services.cobalt import CobaltDownloader
                        cobalt = CobaltDownloader()
                        downloaded_path = await cobalt.download_video(url)
                        
                        if downloaded_path and os.path.exists(downloaded_path):
                            if downloaded_path != temp_path:
                                import shutil
                                shutil.copy2(downloaded_path, temp_path)
                                # Удаляем оригинал Cobalt файл
                                try:
                                    os.remove(downloaded_path)
                                except:
                                    pass
                            logger.info(f"✅ Успешная загрузка {service_type} видео через Cobalt API")
                            download_success = True
                        else:
                            error_messages.append("Файл не найден после загрузки через Cobalt")
                            
                    except Exception as e:
                        logger.warning(f"❌ Не удалось загрузить {service_type} видео через Cobalt: {e}")
                        error_messages.append(f"Ошибка Cobalt: {str(e)}")
                
                # Метод 3: Через yt-dlp (последний резерв)
                if not download_success:
                    try:
                        logger.info(f"Попытка загрузки {service_type} видео через yt-dlp...")
                        await self._download_with_ytdlp(url, temp_path)
                        
                        if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
                            logger.info(f"✅ Успешная загрузка {service_type} видео через yt-dlp")
                            download_success = True
                        else:
                            error_messages.append("yt-dlp загрузил пустой файл")
                            
                    except Exception as e:
                        logger.warning(f"❌ Не удалось загрузить {service_type} видео через yt-dlp: {e}")
                        error_messages.append(f"Ошибка yt-dlp: {str(e)}")
                
                # Проверяем результат
                if not download_success or not os.path.exists(temp_path):
                    error_message = f"Все методы загрузки {service_type} видео не удались:\n" + "\n".join(error_messages)
                    logger.error(error_message)
                    raise Exception(error_message)
                
                # Проверяем размер файла
                file_size = os.path.getsize(temp_path)
                if file_size == 0:
                    raise Exception("Загружен пустой файл (0 байт)")
                
                # Перемещаем файл в конечный путь
                os.rename(temp_path, final_path)
                logger.info(f"✅ Видео успешно загружено: {final_path} (размер: {file_size/1024/1024:.2f} МБ)")
                return final_path
                
            except Exception as e:
                logger.error(f"❌ Критическая ошибка при загрузке видео: {str(e)}")
                # Очищаем временные файлы
                for path in [temp_path, final_path]:
                    if path and os.path.exists(path):
                        try:
                            os.remove(path)
                            logger.debug(f"Удален временный файл: {path}")
                        except Exception as clean_error:
                            logger.error(f"Ошибка при удалении файла {path}: {clean_error}")
                raise
            finally:
                # Принудительная очистка
                if downloader and hasattr(downloader, 'cleanup'):
                    try:
                        await downloader.cleanup()
                    except:
                        pass

    

    # async def set_bot(self, bot):
    #     """Установка экземпляра бота и инициализация путей"""
    #     self.bot = bot
    #     # Получаем базовый путь к файлам бота
    #     test_file = await self.bot.get_file("unknown_file_id")
    #     self.bot_files_base_dir = os.path.dirname(os.path.dirname(test_file.file_path))

    async def set_bot(self, bot):
        """Установка экземпляра бота"""
        self.bot = bot

    async def initialize(self):
        """Асинхронная инициализация после создания бота"""
        try:
            # Создаем все необходимые директории
            for directory in [self.downloads_dir, "models"]:
                os.makedirs(directory, exist_ok=True)
                logger.info(f"Создана директория: {directory}")
                
            # Проверяем права доступа
            for directory in [self.downloads_dir, "models"]:
                if not os.access(directory, os.W_OK):
                    raise PermissionError(f"Нет прав на запись в директорию: {directory}")
                    
            logger.info("Инициализация VideoHandler завершена успешно")
            
        except Exception as e:
            logger.error(f"Ошибка при инициализации VideoHandler: {e}")
            raise

    async def get_file_path(self, file_id: str) -> str:
        """Получение файла через локальный сервер с сохранением в downloads"""
        try:
            file = await self.bot.get_file(file_id)
            # Сохраняем все файлы в downloads
            local_path = os.path.join(self.downloads_dir, f"{file_id}_{os.path.basename(file.file_path)}")
            
            logger.info(f"Сохранение файла в: {local_path}")
            
            # Скачиваем файл в downloads
            await self.bot.download_file(file.file_path, local_path)
            
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"Файл не был загружен: {local_path}")
                
            logger.info(f"Файл успешно сохранен: {local_path}")
            return local_path
            
        except Exception as e:
            logger.error(f"Ошибка получения файла: {e}")
            raise

    async def _register_file(self, original_path: str, renamed_path: Optional[str] = None):
        """Регистрация файла для последующей очистки"""
        file_id = str(uuid.uuid4())
        self.file_registry[file_id] = {
            'original_path': original_path,
            'renamed_path': renamed_path,
            'created_at': time.time()
        }
        return file_id
    
    async def _safe_delete_file(self, file_path: str) -> bool:
        """Безопасное удаление файла с проверками"""
        try:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Успешно удален файл: {file_path}")
                return True
            return False
        except Exception as e:
            logger.error(f"Ошибка при удалении файла {file_path}: {e}")
            return False

    async def init_client(self):
        """Инициализация Pyrogram клиента с обработкой ошибок соединения"""
        if self.app is None:
            try:
                # Удаляем неподдерживаемые параметры
                self.app = Client(
                    "video_downloader",
                    api_id=self.api_id,
                    api_hash=self.api_hash,
                    bot_token=self.bot_token,
                    in_memory=True
                    # Удаляем connect_timeout и max_concurrent_transmissions
                )
                
                # Регистрируем клиент в менеджере соединений
                self.connection_manager.register_client("pyrogram", self.app)
                
                # Пытаемся подключиться с повторами при ошибках
                await self.connection_manager.with_connection_retry(
                    self.app.start
                )
                logger.info("Pyrogram клиент успешно инициализирован")
                
            except Exception as e:
                logger.error(f"Критическая ошибка при инициализации Pyrogram клиента: {e}")
                if self.app:
                    try:
                        await self.app.stop()
                    except:
                        pass
                    self.app = None
                raise

    async def delete_previous_message(self, state: FSMContext):
        """Удаление предыдущего сообщения бота"""
        data = await state.get_data()
        prev_bot_message_id = data.get('prev_bot_message_id')
        chat_id = data.get('chat_id')
        
        if prev_bot_message_id and chat_id:
            try:
                bot = Bot(token=BOT_TOKEN)
                await bot.delete_message(chat_id, prev_bot_message_id)
            except Exception as e:
                logger.warning(f"Не удалось удалить предыдущее сообщение: {e}")

    async def save_bot_message(self, message: types.Message, state: FSMContext):
        """Сохранение ID сообщения бота"""
        await state.update_data(
            prev_bot_message_id=message.message_id,
            chat_id=message.chat.id
        )

    

    def get_service_type(self, url: str) -> tuple:
        """Определяет тип сервиса по URL и возвращает чистый URL"""
        url_lower = url.lower()
        logger.info(f"Определение типа сервиса для URL: {url_lower}")
        
        # Исходный URL по умолчанию
        clean_url = url
        
        # Извлечение URL из сообщения, скопированного из приложения Xiaohongshu
        if 'xhslink.com' in url_lower:
            # Шаблон для поиска URL вида http://xhslink.com/X/XXXXX
            pattern = r'(https?://xhslink\.com/[a-zA-Z0-9/]+)'
            match = re.search(pattern, url)
            if match:
                # Сохраняем только сам URL без дополнительного текста
                clean_url = match.group(1)
                # Обновляем URL в логе для отладки
                logger.info(f"Извлечен чистый URL: {clean_url}")
            return 'rednote', clean_url
        
        # Для полных URL xiaohongshu.com - используем полный URL
        elif 'xiaohongshu.com' in url_lower:
            logger.info("Определен сервис: RedNote (полный URL)")
            return 'rednote', url
        
        # Определение Pinterest ссылок
        elif 'pinterest.com' in url_lower or 'pin.it' in url_lower:
            logger.info("Определен сервис: Pinterest")
            return 'pinterest', url
        
        # Для других сервисов
        elif 'youtube.com' in url_lower or 'youtu.be' in url_lower:
            return 'youtube', url
        elif 'instagram.com' in url_lower or 'instagr.am' in url_lower:
            return 'instagram', url
        elif 'kuaishou.com' in url_lower:
            return 'kuaishou', url
        
        logger.warning(f"Неизвестный тип сервиса для URL: {url}")
        return 'unknown', url


    async def download_telegram_video(self, message: types.Message) -> str:
        """
        Скачивает видео из Telegram с поддержкой больших файлов через MTProto
        """
        file_id = message.video.file_id
        file_path = os.path.join(self.downloads_dir, f"{file_id}.mp4")

        try:
            file_size_mb = message.video.file_size / (1024 * 1024)
            logger.info(f"Начало загрузки видео размером {file_size_mb:.2f} MB")
            
            # Инициализируем клиент, если еще не инициализирован
            await self.init_client()
            
            # Создаем сообщение с прогрессом
            progress_message = await message.reply(
                f"⏳ Загрузка видео: 0%\n({file_size_mb:.1f} MB)"
            )
            
            # Скачиваем файл через MTProto с отслеживанием прогресса
            await self.app.download_media(
                message.video,
                file_name=file_path,
                progress=self._download_progress,
                progress_args=(progress_message,)
            )

            if not os.path.exists(file_path):
                raise Exception("Файл не был загружен")

            actual_size = os.path.getsize(file_path)
            logger.info(f"Видео успешно загружено. Размер: {actual_size/1024/1024:.2f} MB")
            
            # Удаляем сообщение с прогрессом
            await progress_message.delete()
            
            return file_path

        except Exception as e:
            logger.error(f"Ошибка при скачивании видео: {str(e)}")
            if os.path.exists(file_path):
                os.remove(file_path)
            raise Exception(f"Ошибка при скачивании видео: {str(e)}")
    
    async def close_client(self):
        """Закрытие Pyrogram клиента"""
        if self.app and self.app.is_connected:
            await self.app.stop()
        
    async def _download_progress(self, current, total, message):
        """Обновление прогресса загрузки"""
        try:
            if total:
                percentage = (current * 100) / total
                current_mb = current / (1024 * 1024)
                total_mb = total / (1024 * 1024)
                
                if percentage % 5 == 0:  # Обновляем каждые 5%
                    await message.edit_text(
                        f"⏳ Загрузка видео: {percentage:.1f}%\n"
                        f"({current_mb:.1f}/{total_mb:.1f} MB)"
                    )
        except Exception as e:
            logger.error(f"Ошибка обновления прогресса: {e}")

    async def download_video(self, url: str, service_type: str) -> str:
        """Загружает видео с разных сервисов с использованием нескольких методов последовательно"""
        # Генерируем временные имена с использованием timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_name = f"temp_{service_type}_{timestamp}"
        final_name = f"{service_type}_{timestamp}"
        
        temp_path = os.path.join(self.downloads_dir, f"{temp_name}.mp4")
        final_path = os.path.join(self.downloads_dir, f"{final_name}.mp4")
        
        # Флаг для отслеживания успешной загрузки
        download_success = False
        error_messages = []
        
        try:
            # ОБРАБОТКА REDNOTE - ИСПРАВЛЕНО
            if service_type == 'rednote':
                # Метод 1: Специализированный RedNoteDownloader (в приоритете)
                try:
                    logger.info("Попытка загрузки RedNote видео через RedNoteDownloader...")
                    max_attempts = 3
                    for attempt in range(max_attempts):
                        try:
                            success, message, video_info = await self.rednote.get_video_url(url)
                            if success:
                                # ВАЖНО: Правильно обрабатываем новый формат данных
                                if isinstance(video_info, dict):
                                    # Если это данные от AnyDownloader API
                                    if 'medias' in video_info and video_info['medias']:
                                        logger.info("Используем данные AnyDownloader API для скачивания")
                                        if await self.rednote.download_video(video_info, temp_path):
                                            logger.info(f"✅ Успешная загрузка RedNote видео через AnyDownloader API (попытка {attempt+1})")
                                            download_success = True
                                            break
                                    # Если это данные от XHSDownloader или старого API
                                    elif 'video_url' in video_info:
                                        video_url = video_info['video_url']
                                        if await self.rednote.download_video(video_url, temp_path):
                                            logger.info(f"✅ Успешная загрузка RedNote видео через XHSDownloader/старый API (попытка {attempt+1})")
                                            download_success = True
                                            break
                                    else:
                                        logger.warning(f"Неизвестный формат данных video_info: {video_info}")
                                        error_messages.append(f"Неизвестный формат данных: {message}")
                                else:
                                    error_messages.append(f"Неверный тип данных video_info: {type(video_info)}")
                                
                                if attempt < max_attempts - 1:
                                    wait_time = (attempt + 1) * 5
                                    logger.info(f"Повторная попытка через {wait_time} секунд...")
                                    await asyncio.sleep(wait_time)
                                else:
                                    error_messages.append(message)
                                    download_success = False
                            else:
                                error_messages.append(message)
                                if attempt < max_attempts - 1:
                                    wait_time = (attempt + 1) * 5
                                    logger.info(f"Повторная попытка через {wait_time} секунд...")
                                    await asyncio.sleep(wait_time)
                                else:
                                    download_success = False
                        except Exception as e:
                            if attempt < max_attempts - 1:
                                logger.warning(f"Попытка {attempt + 1} не удалась: {str(e)}")
                                await asyncio.sleep(5)
                            else:
                                error_messages.append(f"Ошибка после {max_attempts} попыток: {str(e)}")
                                download_success = False
                except Exception as e:
                    logger.warning(f"❌ Не удалось загрузить RedNote видео через RedNoteDownloader: {e}")
                    error_messages.append(f"Ошибка RedNoteDownloader: {str(e)}")
                    download_success = False
                
                # Метод 2: Через Cobalt, если RedNoteDownloader не сработал
                if not download_success:
                    try:
                        logger.info("Попытка загрузки RedNote видео через Cobalt API...")
                        cobalt = CobaltDownloader()
                        downloaded_path = await cobalt.download_video(url)
                        if downloaded_path and os.path.exists(downloaded_path):
                            # Перемещаем файл в правильное место
                            if downloaded_path != temp_path:
                                import shutil
                                shutil.move(downloaded_path, temp_path)
                            logger.info(f"✅ Успешная загрузка RedNote видео через Cobalt API")
                            download_success = True
                        else:
                            error_messages.append("Файл не найден после загрузки через Cobalt")
                            download_success = False
                    except Exception as e:
                        logger.warning(f"❌ Не удалось загрузить RedNote видео через Cobalt: {e}")
                        error_messages.append(f"Ошибка Cobalt: {str(e)}")
                        download_success = False
                
                # Метод 3: Через yt-dlp, если предыдущие методы не сработали
                if not download_success:
                    try:
                        logger.info("Попытка загрузки RedNote видео через yt-dlp...")
                        await self._download_with_ytdlp(url, temp_path)
                        
                        if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
                            logger.info(f"✅ Успешная загрузка RedNote видео через yt-dlp")
                            download_success = True
                        else:
                            error_messages.append("yt-dlp загрузил пустой файл")
                            download_success = False
                    except Exception as e:
                        logger.warning(f"❌ Не удалось загрузить RedNote видео через yt-dlp: {e}")
                        error_messages.append(f"Ошибка yt-dlp: {str(e)}")
                        download_success = False
                
                # Если все методы не сработали
                if not download_success:
                    error_message = "Все методы загрузки RedNote видео не удались:\n" + "\n".join(error_messages)
                    logger.error(error_message)
                    raise Exception(error_message)

            # ОБРАБОТКА INSTAGRAM
            elif service_type == 'instagram':
                # Метод 1: Наш новый метод скачивания Instagram
                try:
                    from services.instagram_downloader import InstagramDownloader
                    logger.info("Попытка загрузки Instagram видео через новый API метод...")
                    
                    instagram_dl = InstagramDownloader(self.downloads_dir)
                    result_path = await instagram_dl.download_video(url, temp_path)
                    
                    if result_path and os.path.exists(result_path):
                        if result_path != temp_path and os.path.exists(result_path):
                            import shutil
                            shutil.copy2(result_path, temp_path)
                        logger.info(f"✅ Успешная загрузка Instagram видео через новый API метод")
                        download_success = True
                    else:
                        error_messages.append("Не удалось загрузить через новый Instagram API метод")
                        download_success = False
                except Exception as e:
                    logger.warning(f"❌ Не удалось загрузить Instagram видео через новый API метод: {e}")
                    error_messages.append(f"Ошибка нового Instagram API метода: {str(e)}")
                    download_success = False
                
                # Метод 2: Через Cobalt, если новый метод не сработал
                if not download_success:
                    try:
                        logger.info("Попытка загрузки Instagram видео через Cobalt API...")
                        cobalt = CobaltDownloader()
                        downloaded_path = await cobalt.download_video(url)
                        if downloaded_path and os.path.exists(downloaded_path):
                            os.rename(downloaded_path, temp_path)
                            logger.info(f"✅ Успешная загрузка Instagram видео через Cobalt API")
                            download_success = True
                        else:
                            error_messages.append("Файл не найден после загрузки через Cobalt")
                            download_success = False
                    except Exception as e:
                        logger.warning(f"❌ Не удалось загрузить Instagram видео через Cobalt: {e}")
                        error_messages.append(f"Ошибка Cobalt: {str(e)}")
                        download_success = False
                
                # Метод 3: Через yt-dlp, если предыдущие методы не сработали
                if not download_success:
                    try:
                        logger.info("Попытка загрузки Instagram видео через yt-dlp...")
                        await self._download_with_ytdlp(url, temp_path)
                        
                        if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
                            logger.info(f"✅ Успешная загрузка Instagram видео через yt-dlp")
                            download_success = True
                        else:
                            error_messages.append("yt-dlp загрузил пустой файл")
                            download_success = False
                    except Exception as e:
                        logger.warning(f"❌ Не удалось загрузить Instagram видео через yt-dlp: {e}")
                        error_messages.append(f"Ошибка yt-dlp: {str(e)}")
                        download_success = False
                
                # Если все методы не сработали
                if not download_success:
                    error_message = "Все методы загрузки Instagram видео не удались:\n" + "\n".join(error_messages)
                    logger.error(error_message)
                    raise Exception(error_message)
                    
            # ОБРАБОТКА KUAISHOU
            elif service_type == 'kuaishou':
                # Метод 1: Специализированный метод для Kuaishou
                try:
                    logger.info("Попытка загрузки Kuaishou видео через специализированный метод...")
                    result = await self.kuaishou.download_video(url, temp_path)
                    if result and os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
                        logger.info(f"✅ Успешная загрузка Kuaishou видео через специализированный метод")
                        download_success = True
                    else:
                        error_messages.append("Специализированный метод не смог загрузить видео")
                        download_success = False
                except Exception as e:
                    logger.warning(f"❌ Не удалось загрузить Kuaishou видео через специализированный метод: {e}")
                    error_messages.append(f"Ошибка специализированного метода: {str(e)}")
                    download_success = False
                
                # Метод 2: Через Cobalt, если специализированный метод не сработал
                if not download_success:
                    try:
                        logger.info("Попытка загрузки Kuaishou видео через Cobalt API...")
                        cobalt = CobaltDownloader()
                        downloaded_path = await cobalt.download_video(url)
                        if downloaded_path and os.path.exists(downloaded_path):
                            os.rename(downloaded_path, temp_path)
                            logger.info(f"✅ Успешная загрузка Kuaishou видео через Cobalt API")
                            download_success = True
                        else:
                            error_messages.append("Файл не найден после загрузки через Cobalt")
                            download_success = False
                    except Exception as e:
                        logger.warning(f"❌ Не удалось загрузить Kuaishou видео через Cobalt: {e}")
                        error_messages.append(f"Ошибка Cobalt: {str(e)}")
                        download_success = False
                
                # Метод 3: Через yt-dlp, если предыдущие методы не сработали
                if not download_success:
                    try:
                        logger.info("Попытка загрузки Kuaishou видео через yt-dlp...")
                        await self._download_with_ytdlp(url, temp_path)
                        
                        if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
                            logger.info(f"✅ Успешная загрузка Kuaishou видео через yt-dlp")
                            download_success = True
                        else:
                            error_messages.append("yt-dlp загрузил пустой файл")
                            download_success = False
                    except Exception as e:
                        logger.warning(f"❌ Не удалось загрузить Kuaishou видео через yt-dlp: {e}")
                        error_messages.append(f"Ошибка yt-dlp: {str(e)}")
                        download_success = False
                
                # Если все методы не сработали
                if not download_success:
                    error_message = "Все методы загрузки Kuaishou видео не удались:\n" + "\n".join(error_messages)
                    logger.error(error_message)
                    raise Exception(error_message)
            
            # ОБРАБОТКА PINTEREST
            elif service_type == 'pinterest':
                # Метод 1: Через Cobalt для Pinterest
                try:
                    logger.info("Попытка загрузки Pinterest видео через Cobalt API...")
                    cobalt = CobaltDownloader()
                    downloaded_path = await cobalt.download_video(url)
                    
                    if downloaded_path and os.path.exists(downloaded_path):
                        # Копируем или перемещаем файл в нужное место
                        if downloaded_path != temp_path:
                            import shutil
                            shutil.copy2(downloaded_path, temp_path)
                            logger.info(f"Скопировано видео из {downloaded_path} в {temp_path}")
                        
                        logger.info(f"✅ Успешная загрузка Pinterest видео через Cobalt API")
                        download_success = True
                    else:
                        error_messages.append("Файл не найден после загрузки через Cobalt")
                        download_success = False
                except Exception as e:
                    logger.warning(f"❌ Не удалось загрузить Pinterest видео через Cobalt: {e}")
                    error_messages.append(f"Ошибка Cobalt: {str(e)}")
                    download_success = False
                
                # Метод 2: Через yt-dlp, если Cobalt не сработал
                if not download_success:
                    try:
                        logger.info("Попытка загрузки Pinterest видео через yt-dlp...")
                        await self._download_with_ytdlp(url, temp_path)
                        
                        if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
                            logger.info(f"✅ Успешная загрузка Pinterest видео через yt-dlp")
                            download_success = True
                        else:
                            error_messages.append("yt-dlp загрузил пустой файл")
                            download_success = False
                    except Exception as e:
                        logger.warning(f"❌ Не удалось загрузить Pinterest видео через yt-dlp: {e}")
                        error_messages.append(f"Ошибка yt-dlp: {str(e)}")
                        download_success = False
                
                # Если все методы не сработали
                if not download_success:
                    error_message = "Все методы загрузки Pinterest видео не удались:\n" + "\n".join(error_messages)
                    logger.error(error_message)
                    raise Exception(error_message)
                    
            # ОБРАБОТКА ДРУГИХ СЕРВИСОВ (YouTube и прочие)
            else:
                # Метод 1: Через Cobalt (основной для YouTube и других)
                try:
                    logger.info(f"Попытка загрузки {service_type} видео через Cobalt API...")
                    cobalt = CobaltDownloader()
                    downloaded_path = await cobalt.download_video(url)
                    if downloaded_path and os.path.exists(downloaded_path):
                        os.rename(downloaded_path, temp_path)
                        logger.info(f"✅ Успешная загрузка {service_type} видео через Cobalt API")
                        download_success = True
                    else:
                        error_messages.append("Файл не найден после загрузки через Cobalt")
                        download_success = False
                except Exception as e:
                    logger.warning(f"❌ Не удалось загрузить {service_type} видео через Cobalt: {e}")
                    error_messages.append(f"Ошибка Cobalt: {str(e)}")
                    download_success = False
                    
                
                # Метод 2: Через yt-dlp , если Cobalt не сработал
                if not download_success:
                    try:
                        logger.info(f"Попытка загрузки {service_type} видео через yt-dlp...")
                        await self._download_with_ytdlp(url, temp_path)
                        
                        if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
                            logger.info(f"✅ Успешная загрузка {service_type} видео через yt-dlp")
                            download_success = True
                        else:
                            error_messages.append("yt-dlp загрузил пустой файл")
                            download_success = False
                    except Exception as e:
                        logger.warning(f"❌ Не удалось загрузить {service_type} видео через yt-dlp: {e}")
                        error_messages.append(f"Ошибка yt-dlp: {str(e)}")
                        download_success = False
                        
                
                # Если все методы не сработали
                if not download_success:
                    error_message = f"Все методы загрузки {service_type} видео не удались:\n" + "\n".join(error_messages)
                    logger.error(error_message)
                    raise Exception(error_message)

            # Проверяем успешность загрузки
            if not os.path.exists(temp_path):
                raise Exception("Видео не было загружено, файл не существует")
                
            # Проверяем размер файла
            file_size = os.path.getsize(temp_path)
            if file_size == 0:
                raise Exception("Загружен пустой файл (0 байт)")
                
            # Перемещаем файл в конечный путь
            os.rename(temp_path, final_path)
            logger.info(f"✅ Видео успешно загружено и сохранено в {final_path} (размер: {file_size/1024/1024:.2f} МБ)")
            return final_path

        except Exception as e:
            logger.error(f"❌ Критическая ошибка при загрузке видео: {str(e)}")
            # Очищаем временные файлы
            for path in [temp_path, final_path]:
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                        logger.info(f"Удален временный файл: {path}")
                    except Exception as clean_error:
                        logger.error(f"Ошибка при удалении файла {path}: {clean_error}")
            raise

        # Вспомогательный метод для скачивания через yt-dlp
    async def _download_with_ytdlp(self, url: str, output_path: str) -> bool:
        """Скачивание видео через yt-dlp"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1'
        }
        
        ydl_opts = {
            # Формат указывает максимальное разрешение 1080p
            'format': 'bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080][ext=mp4]/best[height<=1080]',
            'outtmpl': output_path,
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False,
            'no_color': True,
            'http_headers': headers,
            'merge_output_format': 'mp4',
            'prefer_ffmpeg': True,
            # Постпроцессоры для обеспечения формата MP4 
            'postprocessors': [{
                'key': 'FFmpegVideoConvertor',
                'preferedformat': 'mp4',
            }],
            # Сортировка форматов с приоритетом высоких, но не более 1080p
            'format_sort': [
                'height:1080',        # Приоритет 1080p
                'height:720',         # Затем 720p
                'ext:mp4:m4a',        # Предпочитаем MP4
                'codec:h264:aac',     # H.264 и AAC кодеки
                'size',               # Размер файла
                'br',                 # Битрейт
                'fps',                # Частота кадров
                'quality', 
            ],
            'retries': 3,
            'fragment_retries': 3,
            'skip_unavailable_fragments': True,
            'keepvideo': True,
            'sleep_interval': 3,
            'max_sleep_interval': 6,
            'sleep_interval_requests': 1,
        }
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: yt_dlp.YoutubeDL(ydl_opts).download([url])
        )
        
        # Проверяем, существует ли файл и не пустой ли он
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            return True
        return False

    async def get_available_formats(self, url: str) -> list:
        """Получение информации о доступных форматах видео"""
        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': False
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                formats = info.get('formats', [])
                
                # Фильтруем и сортируем форматы
                video_formats = [f for f in formats if f.get('vcodec') != 'none']
                sorted_formats = sorted(
                    video_formats,
                    key=lambda x: (
                        x.get('height', 0),
                        x.get('filesize', 0),
                        x.get('tbr', 0)
                    ),
                    reverse=True
                )
                
                return sorted_formats
                
        except Exception as e:
            logger.error(f"Ошибка при получении форматов: {str(e)}")
            return []

    async def process_url(self, message: types.Message, state: FSMContext):
        """ИСПРАВЛЕННЫЙ метод обработки URL с правильным управлением состоянием"""
        logger.info(f"Получен URL для обработки: {message.text}")
        user_id = message.from_user.id
        
        # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Используем новый метод проверки активности
        if not self.add_active_user(user_id):
            await message.reply("⏳ Пожалуйста, дождитесь окончания обработки предыдущего запроса")
            return
        
        video_path = None
        status_message = None
        
        try:
            # Сначала сохраняем исходное сообщение в состояние
            await state.update_data(
                original_message=message,
                request_type='url'
            )
            
            # Определяем тип сервиса и извлекаем чистый URL
            service_type, url_to_process = self.get_service_type(message.text)
            status_message = await message.reply("⏳ Начинаю загрузку видео...")
            
            # Загружаем видео с использованием очищенного URL
            video_path = await self.download_video(url_to_process, service_type)
            
            # После успешной загрузки обновляем состояние
            await state.update_data(
                video_path=video_path,
                service_type=service_type
            )
            
            # Показываем выбор действия для всех типов видео
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="📥 Скачать", callback_data="action_download"),
                        InlineKeyboardButton(text="🎯 Распознать", callback_data="action_recognize")
                    ],
                    [
                        InlineKeyboardButton(text="⚡ Ускорить", callback_data="action_speedup")
                    ]
                ]
            )
            
            await status_message.edit_text(
                "🎥 Выберите действие с видео:",
                reply_markup=keyboard
            )
            
            await state.set_state(VideoProcessing.WAITING_FOR_ACTION)
                
            # Логируем успешную обработку
            self.db.log_url(
                user_id=message.from_user.id,
                username=message.from_user.username,
                url=url_to_process,
                status="success"
            )

        except Exception as e:
            error_msg = f"❌ Ошибка при обработке видео: {str(e)}"
            logger.error(error_msg)
            
            if status_message:
                await status_message.edit_text(error_msg)
            else:
                await message.reply(error_msg)
            
            if video_path and os.path.exists(video_path):
                try:
                    os.remove(video_path)
                except Exception as clean_error:
                    logger.error(f"Ошибка при очистке файла {video_path}: {clean_error}")
                        
        finally:
            # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Всегда удаляем пользователя из активных
            self.remove_active_user(user_id)

    async def _process_chinese_transcription(self, message: types.Message, state: FSMContext, status_message: types.Message):
        """Обработка китайской транскрипции"""
        user_id = message.from_user.id
        max_retries = 3
        retry_delay = 5
        
        try:
            data = await state.get_data()
            video_path = data.get('video_path')
            audio_path = data.get('audio_path')

            # Добавляем цикл повторных попыток
            for attempt in range(max_retries):
                try:
                    await status_message.edit_text(f"🎯 Распознаю речь на китайском... Попытка {attempt + 1}/{max_retries}")
                    text = await self.transcriber.transcribe(audio_path, 'zh')
                    
                    if text:  # Если успешно получили текст, прерываем цикл
                        break
                        
                except Exception as e:
                    logger.warning(f"Попытка {attempt + 1} не удалась: {str(e)}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay)
                    else:
                        raise

            # Отправляем результат
            if text and text.strip():
                header = "🎯 Распознанный текст (zh):\n\n"
                data = await state.get_data()
                service_type = data.get('service_type', 'unknown')
                
                # Определяем максимальную длину подписи
                max_caption_length = 1024 - len(header)
                
                filename = self.generate_video_filename(
                    service_type=service_type,
                    action='recognition',
                    text_lang='zh'
                )
                
                try:
                    # Отправляем видео с текстом
                    await self.app.send_video(
                        chat_id=message.chat.id,
                        video=video_path,
                        caption=f"{header}{text[:max_caption_length]}" if len(text) <= max_caption_length else f"{header}(текст будет отправлен отдельно)"
                    )

                    # Если текст слишком длинный, отправляем его отдельно
                    if len(text) > max_caption_length:
                        for i in range(0, len(text), 4000):
                            chunk = text[i:i + 4000]
                            await asyncio.sleep(2)
                            await message.reply(chunk)

                except Exception as e:
                    raise Exception(f"Ошибка при отправке результата: {str(e)}")
            else:
                await message.reply("❌ Не удалось распознать текст")

        except Exception as e:
            error_msg = f"❌ Ошибка при обработке китайского языка: {str(e)}"
            logger.error(error_msg)
            await status_message.edit_text(error_msg)
            
        finally:
            # Очистка файлов
            for path in [video_path, audio_path]:
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                    except Exception as e:
                        logger.error(f"Ошибка при удалении файла {path}: {e}")
            
            self.active_users.discard(user_id)

    def get_safe_local_path(self, file_path: str) -> str:
        """Преобразует путь к файлу в безопасный локальный путь"""
        # Убираем специальные символы и пробелы
        base_name = os.path.basename(file_path)
        safe_name = "".join(c for c in base_name if c.isalnum() or c in "._-")
        return os.path.join(self.downloads_dir, safe_name)

    async def process_video(self, message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        
        if user_id in self.active_users:
            await message.reply("⏳ Пожалуйста, дождитесь окончания обработки предыдущего запроса")
            return
            
        self.active_users.add(user_id)
        video_path = None
        status_message = None
            
        try:
            await state.update_data(
                original_message=message,
                request_type='upload'
            )
            
            # Информируем о размере файла
            file_size_mb = message.video.file_size / (1024 * 1024)
            status_message = await message.reply(
                f"⏳ Начинаю обработку видео размером {file_size_mb:.1f} MB..."
            )
            
            try:
                # Получаем информацию о файле из API
                file = await self.bot.get_file(message.video.file_id)
                logger.info(f"File object: {file}")
                logger.info(f"File path from API: {getattr(file, 'file_path', None)}")
                
                # Генерируем новое имя файла
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"video_{timestamp}.mp4"
                video_path = os.path.join(self.downloads_dir, filename)
                
                # Пытаемся определить, где находится файл на сервере
                # Если локальный API-сервер использует directory mapping
                server_file_path = None
                if hasattr(file, 'file_path') and file.file_path:
                    # Возможный путь к файлу внутри local API сервера
                    server_file_path = os.path.join(self.bot_api_dir, file.file_path)
                    logger.info(f"Полный путь к файлу на сервере: {server_file_path}")
                    
                    if os.path.exists(server_file_path):
                        # Копируем файл в нашу директорию
                        import shutil
                        shutil.copy2(server_file_path, video_path)
                        logger.info(f"Файл скопирован из: {server_file_path} в {video_path}")
                    else:
                        logger.warning(f"Файл не найден по пути: {server_file_path}")
                
                # Если не удалось найти файл на сервере, скачиваем через API
                if not os.path.exists(video_path):
                    logger.info("Скачиваем файл через download_file")
                    os.makedirs(os.path.dirname(video_path), exist_ok=True)
                    await self.bot.download_file(file.file_path, video_path)
                
                # Финальная проверка файла
                if not os.path.exists(video_path):
                    raise FileNotFoundError("Файл не был загружен или скопирован")
                
                # Проверяем размер загруженного файла
                actual_size = os.path.getsize(video_path)
                if actual_size == 0:
                    raise ValueError("Загруженный файл пуст")
                    
                # Сохраняем путь к видео
                await state.update_data(video_path=video_path)
                
                # Создаем клавиатуру с кнопками выбора действия
                keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(text="📥 Скачать", callback_data="action_download"),
                            InlineKeyboardButton(text="🎯 Распознать", callback_data="action_recognize")
                        ],
                        [
                            InlineKeyboardButton(text="⚡ Ускорить", callback_data="action_speedup")
                        ]
                    ]
                )
                
                await status_message.edit_text(
                    "🎥 Выберите действие с видео:",
                    reply_markup=keyboard
                )
                
                await state.set_state(VideoProcessing.WAITING_FOR_ACTION)
                logger.info(f"Видео успешно обработано и сохранено: {video_path}")

            except Exception as e:
                logger.error(f"Ошибка при обработке видео: {str(e)}")
                if video_path and os.path.exists(video_path):
                    os.remove(video_path)
                raise Exception(f"Не удалось загрузить видео файл: {str(e)}")

        except Exception as e:
            error_msg = f"❌ Ошибка при обработке видео: {str(e)}"
            logger.error(error_msg)
            
            if status_message:
                await status_message.edit_text(error_msg)
            else:
                await message.reply(error_msg)
            
            if video_path and os.path.exists(video_path):
                try:
                    os.remove(video_path)
                except Exception as clean_error:
                    logger.error(f"Ошибка при очистке файла {video_path}: {clean_error}")
        finally:
            self.active_users.discard(user_id)


    
    def log_file_info(self, file):
        """Логирование информации о файле"""
        try:
            file_info = {
                'file_id': getattr(file, 'file_id', 'N/A'),
                'file_path': getattr(file, 'file_path', 'N/A'),
                'file_size': getattr(file, 'file_size', 'N/A'),
                'file_unique_id': getattr(file, 'file_unique_id', 'N/A')
            }
            logger.info(f"File info: {file_info}")
        except Exception as e:
            logger.error(f"Error logging file info: {e}")

    async def cleanup_active_users(self):
        """Периодическая очистка зависших активных пользователей"""
        try:
            self.active_users.clear()
            logger.info("Active users list cleared")
        except Exception as e:
            logger.error(f"Error cleaning active users: {e}")

    async def cleanup_files(self, file_id: str):
        """Очистка всех файлов, связанных с определенной операцией"""
        try:
            if file_id in self.file_registry:
                file_info = self.file_registry[file_id]
                
                # Получаем базовое имя файла без расширения
                base_path = os.path.splitext(file_info['original_path'])[0]
                
                # Удаляем оригинальный файл
                await self._safe_delete_file(file_info['original_path'])
                
                # Удаляем переименованный файл, если он существует
                if file_info['renamed_path']:
                    await self._safe_delete_file(file_info['renamed_path'])
                
                # Ищем и удаляем все временные файлы, начинающиеся с temp_
                for filename in os.listdir(self.downloads_dir):
                    if filename.startswith('temp_'):
                        # Для файлов yt-dlp (.fdash- и другие временные файлы)
                        if '.fdash-' in filename or '.f' in filename:
                            await self._safe_delete_file(os.path.join(self.downloads_dir, filename))
                            
                # Удаляем запись из реестра
                del self.file_registry[file_id]
                logger.info(f"Завершена очистка файлов для {file_id}")
                
        except Exception as e:
            logger.error(f"Ошибка при очистке файлов для {file_id}: {e}")

    async def periodic_cleanup(self):
        """Периодическая очистка старых файлов"""
        try:
            current_time = time.time()
            # Очистка файлов из реестра
            for file_id, file_info in list(self.file_registry.items()):
                if current_time - file_info['created_at'] > 3600:  # 1 час
                    await self.cleanup_files(file_id)
                    
            # Очистка потерянных временных файлов
            if os.path.exists(self.downloads_dir):
                for filename in os.listdir(self.downloads_dir):
                    try:
                        if filename.startswith('temp_') and (
                            '.fdash-' in filename or 
                            '.f' in filename or 
                            filename.endswith('.m4a') or 
                            filename.endswith('.mp4')
                        ):
                            file_path = os.path.join(self.downloads_dir, filename)
                            if os.path.getctime(file_path) < current_time - 3600:
                                await self._safe_delete_file(file_path)
                    except Exception as e:
                        logger.error(f"Ошибка при удалении файла {filename}: {e}")
                        
        except Exception as e:
            logger.error(f"Ошибка при периодической очистке: {e}")

    async def cleanup_old_files(self):
        """Очистка старых временных файлов"""
        try:
            current_time = time.time()
            for filename in os.listdir(self.downloads_dir):
                file_path = os.path.join(self.downloads_dir, filename)
                if os.path.getmtime(file_path) < current_time - 86400:  # 24 часа
                    os.remove(file_path)
        except Exception as e:
            logger.error(f"Ошибка при очистке временных файлов: {e}")

    async def handle_flood_control(self, callback_query: types.CallbackQuery, retry_after: int):
        """Обработка флуд-контроля"""
        try:
            await callback_query.answer(f"⏳ Превышен лимит сообщений. Подождите {retry_after} секунд...")
            await asyncio.sleep(retry_after)
            return True
        except Exception as e:
            logger.error(f"Ошибка при обработке флуд-контроля: {e}")
            return False

    async def safe_send_message(self, message: types.Message, text: str, **kwargs):
        max_retries = 3
        current_retry = 0
        
        while current_retry < max_retries:
            try:
                return await message.reply(text, **kwargs)
            except TelegramRetryAfter as e:
                current_retry += 1
                retry_after = e.retry_after
                await self.handle_flood_control(message, retry_after)
                if current_retry == max_retries:
                    raise
            except Exception as e:
                raise

    async def handle_language_selection(self, callback_query: types.CallbackQuery, state: FSMContext):
        file_id = None
        message_with_buttons = callback_query.message
        user_id = callback_query.from_user.id
        
        try:
            if user_id in self.active_users:
                await callback_query.answer("⏳ Пожалуйста, дождитесь окончания обработки")
                return
                    
            self.active_users.add(user_id)
            await callback_query.answer()
                        
            data = await state.get_data()
            video_path = data.get('video_path')
            audio_path = data.get('audio_path')
            wav_path = data.get('wav_path')
            original_message = data.get('original_message')
            request_type = data.get('request_type', 'url')

            # Регистрируем файл для очистки
            if video_path:
                file_id = await self._register_file(video_path)
                
            if not all([original_message]) or not any([video_path, wav_path]):
                await message_with_buttons.edit_text("❌ Произошла ошибка: файлы не найдены") 
                return

            lang = callback_query.data.split('_')[1]
            await message_with_buttons.edit_text(f"🎯 Распознаю речь на {lang}...")

            text = None
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    text = await self.transcriber.transcribe(wav_path, lang)
                    if text:
                        break
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Попытка {attempt + 1} распознавания не удалась: {e}")
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(2)

            if text:
                header = f"🎯 Распознанный текст ({lang}):\n\n"
                if request_type == 'url' and video_path:
                    service_type = data.get('service_type', 'unknown')
                    filename = self.generate_video_filename(
                        service_type=service_type,
                        action='recognition',
                        text_lang=lang
                    )
                    
                    async with aiofiles.open(video_path, 'rb') as video_file:
                        video_data = await video_file.read()
                        
                    if len(text) <= (1024 - len(header)):
                        await self.app.send_video(
                            chat_id=original_message.chat.id,
                            video=video_path,
                            caption=f"{header}{text}"
                        )
                    else:
                        # Используем тот же путь к файлу для второго случая
                        await self.app.send_video(
                            chat_id=original_message.chat.id,
                            video=video_path,
                            caption=f"{header}(текст будет отправлен отдельно)"
                        )
                        # Отправляем текст отдельно
                        for i in range(0, len(text), 4000):
                            chunk = text[i:i + 4000]
                            await asyncio.sleep(2)
                            await self.app.send_message(
                                chat_id=original_message.chat.id,
                                text=chunk
                            )
                    
                    # После успешной отправки очищаем все файлы
                    if file_id:
                        await self.cleanup_files(file_id)
                        
                else:
                    for i in range(0, len(text), 4000):
                        chunk = text[i:i + 4000]
                        await asyncio.sleep(2)
                        await original_message.reply(f"{header if i == 0 else ''}{chunk}")
            else:
                await original_message.reply("❌ Не удалось распознать текст")

        except Exception as e:
            error_msg = f"❌ Ошибка: {str(e)}"
            logger.error(error_msg)
            if message_with_buttons:
                await message_with_buttons.edit_text(error_msg)
        finally:
            # Очищаем все файлы независимо от результата
            if file_id:
                await self.cleanup_files(file_id)
                
            if wav_path and os.path.exists(wav_path):
                try:
                    os.remove(wav_path)
                except Exception as e:
                    logger.error(f"Ошибка при удалении wav файла: {e}")
            
            if message_with_buttons:
                try:
                    await message_with_buttons.delete()
                except Exception as e:
                    logger.error(f"Ошибка при удалении сообщения: {e}")
            
            self.active_users.discard(user_id)

    async def send_video_safe(self, chat_id: int, video_path: str, caption: str = None):
        """Безопасная отправка видео с проверкой состояния клиента"""
        try:
            if not self.app or not self.app.is_connected:
                await self.init_client()
                
            if not os.path.exists(video_path):
                raise FileNotFoundError(f"Файл не найден: {video_path}")
                
            # Проверяем размер файла
            file_size = os.path.getsize(video_path)
            logger.info(f"Отправка видео размером: {file_size/1024/1024:.2f} MB")
            
            # Отправляем видео
            await self.app.send_video(
                chat_id=chat_id,
                video=video_path,
                caption=caption,
                progress=self._upload_progress
            )
            logger.info(f"Видео успешно отправлено: {video_path}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при отправке видео: {e}")
            raise


    async def send_video(self, chat_id: int, video_path: str, caption: str = None):
        """Отправка видео через локальный сервер с потоковой передачей и таймаутами"""
        try:
            if not self.session:
                await self.init_session()

            # Проверяем существование файла
            if not os.path.exists(video_path):
                raise FileNotFoundError(f"Файл не найден: {video_path}")

            file_size_mb = os.path.getsize(video_path) / (1024 * 1024)
            logger.info(f"Подготовка к отправке видео размером {file_size_mb:.2f} MB")

            # Формируем multipart данные с потоковой передачей
            form = aiohttp.FormData()
            form.add_field(
                'video',
                open(video_path, 'rb'),  # Файл читается потоково
                filename=os.path.basename(video_path),
                content_type='video/mp4'
            )
            form.add_field('chat_id', str(chat_id))
            if caption:
                form.add_field('caption', caption)

            # Устанавливаем таймаут (10 минут на всю операцию)
            timeout = aiohttp.ClientTimeout(total=600)

            # Отправляем запрос с чанковой передачей
            async with self.session.post(
                f"/bot{BOT_TOKEN}/sendVideo",
                data=form,
                chunked=True,  # Включаем чанковую передачу
                timeout=timeout
            ) as response:
                response.raise_for_status()
                result = await response.json()
                logger.info(f"Видео успешно отправлено: {video_path}")
                return result

        except asyncio.TimeoutError:
            logger.error(f"Превышен таймаут при отправке видео: {video_path}")
            raise Exception("Превышен таймаут отправки видео (10 минут)")
        except FileNotFoundError as e:
            logger.error(f"Файл не найден: {e}")
            raise
        except Exception as e:
            logger.error(f"Ошибка при отправке видео через локальный сервер: {e}")
            raise

    async def handle_tts_command(self, message: types.Message, state: FSMContext):
        """Обработка команды /tts"""
        try:
            user_id = message.from_user.id
            if user_id in self.active_users:
                await message.reply("⏳ Пожалуйста, дождитесь окончания обработки предыдущего запроса")
                return
                
            self.active_users.add(user_id)
            
            # Получаем текст после команды
            text = message.text.replace('/tts', '', 1).strip()
            
            if not text:
                await message.reply(
                    "ℹ️ Пожалуйста, добавьте текст после команды.\n"
                    "Пример: /tts Привет, как дела?"
                )
                return
                
            # Проверяем длину текста
            if len(text) > 1000:
                await message.reply("⚠️ Текст слишком длинный. Максимум 1000 символов.")
                return
                
            # Сохраняем текст в состояние
            await state.update_data(tts_text=text)
            
            # Создаем клавиатуру с голосами
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text=voice_data["name"],
                        callback_data=f"voice_{voice_key}"
                    )
                    for voice_key, voice_data in ELEVENLABS_VOICES.items()
                ]]
            )
            
            # Отправляем сообщение с выбором голоса
            await message.reply(
                "🎤 Выберите голос для озвучивания:",
                reply_markup=keyboard
            )
            
        except Exception as e:
            error_msg = f"❌ Ошибка при обработке команды: {str(e)}"
            logger.error(error_msg)
            await message.reply(error_msg)
        finally:
            self.active_users.discard(user_id)

    async def handle_voice_selection(self, callback_query: types.CallbackQuery, state: FSMContext):
        """Обработка выбора голоса"""
        try:
            message = callback_query.message
            user_id = callback_query.from_user.id
            
            if user_id in self.active_users:
                await callback_query.answer("⏳ Пожалуйста, дождитесь окончания обработки")
                return
                    
            self.active_users.add(user_id)
            await callback_query.answer()
            
            voice_key = callback_query.data.split('_')[1]
            data = await state.get_data()
            text = data.get('tts_text')
            
            if not text:
                await message.edit_text("❌ Ошибка: текст не найден")
                return
                    
            voice_config = ELEVENLABS_VOICES[voice_key]
            voice_name = voice_config["name"]
            
            await message.edit_text(f"🎵 Генерирую аудио с голосом «{voice_name}»...")
            
            # Получаем аудио и имя файла
            audio_data, filename = await self.tts_service.text_to_speech(text, voice_config)
            
            if audio_data and filename:
                # Отправляем аудио с новым форматом
                await callback_query.message.answer_audio(
                    audio=types.BufferedInputFile(
                        audio_data,
                        filename=filename
                    ),
                    caption=f"🎤 Голос: {voice_name}\n📝 Текст: {text[:100]}{'...' if len(text) > 100 else ''}"
                )
                await message.delete()
            else:
                await message.edit_text("❌ Не удалось сгенерировать аудио")
                
        except Exception as e:
            error_msg = f"❌ Ошибка при генерации аудио: {str(e)}"
            logger.error(error_msg)
            await message.edit_text(error_msg)
        finally:
            self.active_users.discard(user_id)

    async def handle_action_selection(self, callback_query: types.CallbackQuery, state: FSMContext):
        """Обработка выбора действия с видео"""
        message_with_buttons = callback_query.message
        user_id = callback_query.from_user.id
        file_id = None
        
        try:
            if not self.add_active_user(user_id):
                await callback_query.answer("⏳ Дождитесь окончания обработки")
                return
                            
            await callback_query.answer()
            
            if not self.app:
                await self.init_client()
            
            data = await state.get_data()
            video_path = data.get('video_path')
            original_message = data.get('original_message')
            service_type = data.get('service_type', 'unknown')
            
            if not all([video_path, original_message]):
                await message_with_buttons.edit_text("❌ Произошла ошибка: файлы не найдены")
                return
            
            # ИСПРАВЛЕНИЕ: Регистрируем файл ТОЛЬКО для download, не для speedup
            if callback_query.data.split('_')[1] == 'download':
                file_id = await self._register_file(video_path)
            
            action = callback_query.data.split('_')[1]
            
            if not os.path.exists(video_path):
                logger.error(f"Файл не найден: {video_path}")
                await message_with_buttons.edit_text("❌ Файл не найден")
                return
            
            # ИСПРАВЛЕНИЕ: для speedup НЕ удаляем пользователя из активных и НЕ чистим файлы
            if action == 'speedup':
                logger.info("=" * 60)
                logger.info("⚡ ПОЛЬЗОВАТЕЛЬ ВЫБРАЛ УСКОРЕНИЕ")
                logger.info(f"👤 User ID: {user_id}")
                logger.info(f"📁 Video path: {video_path}")
                logger.info(f"📊 Current state: {await state.get_state()}")
                logger.info("=" * 60)
                
                await message_with_buttons.edit_text(
                    "⚡ Введите коэффициент ускорения от 1 до 10:\n\n"
                    "1 = 1.01x (почти незаметно)\n"
                    "5 = 1.05x (умеренное ускорение)\n"
                    "10 = 1.10x (заметное ускорение)\n\n"
                    "Просто отправьте число от 1 до 10"
                )
                await state.set_state(VideoProcessing.WAITING_FOR_SPEED_COEFFICIENT)
                
                logger.info(f"✅ Состояние установлено: {await state.get_state()}")
                logger.info("=" * 60)
                
                return
                        
            if action == 'download':
                await message_with_buttons.edit_text("📤 Подготовка к отправке...")
                
                file_size = os.path.getsize(video_path)
                file_size_mb = file_size / (1024 * 1024)
                
                try:
                    filename = self.generate_video_filename(service_type)
                    
                    progress_message = await message_with_buttons.edit_text(
                        f"📤 Начинаю отправку видео ({file_size_mb:.1f} MB)..."
                    )
                    
                    video_caption = f"✅ Видео успешно загружено\n📁 Имя файла: {filename}"
                    await self.send_video(
                        chat_id=original_message.chat.id,
                        video_path=video_path,
                        caption=video_caption
                    )
                    
                    await progress_message.edit_text("✅ Видео успешно отправлено!")
                    await asyncio.sleep(1)
                    await progress_message.delete()
                    
                except Exception as e:
                    logger.error(f"Ошибка при отправке видео: {e}")
                    await message_with_buttons.edit_text(f"❌ Ошибка при отправке видео: {str(e)[:100]}")
                    raise
                        
            elif action == 'recognize':
                wav_path = os.path.join(self.downloads_dir, f"{os.path.splitext(os.path.basename(video_path))[0]}.wav")
            
                await message_with_buttons.edit_text("🎵 Извлекаю аудио из видео...")
                
                success = await self.transcriber.extract_audio(video_path, wav_path)
                
                if not success:
                    logger.error(f"Не удалось извлечь аудио из {video_path} в {wav_path}")
                    await message_with_buttons.edit_text("❌ Ошибка при извлечении аудио")
                    return
                
                await state.update_data(
                    audio_path=wav_path,
                    wav_path=wav_path
                )
                                    
                if service_type == 'kuaishou':
                    await self._process_chinese_transcription(original_message, state, message_with_buttons)
                else:
                    keyboard = InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang_ru"),
                                InlineKeyboardButton(text="🇬🇧 English", callback_data="lang_en"),
                                InlineKeyboardButton(text="🇨🇳 中文", callback_data="lang_zh")
                            ]
                        ]
                    )
                    
                    await message_with_buttons.edit_text(
                        "🌍 Выберите язык видео:",
                        reply_markup=keyboard
                    )
                                
        except Exception as e:
            error_msg = f"❌ Ошибка: {str(e)}"
            logger.error(error_msg)
            if message_with_buttons:
                await message_with_buttons.edit_text(error_msg)
                        
        finally:
            # ИСПРАВЛЕНИЕ: Удаляем пользователя из активных ТОЛЬКО для download и recognize
            action = callback_query.data.split('_')[1]
            if action != 'speedup':
                self.remove_active_user(user_id)
                
                # Очищаем файлы только для download
                if action == 'download' and file_id:
                    await self.cleanup_files(file_id)

    async def handle_speed_coefficient_input(self, message: types.Message, state: FSMContext):
        """Обработка ввода коэффициента ускорения"""
        user_id = message.from_user.id
        
        logger.info("=" * 60)
        logger.info(f"🎯 ПОЛУЧЕН ВВОД КОЭФФИЦИЕНТА от пользователя {user_id}")
        logger.info(f"📝 Текст сообщения: '{message.text}'")
        logger.info("=" * 60)
        
        try:
            # НЕ ПРОВЕРЯЕМ активность - пользователь уже активен после нажатия кнопки
            
            # Проверяем что это число
            try:
                coefficient = int(message.text.strip())
                logger.info(f"✅ Коэффициент распознан: {coefficient}")
            except ValueError:
                logger.warning(f"❌ Не удалось распознать число: {message.text}")
                await message.reply(
                    "❌ Пожалуйста, отправьте число от 1 до 10\n"
                    "Например: 5"
                )
                return
            
            if not 1 <= coefficient <= 10:
                logger.warning(f"❌ Коэффициент вне диапазона: {coefficient}")
                await message.reply(
                    "❌ Число должно быть от 1 до 10\n"
                    "Попробуйте еще раз"
                )
                return
            
            data = await state.get_data()
            video_path = data.get('video_path')
            service_type = data.get('service_type', 'unknown')
            
            logger.info(f"📁 Путь к видео из состояния: {video_path}")
            logger.info(f"🎬 Тип сервиса: {service_type}")
            
            if not video_path:
                logger.error("❌ video_path отсутствует в состоянии!")
                await message.reply("❌ Видео файл не найден в состоянии")
                self.remove_active_user(user_id)
                return
                
            if not os.path.exists(video_path):
                logger.error(f"❌ Файл не существует: {video_path}")
                await message.reply("❌ Видео файл не найден на диске")
                self.remove_active_user(user_id)
                return
            
            # Показываем статус
            status_message = await message.reply(
                f"⚡ Ускоряю видео с коэффициентом 1.{coefficient:02d}x...\n"
                f"Это может занять некоторое время..."
            )
            
            logger.info(f"🚀 Запускаем ускорение видео...")
            
            # Ускоряем видео
            processed_path = await self.video_speed_service.speed_up_video(
                input_path=video_path,
                speed_coefficient=coefficient,
                keep_original=True
            )
            
            logger.info(f"📤 Результат обработки: {processed_path}")
            
            if not processed_path or not os.path.exists(processed_path):
                logger.error("❌ Обработанный файл не создан")
                await status_message.edit_text("❌ Не удалось обработать видео")
                self.remove_active_user(user_id)
                return
            
            # Проверяем размер
            processed_size = os.path.getsize(processed_path)
            processed_size_mb = processed_size / (1024 * 1024)
            logger.info(f"✅ Обработанное видео: {processed_path} ({processed_size_mb:.2f} MB)")
            
            # Отправляем результат
            await status_message.edit_text("📤 Отправляю обработанное видео...")
            
            if not self.app:
                await self.init_client()
            
            filename = self.generate_video_filename(
                service_type=service_type,
                action=f'speed{coefficient}x'
            )
            
            video_caption = (
                f"✅ Видео ускорено в 1.{coefficient:02d}x\n"
                f"📁 Имя файла: {filename}\n"
                f"📦 Размер: {processed_size_mb:.1f} MB"
            )
            
            try:
                await self.app.send_video(
                    chat_id=message.chat.id,
                    video=processed_path,
                    caption=video_caption
                )
                
                logger.info(f"✅ Обработанное видео успешно отправлено")
                await status_message.delete()
                
            except Exception as send_error:
                logger.error(f"❌ Ошибка при отправке через Pyrogram: {send_error}")
                
                try:
                    await status_message.edit_text("📤 Пробую альтернативный способ отправки...")
                    
                    async with aiofiles.open(processed_path, 'rb') as video_file:
                        await self.bot.send_video(
                            chat_id=message.chat.id,
                            video=types.BufferedInputFile(
                                await video_file.read(),
                                filename=filename
                            ),
                            caption=video_caption
                        )
                    
                    await status_message.delete()
                    logger.info(f"✅ Видео отправлено через fallback метод")
                    
                except Exception as fallback_error:
                    logger.error(f"❌ Fallback также не сработал: {fallback_error}")
                    await status_message.edit_text(f"❌ Не удалось отправить видео")
            
            # Очищаем файлы
            try:
                if os.path.exists(processed_path):
                    os.remove(processed_path)
                    logger.info(f"🗑 Удален обработанный файл: {processed_path}")
                
                if os.path.exists(video_path):
                    os.remove(video_path)
                    logger.info(f"🗑 Удален оригинальный файл: {video_path}")
                    
            except Exception as e:
                logger.error(f"Ошибка при удалении файлов: {e}")
            
            # Сбрасываем состояние
            await state.clear()
            
        except Exception as e:
            error_msg = f"❌ Ошибка при ускорении видео: {str(e)}"
            logger.error(error_msg, exc_info=True)
            await message.reply(error_msg)
            
        finally:
            self.remove_active_user(user_id)

    async def _upload_progress(self, current, total, message):
        """Обновление прогресса отправки"""
        try:
            if total:
                percentage = (current * 100) / total
                current_mb = current / (1024 * 1024)
                total_mb = total / (1024 * 1024)
                
                if percentage % 5 == 0:  # Обновляем каждые 5%
                    await message.edit_text(
                        f"📤 Отправка видео: {percentage:.1f}%\n"
                        f"({current_mb:.1f}/{total_mb:.1f} MB)"
                    )
        except Exception as e:
            logger.error(f"Ошибка обновления прогресса отправки: {e}")

    async def handle_audio_action(self, callback_query: types.CallbackQuery, state: FSMContext):
        message_with_buttons = callback_query.message
        user_id = callback_query.from_user.id
        
        if user_id in self.active_users:
            await callback_query.answer("⏳ Дождитесь окончания обработки")
            return
            
        self.active_users.add(user_id)
        await callback_query.answer()
        
        try:
            data = await state.get_data()
            audio_path = data.get('audio_path')
            original_message = data.get('original_message')
            
            if not all([audio_path, original_message]):
                await message_with_buttons.edit_text("❌ Файлы не найдены")
                return
                
            action = callback_query.data.replace('audio_', '')
            logger.info(f"Обработка аудио действия: {action}")
            
            if action == 'silence':
                await message_with_buttons.edit_text("✂️ Удаляю паузы...")
                
                processed_filename = f"processed_{os.path.basename(audio_path)}"
                processed_path = os.path.join(self.downloads_dir, processed_filename)
                
                processed_path = await self.audio_handler.process_audio(audio_path)
                
                if processed_path and os.path.exists(processed_path):
                    try:
                        async with aiofiles.open(processed_path, 'rb') as audio_file:
                            await self.bot.send_audio(
                                chat_id=original_message.chat.id,
                                audio=types.BufferedInputFile(
                                    await audio_file.read(),
                                    filename=processed_filename
                                ),
                                caption="✅ Паузы удалены"
                            )
                        await message_with_buttons.delete()
                    except Exception as e:
                        logger.error(f"Ошибка при отправке обработанного аудио: {e}")
                        await message_with_buttons.edit_text("❌ Ошибка при отправке обработанного аудио")
                else:
                    await message_with_buttons.edit_text("❌ Не удалось обработать аудио")
                    
            elif action == 'recognize':
                await message_with_buttons.edit_text("🎵 Подготовка к распознаванию...")
                wav_path = f"{audio_path}.wav"
                
                # Конвертация в WAV
                await self.transcriber.extract_audio(audio_path, wav_path)
                await state.update_data(wav_path=wav_path)
                
                keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang_ru"),
                            InlineKeyboardButton(text="🇬🇧 English", callback_data="lang_en"),
                            InlineKeyboardButton(text="🇨🇳 中文", callback_data="lang_zh")
                        ]
                    ]
                )
                
                await message_with_buttons.edit_text(
                    "🌍 Выберите язык аудио:",
                    reply_markup=keyboard
                )
                
        except Exception as e:
            error_msg = f"❌ Ошибка при обработке аудио: {str(e)}"
            logger.error(error_msg)
            await message_with_buttons.edit_text(error_msg)
            
        finally:
            try:
                if 'processed_path' in locals() and os.path.exists(processed_path):
                    os.remove(processed_path)
                    logger.info(f"Удален временный файл: {processed_path}")
            except Exception as e:
                logger.error(f"Ошибка при удалении временного файла: {e}")
                
            self.active_users.discard(user_id)

    async def process_audio(self, message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        
        if user_id in self.active_users:
            await message.reply("⏳ Дождитесь окончания предыдущей обработки")
            return
            
        self.active_users.add(user_id)
        audio_path = None
        status_message = None
        
        try:
            await state.update_data(
                original_message=message,
                request_type='audio'
            )
            
            status_message = await message.reply(
                f"⏳ Начинаю обработку аудио..."
            )

            try:
                # Получаем информацию о файле
                file = await self.bot.get_file(message.audio.file_id)
                logger.info(f"Получен файл: {file.file_path}")
                
                # Генерируем имя файла
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_ext = os.path.splitext(message.audio.file_name)[1] or '.mp3'
                safe_filename = f"audio_{timestamp}{file_ext}"
                audio_path = os.path.join(self.downloads_dir, safe_filename)
                
                # Создаем директорию если её нет
                os.makedirs(os.path.dirname(audio_path), exist_ok=True)

                # Скачиваем файл
                await self.bot.download_file(
                    file.file_path,
                    destination=audio_path
                )
                
                if not os.path.exists(audio_path):
                    raise FileNotFoundError("Файл не был загружен")
                
                await state.update_data(audio_path=audio_path)
                
                # Создаем клавиатуру
                keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(text="🔇 Удалить паузы", callback_data="audio_silence"),
                            InlineKeyboardButton(text="🎯 Распознать", callback_data="audio_recognize")
                        ]
                    ]
                )
                
                await status_message.edit_text(
                    "🎵 Выберите действие с аудио:",
                    reply_markup=keyboard
                )
                
                await state.set_state(VideoProcessing.WAITING_FOR_ACTION)
                logger.info(f"Аудио успешно обработано: {audio_path}")

            except Exception as e:
                logger.error(f"Ошибка при скачивании аудио: {str(e)}")
                if audio_path and os.path.exists(audio_path):
                    os.remove(audio_path)
                raise Exception(f"Не удалось загрузить аудио файл: {str(e)}")

        except Exception as e:
            error_msg = f"❌ Ошибка при обработке аудио: {str(e)}"
            logger.error(error_msg)
            
            if status_message:
                await status_message.edit_text(error_msg)
            else:
                await message.reply(error_msg)
                
        finally:
            self.active_users.discard(user_id)

    def generate_video_filename(self, service_type: str, action: str = 'download', text_lang: str = None) -> str:
        """Генерация уникального имени файла"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        service_prefix = {
            'rednote': 'RN',
            'kuaishou': 'KS',
            'youtube': 'YT',
            'instagram': 'IG',
            'pinterest': 'PT',
            'unknown': 'VIDEO'
        }.get(service_type, 'VIDEO')
        
        if action == 'recognition' and text_lang:
            lang_suffix = {
                'ru': 'RUS',
                'en': 'ENG',
                'zh': 'CHN'
            }.get(text_lang, '')
            return f"{service_prefix}_RECOG_{lang_suffix}_{timestamp}.mp4"
        
        # Для ускоренных видео
        if action.startswith('speed'):
            return f"{service_prefix}_{action.upper()}_{timestamp}.mp4"
        
        return f"{service_prefix}_{timestamp}.mp4"