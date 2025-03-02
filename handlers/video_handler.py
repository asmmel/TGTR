import time
import logging
import aiohttp
import uuid
from datetime import datetime
from typing import Optional
import asyncio
import aiofiles
from typing import Optional, List
import yt_dlp
from moviepy.editor import VideoFileClip
from pyrogram import Client
import os
from os import path
import math

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
from services.connection_manager import ConnectionManager
from services.video_streaming import VideoStreamingService
from services.chunk_uploader import ChunkUploader

from pyrogram import Client
import os
from os import path
import math


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
        self.active_users = set()
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

    def get_service_type(self, url: str) -> str:
        """Определяет тип сервиса по URL"""
        url = url.lower()
        logger.info(f"Определение типа сервиса для URL: {url}")
        
        if 'youtube.com' in url or 'youtu.be' in url:
            return 'youtube'
        elif 'instagram.com' in url:
            return 'instagram'
        elif 'kuaishou.com' in url:
            return 'kuaishou'
        elif 'xiaohongshu.com' in url or 'xhslink.com' in url:
            logger.info("Определен сервис: RedNote")
            return 'rednote'
        
        logger.warning(f"Неизвестный тип сервиса для URL: {url}")
        return 'unknown'


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
        """Загружает видео с разных сервисов"""
        # Генерируем временные имена с использованием timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_name = f"temp_{service_type}_{timestamp}"
        final_name = f"{service_type}_{timestamp}"
        
        temp_path = os.path.join(self.downloads_dir, f"{temp_name}.mp4")
        final_path = os.path.join(self.downloads_dir, f"{final_name}.mp4")
        
        try:
            if service_type == 'rednote':
                max_attempts = 3
                for attempt in range(max_attempts):
                    try:
                        success, message, video_info = await self.rednote.get_video_url(url)
                        if success:
                            video_url = video_info['video_url']
                            if await self.rednote.download_video(video_url, temp_path):
                                break
                        
                        if attempt < max_attempts - 1:
                            wait_time = (attempt + 1) * 5
                            logger.info(f"Повторная попытка через {wait_time} секунд...")
                            await asyncio.sleep(wait_time)
                        else:
                            raise Exception(message)
                    except Exception as e:
                        if attempt == max_attempts - 1:
                            raise
                        logger.warning(f"Попытка {attempt + 1} не удалась: {str(e)}")
                        await asyncio.sleep(5)
                
            elif service_type == 'kuaishou':
                result = await self.kuaishou.download_video(url, temp_path)
                if not result:
                    raise Exception("Не удалось загрузить видео с Kuaishou")
            else:
                # Настраиваем заголовки
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

                try:
                    logger.info("Попытка загрузки через yt-dlp...")
                    ydl_opts = {
                        # Формат указывает максимальное разрешение 1080p
                        'format': 'bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080][ext=mp4]/best[height<=1080]',
                        'outtmpl': temp_path,
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

                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        logger.info("Начало загрузки видео...")
                        ydl.download([url])
                        logger.info("Видео успешно загружено")

                except Exception as yt_error:
                    logger.warning(f"yt-dlp не смог загрузить видео: {str(yt_error)}")
                    logger.info("Пробуем загрузить через Cobalt API...")
                    
                    try:
                        cobalt = CobaltDownloader()
                        downloaded_path = await cobalt.download_video(url)
                        if os.path.exists(downloaded_path):
                            os.rename(downloaded_path, temp_path)
                        else:
                            raise Exception("Файл не найден после загрузки через Cobalt")
                    except Exception as cobalt_error:
                        logger.error(f"Ошибка при загрузке через Cobalt: {str(cobalt_error)}")
                        raise Exception("Не удалось загрузить видео ни одним из доступных методов")

            # Проверяем успешность загрузки
            if not os.path.exists(temp_path):
                raise Exception("Видео не было загружено")
                
            # Просто перемещаем файл
            os.rename(temp_path, final_path)
            return final_path

        except Exception as e:
            logger.error(f"Ошибка при загрузке видео: {str(e)}")
            for path in [temp_path, final_path]:
                if path and os.path.exists(path):
                    os.remove(path)
            raise

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
        """Обработка URL видео"""
        logger.info(f"Получен URL для обработки: {message.text}")
        user_id = message.from_user.id
        
        if user_id in self.active_users:
            await message.reply("⏳ Пожалуйста, дождитесь окончания обработки предыдущего запроса")
            return
            
        self.active_users.add(user_id)
        video_path = None
        status_message = None
        
        try:
            # Сначала сохраняем исходное сообщение в состояние
            await state.update_data(
                original_message=message,
                request_type='url'
            )
            
            # Определяем тип сервиса
            service_type = self.get_service_type(message.text)
            status_message = await message.reply("⏳ Начинаю загрузку видео...")
            
            # Загружаем видео
            video_path = await self.download_video(message.text, service_type)
            
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
                url=message.text,
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
            self.active_users.discard(user_id)

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
        """Обработка загруженного видео"""
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
                # Получаем информацию о файле
                if not message.video or not message.video.file_id:
                    raise ValueError("Видео файл отсутствует или поврежден")
                
                file = await self.bot.get_file(message.video.file_id)
                if not file or not file.file_path:
                    raise ValueError("Не удалось получить информацию о файле")
                
                # Генерируем безопасное имя файла
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"video_{timestamp}.mp4"
                video_path = os.path.join(self.downloads_dir, filename)
                
                # Создаем директорию, если её нет
                os.makedirs(os.path.dirname(video_path), exist_ok=True)
                
                # Всегда скачиваем файл через API, не пытаясь проверить локальный путь
                try:
                    logger.info(f"Скачивание файла {file.file_path} в {video_path}")
                    
                    await self.bot.download_file(
                        file.file_path,
                        video_path,
                        timeout=60
                    )
                except Exception as download_error:
                    logger.error(f"Ошибка при скачивании файла: {download_error}")
                    raise FileNotFoundError(f"Не удалось скачать файл: {download_error}")
                
                if not os.path.exists(video_path):
                    raise FileNotFoundError("Файл не был загружен")

                # Проверяем размер загруженного файла
                actual_size = os.path.getsize(video_path)
                if actual_size == 0:
                    raise ValueError("Загруженный файл пуст")
                
                logger.info(f"Видео успешно загружено в {video_path}, размер: {actual_size/1024/1024:.2f} MB")

                # Сохраняем путь к видео
                await state.update_data(video_path=video_path)
                
                # Создаем клавиатуру с кнопками выбора действия
                keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(text="📥 Скачать", callback_data="action_download"),
                            InlineKeyboardButton(text="🎯 Распознать", callback_data="action_recognize")
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
                logger.error(f"Ошибка при загрузке видео: {str(e)}")
                if video_path and os.path.exists(video_path):
                    try:
                        os.remove(video_path)
                        logger.info(f"Удален неполный файл: {video_path}")
                    except Exception as del_error:
                        logger.error(f"Ошибка при удалении файла: {del_error}")
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
                    logger.info(f"Удален файл после ошибки: {video_path}")
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
        """Обработка выбора действия"""
        message_with_buttons = callback_query.message
        user_id = callback_query.from_user.id
        file_id = None
        
        try:
            if user_id in self.active_users:
                await callback_query.answer("⏳ Дождитесь окончания обработки")
                return
                            
            self.active_users.add(user_id)
            await callback_query.answer()
            
            # Инициализируем клиент, если еще не инициализирован
            if not self.app:
                await self.init_client()
            
            data = await state.get_data()
            video_path = data.get('video_path')
            original_message = data.get('original_message')
            service_type = data.get('service_type', 'unknown')
            
            if not all([video_path, original_message]):
                await message_with_buttons.edit_text("❌ Произошла ошибка: файлы не найдены")
                return
            
            # Регистрируем файл в начале обработки
            file_id = await self._register_file(video_path)
            
            action = callback_query.data.split('_')[1]
            
            # Проверяем путь к видео
            if not os.path.exists(video_path):
                logger.error(f"Файл не найден: {video_path}")
                await message_with_buttons.edit_text("❌ Файл не найден")
                return
                        
            if action == 'download':
                await message_with_buttons.edit_text("📤 Подготовка к отправке...")
                
                # Проверим размер файла
                file_size = os.path.getsize(video_path)
                file_size_mb = file_size / (1024 * 1024)
                
                try:
                    # Имя файла для отправки
                    filename = self.generate_video_filename(service_type)
                    
                    # Обновляем сообщение статуса
                    progress_message = await message_with_buttons.edit_text(
                        f"📤 Начинаю отправку видео ({file_size_mb:.1f} MB)..."
                    )
                    
                    # Отправляем видео через единый метод
                    video_caption = f"✅ Видео успешно загружено\n📁 Имя файла: {filename}"
                    await self.send_video(
                        chat_id=original_message.chat.id,
                        video_path=video_path,
                        caption=video_caption
                    )
                    
                    # Успешная отправка - удаляем сообщение с прогрессом
                    await progress_message.edit_text("✅ Видео успешно отправлено!")
                    await asyncio.sleep(1)  # Даем пользователю увидеть сообщение
                    await progress_message.delete()
                    
                except Exception as e:
                    logger.error(f"Ошибка при отправке видео: {e}")
                    await message_with_buttons.edit_text(f"❌ Ошибка при отправке видео: {str(e)[:100]}")
                    raise
                    
            elif action == 'recognize':
                # Безопасное создание пути к wav файлу
                try:
                    if video_path and os.path.exists(video_path):
                        video_basename = os.path.basename(video_path)
                        video_filename_without_ext = os.path.splitext(video_basename)[0]
                        wav_path = os.path.join(self.downloads_dir, f"{video_filename_without_ext}.wav")
                    else:
                        # Если путь к видео отсутствует или файл не существует, создаем временный путь для wav
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        wav_path = os.path.join(self.downloads_dir, f"temp_audio_{timestamp}.wav")
                        logger.warning(f"Создан временный путь для wav файла: {wav_path}")
                
                    await message_with_buttons.edit_text("🎵 Извлекаю аудио из видео...")
                    
                    # Убедимся, что директория существует
                    os.makedirs(os.path.dirname(wav_path), exist_ok=True)
                    
                    # Не удаляем wav_path если он существует, он может понадобиться
                    success = await self.transcriber.extract_audio(video_path, wav_path)
                    
                    if not success:
                        logger.error(f"Не удалось извлечь аудио из {video_path} в {wav_path}")
                        await message_with_buttons.edit_text("❌ Ошибка при извлечении аудио")
                        return
                    
                    # Сохраняем путь к wav файлу
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
                except Exception as audio_error:
                    logger.error(f"Ошибка при обработке аудио: {audio_error}")
                    await message_with_buttons.edit_text(f"❌ Ошибка при обработке аудио: {str(audio_error)[:100]}")

        except Exception as e:
            error_msg = f"❌ Ошибка: {str(e)}"
            logger.error(error_msg)
            if message_with_buttons:
                await message_with_buttons.edit_text(error_msg)
                    
        finally:
            # Очищаем файлы только если это была операция download
            if action == 'download' and file_id:
                await self.cleanup_files(file_id)
                
            self.active_users.discard(user_id)

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
                
                # Проверяем существование файла
                if not os.path.exists(audio_path):
                    logger.error(f"Аудио файл не найден: {audio_path}")
                    await message_with_buttons.edit_text("❌ Файл не найден")
                    return
                
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
                
                # Проверяем существование файла
                if not os.path.exists(audio_path):
                    logger.error(f"Аудио файл не найден: {audio_path}")
                    await message_with_buttons.edit_text("❌ Файл не найден")
                    return
                
                try:
                    # Создаем WAV файл с уникальным именем
                    wav_path = f"{audio_path}.wav"
                    
                    # Убедимся, что директория существует
                    os.makedirs(os.path.dirname(wav_path), exist_ok=True)
                    
                    # Конвертация в WAV
                    success = await self.transcriber.extract_audio(audio_path, wav_path)
                    
                    if not success:
                        logger.error(f"Не удалось извлечь аудио из {audio_path}")
                        await message_with_buttons.edit_text("❌ Ошибка при конвертации аудио")
                        return
                    
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
                except Exception as audio_error:
                    logger.error(f"Ошибка при обработке аудио: {audio_error}")
                    await message_with_buttons.edit_text(f"❌ Ошибка при обработке аудио: {str(audio_error)[:100]}")
                    
        except Exception as e:
            error_msg = f"❌ Ошибка при обработке аудио: {str(e)}"
            logger.error(error_msg)
            await message_with_buttons.edit_text(error_msg)
            
        finally:
            try:
                if 'processed_path' in locals() and processed_path and os.path.exists(processed_path):
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
        """
        Генерация уникального имени файла
        Args:
            service_type: Тип сервиса (rednote, kuaishou и т.д.)
            action: Тип действия (download, recognition)
            text_lang: Язык распознавания (если есть)
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Префиксы для разных сервисов
        service_prefix = {
            'rednote': 'RN',
            'kuaishou': 'KS',
            'youtube': 'YT',
            'instagram': 'IG',
            'unknown': 'VIDEO'
        }.get(service_type, 'VIDEO')
        
        # Добавляем информацию о действии и языке
        if action == 'recognition' and text_lang:
            lang_suffix = {
                'ru': 'RUS',
                'en': 'ENG',
                'zh': 'CHN'
            }.get(text_lang, '')
            return f"{service_prefix}_RECOG_{lang_suffix}_{timestamp}.mp4"
        
        return f"{service_prefix}_{timestamp}.mp4"