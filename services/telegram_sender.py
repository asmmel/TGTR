import asyncio
import logging
import os
from typing import Optional, Dict, Any
import aiofiles
from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter, TelegramNetworkError
from aiogram.types import Message, InputFile

logger = logging.getLogger(__name__)

# Определение исключений, если они отсутствуют в aiogram
class TelegramServerError(Exception):
    """Исключение для ошибок сервера Telegram"""
    pass

class TelegramError(Exception):
    """Общее исключение для ошибок Telegram"""
    pass

class TelegramSender:
    """Класс для безопасной отправки сообщений в Telegram с обработкой ограничений"""
    
    def __init__(self, bot):
        self.bot = bot
        self.retry_delays = [1, 2, 5, 10, 30]  # Экспоненциально растущие задержки
        self.pyrogram_app = None  # будет установлено позже
        
    def set_pyrogram_app(self, app):
        """Устанавливает Pyrogram клиент для отправки больших файлов"""
        self.pyrogram_app = app
        
    async def send_message(self, chat_id, text, **kwargs):
        """Отправляет сообщение с обработкой флуд-контроля и других ошибок"""
        for attempt, delay in enumerate(self.retry_delays):
            try:
                return await self.bot.send_message(chat_id, text, **kwargs)
            except TelegramRetryAfter as e:
                # Специальная обработка флуд-контроля
                retry_after = max(e.retry_after, delay)
                logger.warning(f"Флуд-контроль: ожидание {retry_after} сек (попытка {attempt+1}/{len(self.retry_delays)})")
                await asyncio.sleep(retry_after)
            except Exception as e:
                # Другие ошибки - логируем и повторяем через задержку
                logger.warning(f"Ошибка при отправке сообщения: {e} - ожидание {delay} сек (попытка {attempt+1}/{len(self.retry_delays)})")
                await asyncio.sleep(delay)
                
                # Если это последняя попытка, пробрасываем ошибку
                if attempt == len(self.retry_delays) - 1:
                    raise
        
        # Если все попытки исчерпаны (не должны сюда попасть, но на всякий случай)
        raise TelegramError("Превышено количество попыток отправки сообщения")
    
    async def send_video(self, chat_id, video, caption=None, **kwargs):
        """Отправляет видео с обработкой всех возможных ошибок"""
        progress_message = None
        
        for attempt, delay in enumerate(self.retry_delays):
            try:
                # Определяем, это путь к файлу или данные файла
                if isinstance(video, str) and os.path.exists(video):
                    # Это путь к файлу - проверяем его
                    if not os.access(video, os.R_OK):
                        raise FileNotFoundError(f"Нет доступа к файлу: {video}")
                    
                    file_size = os.path.getsize(video)
                    if file_size == 0:
                        raise ValueError(f"Файл пуст: {video}")
                    
                    # Для больших файлов используем stream загрузку через Pyrogram
                    if file_size > 10 * 1024 * 1024 and self.pyrogram_app:  # > 10 MB
                        try:
                            return await self._send_large_video(chat_id, video, caption, **kwargs)
                        except Exception as e:
                            logger.warning(f"Ошибка при отправке через Pyrogram: {e}, пробуем стандартный метод")
                    
                    # Стандартная отправка через aiogram
                    async with aiofiles.open(video, 'rb') as f:
                        return await self.bot.send_video(
                            chat_id=chat_id, 
                            video=InputFile(await f.read()),
                            caption=caption, 
                            **kwargs
                        )
                else:
                    # Это уже готовые данные для отправки
                    return await self.bot.send_video(chat_id, video, caption=caption, **kwargs)
                    
            except TelegramRetryAfter as e:
                retry_after = max(e.retry_after, delay)
                logger.warning(f"Флуд-контроль при отправке видео: ожидание {retry_after} сек")
                await asyncio.sleep(retry_after)
            except Exception as e:
                logger.warning(f"Ошибка при отправке видео: {e} - ожидание {delay} сек (попытка {attempt+1}/{len(self.retry_delays)})")
                await asyncio.sleep(delay)
                
                # Если это последняя попытка, пробрасываем ошибку
                if attempt == len(self.retry_delays) - 1:
                    raise
        
        # Если все попытки исчерпаны
        raise TelegramError("Превышено количество попыток отправки видео")
    
    async def _send_large_video(self, chat_id, video_path, caption=None, **kwargs):
        """Отправка большого видео через Pyrogram с прогресс-баром"""
        if not self.pyrogram_app or not self.pyrogram_app.is_connected:
            raise ValueError("Pyrogram клиент не инициализирован или не подключен")
        
        # Создаём временное сообщение с прогрессом
        progress_message = await self.send_message(chat_id, "📤 Подготовка видео к отправке...")
        
        try:
            # Используем Pyrogram для отправки с отслеживанием прогресса
            return await self.pyrogram_app.send_video(
                chat_id=chat_id,
                video=video_path,
                caption=caption,
                progress=self._upload_progress_callback,
                progress_args=(progress_message,)
            )
        finally:
            # Удаляем сообщение о прогрессе после отправки
            try:
                await self.bot.delete_message(chat_id, progress_message.message_id)
            except:
                pass
    
    async def _upload_progress_callback(self, current, total, message):
        """Callback для отображения прогресса загрузки"""
        try:
            if total:
                progress = (current / total) * 100
                # Обновляем только каждые 5%
                if int(progress) % 5 == 0:
                    await self.bot.edit_message_text(
                        f"📤 Загрузка видео: {progress:.1f}%\n"
                        f"({current/(1024*1024):.1f}/{total/(1024*1024):.1f} MB)",
                        chat_id=message.chat.id,
                        message_id=message.message_id
                    )
        except Exception as e:
            logger.debug(f"Ошибка при обновлении прогресса: {e}")