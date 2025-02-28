# services/video_streaming.py
import os
import logging
import asyncio
import time
from typing import Optional, Callable, Union, BinaryIO
import math
from aiogram import Bot
from aiogram.types import BufferedInputFile
from config.config import setup_logging

logger = setup_logging(__name__)

class VideoStreamingService:
    """Сервис для потоковой отправки крупных видео с обработкой ошибок"""
    
    def __init__(self, bot: Bot):
        self.bot = bot
        self.chunk_size = 10 * 1024 * 1024  # 10MB чанк по умолчанию
        self.retry_count = 5
        self.initial_retry_delay = 2
        self.max_retry_delay = 30
        
    async def send_large_video(
        self, 
        chat_id: int, 
        video_path: str, 
        caption: Optional[str] = None,
        progress_callback: Optional[Callable] = None
    ) -> bool:
        """
        Отправка видео файла с поддержкой больших размеров
        
        Args:
            chat_id: ID чата
            video_path: Путь к видеофайлу
            caption: Подпись к видео
            progress_callback: Функция для отслеживания прогресса
            
        Returns:
            bool: Успешность отправки
        """
        try:
            file_size = os.path.getsize(video_path)
            logger.info(f"Начинаем отправку видео размером {file_size / (1024 * 1024):.2f} MB")
            
            # Если файл меньше 50 МБ, отправляем напрямую
            if file_size < 50 * 1024 * 1024:
                return await self._send_direct(chat_id, video_path, caption, progress_callback)
            
            # Для больших файлов: отправляем с ретраями и большим таймаутом
            return await self._send_with_retries(chat_id, video_path, caption, progress_callback)
            
        except Exception as e:
            logger.error(f"Ошибка при отправке видео: {e}")
            return False
            
    async def _send_direct(
        self, 
        chat_id: int, 
        video_path: str, 
        caption: Optional[str] = None,
        progress_callback: Optional[Callable] = None
    ) -> bool:
        """Прямая отправка видео"""
        try:
            with open(video_path, 'rb') as video_file:
                video_data = video_file.read()
                filename = os.path.basename(video_path)
                
                video_input = BufferedInputFile(
                    video_data,
                    filename=filename
                )
                
                await self.bot.send_video(
                    chat_id=chat_id,
                    video=video_input, 
                    caption=caption,
                    # Конфигурируем увеличенные таймауты
                    request_timeout=120
                )
                
                return True
                
        except Exception as e:
            logger.error(f"Ошибка при прямой отправке видео: {e}")
            return False
            
    async def _send_with_retries(
        self, 
        chat_id: int, 
        video_path: str, 
        caption: Optional[str] = None,
        progress_callback: Optional[Callable] = None
    ) -> bool:
        """Отправка видео с повторными попытками и прогрессивным увеличением задержки"""
        last_exception = None
        
        for attempt in range(self.retry_count):
            try:
                # Увеличиваем таймаут для каждой следующей попытки
                timeout_multiplier = attempt + 1
                timeout = 60 * timeout_multiplier  # От 60 до 300 секунд
                
                with open(video_path, 'rb') as video_file:
                    video_data = video_file.read()
                    filename = os.path.basename(video_path)
                    
                    video_input = BufferedInputFile(
                        video_data,
                        filename=filename
                    )
                    
                    if progress_callback:
                        await progress_callback(f"Отправка видео (попытка {attempt+1}/{self.retry_count})")
                    
                    logger.info(f"Отправка видео, попытка {attempt+1}/{self.retry_count}, таймаут: {timeout}с")
                    
                    await self.bot.send_video(
                        chat_id=chat_id,
                        video=video_input, 
                        caption=caption,
                        request_timeout=timeout
                    )
                    
                    logger.info(f"Видео успешно отправлено!")
                    return True
                    
            except asyncio.TimeoutError as e:
                last_exception = e
                retry_delay = min(
                    self.initial_retry_delay * (2 ** attempt),
                    self.max_retry_delay
                )
                
                logger.warning(f"Тайм-аут при отправке видео (попытка {attempt+1}/{self.retry_count}). "
                               f"Повторная попытка через {retry_delay} сек.")
                
                if progress_callback:
                    await progress_callback(f"⏱ Тайм-аут. Повторная попытка через {retry_delay} сек.")
                    
                await asyncio.sleep(retry_delay)
                
            except Exception as e:
                logger.error(f"Ошибка при отправке видео (попытка {attempt+1}/{self.retry_count}): {e}")
                last_exception = e
                
                # Для некоторых ошибок повторяем быстрее
                if "rate limit" in str(e).lower() or "flood" in str(e).lower():
                    retry_delay = 5 * (attempt + 1)
                else:
                    retry_delay = min(
                        self.initial_retry_delay * (2 ** attempt),
                        self.max_retry_delay
                    )
                
                if progress_callback:
                    await progress_callback(f"❌ Ошибка. Повторная попытка через {retry_delay} сек.")
                    
                await asyncio.sleep(retry_delay)
        
        logger.error(f"Не удалось отправить видео после {self.retry_count} попыток. "
                     f"Последняя ошибка: {last_exception}")
        return False