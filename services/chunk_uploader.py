# services/chunk_uploader.py
import os
import logging
import aiohttp
import aiofiles
import asyncio
import time
from typing import Optional, Dict, Callable, Any
from config.config import BOT_TOKEN, setup_logging

logger = setup_logging(__name__)

class ChunkUploader:
    """Сервис для загрузки больших файлов через локальный сервер Telegram по частям"""
    
    def __init__(self, 
                 base_url: str = "http://localhost:8081",
                 chunk_size: int = 8 * 1024 * 1024,  # 8MB
                 max_retries: int = 5):
        """
        Инициализация сервиса
        
        Args:
            base_url: URL локального сервера Telegram
            chunk_size: Размер частей для загрузки в байтах
            max_retries: Максимальное количество повторных попыток
        """
        self.base_url = base_url
        self.api_endpoint = f"{base_url}/bot{BOT_TOKEN}"
        self.chunk_size = chunk_size
        self.max_retries = max_retries
        self.session = None
        
    async def ensure_session(self):
        """Убеждаемся что сессия создана"""
        if not self.session:
            self.session = aiohttp.ClientSession(
                base_url=self.base_url,
                timeout=aiohttp.ClientTimeout(total=300)  # 5 минут таймаут
            )
    
    async def close(self):
        """Закрытие сессии"""
        if self.session:
            await self.session.close()
            self.session = None
            
    async def _exponential_backoff(self, attempt: int) -> float:
        """Расчет времени задержки с экспоненциальным ростом"""
        delay = min(2 ** attempt, 60)  # Максимум 60 секунд
        jitter = (0.5 * delay) * (2 * (0.5 - (time.time() % 1)))  # ±25% случайности
        final_delay = delay + jitter
        return max(1, final_delay)
            
    async def send_large_video(self, 
                              chat_id: int, 
                              video_path: str, 
                              caption: Optional[str] = None,
                              progress_callback: Optional[Callable[[str], Any]] = None) -> bool:
        """
        Отправка большого видео файла по частям через локальный сервер API
        
        Args:
            chat_id: ID чата
            video_path: Путь к видео файлу
            caption: Подпись к видео
            progress_callback: Функция обратного вызова для отображения прогресса
            
        Returns:
            bool: Успешность операции
        """
        if not os.path.exists(video_path):
            logger.error(f"Файл не найден: {video_path}")
            return False
            
        file_size = os.path.getsize(video_path)
        file_name = os.path.basename(video_path)
        logger.info(f"Начинаем отправку файла {file_name} размером {file_size/(1024*1024):.2f} MB")
        
        # Формируем запрос для API
        await self.ensure_session()
            
        # Сначала попробуем обычную отправку с увеличенным таймаутом
        # для файлов меньше 50MB
        if file_size < 50 * 1024 * 1024:
            try:
                if progress_callback:
                    await progress_callback(f"📤 Отправка файла напрямую ({file_size/(1024*1024):.1f} MB)...")
                    
                form = aiohttp.FormData()
                form.add_field('chat_id', str(chat_id))
                if caption:
                    form.add_field('caption', caption)
                    
                async with aiofiles.open(video_path, 'rb') as f:
                    form.add_field('video', 
                                  await f.read(),
                                  filename=file_name,
                                  content_type='video/mp4')
                                  
                async with self.session.post(
                    f"{self.api_endpoint}/sendVideo",
                    data=form,
                    timeout=aiohttp.ClientTimeout(total=300)  # 5 минут таймаут
                ) as response:
                    if response.status == 200:
                        logger.info(f"Файл {file_name} успешно отправлен напрямую")
                        return True
                    else:
                        logger.warning(f"Неудачная прямая отправка: {response.status}, переходим к чанкам")
                        # Продолжаем с отправкой по частям
            except Exception as e:
                logger.error(f"Ошибка при прямой отправке: {e}")
                # Продолжаем с отправкой по частям
        
        # Для больших файлов или если прямая отправка не удалась - отправляем по частям
        try:
            # Готовим запрос для инициализации загрузки
            if progress_callback:
                await progress_callback(f"📤 Инициализация отправки большого файла ({file_size/(1024*1024):.1f} MB)...")
                
            # Шаг 1: Инициализация загрузки
            form = aiohttp.FormData()
            form.add_field('chat_id', str(chat_id))
            form.add_field('type', 'video')
            form.add_field('file_size', str(file_size))
            
            # Повторяем запрос при необходимости
            for attempt in range(self.max_retries):
                try:
                    async with self.session.post(
                        f"{self.api_endpoint}/initUpload",
                        data=form
                    ) as response:
                        if response.status == 200:
                            response_data = await response.json()
                            if response_data.get('ok'):
                                upload_id = response_data.get('result', {}).get('upload_id')
                                if upload_id:
                                    logger.info(f"Загрузка инициализирована, upload_id: {upload_id}")
                                    break
                        
                        logger.warning(f"Ошибка инициализации загрузки (попытка {attempt+1}): {response.status}")
                        
                        if attempt < self.max_retries - 1:
                            delay = await self._exponential_backoff(attempt)
                            logger.info(f"Повторная попытка через {delay:.1f} сек...")
                            await asyncio.sleep(delay)
                        else:
                            logger.error("Превышено количество попыток инициализации")
                            return False
                            
                except Exception as e:
                    logger.error(f"Исключение при инициализации (попытка {attempt+1}): {e}")
                    if attempt < self.max_retries - 1:
                        delay = await self._exponential_backoff(attempt)
                        await asyncio.sleep(delay)
                    else:
                        raise
            
            # Шаг 2: Загрузка файла по частям
            async with aiofiles.open(video_path, 'rb') as f:
                chunk_number = 0
                total_chunks = (file_size + self.chunk_size - 1) // self.chunk_size
                
                while True:
                    chunk_data = await f.read(self.chunk_size)
                    if not chunk_data:
                        break
                        
                    chunk_number += 1
                    progress = (chunk_number / total_chunks) * 100
                    
                    if progress_callback:
                        await progress_callback(
                            f"📤 Отправка части {chunk_number}/{total_chunks} ({progress:.1f}%)"
                        )
                    
                    # Отправляем часть с повторными попытками
                    success = await self._upload_chunk(
                        upload_id, 
                        chunk_number, 
                        chunk_data
                    )
                    
                    if not success:
                        logger.error(f"Не удалось загрузить часть {chunk_number}")
                        return False
            
            # Шаг 3: Завершаем загрузку
            if progress_callback:
                await progress_callback("📤 Финализация загрузки...")
                
            form = aiohttp.FormData()
            form.add_field('upload_id', upload_id)
            form.add_field('chat_id', str(chat_id))
            if caption:
                form.add_field('caption', caption)
            
            # Финализируем с повторными попытками
            for attempt in range(self.max_retries):
                try:
                    async with self.session.post(
                        f"{self.api_endpoint}/finalizeUpload",
                        data=form
                    ) as response:
                        if response.status == 200:
                            response_data = await response.json()
                            if response_data.get('ok'):
                                logger.info("Файл успешно загружен и отправлен")
                                return True
                        
                        logger.warning(f"Ошибка финализации (попытка {attempt+1}): {response.status}")
                        
                        if attempt < self.max_retries - 1:
                            delay = await self._exponential_backoff(attempt)
                            await asyncio.sleep(delay)
                        else:
                            logger.error("Превышено количество попыток финализации")
                            return False
                            
                except Exception as e:
                    logger.error(f"Исключение при финализации (попытка {attempt+1}): {e}")
                    if attempt < self.max_retries - 1:
                        delay = await self._exponential_backoff(attempt)
                        await asyncio.sleep(delay)
                    else:
                        raise
            
            return False  # Если дошли сюда, значит были проблемы
            
        except Exception as e:
            logger.error(f"Ошибка при отправке большого файла: {e}")
            return False
            
    async def stream_video_to_telegram(self, chat_id: int, video_path: str, caption: str = None):
        """Отправляет видео через поток, не загружая его полностью в память"""
        try:
            if not os.path.exists(video_path):
                logger.error(f"Файл не найден: {video_path}")
                return False
                
            file_size = os.path.getsize(video_path)
            logger.info(f"Начинаем потоковую отправку файла {os.path.basename(video_path)} ({file_size/(1024*1024):.2f} MB)")
            
            # Создаем сессию с увеличенным таймаутом
            if not self.session:
                connector = aiohttp.TCPConnector(force_close=True, limit=10)
                timeout = aiohttp.ClientTimeout(total=600, connect=60, sock_read=300, sock_connect=60)
                self.session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout
                )
            
            # Используем StreamReader для чтения файла
            form = aiohttp.FormData()
            form.add_field('chat_id', str(chat_id))
            if caption:
                form.add_field('caption', caption)
                
            # Добавляем файл как поток, не читая его в память
            with open(video_path, 'rb') as file:
                form.add_field('video', 
                    file, 
                    filename=os.path.basename(video_path),
                    content_type='video/mp4'
                )
                
                # Отправляем с увеличенным таймаутом
                async with self.session.post(
                    f"{self.api_endpoint}/sendVideo", 
                    data=form,
                    timeout=aiohttp.ClientTimeout(total=600)
                ) as response:
                    if response.status == 200:
                        logger.info(f"Файл успешно отправлен")
                        return True
                    else:
                        response_text = await response.text()
                        logger.error(f"Ошибка при отправке: {response.status}, {response_text}")
                        return False
                    
        except Exception as e:
            logger.error(f"Ошибка при потоковой отправке файла: {str(e)}")
            return False

    async def _upload_chunk(self, upload_id: str, chunk_number: int, chunk_data: bytes) -> bool:
        """Загрузка одной части файла"""
        for attempt in range(self.max_retries):
            try:
                form = aiohttp.FormData()
                form.add_field('upload_id', upload_id)
                form.add_field('chunk_number', str(chunk_number))
                form.add_field('data', chunk_data)
                
                async with self.session.post(
                    f"{self.api_endpoint}/uploadChunk",
                    data=form
                ) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        if response_data.get('ok'):
                            logger.info(f"Часть {chunk_number} успешно загружена")
                            return True
                    
                    logger.warning(f"Ошибка загрузки части {chunk_number} (попытка {attempt+1}): {response.status}")
                    
                    if attempt < self.max_retries - 1:
                        delay = await self._exponential_backoff(attempt)
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"Превышено количество попыток для части {chunk_number}")
                        return False
                        
            except Exception as e:
                logger.error(f"Исключение при загрузке части {chunk_number} (попытка {attempt+1}): {e}")
                if attempt < self.max_retries - 1:
                    delay = await self._exponential_backoff(attempt)
                    await asyncio.sleep(delay)
                else:
                    return False
                    
        return False