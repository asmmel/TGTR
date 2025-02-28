from abc import ABC, abstractmethod
import os
import random
import time
import logging
import aiohttp
import asyncio
import platform
import aiofiles
from datetime import datetime
from typing import Dict, List, Optional, Any
import ctypes

class BaseDownloader(ABC):
    """Базовый класс для всех загрузчиков видео"""
    
    def __init__(self, downloads_dir="downloads"):
        self.downloads_dir = downloads_dir
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        os.makedirs(downloads_dir, exist_ok=True)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    async def download_video(self, url: str, output_path: str) -> Optional[str]:
        """
        Загружает видео по URL и сохраняет по указанному пути
        
        Args:
            url: URL видео для загрузки
            output_path: Путь для сохранения видео
            
        Returns:
            str: Путь к сохраненному файлу в случае успеха, None при ошибке
        """
        pass
    
    @abstractmethod
    async def extract_video_info(self, url: str) -> Dict:
        """
        Извлекает информацию о видео (заголовок, длительность и т.д.)
        
        Args:
            url: URL видео
            
        Returns:
            Dict: Словарь с информацией о видео
        """
        pass
    
    def generate_temp_filename(self, prefix: str = "") -> str:
        """Генерирует уникальное имя для временного файла"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        random_str = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=5))
        return os.path.join(self.downloads_dir, f"temp_{prefix}_{timestamp}_{random_str}.mp4")
    
    def generate_output_filename(self, prefix: str = "") -> str:
        """Генерирует имя для выходного файла"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(self.downloads_dir, f"{prefix}_{timestamp}.mp4")
    
    async def _ensure_directory_exists(self, path: str) -> None:
        """Гарантирует, что директория для указанного пути существует"""
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    
    async def _download_with_session(self, url: str, output_path: str, headers: Dict = None) -> bool:
        """
        Базовый метод загрузки через сессию с поддержкой прогресса
        """
        temp_path = output_path + ".temp"
        
        try:
            # Создаем директорию, если её нет
            await self._ensure_directory_exists(temp_path)
            
            # Базовые заголовки, если не предоставлены
            if not headers:
                headers = {
                    'User-Agent': self.user_agent,
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Connection': 'keep-alive',
                }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=60) as response:
                    response.raise_for_status()
                    total_size = int(response.headers.get('Content-Length', 0))
                    
                    # Проверяем, достаточно ли места на диске
                    if total_size > 0:
                        free_space = await self._get_free_disk_space(self.downloads_dir)
                        if free_space < total_size * 1.2:  # 20% запас
                            raise IOError(f"Недостаточно места на диске: требуется {total_size/(1024*1024):.1f} MB")
                    
                    # Загружаем с отслеживанием прогресса
                    downloaded = 0
                    start_time = time.time()
                    
                    async with aiofiles.open(temp_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(1024*1024):
                            await f.write(chunk)
                            downloaded += len(chunk)
                            
                            # Расчет скорости и оставшегося времени
                            if downloaded % (5*1024*1024) == 0:  # Каждые 5 MB
                                elapsed = time.time() - start_time
                                speed = downloaded / elapsed if elapsed > 0 else 0
                                remaining = (total_size - downloaded) / speed if speed > 0 else 0
                                
                                self.logger.info(
                                    f"Загрузка: {downloaded/(1024*1024):.1f}/{total_size/(1024*1024):.1f} MB "
                                    f"({downloaded/total_size*100:.1f}%) - "
                                    f"{speed/(1024*1024):.1f} MB/s, осталось {remaining:.1f} сек"
                                )
            
            # Проверяем результат загрузки
            if os.path.exists(temp_path):
                actual_size = os.path.getsize(temp_path)
                if actual_size == 0:
                    raise ValueError("Загружен пустой файл")
                
                if total_size > 0 and actual_size < total_size * 0.99:
                    raise ValueError(f"Неполная загрузка: {actual_size}/{total_size} байт")
                
                # Перемещаем файл в финальный путь
                os.replace(temp_path, output_path)
                return True
            
            return False
            
        except aiohttp.ClientError as e:
            self.logger.error(f"Сетевая ошибка при загрузке: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке: {e}")
            return False
        finally:
            # Удаляем временный файл в случае ошибки
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
    
    async def _get_free_disk_space(self, path: str) -> int:
        """Возвращает свободное место на диске в байтах"""
        if platform.system() == 'Windows':
            free_bytes = ctypes.c_ulonglong(0)
            ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                ctypes.c_wchar_p(path), None, None, ctypes.pointer(free_bytes)
            )
            return free_bytes.value
        else:
            st = os.statvfs(path)
            return st.f_bavail * st.f_frsize