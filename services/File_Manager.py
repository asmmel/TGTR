from typing import Set, Optional
import os
import logging
import time
import asyncio
from datetime import datetime, timedelta
from config.config import setup_logging

logger = setup_logging(__name__)

class FileManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FileManager, cls).__new__(cls)
            cls._instance.initialize()
        return cls._instance
    
    def initialize(self):
        """Инициализация менеджера файлов"""
        try:
            self.active_files = set()
            self.downloads_dir = "downloads"
            self.cleanup_threshold = 3600  # 1 час
            os.makedirs(self.downloads_dir, exist_ok=True)
            logger.info("FileManager успешно инициализирован")
        except Exception as e:
            logger.error(f"Ошибка при инициализации FileManager: {e}")
            raise

    def register_file(self, filepath: str) -> None:
        """Регистрация нового файла"""
        if filepath:
            self.active_files.add(os.path.abspath(filepath))
            logger.info(f"Зарегистрирован новый файл: {filepath}")
    
    def safe_register_file(self, file_path: str) -> None:
        """Безопасная регистрация файла с проверкой существования"""
        if file_path and os.path.exists(file_path):
            self.register_file(file_path)  # Обращаемся к методу текущего экземпляра
            
    def unregister_file(self, filepath: str) -> None:
        """Удаление файла из отслеживания"""
        if filepath:
            abs_path = os.path.abspath(filepath)
            self.active_files.discard(abs_path)
            logger.info(f"Файл удален из отслеживания: {filepath}")
                            
    def cleanup_file(self, filepath: str) -> None:
        """Очистка конкретного файла"""
        if not filepath:
            return

        abs_path = os.path.abspath(filepath)
        if not os.path.exists(abs_path):
            logger.warning(f"⚠️ Попытка удалить несуществующий файл: {abs_path}")
            return

        logger.info(f"🗑 Удаление файла: {abs_path}")
        
        try:
            os.remove(abs_path)
            logger.info(f"✅ Файл успешно удалён: {abs_path}")
        except Exception as e:
            logger.error(f"Ошибка при удалении файла {abs_path}: {e}")
            
    async def cleanup_old_files(self) -> None:
        current_time = time.time()
        for filename in os.listdir(self.downloads_dir):
            filepath = os.path.join(self.downloads_dir, filename)
            if os.path.abspath(filepath) in self.active_files:  # 🛡️ Пропускаем активные файлы
                continue
            if (current_time - os.path.getmtime(filepath)) > self.cleanup_threshold:
                self.cleanup_file(filepath)
                
    async def start_cleanup_task(self) -> None:
        """Запуск периодической очистки"""
        while True:
            await self.cleanup_old_files()
            await asyncio.sleep(1800)  # Проверка каждые 30 минут
            
    def get_active_files(self) -> Set[str]:
        """Получение списка активных файлов"""
        return self.active_files.copy()
    
    def file_exists(self, filepath: str) -> bool:
        """Проверка существования файла"""
        return filepath and os.path.exists(filepath)
        
    async def cleanup_on_shutdown(self) -> None:
        """Очистка всех файлов при выключении"""
        try:
            active_files = self.get_active_files()
            logger.info(f"Очистка {len(active_files)} файлов при выключении")
            for file_path in active_files:  # Используем цикл вместо cleanup_files
                self.cleanup_file(file_path)
        except Exception as e:
            logger.error(f"Ошибка при очистке файлов при выключении: {e}")

        