from typing import Dict, Optional
from services.base_downloader import BaseDownloader
from services.youtube_downloader import YouTubeDownloader
import logging

logger = logging.getLogger(__name__)

class DownloaderFactory:
    """Фабрика для создания загрузчиков по типу сервиса"""
    
    _instance = None
    _downloaders = {}  # Кеш загрузчиков
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DownloaderFactory, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        """Инициализация фабрики"""
        self.downloads_dir = "downloads"
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def get_downloader(self, service_type: str) -> BaseDownloader:
        """Возвращает загрузчик для указанного типа сервиса"""
        # Проверяем, есть ли уже созданный загрузчик
        if service_type in self._downloaders:
            return self._downloaders[service_type]
            
        # Создаем новый загрузчик
        downloader = self._create_downloader(service_type)
        
        # Кешируем для последующего использования
        self._downloaders[service_type] = downloader
        
        return downloader
    
    def _create_downloader(self, service_type: str) -> BaseDownloader:
        """Создает новый экземпляр загрузчика"""
        try:
            if service_type == 'youtube':
                return YouTubeDownloader(self.downloads_dir)
            elif service_type == 'instagram':
                # Здесь можно создать инстаграм-загрузчик, когда он будет реализован
                self.logger.info(f"Специальный загрузчик для {service_type} не реализован, используем YouTube")
                return YouTubeDownloader(self.downloads_dir)
            elif service_type == 'kuaishou':
                # Импортируем здесь, чтобы избежать циклических импортов
                from services.kuaishou import KuaishouDownloader
                return KuaishouDownloader()
            elif service_type == 'rednote':
                from services.rednote import RedNoteDownloader
                return RedNoteDownloader()
            else:
                # Для неизвестных сервисов используем YouTube загрузчик
                self.logger.warning(f"Неизвестный тип сервиса: {service_type}, используем YouTube загрузчик")
                return YouTubeDownloader(self.downloads_dir)
        except Exception as e:
            self.logger.error(f"Ошибка при создании загрузчика для {service_type}: {e}")
            # Возвращаем YT загрузчик как запасной вариант
            return YouTubeDownloader(self.downloads_dir)
    
    def clear_cache(self):
        """Очищает кеш загрузчиков"""
        self._downloaders.clear()