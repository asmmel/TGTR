# Добавьте этот код в файл services/connection_manager.py

import asyncio
import logging
import random
import time
from typing import Optional, Callable, Any, Dict

logger = logging.getLogger(__name__)

class ConnectionManager:
    """Менеджер сетевых подключений с обработкой ошибок и переподключением"""
    
    def __init__(self, name: str = "default"):
        self.name = name
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.base_delay = 1.0
        self.max_delay = 30.0
        self.jitter = 0.1  # случайное отклонение для избежания одновременных подключений
        self.last_connection_time = 0
        self.connected = False
        self.clients = {}
        
    def register_client(self, client_id: str, client: Any):
        """Регистрация клиента для управления"""
        self.clients[client_id] = client
        logger.info(f"Клиент {client_id} зарегистрирован в менеджере подключений")
        
    def get_reconnect_delay(self) -> float:
        """Расчет задержки перед переподключением с экспоненциальным ростом"""
        if self.reconnect_attempts == 0:
            return 0
            
        delay = min(
            self.base_delay * (2 ** (self.reconnect_attempts - 1)),
            self.max_delay
        )
        
        # Добавляем случайность (jitter) для предотвращения Thunder Herd problem
        jitter_value = random.uniform(-self.jitter * delay, self.jitter * delay)
        final_delay = delay + jitter_value
        
        return max(0.1, final_delay)  # Минимум 100мс задержки
        
    async def handle_connection_error(self, error: Exception) -> bool:
        """Обработка ошибки соединения с задержкой и логированием"""
        self.reconnect_attempts += 1
        
        if self.reconnect_attempts > self.max_reconnect_attempts:
            logger.critical(
                f"Превышено максимальное количество попыток подключения ({self.max_reconnect_attempts}). "
                f"Последняя ошибка: {error}"
            )
            return False
            
        delay = self.get_reconnect_delay()
        logger.warning(
            f"Ошибка подключения: {error}. "
            f"Повторная попытка {self.reconnect_attempts}/{self.max_reconnect_attempts} через {delay:.2f} сек."
        )
        
        await asyncio.sleep(delay)
        return True
        
    def connection_successful(self):
        """Сброс счетчика попыток после успешного подключения"""
        self.reconnect_attempts = 0
        self.last_connection_time = time.time()
        self.connected = True
        logger.info(f"Подключение '{self.name}' успешно установлено")
        
    def is_safe_to_reconnect(self) -> bool:
        """Проверка возможности переподключения без перегрузки сервера"""
        # Если последнее подключение было менее 3 секунд назад, 
        # то делаем более длительную паузу
        if time.time() - self.last_connection_time < 3:
            logger.warning("Слишком частые переподключения! Увеличиваем задержку.")
            self.reconnect_attempts = min(self.reconnect_attempts + 2, self.max_reconnect_attempts)
            return False
        return True
        
    async def with_connection_retry(self, func: Callable, *args, **kwargs) -> Any:
        """Выполнение функции с автоматическими повторами при ошибках соединения"""
        while True:
            try:
                result = await func(*args, **kwargs)
                self.connection_successful()
                return result
            except (ConnectionError, TimeoutError, asyncio.TimeoutError) as e:
                if not await self.handle_connection_error(e):
                    raise
                if not self.is_safe_to_reconnect():
                    await asyncio.sleep(5)  # Дополнительная защитная задержка