import asyncio
import logging
import time
import threading
from typing import Dict, Set
from collections import defaultdict

logger = logging.getLogger(__name__)

class UserActivityManager:
    """Улучшенный класс для управления активностью пользователей с защитой от зависаний"""
    
    def __init__(self, timeout_seconds=300):
        self.active_users: Dict[int, float] = {}  # user_id -> timestamp
        self.user_locks: Dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.timeout_seconds = timeout_seconds
        self.global_lock = asyncio.Lock()
        self._cleanup_task = None
        self._start_cleanup_task()
    
    def _start_cleanup_task(self):
        """Запуск фоновой задачи очистки"""
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
    
    async def _periodic_cleanup(self):
        """Периодическая очистка устаревших пользователей"""
        while True:
            try:
                await asyncio.sleep(60)  # Проверяем каждую минуту
                await self.cleanup_stale_users()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в периодической очистке: {e}")
    
    async def add_user(self, user_id: int, force: bool = False) -> bool:
        """
        Добавляет пользователя в список активных с защитой от гонок
        
        Args:
            user_id: ID пользователя
            force: Принудительно добавить пользователя, даже если он активен
            
        Returns:
            bool: True если пользователь добавлен, False если уже активен
        """
        async with self.global_lock:
            current_time = time.time()
            
            # Проверяем, не устарела ли активность пользователя
            if user_id in self.active_users and not force:
                last_activity = self.active_users[user_id]
                if current_time - last_activity < self.timeout_seconds:
                    logger.debug(f"Пользователь {user_id} уже активен")
                    return False  # Пользователь уже активен
                else:
                    logger.info(f"Таймаут для пользователя {user_id} истек, обновляем активность")
            
            # Добавляем или обновляем активность пользователя
            self.active_users[user_id] = current_time
            logger.info(f"Пользователь {user_id} добавлен в активные. Всего активных: {len(self.active_users)}")
            return True
    
    async def remove_user(self, user_id: int) -> None:
        """Безопасно удаляет пользователя из списка активных"""
        async with self.global_lock:
            removed = self.active_users.pop(user_id, None)
            if removed:
                logger.info(f"Пользователь {user_id} удален из активных. Осталось активных: {len(self.active_users)}")
            
            # Также удаляем блокировку если она есть
            if user_id in self.user_locks:
                del self.user_locks[user_id]
    
    async def cleanup_stale_users(self) -> int:
        """Очищает устаревшие активности и возвращает количество очищенных пользователей"""
        current_time = time.time()
        stale_users = []
        
        async with self.global_lock:
            for user_id, timestamp in list(self.active_users.items()):
                if current_time - timestamp > self.timeout_seconds:
                    stale_users.append(user_id)
            
            # Удаляем устаревших пользователей
            for user_id in stale_users:
                self.active_users.pop(user_id, None)
                if user_id in self.user_locks:
                    del self.user_locks[user_id]
        
        if stale_users:
            logger.info(f"Очищены устаревшие активности пользователей: {stale_users}")
        
        return len(stale_users)
    
    def is_user_active(self, user_id: int) -> bool:
        """Проверяет, активен ли пользователь"""
        if user_id not in self.active_users:
            return False
            
        # Проверяем, не устарела ли активность
        current_time = time.time()
        last_activity = self.active_users[user_id]
        if current_time - last_activity > self.timeout_seconds:
            return False
            
        return True
    
    async def get_user_lock(self, user_id: int) -> asyncio.Lock:
        """Получает индивидуальную блокировку для пользователя"""
        async with self.global_lock:
            if user_id not in self.user_locks:
                self.user_locks[user_id] = asyncio.Lock()
            return self.user_locks[user_id]
    
    async def force_cleanup_user(self, user_id: int) -> None:
        """Принудительно очищает все данные пользователя"""
        async with self.global_lock:
            self.active_users.pop(user_id, None)
            self.user_locks.pop(user_id, None)
            logger.info(f"Принудительно очищен пользователь {user_id}")
    
    def get_stats(self) -> Dict:
        """Возвращает статистику активности"""
        current_time = time.time()
        active_count = 0
        stale_count = 0
        
        for user_id, timestamp in self.active_users.items():
            if current_time - timestamp < self.timeout_seconds:
                active_count += 1
            else:
                stale_count += 1
        
        return {
            'total_users': len(self.active_users),
            'active_users': active_count,
            'stale_users': stale_count,
            'locks_count': len(self.user_locks)
        }
    
    async def shutdown(self):
        """Корректное завершение работы менеджера"""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        async with self.global_lock:
            self.active_users.clear()
            self.user_locks.clear()
        
        logger.info("UserActivityManager корректно завершен")
    
    def __del__(self):
        """Деструктор для очистки ресурсов"""
        if hasattr(self, '_cleanup_task') and self._cleanup_task and not self._cleanup_task.done():
            try:
                self._cleanup_task.cancel()
            except:
                pass