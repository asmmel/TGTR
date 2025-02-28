import asyncio
import logging
import time

logger = logging.getLogger(__name__)

class UserActivityManager:
    """Класс для управления активностью пользователей с защитой от зависаний"""
    
    def __init__(self, timeout_seconds=300):
        self.active_users = {}  # словарь {user_id: timestamp}
        self.timeout_seconds = timeout_seconds
        self.lock = asyncio.Lock()  # для потокобезопасных операций
    
    async def add_user(self, user_id: int) -> bool:
        """Добавляет пользователя в список активных с защитой от гонок"""
        async with self.lock:
            # Проверяем, не устарела ли активность пользователя
            if user_id in self.active_users:
                last_activity = self.active_users[user_id]
                if time.time() - last_activity < self.timeout_seconds:
                    return False  # Пользователь уже активен
                
            # Добавляем или обновляем активность пользователя
            self.active_users[user_id] = time.time()
            return True
    
    async def remove_user(self, user_id: int) -> None:
        """Безопасно удаляет пользователя из списка активных"""
        async with self.lock:
            self.active_users.pop(user_id, None)
    
    async def cleanup_stale_users(self) -> int:
        """Очищает устаревшие активности и возвращает количество очищенных пользователей"""
        current_time = time.time()
        stale_users = []
        
        async with self.lock:
            for user_id, timestamp in list(self.active_users.items()):
                if current_time - timestamp > self.timeout_seconds:
                    stale_users.append(user_id)
                    self.active_users.pop(user_id, None)
        
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
            # В этом случае не удаляем, т.к. нет блокировки, только проверяем
            return False
            
        return True