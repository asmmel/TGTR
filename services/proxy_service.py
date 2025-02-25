# services/proxy_service.py
import base64
import logging
from typing import Optional, Dict, Tuple
from aiohttp_socks import ProxyType, ProxyConnector
import re
import urllib.parse

class ProxyService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def parse_ss_url(self, ss_url: str) -> Optional[Dict[str, str]]:
        """
        Парсит Shadowsocks URL в параметры прокси
        Формат: ss://base64(method:password)@hostname:port
        """
        try:
            # Убираем префикс ss:// и очищаем URL
            ss_data = ss_url.replace('ss://', '')
            
            # Ищем и удаляем query параметры и фрагменты до разбора основного URL
            if '?' in ss_data:
                ss_data = ss_data.split('?')[0]
            if '#' in ss_data:
                ss_data = ss_data.split('#')[0]
            
            # Разделяем на credentials и server part
            if '@' not in ss_data:
                raise ValueError("Неверный формат SS URL")
                
            credentials_b64, server_part = ss_data.split('@', 1)
            
            # Парсим адрес и порт
            if ':' not in server_part:
                raise ValueError("Не найден порт в адресе сервера")
                
            host, port = server_part.split(':', 1)
            # Очищаем порт от любых trailing символов
            port = re.search(r'\d+', port).group()
            
            # Декодируем credentials
            try:
                # Добавляем padding если необходимо
                padding = '=' * (-len(credentials_b64) % 4)
                credentials = base64.b64decode(credentials_b64 + padding).decode()
            except:
                # Если не удалось декодировать, пробуем как URL-encoded строку
                credentials = urllib.parse.unquote(credentials_b64)
                
            if ':' not in credentials:
                raise ValueError("Неверный формат credentials")
                
            method, password = credentials.split(':', 1)
            
            self.logger.info(f"Успешно распарсен SS URL. Host: {host}, Port: {port}")
            
            return {
                'host': host.strip(),
                'port': int(port),
                'username': method.strip(),
                'password': password.strip()
            }
            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга SS URL: {e}")
            return None

    async def create_proxy_connector(self, ss_url: str) -> Optional[ProxyConnector]:
        """Создает ProxyConnector для aiohttp"""
        try:
            proxy_data = self.parse_ss_url(ss_url)
            if not proxy_data:
                self.logger.error("Не удалось получить данные прокси")
                return None
                
            self.logger.info(f"Создание connector для {proxy_data['host']}:{proxy_data['port']}")
                
            # Создаем connector с увеличенными таймаутами
            connector = ProxyConnector(
                proxy_type=ProxyType.SOCKS5,
                host=proxy_data['host'],
                port=proxy_data['port'],
                username=proxy_data['username'],
                password=proxy_data['password'],
                rdns=True,
                ssl=False,
                verify_ssl=False
            )
            
            self.logger.info("Прокси connector успешно создан")
            return connector
            
        except Exception as e:
            self.logger.error(f"Ошибка создания proxy connector: {e}")
            return None