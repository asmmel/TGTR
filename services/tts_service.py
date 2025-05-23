import aiohttp
import logging
from typing import Optional, Tuple
from datetime import datetime
from config.config import ELEVENLABS_API_KEY
import asyncio

logger = logging.getLogger(__name__)

class TTSService:
    def __init__(self, proxy: Optional[str] = None):
        self.api_key = ELEVENLABS_API_KEY
        if not self.api_key:
            raise ValueError("ELEVENLABS_API_KEY не настроен")
            
        self.base_url = "https://api.elevenlabs.io/v1"
        self.headers = {
            "xi-api-key": self.api_key,
            "Accept": "audio/mpeg",
            "Content-Type": "application/json"
        }
        proxy = "38.153.57.185:9145:JaHka2:V5q2XY"
        # Настройка прокси
        self.proxy = proxy
        if self.proxy:
            proxy_parts = self.proxy.split(':')
            self.proxy_url = f"http://{proxy_parts[2]}:{proxy_parts[3]}@{proxy_parts[0]}:{proxy_parts[1]}"

    async def text_to_speech(self, text: str, voice_config: dict) -> Tuple[Optional[bytes], Optional[str]]:
        """
        Асинхронное преобразование текста в речь
        
        Args:
            text: Текст для преобразования
            voice_config: Конфигурация голоса
        Returns:
            Tuple[bytes, str]: Аудио данные и название файла
        """
        try:
            voice_id = voice_config['id']
            voice_name = voice_config['name']
            
            data = {
                "text": text,
                "model_id": "eleven_multilingual_v2",
                "voice_settings": {
                    "stability": voice_config.get('stability', 0.5),
                    "similarity_boost": voice_config.get('similarity_boost', 0.75)
                }
            }
            
            # Добавляем прокси в конфигурацию сессии если он установлен
            session_kwargs = {"headers": self.headers}
            if self.proxy:
                session_kwargs["proxy"] = self.proxy_url
            
            async with aiohttp.ClientSession(**session_kwargs) as session:
                async with session.post(
                    f"{self.base_url}/text-to-speech/{voice_id}",
                    json=data
                ) as response:
                    if response.status == 200:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"voice_{voice_name}_{timestamp}.mp3"
                        return await response.read(), filename
                    
                    error_text = await response.text()
                    logger.error(f"Ошибка API ElevenLabs: {error_text}")
                    return None, None
                    
        except Exception as e:
            logger.error(f"Ошибка при генерации речи: {e}")
            return None, None
