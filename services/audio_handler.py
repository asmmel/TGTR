from pydub import AudioSegment
from pydub.silence import split_on_silence
import os
import logging
from typing import Optional
import asyncio
from config.config import setup_logging

logger = setup_logging(__name__)

class AudioHandler:
    def __init__(self):
        self.downloads_dir = "downloads"
        os.makedirs(self.downloads_dir, exist_ok=True)

    async def process_audio(self, file_path: str) -> Optional[str]:
        """Обработка аудио файла с удалением пауз"""
        try:
            output_path = os.path.join(
                self.downloads_dir,
                f"processed_{os.path.basename(file_path)}"
            )

            # Запускаем удаление пауз в отдельном потоке
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self._remove_silence,
                file_path,
                output_path
            )

            if os.path.exists(output_path):
                return output_path
            return None

        except Exception as e:
            logger.error(f"Ошибка при обработке аудио: {e}")
            return None

    def _remove_silence(
        self,
        input_path: str,
        output_path: str,
        min_silence_len: int = 20,
        silence_thresh: int = -40
    ):
        """Удаление пауз из аудио"""
        try:
            audio = AudioSegment.from_file(input_path)
            
            # Разделение на сегменты без тишины
            segments = split_on_silence(
                audio,
                min_silence_len=min_silence_len,
                silence_thresh=silence_thresh,
                keep_silence=35
            )
            
            # Соединение сегментов
            cleaned_audio = sum(segments)
            
            # Сохранение результата
            cleaned_audio.export(output_path, format="mp3")
            
        except Exception as e:
            logger.error(f"Ошибка при удалении пауз: {e}")
            raise
