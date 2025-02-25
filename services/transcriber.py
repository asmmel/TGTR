from vosk import Model, KaldiRecognizer, SetLogLevel
import wave
import json
import os
from pydub import AudioSegment
import logging
import langdetect
from typing import Dict, Optional, Tuple
from config.config import setup_logging

# Инициализируем логгер
logger = setup_logging(__name__)

class VideoTranscriber:
    def __init__(self, models_dir: str = "models"):
        self.models_dir = models_dir
        self.model_paths = {
            'ru': os.path.join(models_dir, 'vosk-model-ru'),
            'en': os.path.join(models_dir, 'vosk-model-en-us'),
            'zh': os.path.join(models_dir, 'vosk-model-cn')
        }
        self.loaded_model = None
        self.current_lang = None

    def setup_logging(self):
        """Настройка логирования"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('transcription.log'),
                logging.StreamHandler()
            ]
        )

    def get_model(self, lang: str) -> Optional[Model]:
        """Получение модели с очисткой предыдущей"""
        if self.current_lang == lang and self.loaded_model:
            return self.loaded_model

        # Очищаем предыдущую модель если она есть
        if self.loaded_model:
            del self.loaded_model
            self.loaded_model = None
            self.current_lang = None

        try:
            path = self.model_paths.get(lang)
            if not path or not os.path.exists(path):
                logging.warning(f"Модель {lang} не найдена в {path}")
                return None

            self.loaded_model = Model(path)
            self.current_lang = lang
            logging.info(f"Модель {lang} успешно загружена")
            return self.loaded_model
        except Exception as e:
            logging.error(f"Ошибка загрузки модели {lang}: {str(e)}")
            return None

    async def extract_audio(self, video_path: str, output_path: str) -> bool:
        """Извлечение аудио из видео"""
        try:
            logger.info(f"Извлечение аудио из {video_path} в {output_path}")
            
            if not os.path.exists(video_path):
                logger.error(f"Видео файл не найден: {video_path}")
                return False
                
            audio = AudioSegment.from_file(video_path)
            audio = audio.set_frame_rate(16000)
            audio = audio.set_channels(1)
            audio.export(output_path, format="wav")
            
            if not os.path.exists(output_path):
                logger.error(f"Аудио файл не был создан: {output_path}")
                return False
                
            logger.info(f"Аудио успешно извлечено в {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении аудио: {str(e)}")
            return False

    async def transcribe(self, wav_path: str, lang: str) -> Optional[str]:
        """Транскрибация аудио файла на заданном языке"""
        model = self.get_model(lang)
        if not model:
            return None

        try:
            with wave.open(wav_path, "rb") as wf:
                if wf.getnchannels() != 1 or wf.getsampwidth() != 2:
                    raise Exception("Неправильный формат аудио")

                rec = KaldiRecognizer(model, wf.getframerate())
                rec.SetWords(True)

                results = []
                while True:
                    data = wf.readframes(4000)
                    if len(data) == 0:
                        break
                    if rec.AcceptWaveform(data):
                        part_result = json.loads(rec.Result())
                        if part_result.get('text', ''):
                            results.append(part_result['text'])

                part_result = json.loads(rec.FinalResult())
                if part_result.get('text', ''):
                    results.append(part_result['text'])

                return ' '.join(results)

        except Exception as e:
            logging.error(f"Ошибка при транскрибации: {str(e)}")
            return None
        

        