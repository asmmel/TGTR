from vosk import Model, KaldiRecognizer, SetLogLevel
import wave
import json
import os
from pydub import AudioSegment
import logging
import langdetect
from typing import Dict, Optional, Tuple
import aiohttp
import asyncio
from config.config import setup_logging, ELEVENLABS_API_KEY

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
        
        # Новый параметр: использовать ли ElevenLabs (по умолчанию False)
        # Можно переключить в файле .env: USE_ELEVENLABS_TRANSCRIBER=true
        self.use_elevenlabs = os.environ.get('USE_ELEVENLABS_TRANSCRIBER', 'false').lower() == 'true'
        self.api_key = ELEVENLABS_API_KEY
        
        # Настройка прокси для ElevenLabs (если нужно)
        self.proxy = os.environ.get('PROXY_TTS_1')
        if self.proxy:
            proxy_parts = self.proxy.split(':')
            self.proxy_url = f"http://{proxy_parts[2]}:{proxy_parts[3]}@{proxy_parts[0]}:{proxy_parts[1]}"
        else:
            self.proxy_url = None
        
        logger.info(f"Транскрайбер инициализирован. Использование ElevenLabs: {self.use_elevenlabs}")

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
            
            # Проверка входных параметров
            if not video_path or not isinstance(video_path, str):
                logger.error(f"Некорректный путь к видео файлу: {video_path}")
                return False
                
            if not output_path or not isinstance(output_path, str):
                logger.error(f"Некорректный путь для выходного файла: {output_path}")
                return False
            
            if not os.path.exists(video_path):
                logger.error(f"Видео файл не найден: {video_path}")
                return False
                
            # Создаем родительскую директорию, если ее нет
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                
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

    async def transcribe_with_elevenlabs(self, wav_path: str, lang: str) -> Optional[str]:
        """Транскрибация аудио с помощью ElevenLabs API"""
        try:
            if not self.api_key:
                logger.error("API ключ ElevenLabs не настроен")
                return None
            
            # Проверка входных данных
            if not wav_path or not isinstance(wav_path, str):
                logger.error(f"Некорректный путь к аудио файлу: {wav_path}")
                return None
                
            if not os.path.exists(wav_path):
                logger.error(f"Файл не найден: {wav_path}")
                return None
                
            file_size = os.path.getsize(wav_path)
            if file_size == 0:
                logger.error(f"Аудио файл пуст: {wav_path}")
                return None
                
            logger.info(f"Начинаем транскрибацию файла {wav_path} размером {file_size/1024/1024:.2f} MB через ElevenLabs")
            
            # Проверка размера файла для API
            max_file_size = 25 * 1024 * 1024  # 25 MB
            if file_size > max_file_size:
                logger.warning(f"Файл превышает максимальный размер для API ElevenLabs: {file_size/1024/1024:.2f} MB > 25 MB")
                logger.info("Попытка транскрибации большого файла...")
                
            # Базовый URL для API ElevenLabs
            base_url = "https://api.elevenlabs.io/v1"
            
            # Маппинг языковых кодов для ElevenLabs
            # ISO-639-3 коды языков (трехбуквенные)
            language_code_map = {
                "ru": "rus",   # русский
                "en": "eng",   # английский
                "zh": "zho"    # китайский
            }
            iso_lang_code = language_code_map.get(lang, lang)
            
            # Настройка заголовков запроса
            headers = {
                "xi-api-key": self.api_key,
                "Accept": "application/json"
            }
            
            # Настройка сессии с прокси, если он указан
            session_kwargs = {"headers": headers}
            if self.proxy_url:
                session_kwargs["proxy"] = self.proxy_url
                
            async with aiohttp.ClientSession(**session_kwargs) as session:
                # Создаем форму для отправки файла
                form = aiohttp.FormData()
                form.add_field('file', 
                               open(wav_path, 'rb'),
                               filename=os.path.basename(wav_path),
                               content_type='audio/wav')
                
                # Добавляем необходимые параметры
                form.add_field('model_id', 'scribe_v1')  # ID модели для транскрибации
                
                # Добавляем код языка в ISO формате, если он известен
                if iso_lang_code:
                    form.add_field('language_code', iso_lang_code)
                    logger.info(f"Используем язык с кодом: {iso_lang_code}")
                
                # Увеличиваем таймаут для больших файлов
                timeout = aiohttp.ClientTimeout(total=600)  # 10 минут
                
                # Отправляем запрос
                async with session.post(
                    f"{base_url}/speech-to-text",
                    data=form,
                    timeout=timeout
                ) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        logger.info(f"Успешный ответ от ElevenLabs API")
                        
                        # Извлекаем текст из ответа
                        transcribed_text = response_data.get('text', '')
                        if transcribed_text:
                            logger.info(f"Получено {len(transcribed_text)} символов текста")
                            return transcribed_text
                        else:
                            logger.warning("Получен пустой текст от API")
                            return None
                    else:
                        error_text = await response.text()
                        logger.error(f"Ошибка API ElevenLabs: {response.status}, {error_text}")
                        return None

        except Exception as e:
            logger.error(f"Ошибка при транскрибации через ElevenLabs: {e}")
            return None

    async def transcribe(self, wav_path: str, lang: str) -> Optional[str]:
        """Транскрибация аудио файла на заданном языке"""
        # Проверка входных данных
        if not wav_path or not isinstance(wav_path, str):
            logger.error(f"Некорректный путь к аудио файлу: {wav_path}")
            return None
            
        if not os.path.exists(wav_path):
            logger.error(f"Аудио файл не найден по пути: {wav_path}")
            return None
        
        # Сначала пробуем ElevenLabs, если включено
        if self.use_elevenlabs:
            try:
                logger.info("Пробуем использовать ElevenLabs для транскрибации")
                result = await self.transcribe_with_elevenlabs(wav_path, lang)
                if result:
                    return result
                logger.warning("ElevenLabs не вернул результат, переключаемся на локальную модель")
            except Exception as e:
                logger.error(f"Ошибка при использовании ElevenLabs: {e}")
                logger.info("Переключаемся на локальную модель")
        
        # Используем локальную модель Vosk, если ElevenLabs не сработал или отключен
        model = self.get_model(lang)
        if not model:
            return None

        try:
            # Дополнительная проверка файла перед открытием
            try:
                file_size = os.path.getsize(wav_path)
                if file_size == 0:
                    logger.error(f"Аудио файл пуст: {wav_path}")
                    return None
                logger.info(f"Размер аудио файла для Vosk: {file_size/1024/1024:.2f} MB")
            except Exception as e:
                logger.error(f"Не удалось проверить размер файла: {e}")
            
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
            logging.error(f"Ошибка при транскрибации с локальной моделью: {str(e)}")
            return None