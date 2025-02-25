import os
from dotenv import load_dotenv
import logging
import sys
import codecs

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")


# Добавить пути к директориям
DOWNLOADS_DIR = "downloads"  # Уже есть в вашем конфиге
MODELS_DIR = "models"

# Настройки файлов
MAX_VIDEO_SIZE = 45 * 1024 * 1024  # 45MB
SUPPORTED_LANGUAGES = ['ru', 'en', 'zh']

# Настройки базы данных
DB_FILE = "bot_database.db"

# Таймауты и повторы
RETRY_COUNT = 5
DOWNLOAD_TIMEOUT = 60

class UnicodeStreamHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        if stream is None:
            stream = sys.stdout
        try:
            stream = codecs.getwriter('utf8')(stream.buffer)
        except AttributeError:
            pass
        super().__init__(stream)

    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

def setup_logging(name=None):
    """Настройка логирования с поддержкой Unicode"""
    logger = logging.getLogger(name)
    
    # Проверяем, не настроен ли уже логгер и очищаем существующие хендлеры
    if logger.handlers:
        logger.handlers.clear()
    
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Отключаем propagation чтобы избежать дублирования
    logger.propagate = False
    
    # Файловый хендлер с указанием кодировки
    file_handler = logging.FileHandler('bot.log', encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    
    return logger



# Настройки прокси
PROXY_ENABLED = os.getenv("PROXY_ENABLED", "false").lower() == "true"
PROXY_CONFIG = {
    'proxy_type': os.getenv("PROXY_TYPE", "socks5"),
    'addr': os.getenv("PROXY_HOST"),
    'port': int(os.getenv("PROXY_PORT", "0")),
    'username': os.getenv("PROXY_USERNAME"),
    'password': os.getenv("PROXY_PASSWORD")
}

PROXY_DLP = os.getenv("PROXY_DLP")

PROXY_TTS = os.getenv("PROXY_TTS_1")

ELEVENLABS_API_KEY = os.getenv("API_ELEVENLABS")

TTS_CONFIG = {
    'MAX_TEXT_LENGTH': 1000,  # Максимальная длина текста
    'MAX_RETRIES': 3,         # Максимальное количество попыток
    'RETRY_DELAY': 2,         # Начальная задержка между попытками
    'REQUEST_TIMEOUT': 60,    # Таймаут запроса
    'RATE_LIMIT_DELAY': 5,    # Задержка при превышении лимита
}


ELEVENLABS_VOICES = {
    "george": {
        "name": "Стандарт",
        "id": "JBFqnCBsd6RMkjVDRZzb",
        "stability": 0.5,
        "similarity_boost": 0.75
    },
    "ded": {
        "name": "Дедушка",
        "id": "nTu7K1WwHHHw2Fhh194q",
        "stability": 0.5,
        "similarity_boost": 0.75
        
    },
    "malisheva": {
        "name": "Малышева",
        "id": "cEhmt1K2oPuBCf1vXWTk",
        "stability": 0.5,
        "similarity_boost": 0.75
       
    },
    "Bill": {
        "name": "Билл",
        "id": "pqHfZKP75CvOlQylNhV4",
        "stability": 0.5,
        "similarity_boost": 0.75
}}