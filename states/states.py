# states/states.py
from aiogram.fsm.state import State, StatesGroup  # Новый путь импорта

class VideoProcessing(StatesGroup):
    """States для обработки видео"""
    WAITING_FOR_VIDEO = State()
    PROCESSING_VIDEO = State()
    WAITING_FOR_LANGUAGE = State()
    WAITING_FOR_ACTION = State()
    WAITING_FOR_VOICE = State()  # Новое состояние для выбора голоса
    PROCESSING_AUDIO = State()