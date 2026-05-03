from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.filters import Command, StateFilter

from handlers.video_handler import VideoHandler
from states.states import VideoProcessing
from services.File_Manager import FileManager
from services.chunk_uploader import ChunkUploader


from config.config import BOT_TOKEN, ADMIN_IDS, setup_logging
from services.proxy_storage import get_proxies, set_proxies
import logging
import os
import asyncio

logger = setup_logging(__name__)

class VideoBot:
    def __init__(self):
        self.storage = MemoryStorage()
        self.bot = None
        self.dp = None
        self.video_handler = VideoHandler()
        self.file_manager = FileManager()

    async def initialize(self):
        """Инициализация бота и диспетчера"""
        if not self.bot:
            self.bot = Bot(token=BOT_TOKEN)
            
        if not self.dp:
            self.dp = Dispatcher(storage=self.storage)
            self.register_handlers()
            await self.video_handler.set_bot(self.bot)

    async def start(self):
        """Запуск бота"""
        try:
            await self.initialize()
            
            # Создаем директории если их нет
            os.makedirs("downloads", exist_ok=True)
            os.makedirs("models", exist_ok=True)
            
            logger.info("Бот запущен")
            
            # Запускаем очистку
            cleanup_task = asyncio.create_task(self.video_handler.periodic_cleanup())
            
            # Запускаем поллинг
            await self.dp.start_polling(self.bot)
            
        except Exception as e:
            logger.error(f"Ошибка при запуске бота: {e}")
            raise

    # async def _create_session(self):
    #     """Создание сессии с настройками для локального сервера"""
    #     return ClientSession(
    #         timeout=ClientSession.timeout_class(
    #             connect=30,
    #             sock_read=30,
    #             sock_connect=30
    #         )
    #     )

    def register_handlers(self):
        """Регистрация обработчиков команд"""
        
        @self.dp.message(Command(commands=['start', 'help']))
        async def send_welcome(message: types.Message, state: FSMContext):
            welcome_text = """
        🎥 Привет! Я бот для обработки видео и извлечения из них текста.

        Что я умею:
        - Скачивать видео с YouTube, Instagram и Kuaishou
        - Обрабатывать загруженные видео
        - Распознавать речь на русском, английском и китайском языках
        - Озвучивать текст разными голосами (/tts)
        
        Как использовать:
        1. Отправьте мне видео или ссылку
        2. Выберите действие:
        - 📥 Скачать - просто получить видео
        - 🎯 Распознать - извлечь текст из видео
        3. Для озвучки текста используйте команду:
        /tts Ваш текст для озвучки

        Поддерживаемые платформы:
        - YouTube
        - Instagram
        - Kuaishou
            """
            await message.reply(welcome_text)
            await state.set_state(VideoProcessing.WAITING_FOR_VIDEO)

        @self.dp.message(StateFilter(VideoProcessing.WAITING_FOR_SPEED_COEFFICIENT))
        async def speed_coefficient_handler(message: types.Message, state: FSMContext):
            """Обработчик ввода коэффициента ускорения"""
            logger.info("🔥🔥🔥 ОБРАБОТЧИК ВЫЗВАН!")  # ВРЕМЕННОЕ ЛОГИРОВАНИЕ
            await self.video_handler.handle_speed_coefficient_input(message, state)

        @self.dp.message(lambda m: m.text and any(x in m.text.lower() for x in ['youtube.com', 'youtu.be', 'instagram.com', 'kuaishou.com', 'pin.it', 'pinterest.com']))
        async def url_handler(message: types.Message, state: FSMContext):
            if message.from_user.id in self.video_handler.active_users:
                await message.reply("⏳ Пожалуйста, дождитесь окончания обработки предыдущего запроса")
                return
            await self.video_handler.process_url(message, state)

        @self.dp.message(lambda m: m.text and any(x in m.text.lower() for x in ['xiaohongshu.com', 'xhslink.com']))
        async def rednote_handler(message: types.Message, state: FSMContext):
            if message.from_user.id in self.video_handler.active_users:
                await message.reply("⏳ Пожалуйста, дождитесь окончания обработки предыдущего запроса")
                return
            await self.video_handler.process_url(message, state)

        @self.dp.message(lambda m: m.content_type == 'video')
        async def video_handler(message: types.Message, state: FSMContext):
            if message.from_user.id in self.video_handler.active_users:
                await message.reply("⏳ Пожалуйста, дождитесь окончания обработки предыдущего запроса")
                return
            await self.video_handler.process_video(message, state)

        @self.dp.callback_query(lambda c: c.data and c.data.startswith('action_'))
        async def action_callback_handler(callback_query: types.CallbackQuery, state: FSMContext):
            await self.video_handler.handle_action_selection(callback_query, state)

        @self.dp.callback_query(lambda c: c.data and c.data.startswith('lang_'))
        async def language_callback_handler(callback_query: types.CallbackQuery, state: FSMContext):
            await self.video_handler.handle_language_selection(callback_query, state)

        @self.dp.message(Command(commands=['urlbase']))
        async def url_base_command(message: types.Message):
            try:
                history = self.video_handler.db.get_all_history(30)
                
                if not history:
                    await message.reply("📭 История запросов пуста")
                    return
                    
                response = "📊 История обработки URL:\n\n"
                for entry in history:
                    username, url, status, error, timestamp = entry
                    status_emoji = "✅" if status == "success" else "❌"
                    
                    entry_text = f"{status_emoji} @{username}\n"
                    entry_text += f"🔗 {url}\n"
                    entry_text += f"📅 {timestamp}\n"
                    
                    if error:
                        entry_text += f"⚠️ Ошибка: {error}\n"
                        
                    entry_text += "➖➖➖➖➖➖➖➖\n"
                    
                    if len(response + entry_text) > 4000:
                        await message.reply(response)
                        response = entry_text
                    else:
                        response += entry_text
                
                if response:
                    await message.reply(response)
                    
            except Exception as e:
                error_msg = f"❌ Ошибка при получении истории: {str(e)}"
                logger.error(error_msg)
                await message.reply(error_msg)

        

        @self.dp.message(Command(commands=['tts']))
        async def tts_command(message: types.Message, state: FSMContext):
            await self.video_handler.handle_tts_command(message, state)

        @self.dp.callback_query(lambda c: c.data and c.data.startswith('voice_'))
        async def voice_callback_handler(callback_query: types.CallbackQuery, state: FSMContext):
            await self.video_handler.handle_voice_selection(callback_query, state)

        @self.dp.message(lambda m: m.content_type == 'audio')
        async def audio_handler(message: types.Message, state: FSMContext):
            await self.video_handler.process_audio(message, state)

        @self.dp.callback_query(lambda c: c.data and (c.data == 'audio_silence' or c.data == 'audio_recognize'))
        async def audio_action_handler(callback_query: types.CallbackQuery, state: FSMContext):
            await self.video_handler.handle_audio_action(callback_query, state)

        

        @self.dp.message(Command(commands=['myid']))
        async def myid_command(message: types.Message):
            await message.reply(
                f"Твой Telegram ID: <code>{message.from_user.id}</code>\n\n"
                f"Чтобы получить доступ к админ-командам, добавь его в "
                f"<code>ADMIN_IDS</code> в .env",
                parse_mode="HTML"
            )

        @self.dp.message(Command(commands=['proxies']))
        async def proxies_command(message: types.Message):
            if not ADMIN_IDS or message.from_user.id not in ADMIN_IDS:
                return
            proxies = get_proxies()
            if not proxies:
                await message.reply("📋 Instagram прокси не настроены")
                return
            text = f"📋 Текущие Instagram прокси ({len(proxies)} шт.):\n\n"
            for i, p in enumerate(proxies, 1):
                text += f"{i}. <code>{p}</code>\n"
            await message.reply(text, parse_mode="HTML")

        @self.dp.message(Command(commands=['setproxies']))
        async def setproxies_command(message: types.Message):
            if not ADMIN_IDS or message.from_user.id not in ADMIN_IDS:
                return
            text = message.text or ""
            lines = [ln.strip() for ln in text.split('\n')]
            new_proxies = [ln for ln in lines[1:] if ln and not ln.startswith('/')]
            if not new_proxies:
                await message.reply(
                    "Использование (каждая строка — отдельный прокси):\n\n"
                    "<pre>/setproxies\n"
                    "login:pass@host:port\n"
                    "login2:pass2@host2:port2</pre>",
                    parse_mode="HTML"
                )
                return
            count = set_proxies(new_proxies)
            await message.reply(f"✅ Сохранено {count} прокси. Изменения применятся к следующему запросу.")

        @self.dp.message(Command(commands=['clearproxies']))
        async def clearproxies_command(message: types.Message):
            if not ADMIN_IDS or message.from_user.id not in ADMIN_IDS:
                return
            set_proxies([])
            await message.reply("🗑 Список прокси очищен. Будут использованы из .env (если есть).")

        # Добавляем обработчик ошибок
        @self.dp.errors()
        async def error_handler(event: types.ErrorEvent):
            """Обработчик ошибок"""
            try:
                update = event.update
                exception = event.exception
                
                if isinstance(update, types.Message):
                    user_id = update.from_user.id
                    self.video_handler.active_users.discard(user_id)
                    await update.reply("❌ Произошла ошибка при обработке запроса")
                
                logger.error(f"Update: {update}\nError: {exception}")
            except Exception as e:
                logger.error(f"Error in error handler: {e}")

    async def _periodic_cleanup(self):
        """Периодическая очистка"""
        while True:
            try:
                await asyncio.sleep(3600)  # Раз в час
                logger.info("Starting periodic cleanup")
                await self.video_handler.cleanup_active_users()  # Теперь это включает очистку файлов
                logger.info("Periodic cleanup completed")
            except Exception as e:
                logger.error(f"Error in periodic cleanup: {e}")

    async def stop(self):
        """Корректное завершение работы бота"""
        try:
            # Очистка временных файлов
            await self.file_manager.cleanup_on_shutdown()
            
            # Закрываем сессию видео хэндлера
            await self.video_handler.close_session()
            
            # Закрываем сессию бота
            if hasattr(self.bot, 'session'):
                await self.bot.session.close()
            
            # Закрываем клиент
            if self.video_handler.app:
                await self.video_handler.app.stop()
                
            logger.info("Бот успешно остановлен")
            
        except Exception as e:
            logger.error(f"Ошибка при остановке бота: {e}")

