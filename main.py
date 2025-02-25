import logging
import asyncio
from config.config import BOT_TOKEN, setup_logging
from bot import VideoBot
from local_server import TelegramLocalServer, load_config
from aiogram.client.telegram import TelegramAPIServer

async def main():
    logger = setup_logging(__name__)
    server_config = load_config()
    server = TelegramLocalServer(server_config)
    
    try:
        if not server.start():
            logger.error("Не удалось запустить локальный сервер")
            return

        await asyncio.sleep(5)  # Ждем инициализации сервера

        # Инициализируем и запускаем бота
        bot = VideoBot()
        
        # Инициализируем сессию видео хендлера
        await bot.video_handler.init_session()
        
        try:
            await bot.start()
        except asyncio.CancelledError:
            logger.info("Бот остановлен")
        except Exception as e:
            logger.error(f"Ошибка в работе бота: {e}")
        finally:
            await bot.stop()
            await bot.video_handler.close_session()

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        server.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Программа остановлена пользователем")