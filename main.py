import logging
import asyncio
import signal
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
        
        # Добавляем обработку сигналов для корректного завершения
        loop = asyncio.get_running_loop()
        for signal_name in ('SIGINT', 'SIGTERM'):
            try:
                loop.add_signal_handler(
                    getattr(signal, signal_name),
                    lambda: asyncio.create_task(shutdown(bot, server))
                )
            except NotImplementedError:
                # Для Windows, где сигналы не поддерживаются полностью
                pass
                
        # Инициализируем сессию видео хендлера с повторами
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await bot.video_handler.init_session()
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Ошибка инициализации сессии (попытка {attempt+1}/{max_retries}): {e}")
                    await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка
                else:
                    raise
        
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

async def shutdown(bot, server):
    """Корректное завершение работы"""
    logger = logging.getLogger(__name__)
    logger.info("Получен сигнал завершения, останавливаем бота...")
    
    # Останавливаем бота
    await bot.stop()
    
    # Останавливаем сервер
    server.stop()
    
    # Завершаем программу
    loop = asyncio.get_running_loop()
    loop.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Программа остановлена пользователем")