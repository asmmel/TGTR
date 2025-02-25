import logging
import asyncio
import platform
import signal
from config.config import BOT_TOKEN, setup_logging
from bot import VideoBot
from local_server import TelegramLocalServer, load_config
from aiogram.client.telegram import TelegramAPIServer

async def shutdown(bot, server):
    """Корректное завершение работы"""
    logger = logging.getLogger(__name__)
    logger.info("Получен сигнал завершения, останавливаем бота...")
    
    # Останавливаем бота
    await bot.stop()
    
    # Останавливаем сервер
    server.stop()
    
    # Завершаем программу
    try:
        loop = asyncio.get_running_loop()
        loop.stop()
    except Exception as e:
        logger.error(f"Ошибка при остановке цикла: {e}")

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
        
        # Настройка обработки сигналов (только для POSIX систем)
        if platform.system() != 'Windows':
            # Для POSIX систем используем стандартный обработчик сигналов
            loop = asyncio.get_running_loop()
            for signal_name in ('SIGINT', 'SIGTERM'):
                try:
                    loop.add_signal_handler(
                        getattr(signal, signal_name),
                        lambda: asyncio.create_task(shutdown(bot, server))
                    )
                    logger.info(f"Установлен обработчик для сигнала {signal_name}")
                except (NotImplementedError, AttributeError):
                    logger.warning(f"Не удалось установить обработчик для сигнала {signal_name}")
        
        # Инициализируем сессию видео хендлера с повторами
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await bot.video_handler.init_session()
                logger.info("Сессия видео хендлера успешно инициализирована")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Ошибка инициализации сессии (попытка {attempt+1}/{max_retries}): {e}")
                    await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка
                else:
                    logger.error(f"Не удалось инициализировать сессию после {max_retries} попыток")
                    raise
        
        # Инициализируем соединение с Pyrogram заранее
        try:
            await bot.video_handler.init_client()
            logger.info("Pyrogram клиент успешно инициализирован")
        except Exception as e:
            logger.warning(f"Не удалось инициализировать Pyrogram клиент: {e}")
            logger.info("Продолжаем работу без предварительной инициализации Pyrogram")
        
        try:
            # Запускаем периодическую задачу для проверки соединения
            connection_check_task = asyncio.create_task(connection_health_check(bot))
            
            # Запускаем бота
            logger.info("Запуск бота...")
            await bot.start()
        except asyncio.CancelledError:
            logger.info("Бот остановлен пользователем")
        except Exception as e:
            logger.error(f"Ошибка в работе бота: {e}")
        finally:
            # Отменяем задачу проверки соединения, если она существует
            if 'connection_check_task' in locals() and not connection_check_task.done():
                connection_check_task.cancel()
                try:
                    await connection_check_task
                except asyncio.CancelledError:
                    pass
            
            # Корректное завершение
            await bot.stop()
            await bot.video_handler.close_session()
            logger.info("Бот корректно завершил работу")

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        server.stop()
        logger.info("Локальный сервер остановлен")

async def connection_health_check(bot):
    """Периодическая проверка состояния соединения"""
    logger = logging.getLogger(__name__)
    reconnect_interval = 300  # 5 минут между проверками
    
    while True:
        try:
            await asyncio.sleep(reconnect_interval)
            
            # Проверка соединения Pyrogram
            if bot.video_handler.app:
                if not bot.video_handler.app.is_connected:
                    logger.warning("Обнаружено неактивное соединение Pyrogram, выполняем переподключение...")
                    try:
                        await bot.video_handler.app.stop()
                    except:
                        pass
                    
                    await bot.video_handler.init_client()
                    logger.info("Pyrogram клиент успешно переподключен")
            
            # Проверка соединения бота
            # Простой запрос, чтобы проверить активность соединения
            try:
                me = await bot.bot.get_me()
                logger.debug(f"Соединение с ботом активно: {me.username}")
            except Exception as e:
                logger.warning(f"Проблема с соединением бота: {e}")
                # Пытаемся перезапустить диспетчер
                try:
                    await bot.dp.stop_polling()
                    await asyncio.sleep(2)
                    await bot.dp.start_polling(bot.bot)
                    logger.info("Диспетчер бота успешно перезапущен")
                except Exception as restart_error:
                    logger.error(f"Не удалось перезапустить диспетчер: {restart_error}")
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Ошибка в задаче проверки соединения: {e}")
            await asyncio.sleep(30)  # В случае ошибки ждем 30 секунд

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Программа остановлена пользователем")
    except Exception as e:
        logging.critical(f"Критическая ошибка при запуске: {e}", exc_info=True)