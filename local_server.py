import os
import sys
import subprocess
import time
import logging
import json
import signal
import psutil
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

@dataclass
class ServerConfig:
    api_id: str
    api_hash: str
    local_port: int = 8081
    max_webhook_connections: int = 100
    working_dir: str = "/var/lib/telegram-bot-api"  # Linux путь
    executable_path: str = "/usr/local/bin/telegram-bot-api"  # Путь к исполняемому файлу в Linux

class TelegramLocalServer:
    def __init__(self, config: ServerConfig):
        self.config = config
        self.process: Optional[subprocess.Popen] = None
        self.setup_logging()
        
    def setup_logging(self):
        """Настройка логирования на Linux"""
        log_dir = "/var/log/telegram-bot-api"
        try:
            # Создаем директорию для логов, если её нет
            Path(log_dir).mkdir(parents=True, exist_ok=True)
            
            # Проверяем права доступа
            if not os.access(log_dir, os.W_OK):
                print(f"ВНИМАНИЕ: Нет прав на запись в директорию логов: {log_dir}")
                log_dir = "."  # Используем текущую директорию
        except Exception as e:
            print(f"Ошибка при создании директории логов: {e}")
            log_dir = "."  # Используем текущую директорию
            
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'{log_dir}/telegram_server.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger('TelegramLocalServer')

    def check_server_executable(self) -> bool:
        """Проверяет наличие исполняемого файла сервера"""
        if not os.path.exists(self.config.executable_path):
            self.logger.error(f"Исполняемый файл не найден: {self.config.executable_path}")
            return False
        return True

    def create_working_directory(self) -> bool:
        """Создает рабочую директорию для сервера"""
        try:
            working_dir = Path(self.config.working_dir)
            working_dir.mkdir(parents=True, exist_ok=True)
            
            # Проверка прав доступа
            if not os.access(working_dir, os.W_OK):
                self.logger.error(f"Нет прав на запись в директорию: {working_dir}")
                return False
                
            return True
        except Exception as e:
            self.logger.error(f"Ошибка при создании рабочей директории: {e}")
            return False

    def build_command(self) -> list:
        """Формирует команду запуска сервера"""
        return [
            self.config.executable_path,
            "--local",
            f"--api-id={self.config.api_id}",
            f"--api-hash={self.config.api_hash}",
            f"--http-port={self.config.local_port}",
            f"--max-webhook-connections={self.config.max_webhook_connections}",
            "--verbosity=2",
            f"--dir={self.config.working_dir}",
            "--log=/var/log/telegram-bot-api/telegram-bot-api.log"  # Linux путь для лога
        ]

    def is_port_in_use(self) -> bool:
        """Проверяет, занят ли порт"""
        for conn in psutil.net_connections():
            if hasattr(conn, 'laddr') and hasattr(conn.laddr, 'port') and conn.laddr.port == self.config.local_port:
                return True
        return False

    def kill_existing_process(self):
        """Завершает процесс, если он уже запущен на указанном порту"""
        for proc in psutil.process_iter(['pid', 'name', 'connections']):
            try:
                for conn in proc.connections():
                    if hasattr(conn, 'laddr') and hasattr(conn.laddr, 'port') and conn.laddr.port == self.config.local_port:
                        # В Linux используем os.kill вместо proc.kill()
                        os.kill(proc.pid, signal.SIGTERM)
                        self.logger.info(f"Отправлен SIGTERM процессу {proc.pid} на порту {self.config.local_port}")
                        time.sleep(1)
                        
                        # Проверяем, завершился ли процесс
                        if psutil.pid_exists(proc.pid):
                            os.kill(proc.pid, signal.SIGKILL)
                            self.logger.info(f"Отправлен SIGKILL процессу {proc.pid}")
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

    def start(self) -> bool:
        """Запускает локальный сервер"""
        try:
            if not self.check_server_executable():
                return False

            if not self.create_working_directory():
                return False

            if self.is_port_in_use():
                self.logger.warning(f"Порт {self.config.local_port} занят, освобождаем...")
                self.kill_existing_process()

            command = self.build_command()
            self.logger.info(f"Запуск сервера с командой: {' '.join(command)}")

            self.process = subprocess.Popen(
                command,
                cwd=self.config.working_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            time.sleep(2)
            if self.process.poll() is not None:
                _, stderr = self.process.communicate()
                self.logger.error(f"Процесс сервера завершился: {stderr}")
                return False

            self.logger.info("Сервер успешно запущен")
            return True

        except Exception as e:
            self.logger.error(f"Ошибка запуска сервера: {e}", exc_info=True)
            return False

    def stop(self):
        """Останавливает сервер"""
        if self.process:
            try:
                # Отправляем SIGTERM сигнал (Linux)
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)  # Ждем 5 секунд
                except subprocess.TimeoutExpired:
                    # Если процесс не завершился, отправляем SIGKILL
                    self.process.kill()
                    
                self.logger.info("Сервер остановлен")
                
            except Exception as e:
                self.logger.error(f"Ошибка при остановке сервера: {e}")
            finally:
                self.process = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

def load_config(config_path: str = "config_server.json") -> ServerConfig:
    """Загружает конфигурацию из JSON файла"""
    try:
        with open(config_path, 'r') as f:
            config_data = json.load(f)
        return ServerConfig(**config_data)
    except Exception as e:
        logging.error(f"Ошибка загрузки конфигурации: {e}")
        # Если файл не найден, возвращаем конфигурацию по умолчанию
        return ServerConfig(api_id=os.getenv("API_ID", ""), api_hash=os.getenv("API_HASH", ""))
