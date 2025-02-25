import aiohttp
import time
import json
import os
import uuid
import random
from typing import Dict, Optional
from twocaptcha import TwoCaptcha
import logging
import asyncio
import requests
from config.config import setup_logging

logger = setup_logging(__name__)

class CobaltDownloader:
    def __init__(self):
        self.base_url = "https://api.cobalt.tools"
        self.solver = TwoCaptcha('96936897121fd3fb6942211f6613bb10')
        self.token = None
        self.token_expiry = None
        self.default_download_path = "downloads"
        
        if not os.path.exists(self.default_download_path):
            os.makedirs(self.default_download_path)
        
        self.headers = {
            'authority': 'api.cobalt.tools',
            'accept': 'application/json',
            'accept-language': 'ru,en;q=0.9',
            'content-type': 'application/json',
            'origin': 'https://cobalt.tools',
            'referer': 'https://cobalt.tools/',
            'sec-ch-ua': '"Chromium";v="130", "YaBrowser";v="24.12", "Not?A_Brand";v="99", "Yowser";v="2.5"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 YaBrowser/24.12.0.0 Safari/537.36'
        }

    async def solve_turnstile(self) -> Optional[str]:
        """Асинхронное решение капчи"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.solver.turnstile(
                    sitekey='0x4AAAAAAAhUvTuTxLs2HYH4',
                    url='https://cobalt.tools/',
                    action='submit'
                )
            )
            return result['code']
        except Exception as e:
            logger.error(f"Ошибка при решении капчи: {e}")
            return None

    async def create_session(self) -> bool:
        """Асинхронное создание сессии"""
        turnstile_token = await self.solve_turnstile()
        if not turnstile_token:
            return False

        headers = self.headers.copy()
        headers["cf-turnstile-response"] = turnstile_token

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/session",
                    headers=headers
                ) as response:
                    if response.status == 429:
                        retry_after = int(response.headers.get('ratelimit-reset', 60))
                        logger.warning(f"Превышен лимит запросов. Ожидание {retry_after} секунд...")
                        await asyncio.sleep(retry_after)
                        return await self.create_session()

                    response.raise_for_status()
                    session_data = await response.json()
                    
                    self.token = session_data.get("token")
                    self.token_expiry = session_data.get("exp")
                    
                    logger.info(f"Сессия успешно создана. Token: {self.token[:20]}...")
                    return bool(self.token)

        except Exception as e:
            logger.error(f"Ошибка при создании сессии: {e}")
            return False

    async def process_video(self, url: str) -> Dict:
        """Асинхронная обработка видео"""
        if not self.token:
            if not await self.create_session():
                raise Exception("Не удалось создать сессию")

        headers = self.headers.copy()
        headers["Authorization"] = f"Bearer {self.token}"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.base_url,
                    headers=headers,
                    json={"url": url}
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    logger.info(f"Cobalt API response: {data}")
                    return data

        except aiohttp.ClientError as e:
            logger.error(f"Ошибка при обработке видео: {e}")
            if 'response' in locals():
                error_text = await response.text()
                logger.error(f"Ответ сервера: {error_text}")
            raise

    def download_video_sync(self, url: str, output_path: str) -> bool:
        """Синхронное скачивание видео используя requests"""
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Создаем временный файл
            temp_path = f"{output_path}.temp"
            
            with open(temp_path, 'wb') as file:
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                start_time = time.time()
                
                for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                    if chunk:
                        file.write(chunk)
                        downloaded += len(chunk)
                        elapsed = time.time() - start_time
                        speed = downloaded / (1024 * 1024 * elapsed) if elapsed > 0 else 0
                        
                        if total_size:
                            percent = int(100 * downloaded / total_size)
                            logger.info(f"Прогресс: {percent}%. Скорость: {speed:.2f} MB/s")

            # Проверяем скачанный файл
            if os.path.exists(temp_path):
                actual_size = os.path.getsize(temp_path)
                if actual_size > 0 and (total_size == 0 or actual_size >= total_size * 0.99):
                    os.replace(temp_path, output_path)
                    logger.info(f"Файл успешно загружен: {output_path}")
                    return True
                else:
                    logger.warning(f"Размер файла не соответствует ожидаемому: {actual_size} vs {total_size}")
                    os.remove(temp_path)
            
            return False

        except requests.RequestException as e:
            logger.error(f"Ошибка при скачивании: {e}")
            if 'temp_path' in locals() and os.path.exists(temp_path):
                os.remove(temp_path)
            return False

    async def download_video(self, video_url: str) -> str:
        """Асинхронное скачивание видео"""
        try:
            logger.info("Получение информации о видео...")
            result = await self.process_video(video_url)
            
            if not result:
                raise Exception("Пустой ответ от API")

            download_url = result.get("url")
            filename = result.get("filename", "video.mp4")
            
            if not download_url:
                raise Exception("URL для скачивания не найден")

            output_path = os.path.join(self.default_download_path, filename)
            logger.info(f"Начало загрузки файла: {filename}")
            logger.info(f"URL для скачивания: {download_url}")

            # Используем синхронное скачивание через requests
            loop = asyncio.get_event_loop()
            success = await loop.run_in_executor(None, 
                                               self.download_video_sync,
                                               download_url, 
                                               output_path)
            
            if not success:
                raise Exception("Не удалось скачать файл")

            return output_path

        except Exception as e:
            logger.error(f"Произошла ошибка при загрузке: {e}")
            if 'output_path' in locals() and os.path.exists(output_path):
                os.remove(output_path)
            raise

    async def cleanup(self):
        """Очистка временных файлов"""
        try:
            for filename in os.listdir(self.default_download_path):
                if filename.endswith('.temp'):
                    file_path = os.path.join(self.default_download_path, filename)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        logger.info(f"Удален временный файл: {file_path}")
        except Exception as e:
            logger.error(f"Ошибка при очистке временных файлов: {e}")


async def main():
    try:
        downloader = CobaltDownloader()
        video_url = "https://www.youtube.com/watch?v=lRAYCrGDQaA&rco=1"  # укажите URL вашего видео
        output_path = await downloader.download_video(video_url)
        print(f"Видео успешно загружено: {output_path}")
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        await downloader.cleanup()

if __name__ == "__main__":
    asyncio.run(main())