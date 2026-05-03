"""Скачивание видео из Instagram по URL.

Подход: загрузка страницы рилса через curl_cffi (Chrome TLS) + прокси,
парсинг video_versions из embedded JSON, скачивание CDN-ссылки напрямую.
"""
import re
import json
import os
import asyncio
import random
import string
import time
import logging
from typing import Optional, Dict

import requests
from curl_cffi import requests as cffi_requests

from services.base_downloader import BaseDownloader
from config.config import INSTAGRAM_PROXIES, DOWNLOADS_DIR

logger = logging.getLogger(__name__)


def _get_proxy_url() -> Optional[str]:
    """Случайный прокси из списка для запросов к Instagram."""
    proxies = INSTAGRAM_PROXIES
    if not proxies:
        return None
    proxy_str = random.choice(proxies)
    at_idx = proxy_str.find('@')
    if at_idx == -1:
        return f'http://{proxy_str}'
    creds = proxy_str[:at_idx]
    host_port = proxy_str[at_idx + 1:]
    login, password = creds.split(':', 1)
    if 'proxy-seller' in host_port:
        sticky_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        login = f'{login}_s_{sticky_id}_ttl_10m'
    return f'http://{login}:{password}@{host_port}'


class InstagramDownloader(BaseDownloader):
    """Загрузчик видео из Instagram.

    1) Загружаем страницу рилса через curl_cffi + прокси
    2) Парсим video_versions из embedded JSON в HTML
    3) Скачиваем CDN-ссылку напрямую без прокси
    """

    def __init__(self, downloads_dir=None):
        super().__init__(downloads_dir or DOWNLOADS_DIR)

    def extract_shortcode(self, url: str) -> Optional[str]:
        patterns = [
            r'/p/([A-Za-z0-9_-]+)',
            r'/reel/([A-Za-z0-9_-]+)',
            r'/tv/([A-Za-z0-9_-]+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None

    def _fetch_video_url(self, shortcode: str, attempt: int = 1) -> Optional[str]:
        """Загрузить страницу рилса и извлечь лучший video URL (синхронно).
        curl_cffi + прокси для обхода TLS fingerprinting."""
        logger.info(f"Fetching reel page for {shortcode} (attempt {attempt})")
        proxy_url = _get_proxy_url()
        sess = cffi_requests.Session(impersonate="chrome")

        try:
            resp = sess.get(
                f'https://www.instagram.com/reel/{shortcode}/',
                proxy=proxy_url,
                timeout=30
            )
            if resp.status_code != 200:
                logger.warning(f"Page load HTTP {resp.status_code} for {shortcode}")
                return None

            content = resp.text

            # Метод 1: video_versions JSON array (лучший вариант — выбираем макс. разрешение)
            m = re.search(r'"video_versions":\s*(\[.*?\])', content)
            if m:
                raw = m.group(1).replace('\\/', '/')
                versions = json.loads(raw)
                if versions:
                    best = max(versions, key=lambda x: x.get('width', 0) * x.get('height', 0))
                    url = best.get('url', '')
                    if url:
                        logger.info(f"Found video_versions: {best.get('width')}x{best.get('height')}")
                        return url

            # Метод 2: video_url в embedded data
            m2 = re.search(r'"video_url":"([^"]+)"', content)
            if m2:
                url = m2.group(1).replace('\\u0026', '&').replace('\\/', '/')
                logger.info("Found video_url in page data")
                return url

            logger.warning(f"No video URL found in page for {shortcode}")
            return None

        except Exception as e:
            logger.warning(f"Fetch error for {shortcode} (attempt {attempt}): {e}")
            max_fetch_attempts = min(4, max(3, len(INSTAGRAM_PROXIES or [])))
            if attempt < max_fetch_attempts:
                time.sleep(random.uniform(2, 5))
                return self._fetch_video_url(shortcode, attempt + 1)
            return None
        finally:
            try:
                sess.close()
            except Exception:
                pass

    def _download_file(self, video_url: str, output_path: str,
                       max_attempts: int = 3, timeout: int = 120) -> bool:
        """Скачать видео по прямой CDN-ссылке с retry.

        Стратегия (попытки):
          1. curl_cffi + Chrome TLS напрямую без прокси
          2. curl_cffi + Chrome TLS + через прокси
          3. requests (fallback) + прокси

        Поддерживает HTTP Range для докачивания при оборванном TCP.
        """
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept": "video/*;q=0.9,*/*;q=0.5",
            "Accept-Encoding": "identity",
            "Referer": "https://www.instagram.com/",
        }

        last_err = None
        for attempt in range(1, max_attempts + 1):
            use_cffi = attempt <= 2
            use_proxy = attempt >= 2
            proxy_url = _get_proxy_url() if use_proxy else None

            resume_from = 0
            if os.path.exists(output_path):
                try:
                    resume_from = os.path.getsize(output_path)
                except OSError:
                    resume_from = 0

            req_headers = dict(headers)
            mode = 'wb'
            if resume_from > 0:
                req_headers['Range'] = f'bytes={resume_from}-'
                mode = 'ab'
                logger.info(f"Download attempt {attempt}/{max_attempts}: "
                            f"resuming from {resume_from} bytes "
                            f"(proxy={'yes' if use_proxy else 'no'}, cffi={use_cffi})")
            else:
                logger.info(f"Download attempt {attempt}/{max_attempts} "
                            f"(proxy={'yes' if use_proxy else 'no'}, cffi={use_cffi})")

            try:
                downloaded = resume_from
                if use_cffi:
                    sess = cffi_requests.Session(impersonate="chrome")
                    try:
                        resp = sess.get(video_url, headers=req_headers,
                                        proxy=proxy_url, timeout=timeout,
                                        stream=True)
                        if resp.status_code not in (200, 206):
                            raise Exception(f"HTTP {resp.status_code}")
                        with open(output_path, mode) as f:
                            for chunk in resp.iter_content(chunk_size=65536):
                                if chunk:
                                    f.write(chunk)
                                    downloaded += len(chunk)
                        try:
                            resp.close()
                        except Exception:
                            pass
                    finally:
                        try:
                            sess.close()
                        except Exception:
                            pass
                else:
                    proxies = {'http': proxy_url, 'https': proxy_url} if proxy_url else None
                    with requests.get(video_url, headers=req_headers,
                                      stream=True, timeout=timeout,
                                      proxies=proxies) as resp:
                        resp.raise_for_status()
                        with open(output_path, mode) as f:
                            for chunk in resp.iter_content(chunk_size=65536):
                                if chunk:
                                    f.write(chunk)
                                    downloaded += len(chunk)

                size_mb = downloaded / (1024 * 1024)
                logger.info(f"Downloaded {size_mb:.1f} MB -> {output_path} (attempt {attempt})")
                return True

            except Exception as e:
                last_err = e
                logger.warning(f"Download attempt {attempt}/{max_attempts} failed: {e}")
                if attempt >= max_attempts:
                    if os.path.exists(output_path):
                        try:
                            os.remove(output_path)
                        except OSError:
                            pass
                continue

        logger.error(f"Download failed after {max_attempts} attempts: {last_err}")
        return False

    async def extract_video_info(self, url: str) -> Dict:
        shortcode = self.extract_shortcode(url)
        return {
            'title': f'Instagram Video {shortcode}',
            'duration': 0,
            'thumbnail': '',
            'uploader': 'Instagram User',
            'formats': [],
            'is_live': False
        }

    async def download_video(self, url: str, output_path: str = None) -> Optional[str]:
        """Скачать видео из Instagram.

        1. Парсинг video_url из страницы (curl_cffi + Chrome TLS + прокси)
        2. Скачивание файла (3 попытки: direct → cffi+proxy → requests+proxy)
        3. yt-dlp fallback
        """
        shortcode = self.extract_shortcode(url)
        if not shortcode:
            logger.error(f"Invalid Instagram URL: {url}")
            return None

        if not output_path:
            output_path = self.generate_output_filename("instagram")

        # Метод 1: парсинг video_versions из страницы (curl_cffi + прокси)
        try:
            video_url = await asyncio.to_thread(self._fetch_video_url, shortcode)
            if video_url:
                logger.info(f"Downloading video for {shortcode}...")
                success = await asyncio.to_thread(self._download_file, video_url, output_path)
                if success and os.path.exists(output_path):
                    return output_path
        except Exception as e:
            logger.warning(f"Page parse method failed: {e}")

        # Метод 2: yt-dlp fallback
        try:
            logger.info(f"Trying yt-dlp fallback for {url}")
            import yt_dlp
            ydl_opts = {
                'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                'outtmpl': output_path,
                'quiet': True,
                'no_warnings': True,
                'merge_output_format': 'mp4',
                'retries': 3,
            }
            proxy_url = _get_proxy_url()
            if proxy_url:
                ydl_opts['proxy'] = proxy_url

            def _run_ytdlp():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([url])

            await asyncio.to_thread(_run_ytdlp)
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                return output_path
        except ImportError:
            logger.warning("yt-dlp not installed, skipping fallback")
        except Exception as e:
            logger.warning(f"yt-dlp failed: {e}")

        if os.path.exists(output_path):
            try:
                os.remove(output_path)
            except OSError:
                pass

        return None
