"""Хранилище Instagram прокси с возможностью обновления в рантайме.

Прокси читаются из файла instagram_proxies.json (приоритет),
с фоллбэком на INSTAGRAM_PROXIES из .env.
"""
import json
import os
from typing import List

from config.config import INSTAGRAM_PROXIES

PROXY_FILE = "instagram_proxies.json"


def get_proxies() -> List[str]:
    """Возвращает актуальный список прокси: сначала пробует файл, потом .env."""
    if os.path.exists(PROXY_FILE):
        try:
            with open(PROXY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                proxies = data.get("proxies", [])
                if isinstance(proxies, list):
                    return [str(p).strip() for p in proxies if str(p).strip()]
        except Exception:
            pass
    return list(INSTAGRAM_PROXIES)


def set_proxies(proxies: List[str]) -> int:
    """Сохраняет список прокси в файл. Возвращает количество сохранённых."""
    cleaned = [str(p).strip() for p in proxies if str(p).strip()]
    with open(PROXY_FILE, "w", encoding="utf-8") as f:
        json.dump({"proxies": cleaned}, f, indent=2, ensure_ascii=False)
    return len(cleaned)
