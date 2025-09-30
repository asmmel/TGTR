import os
import subprocess
import logging
import asyncio
from typing import Optional
from datetime import datetime
from config.config import setup_logging

logger = setup_logging(__name__)

class VideoSpeedService:
    """Сервис для ускорения видео через FFmpeg"""
    
    def __init__(self, downloads_dir="downloads"):
        self.downloads_dir = downloads_dir
        os.makedirs(downloads_dir, exist_ok=True)
    
    async def speed_up_video(
        self, 
        input_path: str, 
        speed_coefficient: int,
        pitch_shift: float = -0.5,
        keep_original: bool = False
    ) -> Optional[str]:
        """
        Ускоряет видео через FFmpeg
        """
        # НОРМАЛИЗУЕМ ПУТЬ ДЛЯ ТЕКУЩЕЙ ОС
        input_path = os.path.normpath(input_path)
        
        # ДОБАВЬ ЭТО ЛОГИРОВАНИЕ
        logger.info(f"=" * 60)
        logger.info(f"🔍 НАЧАЛО ОБРАБОТКИ ВИДЕО")
        logger.info(f"📁 Входной путь (после нормализации): {input_path}")
        logger.info(f"📂 Рабочая директория: {os.getcwd()}")
        logger.info(f"✅ Файл существует: {os.path.exists(input_path)}")
        
        if os.path.exists(input_path):
            logger.info(f"📊 Размер файла: {os.path.getsize(input_path) / (1024*1024):.2f} MB")
            logger.info(f"📝 Абсолютный путь: {os.path.abspath(input_path)}")
        else:
            # ПРОВЕРЯЕМ АЛЬТЕРНАТИВНЫЕ ПУТИ
            logger.error(f"❌ Файл НЕ найден по пути: {input_path}")
            
            # Пробуем найти файл в разных местах
            possible_paths = [
                input_path,
                os.path.abspath(input_path),
                os.path.join(os.getcwd(), input_path),
                os.path.join("downloads", os.path.basename(input_path)),
                os.path.join(self.downloads_dir, os.path.basename(input_path))
            ]
            
            logger.info("🔍 Проверяем альтернативные пути:")
            for path in possible_paths:
                exists = os.path.exists(path)
                logger.info(f"  {'✅' if exists else '❌'} {path}")
                if exists:
                    logger.info(f"🎯 ФАЙЛ НАЙДЕН! Используем путь: {path}")
                    input_path = path
                    break
            else:
                logger.error("❌ Файл не найден ни по одному из путей!")
                return None
        
        logger.info(f"=" * 60)
        
        if not 1 <= speed_coefficient <= 10:
            logger.error(f"❌ Неверный коэффициент: {speed_coefficient}. Должен быть от 1 до 10")
            return None
        
        # Преобразуем 1-10 в 1.01-1.10
        speed_factor = 1.0 + (speed_coefficient / 100)
        
        try:
            file_dir = os.path.dirname(input_path)
            file_name = os.path.basename(input_path)
            name, ext = os.path.splitext(file_name)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # ИСПРАВЛЕНИЕ: Используем абсолютный путь для выходного файла
            if not file_dir or file_dir == '.':
                file_dir = os.path.abspath(self.downloads_dir)
            
            output_path = os.path.join(
                file_dir, 
                f"{name}_speed{speed_coefficient}x_{timestamp}{ext}"
            )
            
            logger.info(f"📤 Выходной путь: {output_path}")
            logger.info(f"⚡ Ускоряем видео с коэффициентом {speed_factor}x...")
            
            # Рассчитываем параметры для изменения тона
            pitch_ratio = 2 ** (pitch_shift / 12)
            new_sample_rate = int(48000 * pitch_ratio)
            
            # ИСПОЛЬЗУЕМ АБСОЛЮТНЫЕ ПУТИ для FFmpeg
            abs_input_path = os.path.abspath(input_path)
            abs_output_path = os.path.abspath(output_path)
            
            logger.info(f"🎬 FFmpeg входной файл: {abs_input_path}")
            logger.info(f"🎬 FFmpeg выходной файл: {abs_output_path}")
            
            # Формируем команду FFmpeg
            cmd = [
                'ffmpeg',
                '-i', abs_input_path,  # ИСПОЛЬЗУЕМ АБСОЛЮТНЫЙ ПУТЬ
                '-filter_complex',
                f'[0:v]setpts=PTS/{speed_factor}[v];'
                f'[0:a]asetrate={new_sample_rate},aresample=48000,atempo={speed_factor}[a]',
                '-map', '[v]',
                '-map', '[a]',
                '-c:v', 'libx264',
                '-preset', 'medium',
                '-crf', '23',
                '-c:a', 'aac',
                '-b:a', '192k',
                '-y',
                abs_output_path  # ИСПОЛЬЗУЕМ АБСОЛЮТНЫЙ ПУТЬ
            ]
            
            logger.info(f"🎬 Команда FFmpeg: {' '.join(cmd)}")
            
            # Запускаем FFmpeg асинхронно
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                if os.path.exists(abs_output_path):
                    file_size = os.path.getsize(abs_output_path) / (1024 * 1024)
                    logger.info(f"✅ Видео ускорено: {abs_output_path} ({file_size:.2f} MB)")
                    
                    # Удаляем оригинал если не нужно сохранять
                    if not keep_original and abs_input_path != abs_output_path:
                        try:
                            os.remove(abs_input_path)
                            logger.info(f"🗑 Удален оригинальный файл: {abs_input_path}")
                        except Exception as e:
                            logger.warning(f"Не удалось удалить оригинал: {e}")
                    
                    return abs_output_path  # ВОЗВРАЩАЕМ АБСОЛЮТНЫЙ ПУТЬ
                else:
                    logger.error(f"❌ Выходной файл не создан")
                    return None
            else:
                error_msg = stderr.decode('utf-8') if stderr else "Неизвестная ошибка"
                logger.error(f"❌ FFmpeg ошибка:\n{error_msg}")
                return None
                
        except asyncio.TimeoutError:
            logger.error(f"❌ Превышен таймаут обработки видео")
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке видео: {e}", exc_info=True)
            return None
    
    async def get_video_info(self, video_path: str) -> Optional[dict]:
        """Получает информацию о видео через ffprobe"""
        try:
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                '-show_streams',
                video_path
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, _ = await process.communicate()
            
            if process.returncode == 0:
                import json
                info = json.loads(stdout.decode('utf-8'))
                
                # Извлекаем полезную информацию
                duration = float(info.get('format', {}).get('duration', 0))
                size = int(info.get('format', {}).get('size', 0))
                
                return {
                    'duration': duration,
                    'size_mb': size / (1024 * 1024),
                    'format': info.get('format', {}).get('format_name', 'unknown')
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка получения информации о видео: {e}")
            return None