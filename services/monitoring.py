import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import defaultdict
import os
from config.config import setup_logging

logger = setup_logging(__name__)

class MonitoringService:
    def __init__(self):
        self.stats_file = "monitoring_stats.json"
        self.metrics = {
            'api_calls': defaultdict(int),
            'api_errors': defaultdict(list),
            'download_times': [],
            'success_rate': defaultdict(lambda: {'success': 0, 'failed': 0}),
            'hourly_stats': defaultdict(lambda: defaultdict(int))
        }
        self.load_stats()

    def load_stats(self):
        """Загрузка статистики из файла"""
        try:
            if os.path.exists(self.stats_file):
                with open(self.stats_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Преобразуем обычные словари в defaultdict
                    self.metrics['api_calls'] = defaultdict(int, data.get('api_calls', {}))
                    self.metrics['api_errors'] = defaultdict(list, data.get('api_errors', {}))
                    self.metrics['download_times'] = data.get('download_times', [])
                    self.metrics['success_rate'] = defaultdict(
                        lambda: {'success': 0, 'failed': 0},
                        data.get('success_rate', {})
                    )
                    self.metrics['hourly_stats'] = defaultdict(
                        lambda: defaultdict(int),
                        data.get('hourly_stats', {})
                    )
        except Exception as e:
            logger.error(f"Ошибка загрузки статистики: {e}")

    def save_stats(self):
        """Сохранение статистики в файл"""
        try:
            with open(self.stats_file, 'w', encoding='utf-8') as f:
                # Преобразуем defaultdict в обычные словари для сериализации
                data = {
                    'api_calls': dict(self.metrics['api_calls']),
                    'api_errors': dict(self.metrics['api_errors']),
                    'download_times': self.metrics['download_times'][-1000:],  # Храним последние 1000 замеров
                    'success_rate': dict(self.metrics['success_rate']),
                    'hourly_stats': dict(self.metrics['hourly_stats'])
                }
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Ошибка сохранения статистики: {e}")

    def log_api_call(self, service: str, endpoint: str, success: bool, error_msg: Optional[str] = None):
        """Логирование вызова API"""
        current_hour = datetime.now().strftime('%Y-%m-%d %H:00')
        
        self.metrics['api_calls'][f"{service}_{endpoint}"] += 1
        self.metrics['hourly_stats'][current_hour][service] += 1
        
        if success:
            self.metrics['success_rate'][service]['success'] += 1
        else:
            self.metrics['success_rate'][service]['failed'] += 1
            if error_msg:
                self.metrics['api_errors'][service].append({
                    'time': datetime.now().isoformat(),
                    'error': error_msg
                })
        
        # Автоматическое сохранение после каждого 10-го вызова
        if sum(self.metrics['api_calls'].values()) % 10 == 0:
            self.save_stats()

    def log_download_time(self, service: str, duration: float):
        """Логирование времени загрузки"""
        self.metrics['download_times'].append({
            'service': service,
            'time': duration,
            'timestamp': datetime.now().isoformat()
        })

    def get_service_health(self, service: str) -> Dict:
        """Получение состояния здоровья сервиса"""
        stats = self.metrics['success_rate'][service]
        total = stats['success'] + stats['failed']
        success_rate = (stats['success'] / total * 100) if total > 0 else 0
        
        recent_errors = [
            error for error in self.metrics['api_errors'][service][-5:]
            if datetime.fromisoformat(error['time']) > datetime.now() - timedelta(hours=1)
        ]
        
        return {
            'success_rate': success_rate,
            'total_calls': total,
            'recent_errors': recent_errors,
            'status': 'healthy' if success_rate >= 90 else 'degraded' if success_rate >= 70 else 'critical'
        }

    def get_hourly_stats(self, hours: int = 24) -> Dict:
        """Получение почасовой статистики"""
        current = datetime.now()
        stats = {}
        
        for i in range(hours):
            hour = (current - timedelta(hours=i)).strftime('%Y-%m-%d %H:00')
            stats[hour] = dict(self.metrics['hourly_stats'][hour])
        
        return stats

    def get_performance_metrics(self, service: str) -> Dict:
        """Получение метрик производительности"""
        recent_downloads = [
            d for d in self.metrics['download_times']
            if d['service'] == service and
            datetime.fromisoformat(d['timestamp']) > datetime.now() - timedelta(hours=1)
        ]
        
        if not recent_downloads:
            return {'avg_download_time': 0, 'max_download_time': 0}
        
        download_times = [d['time'] for d in recent_downloads]
        return {
            'avg_download_time': sum(download_times) / len(download_times),
            'max_download_time': max(download_times)
        }

    def generate_report(self) -> str:
        """Генерация отчета о состоянии системы"""
        services = set([key.split('_')[0] for key in self.metrics['api_calls'].keys()])
        
        report = "📊 Отчет о состоянии системы\n\n"
        
        for service in services:
            health = self.get_service_health(service)
            perf = self.get_performance_metrics(service)
            
            status_emoji = "🟢" if health['status'] == 'healthy' else "🟡" if health['status'] == 'degraded' else "🔴"
            
            report += f"{status_emoji} {service.upper()}\n"
            report += f"├ Успешность: {health['success_rate']:.1f}%\n"
            report += f"├ Всего запросов: {health['total_calls']}\n"
            report += f"├ Среднее время загрузки: {perf['avg_download_time']:.2f}s\n"
            
            if health['recent_errors']:
                report += "├ Последние ошибки:\n"
                for error in health['recent_errors'][-3:]:
                    report += f"│ ❌ {error['error']}\n"
            
            report += "└──────────\n\n"
        
        return report