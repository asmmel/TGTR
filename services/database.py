import sqlite3
from datetime import datetime

class Database:
    def __init__(self, db_file="bot_database.db"):
        self.db_file = db_file
        self.init_db()

    def init_db(self):
        """Инициализация БД с нужной структурой"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS url_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                url TEXT,
                timestamp DATETIME,
                status TEXT,
                error_message TEXT
            )
        ''')
        conn.commit()
        conn.close()

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_file)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'conn'):
            self.conn.close()
            
    async def cleanup_old_records(self, days=7):
        with sqlite3.connect(self.db_file) as conn:
            c = conn.cursor()
            c.execute('''
                DELETE FROM url_logs  # было requests, исправлено на url_logs
                WHERE timestamp < datetime('now', '-? days')
            ''', (days,))
            conn.commit()



    def log_url(self, user_id: int, username: str, url: str, status: str, error_message: str = None):
        """Логирование URL с результатом обработки"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute('''
            INSERT INTO url_logs (user_id, username, url, timestamp, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_id, username, url, datetime.now(), status, error_message))
        conn.commit()
        conn.close()

    def get_user_history(self, user_id: int, limit: int = 10) -> list:
        """Получение истории запросов пользователя"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute('''
            SELECT url, status, error_message, timestamp 
            FROM url_logs 
            WHERE user_id = ? 
            ORDER BY timestamp DESC 
            LIMIT ?
        ''', (user_id, limit))
        result = c.fetchall()
        conn.close()
        return result

    def get_all_history(self, limit: int = 30) -> list:
        """Получение общей истории запросов"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute('''
            SELECT username, url, status, error_message, timestamp 
            FROM url_logs 
            ORDER BY timestamp DESC 
            LIMIT ?
        ''', (limit,))
        result = c.fetchall()
        conn.close()
        return result