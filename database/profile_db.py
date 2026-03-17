"""
Работа с таблицей профилей пользователей
"""
import sqlite3
import threading
from datetime import datetime
from typing import Optional, Dict, Any, List

from main import DB_NAME, Database, retry_on_db_lock

class ProfileDB:
    def __init__(self, db: Database = None):
        self.db = db or Database(DB_NAME)
        self.lock = threading.RLock()
        self._create_tables()
    
    def _create_tables(self):
        """Создает таблицу профилей, если её нет"""
        with self.lock:
            self.db._execute('''
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT NOT NULL,
                last_name TEXT,
                middle_name TEXT,
                gender TEXT CHECK(gender IN ('male', 'female', NULL)),
                birth_day INTEGER CHECK(birth_day BETWEEN 1 AND 31),
                birth_month INTEGER CHECK(birth_month BETWEEN 1 AND 12),
                birth_year INTEGER CHECK(birth_year > 1900),
                city TEXT,
                region TEXT,
                timezone TEXT DEFAULT 'Europe/Moscow',
                location_manually_set BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            # Индекс для поиска по городу
            self.db._execute('''
            CREATE INDEX IF NOT EXISTS idx_profile_city ON user_profiles(city)
            ''')
            
            self.db.conn.commit()
    
    @retry_on_db_lock()
    def save_profile(self, user_id: int, username: str, data: Dict[str, Any]) -> bool:
        """
        Сохраняет или обновляет профиль пользователя
        """
        with self.lock:
            try:
                # Проверяем существование
                self.db._execute(
                    "SELECT user_id FROM user_profiles WHERE user_id = ?",
                    (user_id,)
                )
                exists = self.db.cursor.fetchone()
                
                if exists:
                    # Обновление
                    query = '''
                    UPDATE user_profiles SET
                        username = ?,
                        first_name = ?,
                        last_name = ?,
                        middle_name = ?,
                        gender = ?,
                        birth_day = ?,
                        birth_month = ?,
                        birth_year = ?,
                        city = ?,
                        region = ?,
                        timezone = ?,
                        location_manually_set = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = ?
                    '''
                    
                    self.db._execute(query, (
                        username,
                        data.get('first_name'),
                        data.get('last_name'),
                        data.get('middle_name'),
                        data.get('gender'),
                        data.get('birth_day'),
                        data.get('birth_month'),
                        data.get('birth_year'),
                        data.get('city'),
                        data.get('region'),
                        data.get('timezone', 'Europe/Moscow'),
                        data.get('location_manually_set', False),
                        user_id
                    ))
                else:
                    # Вставка
                    query = '''
                    INSERT INTO user_profiles (
                        user_id, username, first_name, last_name, middle_name,
                        gender, birth_day, birth_month, birth_year,
                        city, region, timezone, location_manually_set
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    
                    self.db._execute(query, (
                        user_id,
                        username,
                        data.get('first_name'),
                        data.get('last_name'),
                        data.get('middle_name'),
                        data.get('gender'),
                        data.get('birth_day'),
                        data.get('birth_month'),
                        data.get('birth_year'),
                        data.get('city'),
                        data.get('region'),
                        data.get('timezone', 'Europe/Moscow'),
                        data.get('location_manually_set', False)
                    ))
                
                self.db.conn.commit()
                return True
                
            except Exception as e:
                print(f"❌ Ошибка сохранения профиля: {e}")
                return False
    
    @retry_on_db_lock()
    def get_profile(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Получает профиль пользователя
        """
        with self.lock:
            try:
                self.db._execute(
                    "SELECT * FROM user_profiles WHERE user_id = ?",
                    (user_id,)
                )
                row = self.db.cursor.fetchone()
                return dict(row) if row else None
            except Exception as e:
                print(f"❌ Ошибка получения профиля: {e}")
                return None
    
    @retry_on_db_lock()
    def delete_profile(self, user_id: int) -> bool:
        """
        Удаляет профиль пользователя
        """
        with self.lock:
            try:
                self.db._execute(
                    "DELETE FROM user_profiles WHERE user_id = ?",
                    (user_id,)
                )
                self.db.conn.commit()
                return self.db.cursor.rowcount > 0
            except Exception as e:
                print(f"❌ Ошибка удаления профиля: {e}")
                return False
    
    @retry_on_db_lock()
    def get_all_profiles(self) -> List[Dict[str, Any]]:
        """
        Получает все профили (для админов)
        """
        with self.lock:
            try:
                self.db._execute(
                    "SELECT * FROM user_profiles ORDER BY updated_at DESC"
                )
                return [dict(row) for row in self.db.cursor.fetchall()]
            except Exception as e:
                print(f"❌ Ошибка получения всех профилей: {e}")
                return []
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Статистика по профилям
        """
        with self.lock:
            try:
                self.db._execute("SELECT COUNT(*) FROM user_profiles")
                total = self.db.cursor.fetchone()[0]
                
                self.db._execute(
                    "SELECT COUNT(*) FROM user_profiles WHERE city IS NOT NULL"
                )
                with_city = self.db.cursor.fetchone()[0]
                
                return {
                    'total_profiles': total,
                    'with_city': with_city,
                    'percent_with_city': round(with_city / total * 100, 1) if total else 0
                }
            except Exception as e:
                print(f"❌ Ошибка получения статистики профилей: {e}")
                return {'total_profiles': 0, 'with_city': 0, 'percent_with_city': 0}
