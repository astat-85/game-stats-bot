#!/usr/bin/env python3
"""
Telegram Bot для сбора игровых данных
АДАПТИРОВАНО ДЛЯ BOTHOST.RU
ПОЛНОСТЬЮ ИСПРАВЛЕННАЯ ВЕРСИЯ
"""

import sqlite3
import csv
import asyncio
import logging
import logging.handlers
import os
import sys
import threading
import shutil
import traceback
import json
import time
import re
import html
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List, Union
from functools import wraps
from threading import RLock
from collections import defaultdict

# ========== ПУТИ ==========
BASE_DIR = Path(__file__).parent
print(f"📁 Директория: {BASE_DIR}")

# ========== ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS_STR = os.getenv("ADMIN_IDS", "").strip()
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID", "").strip()
TARGET_TOPIC_ID = os.getenv("TARGET_TOPIC_ID", "").strip()
DB_NAME = os.getenv("DB_NAME", str(BASE_DIR / "users_data.db"))

# ========== ВАЛИДАЦИЯ ТОКЕНА ==========
if not BOT_TOKEN or not re.match(r'^\d+:[\w-]+$', BOT_TOKEN):
    print("=" * 60)
    print("❌ ОШИБКА: BOT_TOKEN не установлен или неверный формат!")
    print("\nДобавьте в переменные окружения на Bothost.ru:")
    print("BOT_TOKEN = ваш_токен_бота")
    print("=" * 60)
    sys.exit(1)

# ========== ПАРСИНГ ID ==========
ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(',') if x.strip().isdigit()]
try:
    TARGET_CHAT_ID = int(TARGET_CHAT_ID) if TARGET_CHAT_ID else None
except ValueError:
    print(f"❌ ОШИБКА: TARGET_CHAT_ID должен быть числом: '{TARGET_CHAT_ID}'")
    TARGET_CHAT_ID = None

USE_TOPIC = False
if TARGET_TOPIC_ID and TARGET_TOPIC_ID.strip() not in ("", "0", "None", "none", "null"):
    try:
        TARGET_TOPIC_ID = int(TARGET_TOPIC_ID)
        USE_TOPIC = True
        print(f"✅ Тема: {TARGET_TOPIC_ID}")
    except ValueError:
        print(f"⚠️ Неверный TARGET_TOPIC_ID: '{TARGET_TOPIC_ID}'")

# ========== ДИРЕКТОРИИ ==========
EXPORT_DIR = BASE_DIR / "exports"
BACKUP_DIR = BASE_DIR / "backups"
LOGS_DIR = BASE_DIR / "logs"

for dir_path in [EXPORT_DIR, BACKUP_DIR, LOGS_DIR]:
    dir_path.mkdir(exist_ok=True, parents=True)

# ========== ЛОГИРОВАНИЕ ==========
log_handler = logging.handlers.RotatingFileHandler(
    LOGS_DIR / 'bot.log',
    maxBytes=10*1024*1024,
    backupCount=5,
    encoding='utf-8'
)
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

logging.basicConfig(
    level=logging.INFO,
    handlers=[log_handler, logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ========== AIOGRAM ==========
try:
    from aiogram import Bot, Dispatcher, Router, F
    from aiogram.fsm.context import FSMContext
    from aiogram.fsm.state import State, StatesGroup
    from aiogram.fsm.storage.memory import MemoryStorage
    from aiogram.filters import Command
    from aiogram.types import (
        ReplyKeyboardMarkup,
        KeyboardButton,
        InlineKeyboardMarkup,
        InlineKeyboardButton,
        Message,
        CallbackQuery,
        FSInputFile,
        ChatMemberUpdated
    )
    from aiogram.exceptions import TelegramBadRequest
    from aiogram.types.error_event import ErrorEvent
    from aiogram.enums import ParseMode
    
# ========== НОВЫЕ ИМПОРТЫ ДЛЯ ПРОФИЛЯ ==========
from handlers import profile
from database.profile_db import ProfileDB
from cities.city_db import CityDatabase

    import aiogram
    if aiogram.__version__.startswith('3'):
        try:
            from aiogram.client.default import DefaultBotProperties
            bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        except ImportError:
            bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
    else:
        bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)

    print(f"✅ Aiogram {aiogram.__version__}")

except ImportError as e:
    print(f"❌ Ошибка импорта aiogram: {e}")
    sys.exit(1)

# ========== PSUTIL ==========
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# ========== НАСТРОЙКИ ==========
FIELDS = {
    "nick": "👤 Ник",
    "power": "⚡️ Эл/ст",
    "bm": "⚔️ БМ",
    "pl1": "📍 1пл",
    "pl2": "📍 2пл",
    "pl3": "📍 3пл",
    "dragon": "🐉 Дракон",
    "stands": "🏗️ БС",
    "research": "🔬 БИ"
}

FIELD_FULL_NAMES = {
    "nick": "Ник в игре",
    "power": "Электростанция",
    "bm": "БМ",
    "pl1": "1 плацдарм",
    "pl2": "2 плацдарм",
    "pl3": "3 плацдарм",
    "dragon": "Дракон",
    "stands": "Баф стройки",
    "research": "Баф исследования"
}

FIELD_DB_MAP = {
    "nick": "game_nickname",
    "power": "power",
    "bm": "bm",
    "pl1": "pl1",
    "pl2": "pl2",
    "pl3": "pl3",
    "dragon": "dragon",
    "stands": "buffs_stands",
    "research": "buffs_research"
}

VALID_DB_FIELDS = set(FIELD_DB_MAP.values()) | {"username"}

# Константы для ограничений
MAX_POWER_DRAGON = 99
MAX_BM_PL = 999.9
MAX_BUFF = 9
MAX_NICK_LENGTH = 50
MIN_NICK_LENGTH = 2
CACHE_TTL = 60
RATE_LIMIT_USER = 10
RATE_LIMIT_ADMIN = 30
RATE_LIMIT_WINDOW = 60
ACCOUNTS_PER_PAGE = 10
MAX_BATCH_DELETE = 20

# Глобальная переменная для отмены восстановления
cancel_restore = False

# ========== RATE LIMITER ==========
class RateLimiter:
    def __init__(self):
        self.requests = defaultdict(list)

    def is_limited(self, user_id: int, is_admin: bool = False) -> bool:
        now = datetime.now()
        limit = RATE_LIMIT_ADMIN if is_admin else RATE_LIMIT_USER
        window = timedelta(seconds=RATE_LIMIT_WINDOW)

        # Очищаем старые запросы
        self.requests[user_id] = [t for t in self.requests[user_id] if now - t < window]

        # Проверяем лимит
        if len(self.requests[user_id]) >= limit:
            return True

        # Добавляем запрос только если лимит не превышен
        self.requests[user_id].append(now)
        return False

rate_limiter = RateLimiter()

# ========== ДЕКОРАТОР RETRY ==========
def retry_on_db_lock(max_retries=3, delay=0.1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    if 'database is locked' in str(e) and attempt < max_retries - 1:
                        time.sleep(delay * (attempt + 1))
                        continue
                    raise
            return func(*args, **kwargs)
        return wrapper
    return decorator

# ========== БАЗА ДАННЫХ ==========
class Database:
    def __init__(self, db_name: str = DB_NAME):
        self.db_path = Path(db_name)
        self.lock = threading.RLock()
        self.cache_lock = threading.RLock()
        self.stats_cache = {}
        self.user_cache = {}
        self.cache_ttl = CACHE_TTL
        self.last_cache_update = 0
        self.change_counter = 0
        self.last_vacuum = datetime.now()

        self.conn = None
        self.cursor = None
        # Не подключаемся сразу, чтобы проверить наличие БД
        if self.db_path.exists():
            self._connect()
        else:
            print(f"📁 Файл БД не найден, будет создан при первом обращении")

    def _connect(self):
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False, timeout=10)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self._optimize()
        self._create_tables()

    def _optimize(self):
        try:
            self._execute("PRAGMA journal_mode=WAL")
            self._execute("PRAGMA synchronous=NORMAL")
            self._execute("PRAGMA cache_size=-2000")
            self._execute("PRAGMA foreign_keys=ON")
            self._execute("PRAGMA temp_store=MEMORY")
            self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка оптимизации БД: {e}")

    def _create_tables(self):
        self._execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            game_nickname TEXT NOT NULL,
            power TEXT DEFAULT '',
            bm TEXT DEFAULT '',
            pl1 TEXT DEFAULT '',
            pl2 TEXT DEFAULT '',
            pl3 TEXT DEFAULT '',
            dragon TEXT DEFAULT '',
            buffs_stands TEXT DEFAULT '',
            buffs_research TEXT DEFAULT '',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, game_nickname)
        )
        ''')

        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_user_id ON users(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_game_nickname ON users(game_nickname)",
            "CREATE INDEX IF NOT EXISTS idx_updated_at ON users(updated_at)"
        ]:
            try:
                self._execute(idx)
            except:
                pass

        self.conn.commit()

    def _execute(self, query: str, params: tuple = None):
        with self.lock:
            try:
                if params:
                    self.cursor.execute(query, params)
                else:
                    self.cursor.execute(query)
                return self.cursor
            except Exception as e:
                logger.error(f"SQL Error: {e}\nQuery: {query}")
                raise

    def _validate_field(self, field: str) -> bool:
        return field in VALID_DB_FIELDS

    def invalidate_cache(self):
        with self.cache_lock:
            self.stats_cache = {}
            self.user_cache.clear()
            self.last_cache_update = 0

    def get_user_accounts_cached(self, user_id: int) -> List[Dict]:
        if not self.conn:
            self._connect()
            
        cache_key = f"user_{user_id}"

        with self.cache_lock:
            if cache_key in self.user_cache:
                cache_time, cache_data = self.user_cache[cache_key]
                if time.time() - cache_time < self.cache_ttl:
                    return [dict(item) for item in cache_data] if cache_data else []

        data = self.get_user_accounts(user_id)

        with self.cache_lock:
            self.user_cache[cache_key] = (time.time(), [dict(item) for item in data] if data else [])

        return data

    @retry_on_db_lock()
    def get_user_accounts(self, user_id: int) -> List[Dict]:
        if not self.conn:
            self._connect()
            
        try:
            self._execute("""
            SELECT id, game_nickname, power, bm, pl1, pl2, pl3,
                   dragon, buffs_stands, buffs_research, updated_at
            FROM users
            WHERE user_id = ?
            ORDER BY updated_at DESC
            """, (user_id,))
            return [dict(row) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Ошибка get_user_accounts: {e}")
            return []

    @retry_on_db_lock()
    def get_account_by_id(self, account_id: int) -> Optional[Dict]:
        if not self.conn:
            self._connect()
            
        try:
            self._execute("SELECT * FROM users WHERE id = ?", (account_id,))
            row = self.cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка get_account_by_id: {e}")
            return None

    def is_nickname_taken(self, user_id: int, nickname: str, exclude_id: int = None) -> bool:
        if not self.conn:
            self._connect()
            
        try:
            nickname = nickname.strip().lower()
            query = "SELECT id FROM users WHERE user_id = ? AND LOWER(TRIM(game_nickname)) = ?"
            params = [user_id, nickname]

            if exclude_id:
                query += " AND id != ?"
                params.append(exclude_id)

            self._execute(query, params)
            return self.cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Ошибка is_nickname_taken: {e}")
            return False

    @retry_on_db_lock()
    def create_or_update_account(self, user_id: int, username: str,
                                  game_nickname: str, field_key: str = None,
                                  value: str = None) -> Optional[Dict]:
        if not self.conn:
            self._connect()
            
        try:
            self._execute(
                "SELECT id, game_nickname FROM users WHERE user_id = ? AND game_nickname = ?",
                (user_id, game_nickname)
            )
            existing = self.cursor.fetchone()

            if existing:
                account_id = existing['id']
                old_nick = existing['game_nickname']

                if field_key and value is not None:
                    db_field = FIELD_DB_MAP.get(field_key, field_key)
                    if not self._validate_field(db_field):
                        logger.error(f"Неверное поле: {db_field}")
                        return None

                    self._execute(f"""
                    UPDATE users
                    SET {db_field} = ?,
                        username = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """, (value, username, account_id))

                    if field_key == "nick" and value != old_nick:
                        self._execute("""
                        UPDATE users
                        SET game_nickname = ?
                        WHERE id = ?
                        """, (value, account_id))
            else:
                if field_key and value is not None:
                    db_field = FIELD_DB_MAP.get(field_key, field_key)
                    if not self._validate_field(db_field):
                        logger.error(f"Неверное поле: {db_field}")
                        return None

                    if field_key == "nick":
                        self._execute(f"""
                        INSERT INTO users (user_id, username, game_nickname, {db_field})
                        VALUES (?, ?, ?, ?)
                        """, (user_id, username, value, value))
                    else:
                        self._execute(f"""
                        INSERT INTO users (user_id, username, game_nickname, {db_field})
                        VALUES (?, ?, ?, ?)
                        """, (user_id, username, game_nickname, value))
                else:
                    self._execute("""
                    INSERT INTO users (user_id, username, game_nickname)
                    VALUES (?, ?, ?)
                    """, (user_id, username, game_nickname))

                account_id = self.cursor.lastrowid

            self.conn.commit()
            self.invalidate_cache()

            return self.get_account_by_id(account_id)
        except sqlite3.IntegrityError:
            return None
        except Exception as e:
            logger.error(f"Ошибка create_or_update_account: {e}")
            return None

    @retry_on_db_lock()
    def delete_account(self, account_id: int) -> bool:
        if not self.conn:
            self._connect()
            
        try:
            self._execute("DELETE FROM users WHERE id = ?", (account_id,))
            self.conn.commit()
            self.invalidate_cache()
            return self.cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка delete_account: {e}")
            return False

    @retry_on_db_lock()
    def get_all_accounts(self) -> List[Dict]:
        if not self.conn:
            self._connect()
            
        try:
            self._execute("""
            SELECT
                id, user_id, username,
                COALESCE(game_nickname, '') as game_nickname,
                COALESCE(power, '—') as power,
                COALESCE(bm, '—') as bm,
                COALESCE(pl1, '—') as pl1,
                COALESCE(pl2, '—') as pl2,
                COALESCE(pl3, '—') as pl3,
                COALESCE(dragon, '—') as dragon,
                COALESCE(buffs_stands, '—') as buffs_stands,
                COALESCE(buffs_research, '—') as buffs_research,
                created_at, updated_at
            FROM users
            ORDER BY updated_at DESC
            """)
            return [dict(row) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Ошибка get_all_accounts: {e}")
            return []

    def get_stats(self) -> Dict[str, Any]:
        if not self.conn:
            self._connect()
            
        now = time.time()

        with self.cache_lock:
            if self.stats_cache and now - self.last_cache_update < self.cache_ttl:
                return self.stats_cache.copy()

        try:
            self._execute("SELECT COUNT(DISTINCT user_id) FROM users")
            unique_users = self.cursor.fetchone()[0]

            self._execute("SELECT COUNT(*) FROM users")
            total_accounts = self.cursor.fetchone()[0]

            stats = {
                "unique_users": unique_users,
                "total_accounts": total_accounts,
                "avg_accounts_per_user": round(total_accounts / unique_users, 1) if unique_users > 0 else 0
            }

            with self.cache_lock:
                self.stats_cache = stats.copy()
                self.last_cache_update = now

            return stats
        except Exception as e:
            logger.error(f"Ошибка get_stats: {e}")
            return {"unique_users": 0, "total_accounts": 0, "avg_accounts_per_user": 0}

    def create_backup(self, filename: str = None) -> Optional[str]:
        """
        Создает полный бэкап базы данных со всеми данными
        Возвращает путь к файлу бэкапа или None в случае ошибки
        """
        if not self.conn:
            self._connect()
            
        try:
            # Генерируем имя файла если не указано
            if not filename:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"backup_{timestamp}.db"

            filepath = BACKUP_DIR / filename
            print(f"\n💾 СОЗДАНИЕ БЭКАПА: {filepath}")

            with self.lock:
                # 1. ПРИНУДИТЕЛЬНО СОХРАНЯЕМ ВСЕ НЕЗАВЕРШЕННЫЕ ТРАНЗАКЦИИ
                self.conn.commit()
                print("✅ Транзакции сохранены")

                # 2. ПРОВЕРЯЕМ ЦЕЛОСТНОСТЬ ТЕКУЩЕЙ БД
                self.cursor.execute("PRAGMA integrity_check")
                integrity_result = self.cursor.fetchone()[0]
                if integrity_result != "ok":
                    print(f"❌ Проблема с целостностью БД: {integrity_result}")
                    # Пытаемся восстановить
                    self.cursor.execute("REINDEX")
                    self.conn.commit()
                    print("🔄 Выполнен REINDEX")

                # 3. ПОЛУЧАЕМ ТОЧНОЕ КОЛИЧЕСТВО ЗАПИСЕЙ ДО БЭКАПА
                self.cursor.execute("SELECT COUNT(*) FROM users")
                original_count = self.cursor.fetchone()[0]
                print(f"📊 Записей в БД: {original_count}")

                # 4. СОЗДАЕМ БЭКАП ЧЕРЕЗ SQLite backup API (надежнее чем копирование)
                import sqlite3
                backup_conn = sqlite3.connect(str(filepath))
                self.conn.backup(backup_conn)
                backup_conn.close()
                print("✅ Бэкап создан через backup API")

                # 5. ПРОВЕРЯЕМ СОЗДАННЫЙ БЭКАП
                if filepath.exists():
                    backup_size = filepath.stat().st_size
                    print(f"📦 Размер бэкапа: {backup_size} bytes")

                    # Открываем бэкап и проверяем количество записей
                    check_conn = sqlite3.connect(str(filepath))
                    check_cursor = check_conn.cursor()
                    
                    # Проверяем структуру
                    check_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = check_cursor.fetchall()
                    print(f"📋 Таблицы в бэкапе: {[t[0] for t in tables]}")
                    
                    # Считаем записи
                    check_cursor.execute("SELECT COUNT(*) FROM users")
                    backup_count = check_cursor.fetchone()[0]
                    check_conn.close()
                    
                    print(f"📊 Записей в бэкапе: {backup_count}")
                    
                    # Сравниваем количество
                    if backup_count != original_count:
                        print(f"❌ НЕСООТВЕТСТВИЕ! Оригинал: {original_count}, Бэкап: {backup_count}")
                        # Пробуем еще раз с VACUUM
                        self.cursor.execute("VACUUM")
                        self.conn.commit()
                        
                        # Повторяем бэкап
                        backup_conn = sqlite3.connect(str(filepath))
                        self.conn.backup(backup_conn)
                        backup_conn.close()
                        
                        # Проверяем снова
                        check_conn = sqlite3.connect(str(filepath))
                        check_cursor = check_conn.cursor()
                        check_cursor.execute("SELECT COUNT(*) FROM users")
                        backup_count = check_cursor.fetchone()[0]
                        check_conn.close()
                        print(f"📊 После повторной попытки: {backup_count}")
                    else:
                        print(f"✅ Количество записей совпадает: {backup_count}")

                    # 6. СОЗДАЕМ ТЕКСТОВЫЙ ФАЙЛ С ИНФОРМАЦИЕЙ (для проверки)
                    info_path = filepath.with_suffix('.txt')
                    with open(info_path, 'w', encoding='utf-8') as f:
                        f.write(f"Бэкап создан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                        f.write(f"Оригинал БД: {self.db_path}\n")
                        f.write(f"Размер: {backup_size} bytes\n")
                        f.write(f"Записей в users: {backup_count}\n")
                        
                        # Добавляем список всех пользователей
                        check_conn = sqlite3.connect(str(self.db_path))
                        check_conn.row_factory = sqlite3.Row
                        check_cursor = check_conn.cursor()
                        check_cursor.execute("SELECT id, user_id, game_nickname FROM users ORDER BY id")
                        for row in check_cursor:
                            f.write(f"ID:{row['id']} | User:{row['user_id']} | Nick:{row['game_nickname']}\n")
                        check_conn.close()

                # 7. УДАЛЯЕМ СТАРЫЕ БЭКАПЫ (оставляем только 10 последних)
                backups = sorted(BACKUP_DIR.glob("backup_*.db"), key=os.path.getmtime, reverse=True)
                if len(backups) > 10:
                    for old in backups[10:]:
                        old.unlink()
                        # Удаляем и соответствующий txt файл
                        old_txt = old.with_suffix('.txt')
                        if old_txt.exists():
                            old_txt.unlink()
                    print(f"🧹 Оставлено 10 последних бэкапов")

            logger.info(f"✅ Бэкап успешно создан: {filepath} (записей: {original_count})")
            return str(filepath)

        except Exception as e:
            logger.error(f"❌ Критическая ошибка при создании бэкапа: {e}")
            traceback.print_exc()
            return None

    def export_to_csv(self, filename: str = None) -> Optional[str]:
        """Экспорт в CSV с округлением чисел до 0.1"""
        if not self.conn:
            self._connect()
            
        try:
            if not filename:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"export_{timestamp}.csv"

            filepath = EXPORT_DIR / filename
            accounts = self.get_all_accounts()

            if not accounts:
                return None

            # Подсчет аккаунтов для групп
            accounts_count = {}
            for acc in accounts:
                user_id = acc.get('user_id')
                if user_id:
                    accounts_count[user_id] = accounts_count.get(user_id, 0) + 1

            # Номера групп для мультиаккаунтов
            group_number = 1
            user_group = {}
            for user_id, count in accounts_count.items():
                if count > 1:
                    user_group[user_id] = group_number
                    group_number += 1

            # Функция для форматирования чисел (всегда с ,0)
            def format_number(val):
                if not val or val == '—':
                    return ''
                try:
                    # Заменяем запятую на точку для преобразования
                    val_float = float(val.replace(',', '.'))
                    # Округляем до 1 знака после запятой
                    rounded = round(val_float * 10) / 10
                    # Всегда показываем с одним знаком после запятой
                    return f"{rounded:.1f}".replace('.', ',')
                except:
                    return val

            with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow([
                    "№", "Группа", "Ник в игре", "Эл", "БМ", "Пл 1", "Пл 2", "Пл 3",
                    "Др", "БС", "БИ", "ID имя", "ID номер", "Время", "Дата"
                ])

                for i, acc in enumerate(accounts, 1):
                    updated = acc.get('updated_at', '')
                    time_str = '--:--:--'
                    date_str = '--.--.----'

                    if updated:
                        try:
                            dt = datetime.strptime(updated, '%Y-%m-%d %H:%M:%S')
                            time_str = dt.strftime('%H:%M:%S')
                            date_str = dt.strftime('%d.%m.%Y')
                        except:
                            pass

                    # ===== ФОРМАТИРОВАНИЕ ЧИСЕЛ =====
                    # Дробные поля с форматированием (всегда с ,0)
                    bm = format_number(acc.get('bm', ''))
                    pl1 = format_number(acc.get('pl1', ''))
                    pl2 = format_number(acc.get('pl2', ''))
                    pl3 = format_number(acc.get('pl3', ''))
                    
                    # Целочисленные поля - убираем дробную часть
                    power = acc.get('power', '')
                    if power and power != '—' and ',' in power:
                        power = power.split(',')[0]
                        
                    dragon = acc.get('dragon', '')
                    if dragon and dragon != '—' and ',' in dragon:
                        dragon = dragon.split(',')[0]
                        
                    buffs_stands = acc.get('buffs_stands', '')
                    if buffs_stands and buffs_stands != '—' and ',' in buffs_stands:
                        buffs_stands = buffs_stands.split(',')[0]
                        
                    buffs_research = acc.get('buffs_research', '')
                    if buffs_research and buffs_research != '—' and ',' in buffs_research:
                        buffs_research = buffs_research.split(',')[0]

                    # ID имя - оставляем как есть (с @)
                    username = f"@{acc.get('username', '')}" if acc.get('username') else ''
                    
                    user_id = acc.get('user_id')
                    group = user_group.get(user_id, '')

                    writer.writerow([
                        i,                          # №
                        group,                       # Группа
                        acc.get('game_nickname', ''),# Ник в игре
                        power,                       # Эл
                        bm,                          # БМ (всегда с ,0)
                        pl1,                         # Пл 1 (всегда с ,0)
                        pl2,                         # Пл 2 (всегда с ,0)
                        pl3,                         # Пл 3 (всегда с ,0)
                        dragon,                      # Др
                        buffs_stands,                 # БС
                        buffs_research,               # БИ
                        username,                     # ID имя
                        user_id,                      # ID номер
                        time_str,                     # Время
                        date_str                      # Дата
                    ])

            logger.info(f"✅ Экспорт CSV: {filepath}")
            return str(filepath)
        except Exception as e:
            logger.error(f"❌ Ошибка экспорта CSV: {e}")
            return None

    def export_to_excel(self, filename: str = None) -> Optional[str]:
        """Экспорт в Excel с округлением чисел до 0.1 и правильным выравниванием"""
        if not self.conn:
            self._connect()
        
        # Проверяем наличие openpyxl
        try:
            import openpyxl
            from openpyxl.utils import get_column_letter
            from openpyxl.styles import Font, PatternFill, Alignment
        except ImportError:
            logger.error("❌ openpyxl не установлен. Установите: pip install openpyxl")
            return None
            
        try:
            if not filename:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"export_{timestamp}.xlsx"

            filepath = EXPORT_DIR / filename
            accounts = self.get_all_accounts()

            if not accounts:
                return None

            # Подсчет аккаунтов для групп
            accounts_count = {}
            for acc in accounts:
                user_id = acc.get('user_id')
                if user_id:
                    accounts_count[user_id] = accounts_count.get(user_id, 0) + 1

            # Номера групп для мультиаккаунтов
            group_number = 1
            user_group = {}
            for user_id, count in accounts_count.items():
                if count > 1:
                    user_group[user_id] = group_number
                    group_number += 1

            # Создаем Excel файл
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = "Игроки"

            # Заголовки
            headers = [
                "№", "Группа", "Ник в игре", "Эл", "БМ", "Пл 1", "Пл 2", "Пл 3",
                "Др", "БС", "БИ", "ID имя", "ID номер", "Время", "Дата"
            ]
            
            # Стиль для заголовков
            header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
            header_font_white = Font(bold=True, color="FFFFFF")
            
            for col, header in enumerate(headers, 1):
                cell = ws.cell(row=1, column=col, value=header)
                cell.font = header_font_white
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal='center')

            # Функция для форматирования чисел
            def format_number(val):
                if not val or val == '—':
                    return ''
                try:
                    # Заменяем запятую на точку для преобразования
                    val_float = float(val.replace(',', '.'))
                    # Округляем до 1 знака после запятой
                    rounded = round(val_float * 10) / 10
                    return rounded
                except:
                    return val

            # Данные
            for i, acc in enumerate(accounts, 1):
                updated = acc.get('updated_at', '')
                time_str = '--:--:--'
                date_str = '--.--.----'

                if updated:
                    try:
                        dt = datetime.strptime(updated, '%Y-%m-%d %H:%M:%S')
                        time_str = dt.strftime('%H:%M:%S')
                        date_str = dt.strftime('%d.%m.%Y')
                    except:
                        pass

                # ===== ФОРМАТИРОВАНИЕ ЧИСЕЛ =====
                # Дробные поля с форматированием
                bm = format_number(acc.get('bm', ''))
                pl1 = format_number(acc.get('pl1', ''))
                pl2 = format_number(acc.get('pl2', ''))
                pl3 = format_number(acc.get('pl3', ''))
                
                # Целочисленные поля - преобразуем в int
                power = acc.get('power', '')
                if power and power != '—':
                    try:
                        power = int(float(power.replace(',', '.')))
                    except:
                        pass
                        
                dragon = acc.get('dragon', '')
                if dragon and dragon != '—':
                    try:
                        dragon = int(float(dragon.replace(',', '.')))
                    except:
                        pass
                        
                buffs_stands = acc.get('buffs_stands', '')
                if buffs_stands and buffs_stands != '—':
                    try:
                        buffs_stands = int(float(buffs_stands.replace(',', '.')))
                    except:
                        pass
                        
                buffs_research = acc.get('buffs_research', '')
                if buffs_research and buffs_research != '—':
                    try:
                        buffs_research = int(float(buffs_research.replace(',', '.')))
                    except:
                        pass

                user_id = acc.get('user_id')
                group = user_group.get(user_id, '')

                row_data = [
                    i,                          # №
                    group,                       # Группа
                    acc.get('game_nickname', ''),# Ник в игре
                    power,                       # Эл
                    bm,                          # БМ
                    pl1,                         # Пл 1
                    pl2,                         # Пл 2
                    pl3,                         # Пл 3
                    dragon,                      # Др
                    buffs_stands,                 # БС
                    buffs_research,               # БИ
                    f"@{acc.get('username', '')}" if acc.get('username') else '',  # ID имя
                    acc.get('user_id', ''),       # ID номер
                    time_str,                     # Время
                    date_str                      # Дата
                ]
                
                # Записываем данные и применяем выравнивание
                for col, value in enumerate(row_data, 1):
                    cell = ws.cell(row=i+1, column=col, value=value)
                    
                    # Применяем выравнивание в зависимости от столбца
                    if col == 12:  # ID имя - по левому краю
                        cell.alignment = Alignment(horizontal='left')
                    elif col in [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 13]:  # Числовые столбцы - по правому краю
                        cell.alignment = Alignment(horizontal='right')
                        # Для дробных чисел (БМ, Пл1-3) устанавливаем формат с одним знаком после запятой
                        if col in [5, 6, 7, 8]:  # БМ, Пл1, Пл2, Пл3
                            cell.number_format = '#,##0.0'
                    else:  # Остальные (ник, время, дата) - по центру
                        cell.alignment = Alignment(horizontal='center')

            # АВТОМАТИЧЕСКАЯ ШИРИНА СТОЛБЦОВ С ОТСТУПАМИ
            for col in range(1, len(headers) + 1):
                column_letter = get_column_letter(col)
                max_length = 0
                for row in range(1, len(accounts) + 2):
                    cell_value = ws.cell(row=row, column=col).value
                    if cell_value:
                        max_length = max(max_length, len(str(cell_value)))
                
                width = max(max_length + 3, 8)
                ws.column_dimensions[column_letter].width = width

            wb.save(filepath)
            logger.info(f"✅ Экспорт Excel: {filepath}")
            return str(filepath)

        except Exception as e:
            logger.error(f"❌ Ошибка экспорта в Excel: {e}")
            traceback.print_exc()
            return None
            
    def restore_from_backup(self, backup_path: Path) -> bool:
        try:
            if not backup_path.exists() or backup_path.stat().st_size == 0:
                return False

            self.close()
            shutil.copy2(backup_path, self.db_path)
            self._connect()
            self._create_tables()

            if self.check_integrity():
                logger.info(f"✅ БД восстановлена из {backup_path}")
                return True

            return False
        except Exception as e:
            logger.error(f"❌ Ошибка восстановления: {e}")
            return False

    def check_integrity(self) -> bool:
        if not self.conn:
            self._connect()
            
        try:
            self._execute("PRAGMA integrity_check")
            return self.cursor.fetchone()[0] == "ok"
        except:
            return False

    def maybe_vacuum(self):
        if not self.conn:
            self._connect()
            
        if (datetime.now() - self.last_vacuum).days >= 7:
            try:
                self._execute("VACUUM")
                self.conn.commit()
                self.last_vacuum = datetime.now()
                logger.info("✅ VACUUM выполнен")
            except Exception as e:
                logger.error(f"❌ Ошибка VACUUM: {e}")

    def cleanup_old_files(self, days: int = 14):
        try:
            cutoff = datetime.now().timestamp() - (days * 24 * 3600)

            for pattern in ["export_*.csv", "backup_*.db"]:
                for f in EXPORT_DIR.glob(pattern) if 'export' in pattern else BACKUP_DIR.glob(pattern):
                    try:
                        if f.exists() and f.stat().st_mtime < cutoff:
                            f.unlink()
                    except:
                        pass
        except Exception as e:
            logger.error(f"Ошибка очистки: {e}")

    def close(self):
        try:
            with self.lock:
                if self.conn:
                    self.conn.commit()
                    self.conn.close()
                    self.conn = None
        except:
            pass

db = Database()

# ========== ИНИЦИАЛИЗАЦИЯ МОДУЛЕЙ ПРОФИЛЯ ==========
profile_db = ProfileDB(db)
city_db = CityDatabase()

# ========== FSM ==========
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# ========== ПОДКЛЮЧЕНИЕ РОУТЕРА ПРОФИЛЯ ==========
dp.include_router(profile.router)

class EditState(StatesGroup):
    waiting_field_value = State()
    step_by_step = State()
    waiting_search_query = State()
    waiting_batch_delete = State()
    waiting_for_backup = State()
    batch_selection = State()  # 🔴 НОВОЕ состояние для пакетного удаления с чекбоксами

# ========== КЛАВИАТУРЫ ==========
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def get_main_kb(user_id: int) -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="📊 Мои аккаунты"), KeyboardButton(text="📤 Отправить в группу")],
        [KeyboardButton(text="👤 Мой профиль")]
    ]
    if is_admin(user_id):
        kb.append([KeyboardButton(text="👑 Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_numeric_kb(decimal: bool = True) -> ReplyKeyboardMarkup:
    if decimal:
        kb = [
            [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
            [KeyboardButton(text="4"), KeyboardButton(text="5"), KeyboardButton(text="6")],
            [KeyboardButton(text="7"), KeyboardButton(text="8"), KeyboardButton(text="9")],
            [KeyboardButton(text="0"), KeyboardButton(text=","), KeyboardButton(text="⌫")],
            [KeyboardButton(text="🏁 Завершить"), KeyboardButton(text="⏭ Пропустить"), KeyboardButton(text="✅ Готово")]
        ]
    else:
        kb = [
            [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
            [KeyboardButton(text="4"), KeyboardButton(text="5"), KeyboardButton(text="6")],
            [KeyboardButton(text="7"), KeyboardButton(text="8"), KeyboardButton(text="9")],
            [KeyboardButton(text="0"), KeyboardButton(text="⌫"), KeyboardButton(text="⌫")],
            [KeyboardButton(text="🏁 Завершить"), KeyboardButton(text="⏭ Пропустить"), KeyboardButton(text="✅ Готово")]
        ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_cancel_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🚫 Отмена")]],
        resize_keyboard=True
    )

def get_accounts_kb(accounts: List[Dict]) -> InlineKeyboardMarkup:
    buttons = []
    for acc in accounts[:10]:
        nick = acc.get('game_nickname') or f"ID:{acc.get('id', '?')}"
        acc_id = acc.get('id')
        if acc_id:
            buttons.append([InlineKeyboardButton(
                text=f"👤 {nick[:20]}",
                callback_data=f"select_{acc_id}"
            )])
    buttons.append([InlineKeyboardButton(text="➕ Новый аккаунт", callback_data="new_account")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_account_actions_kb(account_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить ник", callback_data=f"edit_nick_{account_id}")],
        [InlineKeyboardButton(text="📝 Редактировать", callback_data=f"edit_{account_id}")],
        [InlineKeyboardButton(text="🔄 Пошагово", callback_data=f"step_{account_id}")],
        [InlineKeyboardButton(text="📤 Отправить", callback_data=f"send_{account_id}")],
        [InlineKeyboardButton(text="🗑️ Удалить", callback_data=f"delete_{account_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="my_accounts")]
    ])

def get_edit_fields_kb(account_id: int) -> InlineKeyboardMarkup:
    buttons = []
    for key, name in FIELD_FULL_NAMES.items():
        if key != "nick":
            buttons.append([InlineKeyboardButton(
                text=name,
                callback_data=f"field_{account_id}_{key}"
            )])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"select_{account_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_send_kb(accounts: List[Dict]) -> InlineKeyboardMarkup:
    buttons = []
    for acc in accounts[:10]:
        nick = acc.get('game_nickname') or f"ID:{acc.get('id', '?')}"
        acc_id = acc.get('id')
        if acc_id:
            buttons.append([InlineKeyboardButton(
                text=f"📤 {nick[:20]}",
                callback_data=f"send_{acc_id}"
            )])
    buttons.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Таблица", callback_data="admin_table_1")],
        [InlineKeyboardButton(text="📤 Экспорт CSV", callback_data="admin_export")],
        [InlineKeyboardButton(text="📊 Экспорт Excel", callback_data="admin_export_excel")],
        [InlineKeyboardButton(text="🗄️ Управление БД", callback_data="db_management")],
        [InlineKeyboardButton(text="🔍 Поиск", callback_data="admin_search")],
        [InlineKeyboardButton(text="🗑️ Пакетное удаление", callback_data="admin_batch")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_refresh")],
        [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]
    ])

def get_db_management_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💾 Сохранить бэкап", callback_data="db_backup")],
        [InlineKeyboardButton(text="📥 Восстановить из бэкапа", callback_data="db_restore_menu")],
        [InlineKeyboardButton(text="📤 Загрузить с ПК", callback_data="db_restore_pc")],
        [InlineKeyboardButton(text="🧹 Очистка (14 дней)", callback_data="admin_cleanup")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
    ])

def get_confirm_delete_kb(account_id: int, page: int = 1) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да", callback_data=f"confirm_del_{account_id}_{page}"),
            InlineKeyboardButton(text="❌ Нет", callback_data=f"admin_table_{page}")
        ]
    ])

# ========== ФОРМАТТЕРЫ ==========
def format_power(value: str) -> str:
    """Форматирование электростанции (макс 99)"""
    if not value or value == '—':
        return ' —'
    try:
        val = value.replace(',', '').strip()
        if not val.isdigit():
            return ' —'
        num = min(int(val), MAX_POWER_DRAGON)
        return f"{num:2d}"
    except:
        return ' —'

def format_bm(value: str) -> str:
    """Форматирование БМ (макс 999.9)"""
    if not value or value == '—':
        return '   —'
    try:
        val = value.replace(',', '.')
        num = float(val)
        num = min(num, MAX_BM_PL)
        num = round(num, 1)
        return f"{num:5.1f}".replace('.', ',')
    except:
        return '   —'

def format_pl(value: str) -> str:
    """Форматирование плацдарма (макс 999.9)"""
    if not value or value == '—':
        return '   —'
    try:
        val = value.replace(',', '.')
        num = float(val)
        num = min(num, MAX_BM_PL)
        num = round(num, 1)
        return f"{num:5.1f}".replace('.', ',')
    except:
        return '   —'

def format_dragon(value: str) -> str:
    """Форматирование дракона (макс 99)"""
    if not value or value == '—':
        return ' —'
    try:
        val = value.replace(',', '').strip()
        if not val.isdigit():
            return ' —'
        num = min(int(val), MAX_POWER_DRAGON)
        return f"{num:2d}"
    except:
        return ' —'

def format_buff(value: str) -> str:
    """Форматирование баффов (макс 9)"""
    if not value or value == '—':
        return '—'
    try:
        val = value.replace(',', '').strip()
        if not val.isdigit():
            return '—'
        num = min(int(val), MAX_BUFF)
        return str(num)
    except:
        return '—'

def format_accounts_table(accounts: List[Dict], start: int = 0) -> str:
    text = "<code>\n"
    for i, acc in enumerate(accounts, start + 1):
        nick = acc.get('game_nickname', '—')
        if not isinstance(nick, str):
            nick = str(nick) if nick is not None else '—'
        nick = html.escape(nick)
        if len(nick) > 20:
            nick = nick[:17] + '...'

        text += f"{i:2d}. {nick}\n"
        text += f"  ⚡️{format_power(acc.get('power', '—'))} "
        text += f"⚔️{format_bm(acc.get('bm', '—'))} "
        text += f"📍1-{format_pl(acc.get('pl1', '—'))} "
        text += f"📍2-{format_pl(acc.get('pl2', '—'))} "
        text += f"📍3-{format_pl(acc.get('pl3', '—'))} "
        text += f"🐉{format_dragon(acc.get('dragon', '—'))} "
        text += f"🏗️{format_buff(acc.get('buffs_stands', '—'))} "
        text += f"🔬{format_buff(acc.get('buffs_research', '—'))}\n\n"
    text += "</code>"
    return text

def format_account_data(acc: Dict) -> str:
    if not acc:
        return "❌ Аккаунт не найден"
    nick = acc.get('game_nickname', 'Без имени')
    text = f"<b>📋 Аккаунт: {html.escape(nick)}</b>\n\n"
    for key, name in FIELD_FULL_NAMES.items():
        db_field = FIELD_DB_MAP.get(key, key)
        val = acc.get(db_field, '')
        text += f"<b>{name}:</b> {html.escape(str(val)) if val else '—'}\n"
    text += f"\n⏱ <b>Обновлено:</b> {acc.get('updated_at', '—')}"
    return text

# ========== ФУНКЦИИ ВАЛИДАЦИИ ==========
def validate_numeric_input(field: str, value: str) -> tuple[bool, str, str]:
    """
    Проверяет введенное число на соответствие формату и максимуму
    Возвращает: (успех, сообщение_об_ошибке, исправленное_значение)
    """
    try:
        if field in ["bm", "pl1", "pl2", "pl3"]:
            parts = value.split(',')
            if len(parts) > 2:
                return False, "❌ Неверный формат. Используйте: 12,5 или 15", value
            
            if not parts[0].isdigit() or (len(parts) == 2 and not parts[1].isdigit()):
                return False, "❌ Введите корректное число", value
            
            num = float(value.replace(',', '.'))
            if num > MAX_BM_PL:
                return False, f"❌ Максимальное значение: {MAX_BM_PL}", value
            
        elif field in ["power", "dragon"]:
            cleaned = value.replace(',', '')
            if not cleaned.isdigit():
                return False, "❌ Введите целое число", value
            
            num = int(cleaned)
            if num > MAX_POWER_DRAGON:
                return False, f"❌ Максимальное значение: {MAX_POWER_DRAGON}", value
            value = cleaned
            
        elif field in ["stands", "research"]:
            cleaned = value.replace(',', '')
            if not cleaned.isdigit():
                return False, "❌ Введите целое число (0-9)", value
            
            num = int(cleaned)
            if num > MAX_BUFF:
                return False, f"❌ Максимальное значение: {MAX_BUFF}", value
            value = cleaned
            
        return True, "", value
        
    except ValueError:
        return False, "❌ Введите корректное число", value

# ========== SAFE SEND ==========
async def safe_send(obj, text: str, **kwargs):
    MAX_LEN = 4096

    try:
        # Проверяем, что объект и сообщение существуют
        if isinstance(obj, CallbackQuery):
            if not obj.message:
                # Если сообщение удалено, отправляем новое
                if isinstance(obj, CallbackQuery):
                    await obj.answer()
                return
            message = obj.message
        else:
            message = obj

        if len(text) <= MAX_LEN:
            if isinstance(obj, CallbackQuery):
                try:
                    await obj.message.edit_text(text, **kwargs)
                except (TelegramBadRequest, AttributeError):
                    await obj.message.answer(text, **kwargs)
            else:
                await message.answer(text, **kwargs)
        else:
            parts = []
            current = ""
            for line in text.split('\n'):
                if len(current) + len(line) + 1 < MAX_LEN:
                    current += line + '\n'
                else:
                    if current:
                        parts.append(current)
                    current = line + '\n'
            if current:
                parts.append(current)

            for i, part in enumerate(parts):
                if i == 0 and isinstance(obj, CallbackQuery):
                    try:
                        await obj.message.edit_text(part, **kwargs)
                    except (TelegramBadRequest, AttributeError):
                        await obj.message.answer(part, **kwargs)
                else:
                    if isinstance(obj, Message):
                        await obj.answer(part, **kwargs)
                    else:
                        await obj.message.answer(part, **kwargs)
    except Exception as e:
        logger.error(f"Safe send error: {e}")

# ========== ФУНКЦИЯ ПРОВЕРКИ ПОДПИСКИ ==========
async def check_subscription(user_id: int) -> bool:
    global _check_subscription_func
    _check_subscription_func = check_subscription
    """
    Проверяет, подписан ли пользователь на целевую группу
    Возвращает True если подписан, False если нет
    """
    if not TARGET_CHAT_ID:
        # Если группа не настроена - разрешаем доступ
        print("⚠️ TARGET_CHAT_ID не настроен, проверка подписки отключена")
        return True
        
    try:
        # Получаем информацию о пользователе в чате
        member = await bot.get_chat_member(chat_id=TARGET_CHAT_ID, user_id=user_id)
        
        # Статусы, которые считаются "подпиской"
        if member.status in ['creator', 'administrator', 'member']:
            print(f"✅ Пользователь {user_id} подписан на группу")
            return True
        else:
            print(f"❌ Пользователь {user_id} НЕ подписан на группу")
            return False
            
    except Exception as e:
        print(f"⚠️ Ошибка проверки подписки: {e}")
        # В случае ошибки лучше запретить доступ
        return False

# ========== КОМАНДЫ ==========
@router.message(Command("start"))
async def start_cmd(message: Message):
    user_id = message.from_user.id

    if rate_limiter.is_limited(user_id, is_admin(user_id)):
        await message.answer("⏳ Слишком много запросов")
        return

    accounts = db.get_user_accounts_cached(user_id)

    if not accounts:
        text = """🎮 <b>Бот для сбора игровых данных</b>

👋 Добро пожаловать!

У вас нет аккаунтов. Чтобы начать:
1️⃣ Нажмите "📊 Мои аккаунты"
2️⃣ Создайте аккаунт
3️⃣ Введите игровой ник"""
    else:
        text = f"""🎮 <b>С возвращением!</b>

📊 Ваши аккаунты:"""
        for acc in accounts[:3]:
            text += f"\n👤 {acc['game_nickname']}"
        if len(accounts) > 3:
            text += f"\n...и еще {len(accounts) - 3}"

    await message.answer(text, reply_markup=get_main_kb(user_id))

@router.message(Command("help"))
async def help_cmd(message: Message):
    text = """📖 <b>Помощь</b>

<b>Команды:</b>
/start - Запуск
/help - Помощь
/cancel - Отмена
/myid - Мой ID
/admin - Админка
/restore - Восстановить БД из файла

<b>Кнопки:</b>
📊 Мои аккаунты - управление
📤 Отправить в группу - поделиться"""
    await message.answer(text)

@router.message(Command("cancel"))
async def cancel_cmd(message: Message, state: FSMContext):
    """Отмена текущего действия"""
    user_id = message.from_user.id
    
    global cancel_restore
    if is_admin(user_id):
        # Если админ отменяет восстановление при запуске
        cancel_restore = True
        await message.answer("❌ Восстановление отменено. Будет создана новая пустая БД.")
    
    await state.clear()
    await message.answer("❌ Отменено", reply_markup=get_main_kb(user_id))

@router.message(Command("myid"))
async def myid_cmd(message: Message):
    await message.answer(
        f"🆔 <b>Ваш ID:</b> <code>{message.from_user.id}</code>\n"
        f"👤 @{message.from_user.username or '—'}"
    )

@router.message(Command("admin"))
async def admin_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("🚫 Только для админов")
        return

    stats = db.get_stats()
    text = f"""👑 <b>Админ-панель</b>

👥 Пользователей: {stats['unique_users']}
🎮 Аккаунтов: {stats['total_accounts']}"""
    await message.answer(text, reply_markup=get_admin_kb())

@router.message(Command("restore"))
async def restore_command(message: Message, state: FSMContext):
    """Команда для восстановления из бэкапа"""
    if not is_admin(message.from_user.id):
        await message.answer("🚫 Только для админов")
        return
    
    await message.answer(
        "📤 Отправьте файл бэкапа (.db)\n\n"
        "1️⃣ Нажмите на скрепку 📎\n"
        "2️⃣ Выберите 'Документ'\n"
        "3️⃣ Найдите файл .db на вашем устройстве\n"
        "4️⃣ Отправьте его"
    )
    await state.set_state(EditState.waiting_for_backup)
    await state.update_data(restore_mode="command")

# ========== ОСНОВНЫЕ КНОПКИ ==========
@router.message(F.text == "📊 Мои аккаунты")
async def my_accounts(message: Message):
    user_id = message.from_user.id
    accounts = db.get_user_accounts(user_id)

    if not accounts:
        await message.answer(
            "📋 У вас нет аккаунтов",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Создать", callback_data="new_account")],
                [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]
            ])
        )
        return

    text = "<b>📋 Ваши аккаунты:</b>\n\n" + format_accounts_table(accounts)
    await safe_send(message, text, reply_markup=get_accounts_kb(accounts))
    
@router.message(F.text == "👤 Мой профиль")
async def my_profile_button(message: Message, state: FSMContext):
    """Обработка кнопки Мой профиль"""
    from handlers.profile import cmd_profile
    await cmd_profile(message)
    
@router.message(F.text == "📤 Отправить в группу")
async def send_menu(message: Message):
    if not TARGET_CHAT_ID:
        await message.answer("❌ Отправка не настроена")
        return

    accounts = db.get_user_accounts_cached(message.from_user.id)

    if not accounts:
        await message.answer("❌ Сначала создайте аккаунт")
        return

    await message.answer(
        "📤 Выберите аккаунт:",
        reply_markup=get_send_kb(accounts)
    )

@router.message(F.text == "👑 Админ-панель")
async def admin_panel_msg(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("🚫 Доступ запрещен")
        return

    stats = db.get_stats()
    text = f"""👑 <b>Админ-панель</b>

👥 Пользователей: {stats['unique_users']}
🎮 Аккаунтов: {stats['total_accounts']}"""
    await message.answer(text, reply_markup=get_admin_kb())

# ========== ПОШАГОВОЕ ЗАПОЛНЕНИЕ ==========
@router.callback_query(F.data.startswith("step_"))
async def step_start(callback: CallbackQuery, state: FSMContext):
    account_id = int(callback.data.split("_")[1])
    account = db.get_account_by_id(account_id)

    if not account:
        await callback.answer("❌ Аккаунт не найден", show_alert=True)
        return

    steps = [k for k in FIELD_FULL_NAMES if k != "nick"]

    keyboard_guide = """
<b>📱 ИНСТРУКЦИЯ ПО ИСПОЛЬЗОВАНИЮ КЛАВИАТУРЫ:</b>

• <b>Цифры (0-9)</b> - нажимайте для ввода чисел
• <b>«,» (запятая)</b> - для дробных чисел (например: 12,5)
• <b>«⌫»</b> - удалить последний символ
• <b>«✅ Готово»</b> - завершить ввод текущего числа
• <b>«⏭ Пропустить»</b> - оставить поле без изменений
• <b>«🏁 Завершить»</b> - досрочно завершить заполнение

<i>Вы также можете вводить значения вручную с обычной клавиатуры.</i>
"""

    await callback.message.edit_text(
        f"🔄 <b>ПОШАГОВОЕ ЗАПОЛНЕНИЕ АККАУНТА</b>\n\n"
        f"👤 Аккаунт: <b>{account['game_nickname']}</b>\n"
        f"📊 Всего полей для заполнения: <b>{len(steps)}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{keyboard_guide}"
    )

    await asyncio.sleep(3)

    await state.update_data(
        step_account=account_id,
        step_index=0,
        step_steps=steps,
        step_data={},
        step_temp=""
    )

    await step_next(callback.message, state)
    await callback.answer()

async def step_next(msg_or_cb, state: FSMContext):
    data = await state.get_data()
    account_id = data.get("step_account")
    idx = data.get("step_index", 0)
    steps = data.get("step_steps", [])

    if idx >= len(steps):
        await step_finish(msg_or_cb, state)
        return

    field = steps[idx]
    account = db.get_account_by_id(account_id)

    if not account:
        await state.clear()
        return

    name = FIELD_FULL_NAMES.get(field, field)
    current = account.get(FIELD_DB_MAP.get(field, field), '')

    hint = ""
    if field in ["bm", "pl1", "pl2", "pl3"]:
        hint = "💡 Можно вводить дробные числа через запятую (например: 12,5)"
    elif field in ["power", "dragon"]:
        hint = "💡 Вводите только целые числа (например: 50)"
    elif field in ["stands", "research"]:
        hint = "💡 Вводите число от 0 до 9"

    text = f"🔄 <b>ШАГ {idx + 1} ИЗ {len(steps)}</b>\n"
    text += f"━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"👤 Аккаунт: <b>{account['game_nickname']}</b>\n"
    text += f"📌 Поле: <b>{name}</b>\n"
    text += f"💾 Текущее значение: <b>{current or '—'}</b>\n"
    text += f"━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"{hint}\n\n" if hint else "\n"
    text += f"✏️ Введите новое значение:"

    if isinstance(msg_or_cb, Message):
        await msg_or_cb.answer(text)
    else:
        await msg_or_cb.message.edit_text(text)

    if field in ["bm", "pl1", "pl2", "pl3"]:
        kb = get_numeric_kb(decimal=True)
        prompt = f"📝 Введите число для поля «{name}» (можно с запятой):"
    elif field in ["power", "dragon", "stands", "research"]:
        kb = get_numeric_kb(decimal=False)
        prompt = f"📝 Введите целое число для поля «{name}» (0-{MAX_BUFF if field in ['stands','research'] else MAX_POWER_DRAGON}):"
    else:
        kb = get_cancel_kb()
        prompt = f"📝 Введите значение для поля «{name}»:"

    await msg_or_cb.answer(prompt, reply_markup=kb)
    await state.set_state(EditState.step_by_step)
    await state.update_data(step_field=field, step_temp="")

@router.message(EditState.step_by_step)
async def step_input(message: Message, state: FSMContext):
    data = await state.get_data()
    field = data.get("step_field")
    account_id = data.get("step_account")
    step_data = data.get("step_data", {})
    step_temp = data.get("step_temp", "")
    
    field_name = FIELD_FULL_NAMES.get(field, field)

    # ===== ОБРАБОТКА УПРАВЛЯЮЩИХ КНОПОК =====
    if message.text == "🚫 Отмена":
        await message.answer("❌ Действие отменено", reply_markup=get_main_kb(message.from_user.id))
        await state.clear()
        return

    if message.text == "🏁 Завершить":
        await step_finish(message, state, early=True)
        return

    if message.text == "⏭ Пропустить":
        await message.answer(f"⏭ Поле «{field_name}» пропущено")
        await state.update_data(step_index=data.get("step_index", 0) + 1, step_temp="")
        await step_next(message, state)
        return

    # Новая логика: определяем, использует ли пользователь кнопки
    if step_temp is not None and step_temp != "":
        # ===== РЕЖИМ НАБОРА ЧЕРЕЗ КНОПКИ =====
        if message.text in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ","]:
            if message.text == ",":
                if field in ["bm", "pl1", "pl2", "pl3"]:
                    if "," not in step_temp:
                        step_temp += ","
                else:
                    await message.answer(f"📝 Введите целое число без запятой")
                    return
            else:
                step_temp += message.text
                
            await state.update_data(step_temp=step_temp)
            await message.answer(f"📝 Текущее значение: {step_temp}")
            return

        if message.text == "⌫":
            step_temp = step_temp[:-1] if step_temp else ""
            await state.update_data(step_temp=step_temp)
            if step_temp:
                await message.answer(f"📝 Текущее значение: {step_temp}")
            else:
                await message.answer(f"📝 Значение очищено")
            return

        if message.text == "✅ Готово":
            if step_temp:
                value = step_temp
                await state.update_data(step_temp="")
            else:
                await message.answer("❌ Нет введенного значения. Используйте кнопки с цифрами.")
                return
        else:
            # Если пользователь вводит что-то другое во время набора с кнопок
            await message.answer(f"❌ Используйте кнопки для ввода или нажмите ✅ Готово")
            return
    else:
        # ===== ПЕРВОЕ НАЖАТИЕ ИЛИ ВВОД С КЛАВИАТУРЫ =====
        value = message.text.strip()
        
        # Если это цифра или запятая - начинаем набор через кнопки
        if value in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ","]:
            if value == ",":
                if field in ["bm", "pl1", "pl2", "pl3"]:
                    step_temp = ","
                else:
                    await message.answer(f"📝 Введите целое число без запятой")
                    return
            else:
                step_temp = value
                
            await state.update_data(step_temp=step_temp)
            await message.answer(f"📝 Текущее значение: {step_temp}")
            return
        else:
            # Это ввод с клавиатуры (текст или многозначное число)
            print(f"⌨️ Ввод с клавиатуры: '{value}'")
            # Любое число с клавиатуры сохраняем сразу
            if value.replace(',', '').replace('.', '').isdigit():
                print(f"✅ Число с клавиатуры - сохраняем сразу: {value}")
                # Просто продолжаем выполнение
                pass
            elif step_temp:
                # Если есть накопленное значение через кнопки - игнорируем
                await message.answer(f"❌ Сначала завершите набор через ✅ Готово")
                return

    if not value:
        await message.answer("❌ Значение не может быть пустым. Введите число или нажмите «⏭ Пропустить»")
        return

    # Валидация числовых полей
    if field in ["power", "bm", "dragon", "stands", "research", "pl1", "pl2", "pl3"]:
        value = value.replace('.', ',')
        
        success, error_msg, cleaned_value = validate_numeric_input(field, value)
        if not success:
            if field in ["bm", "pl1", "pl2", "pl3"]:
                kb = get_numeric_kb(decimal=True)
            else:
                kb = get_numeric_kb(decimal=False)
            await message.answer(error_msg, reply_markup=kb)
            return
        
        value = cleaned_value

    step_data[field] = value
    await message.answer(f"✅ {field_name}: {value}")

    # Переходим к следующему шагу
    await state.update_data(
        step_data=step_data,
        step_index=data.get("step_index", 0) + 1,
        step_temp=""
    )
    await step_next(message, state)

async def step_finish(msg_or_cb, state: FSMContext, early=False):
    data = await state.get_data()
    account_id = data.get("step_account")
    step_data = data.get("step_data", {})

    account = db.get_account_by_id(account_id)

    if not account:
        await state.clear()
        return

    user_id = msg_or_cb.from_user.id
    username = msg_or_cb.from_user.username or f"user_{user_id}"
    updated = []

    for field, value in step_data.items():
        if value and value.strip():
            db.create_or_update_account(
                user_id, 
                username, 
                account['game_nickname'], 
                field, 
                value
            )
            updated.append(FIELD_FULL_NAMES.get(field, field))

    if early:
        text = "🏁 <b>ПОШАГОВОЕ ЗАПОЛНЕНИЕ ПРЕРВАНО</b>"
    else:
        text = "✅ <b>ПОШАГОВОЕ ЗАПОЛНЕНИЕ ЗАВЕРШЕНО!</b>"

    text += f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"👤 Аккаунт: <b>{account['game_nickname']}</b>\n"

    if updated:
        text += f"📊 Обновлено полей: <b>{len(updated)}</b>\n"
        text += f"━━━━━━━━━━━━━━━━━━━━━━\n"
        text += f"📝 Список обновленных полей:\n"
        for f in updated[:5]:
            text += f"• {f}\n"
        if len(updated) > 5:
            text += f"• ...и еще {len(updated) - 5}\n"
    else:
        text += f"ℹ️ Ни одно поле не было изменено\n"

    if isinstance(msg_or_cb, Message):
        await msg_or_cb.answer(text, reply_markup=get_main_kb(user_id))
    else:
        await msg_or_cb.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📋 Посмотреть аккаунт", callback_data=f"select_{account_id}")],
                [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")]
            ])
        )

    await state.clear()

# ========== ОБРАБОТКА ВВОДА ==========
@router.message(EditState.waiting_field_value)
async def process_input(message: Message, state: FSMContext):
    """Обработка ввода при обычном редактировании"""
    user_id = message.from_user.id
    username = message.from_user.username or f"user_{user_id}"
    data = await state.get_data()
    field = data.get("field")
    new = data.get("new", False)
    account_id = data.get("account_id")
    temp = data.get("temp", "")
    
    print(f"\n📝 process_input: field={field}, text='{message.text}', temp='{temp}'")
    
    # ===== УПРАВЛЯЮЩИЕ КНОПКИ =====
    if message.text == "🚫 Отмена":
        await message.answer("❌ Действие отменено", reply_markup=get_main_kb(user_id))
        await state.clear()
        return

    if message.text == "🏁 Завершить":
        await message.answer("🏁 Редактирование завершено", reply_markup=get_main_kb(user_id))
        await state.clear()
        return

    if message.text == "⏭ Пропустить":
        field_name = FIELD_FULL_NAMES.get(field, field)
        await message.answer(f"⏭ Поле «{field_name}» пропущено", reply_markup=get_main_kb(user_id))
        await state.clear()
        return

    # Новая логика: определяем, использует ли пользователь кнопки
    if temp is not None and temp != "":
        # ===== РЕЖИМ НАБОРА ЧЕРЕЗ КНОПКИ =====
        if message.text in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ","]:
            if message.text == ",":
                if field in ["bm", "pl1", "pl2", "pl3"]:
                    if "," not in temp:
                        temp += ","
                else:
                    await message.answer(f"📝 Введите целое число без запятой")
                    return
            else:
                temp += message.text
                
            await state.update_data(temp=temp)
            await message.answer(f"📝 Текущее значение: {temp}")
            return

        if message.text == "⌫":
            temp = temp[:-1] if temp else ""
            await state.update_data(temp=temp)
            if temp:
                await message.answer(f"📝 Текущее значение: {temp}")
            else:
                await message.answer(f"📝 Значение очищено")
            return

        if message.text == "✅ Готово":
            if temp:
                value = temp
                await state.update_data(temp="")
            else:
                await message.answer("❌ Нет введенного значения. Используйте кнопки с цифрами.")
                return
        else:
            # Если пользователь вводит что-то другое во время набора с кнопок
            await message.answer(f"❌ Используйте кнопки для ввода или нажмите ✅ Готово")
            return
    else:
        # ===== ПЕРВОЕ НАЖАТИЕ ИЛИ ВВОД С КЛАВИАТУРЫ =====
        value = message.text.strip()
        
        # Если это цифра или запятая - начинаем набор через кнопки
        if value in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ","]:
            if value == ",":
                if field in ["bm", "pl1", "pl2", "pl3"]:
                    temp = ","
                else:
                    await message.answer(f"📝 Введите целое число без запятой")
                    return
            else:
                temp = value
                
            await state.update_data(temp=temp)
            await message.answer(f"📝 Текущее значение: {temp}")
            return
        else:
            # Это ввод с клавиатуры (текст или многозначное число)
            print(f"⌨️ Ввод с клавиатуры: '{value}'")
            # Любое число с клавиатуры сохраняем сразу
            if value.replace(',', '').replace('.', '').isdigit():
                print(f"✅ Число с клавиатуры - сохраняем сразу: {value}")
                # Просто продолжаем выполнение
                pass
            elif temp:
                # Если есть накопленное значение через кнопки - игнорируем
                await message.answer(f"❌ Сначала завершите набор через ✅ Готово")
                return
        
    if not value:
        await message.answer("❌ Значение не может быть пустым")
        return

    field_name = FIELD_FULL_NAMES.get(field, field)

    # ===== ОБРАБОТКА НИКА =====
    if field == "nick":
        if not value:
            await message.answer("❌ Ник не может быть пустым", reply_markup=get_cancel_kb())
            return

        if len(value) < MIN_NICK_LENGTH or len(value) > MAX_NICK_LENGTH:
            await message.answer(f"❌ Ник должен быть от {MIN_NICK_LENGTH} до {MAX_NICK_LENGTH} символов", reply_markup=get_cancel_kb())
            return

        if db.is_nickname_taken(user_id, value, account_id):
            await message.answer(f"❌ Ник '{value}' уже используется", reply_markup=get_cancel_kb())
            return

        if new:
            acc = db.create_or_update_account(user_id, username, value)
            if acc:
                await message.answer(
                    f"✅ Аккаунт создан: {value}",
                    reply_markup=get_main_kb(user_id)
                )
                await state.clear()
            else:
                await message.answer("❌ Ошибка создания", reply_markup=get_cancel_kb())
            return

        if account_id:
            acc = db.get_account_by_id(account_id)
            if acc:
                old = acc['game_nickname']
                if value.lower() == old.lower():
                    await message.answer("ℹ️ Ник не изменен", reply_markup=get_main_kb(user_id))
                    await state.clear()
                    return

                db.create_or_update_account(
                    user_id, 
                    username, 
                    old,
                    "nick", 
                    value
                )
                await message.answer(
                    f"✅ Ник изменен: {old} → {value}",
                    reply_markup=get_main_kb(user_id)
                )
                await state.clear()
            return

    # ===== ОБРАБОТКА ЧИСЛОВЫХ ПОЛЕЙ =====
    if field in ["power", "bm", "dragon", "stands", "research", "pl1", "pl2", "pl3"]:
        if value:
            value = value.replace('.', ',')
            
            success, error_msg, cleaned_value = validate_numeric_input(field, value)
            if not success:
                if field in ["bm", "pl1", "pl2", "pl3"]:
                    kb = get_numeric_kb(decimal=True)
                else:
                    kb = get_numeric_kb(decimal=False)
                await message.answer(error_msg, reply_markup=kb)
                return
            
            value = cleaned_value
            print(f"✅ Валидация пройдена: {field} = {value}")

    # ===== СОХРАНЕНИЕ =====
    if account_id:
        account = db.get_account_by_id(account_id)
        if account:
            db.create_or_update_account(user_id, username, account['game_nickname'], field, value)
            display = value if value else 'пусто'
            await message.answer(
                f"✅ {field_name}: {display}",
                reply_markup=get_main_kb(user_id)
            )
            print(f"✅ Значение сохранено для аккаунта {account_id}")

    await state.clear()

# ========== ОБРАБОТКА ФАЙЛОВ ==========
@router.message(EditState.waiting_for_backup, F.document)
async def handle_backup_file(message: Message, state: FSMContext):
    """Обработка загруженного файла бэкапа"""
    print("\n" + "="*50)
    print("📎📎📎 handle_backup_file ВЫЗВАН! 📎📎📎")
    print(f"   user_id = {message.from_user.id}")
    print(f"   is_admin = {is_admin(message.from_user.id)}")
    print(f"   file_name = {message.document.file_name}")
    print(f"   file_size = {message.document.file_size} bytes")
    
    # Проверяем состояние
    current_state = await state.get_state()
    current_data = await state.get_data()
    print(f"📊 Текущее состояние FSM: {current_state}")
    print(f"📊 Данные FSM: {current_data}")
    
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    
    data = await state.get_data()
    restore_mode = data.get("restore_mode", "pc")
    
    if not message.document.file_name.endswith('.db'):
        await message.answer("❌ Нужен файл с расширением .db")
        await state.clear()
        return
    
    status_msg = await message.answer("🔄 Загружаю и восстанавливаю бэкап...")
    
    try:
        # Скачиваем файл
        file = await bot.get_file(message.document.file_id)
        downloaded_file = await bot.download_file(file.file_path)
        
        # Создаем временный файл
        temp_path = BACKUP_DIR / f"restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        with open(temp_path, 'wb') as f:
            f.write(downloaded_file.getvalue())
        
        # Создаем бэкап текущей БД
        current_backup = BACKUP_DIR / f"before_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        if db.db_path.exists():
            shutil.copy2(db.db_path, current_backup)
        
        # Восстанавливаем
        db.close()
        shutil.copy2(temp_path, db.db_path)
        db._connect()
        
        # Проверяем целостность
        if db.check_integrity():
            accounts = db.get_all_accounts()
            if accounts:
                await status_msg.edit_text(
                    f"✅ База данных восстановлена!\n\n"
                    f"📊 Загружено {len(accounts)} аккаунтов\n"
                    f"💾 Предыдущая БД сохранена как: {current_backup.name}"
                )
            else:
                # Откатываем если нет данных
                if current_backup.exists():
                    db.close()
                    shutil.copy2(current_backup, db.db_path)
                    db._connect()
                await status_msg.edit_text("❌ В загруженном файле нет данных. Восстановлена предыдущая БД.")
        else:
            # Откатываем если файл поврежден
            if current_backup.exists():
                db.close()
                shutil.copy2(current_backup, db.db_path)
                db._connect()
            await status_msg.edit_text("❌ Загруженный файл поврежден. Восстановлена предыдущая БД.")
        
    except Exception as e:
        logger.error(f"Ошибка восстановления: {e}")
        await status_msg.edit_text(f"❌ Ошибка: {e}")
        try:
            db._connect()
        except:
            pass
    finally:
        # Очищаем временный файл
        try:
            if 'temp_path' in locals() and temp_path.exists():
                temp_path.unlink()
        except:
            pass
        await state.clear()

# ========== ОБЩИЙ ХЕНДЛЕР ==========
@router.message(F.chat.type == "private")
async def any_message(message: Message, state: FSMContext):
    current_state = await state.get_state()
    print(f"📨 any_message: state={current_state}, text='{message.text}'")
    
    if current_state == EditState.waiting_search_query:
        print("⚠️ Состояние поиска, но обработчик не сработал!")
        # Принудительно вызываем обработчик поиска
        await process_search(message, state)
        return
    
    if current_state is not None:
        return

    if message.text in ["📊 Мои аккаунты", "📤 Отправить в группу", "👑 Админ-панель"]:
        return

    user_id = message.from_user.id

    if rate_limiter.is_limited(user_id, is_admin(user_id)):
        await message.answer("⏳ Слишком много запросов")
        return

    accounts = db.get_user_accounts_cached(user_id)

    if accounts:
        await message.answer(
            "🏠 <b>Главное меню</b>\n\nВыберите действие:",
            reply_markup=get_main_kb(user_id)
        )
        return

    if message.text != "/start":
        await message.answer(
            "👋 <b>Привет! Я бот для сбора игровых данных.</b>\n\n"
            "Чтобы начать работу, нажми кнопку <b>«🚀 Запустить бота»</b> внизу или введи команду /start",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🚀 Запустить бота", callback_data="force_start")]
            ])
        )

# ========== НАВИГАЦИЯ ==========
@router.callback_query(F.data == "force_start")
async def force_start(callback: CallbackQuery):
    await callback.answer()
    await start_cmd(callback.message)

@router.callback_query(F.data == "my_accounts")
async def my_accounts_cb(callback: CallbackQuery):
    await callback.answer()

    user_id = callback.from_user.id
    accounts = db.get_user_accounts(user_id)

    if not accounts:
        await callback.message.edit_text(
            "📋 У вас нет аккаунтов",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Создать", callback_data="new_account")],
                [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]
            ])
        )
        return

    text = "<b>📋 Ваши аккаунты:</b>\n\n" + format_accounts_table(accounts)
    await safe_send(callback, text, reply_markup=get_accounts_kb(accounts))

@router.callback_query(F.data == "new_account")
async def new_account(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    # Проверяем подписку на группу
    is_subscribed = await check_subscription(user_id)
    
    if not is_subscribed:
        # Формируем информацию о группе
        group_info = "целевую группу"
        invite_link = None
        if TARGET_CHAT_ID:
            try:
                chat = await bot.get_chat(TARGET_CHAT_ID)
                group_info = f"группу <b>{chat.title}</b>"
                invite_link = chat.invite_link
            except:
                pass
        
        text = f"❌ <b>Доступ запрещен</b>\n\n"
        text += f"Для создания аккаунта необходимо быть подписчиком {group_info}.\n\n"
        
        if invite_link:
            text += f"👉 Вступите в группу: {invite_link}\n\n"
        else:
            text += f"1️⃣ Вступите в группу\n"
            text += f"2️⃣ После вступления нажмите кнопку ниже\n\n"
        
        text += f"<i>Если вы уже вступили, попробуйте через минуту</i>"
        
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Проверить подписку", callback_data="check_subscription_before_create")],
                [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]
            ])
        )
        await callback.answer()
        return
    
    # Если подписка есть - продолжаем создание
    await callback.message.edit_text(
        "➕ <b>Создание аккаунта</b>\n\nВведите игровой ник:"
    )
    await callback.message.answer(
        f"📝 Введите ник ({MIN_NICK_LENGTH}-{MAX_NICK_LENGTH} символов):",
        reply_markup=get_cancel_kb()
    )
    await state.set_state(EditState.waiting_field_value)
    await state.update_data(
        field="nick",
        new=True,
        first=len(db.get_user_accounts(user_id)) == 0,
        temp=""
    )
    await callback.answer()

@router.callback_query(F.data == "check_subscription_before_create")
async def check_subscription_before_create(callback: CallbackQuery, state: FSMContext):
    """Проверка подписки перед созданием аккаунта"""
    user_id = callback.from_user.id
    
    is_subscribed = await check_subscription(user_id)
    
    if is_subscribed:
        # Если подписался - переходим к созданию
        await callback.message.edit_text(
            "✅ <b>Подписка подтверждена!</b>\n\n"
            "➕ Введите игровой ник:"
        )
        await callback.message.answer(
            f"📝 Введите ник ({MIN_NICK_LENGTH}-{MAX_NICK_LENGTH} символов):",
            reply_markup=get_cancel_kb()
        )
        await state.set_state(EditState.waiting_field_value)
        await state.update_data(
            field="nick",
            new=True,
            first=len(db.get_user_accounts(user_id)) == 0,
            temp=""
        )
    else:
        # Если всё ещё не подписан
        group_info = "целевую группу"
        if TARGET_CHAT_ID:
            try:
                chat = await bot.get_chat(TARGET_CHAT_ID)
                group_info = f"группу <b>{chat.title}</b>"
            except:
                pass
        
        await callback.message.edit_text(
            f"❌ <b>Подписка не найдена</b>\n\n"
            f"Убедитесь, что вы вступили в {group_info}, и попробуйте снова.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Проверить снова", callback_data="check_subscription_before_create")],
                [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]
            ])
        )
    
    await callback.answer()

@router.callback_query(F.data.startswith("select_"))
async def select_account(callback: CallbackQuery):
    try:
        account_id = int(callback.data.split("_")[1])
    except (ValueError, IndexError):
        await callback.answer("❌ Неверный ID аккаунта", show_alert=True)
        return
        
    account = db.get_account_by_id(account_id)

    if not account:
        await callback.answer("❌ Аккаунт не найден", show_alert=True)
        return

    await callback.message.edit_text(
        format_account_data(account),
        reply_markup=get_account_actions_kb(account_id)
    )
    await callback.answer()

@router.callback_query(F.data.startswith("edit_nick_"))
async def edit_nick(callback: CallbackQuery, state: FSMContext):
    try:
        account_id = int(callback.data.split("_")[2])
    except (ValueError, IndexError):
        await callback.answer("❌ Неверный ID аккаунта", show_alert=True)
        return
        
    account = db.get_account_by_id(account_id)

    if not account:
        await callback.answer("❌ Аккаунт не найден", show_alert=True)
        return

    await callback.message.edit_text(
        f"✏️ <b>Изменение ника</b>\n\nТекущий: {account['game_nickname']}\n\nВведите новый ник:"
    )
    await callback.message.answer(
        "📝 Введите новый ник:",
        reply_markup=get_cancel_kb()
    )
    await state.set_state(EditState.waiting_field_value)
    await state.update_data(
        field="nick",
        account_id=account_id,
        temp=""
    )
    await callback.answer()

@router.callback_query(F.data.startswith("edit_"))
async def edit_account(callback: CallbackQuery):
    try:
        account_id = int(callback.data.split("_")[1])
    except (ValueError, IndexError):
        await callback.answer("❌ Неверный ID аккаунта", show_alert=True)
        return
        
    account = db.get_account_by_id(account_id)

    if not account:
        await callback.answer("❌ Аккаунт не найден", show_alert=True)
        return

    await callback.message.edit_text(
        f"✏️ <b>Редактирование</b> {account['game_nickname']}\n\nВыберите поле:",
        reply_markup=get_edit_fields_kb(account_id)
    )
    await callback.answer()

@router.callback_query(F.data.startswith("field_"))
async def edit_field(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) < 3:
        await callback.answer("❌ Неверный формат данных", show_alert=True)
        return
        
    try:
        account_id = int(parts[1])
    except ValueError:
        await callback.answer("❌ Неверный ID аккаунта", show_alert=True)
        return
        
    field = parts[2]

    if field not in FIELDS:
        await callback.answer("❌ Неверное поле", show_alert=True)
        return

    account = db.get_account_by_id(account_id)

    if not account:
        await callback.answer("❌ Аккаунт не найден", show_alert=True)
        return

    db_field = FIELD_DB_MAP.get(field, field)
    current = account.get(db_field, '')
    name = FIELD_FULL_NAMES.get(field, field)

    await callback.message.edit_text(
        f"✏️ <b>{name}</b>\n\nТекущее: {current or '—'}\n\nВведите новое значение:"
    )

    if field in ["bm", "pl1", "pl2", "pl3"]:
        await callback.message.answer(
            "📝 Введите число (можно с запятой, макс. 999.9):",
            reply_markup=get_numeric_kb(decimal=True)
        )
    elif field in ["power", "dragon"]:
        await callback.message.answer(
            f"📝 Введите целое число (0-{MAX_POWER_DRAGON}):",
            reply_markup=get_numeric_kb(decimal=False)
        )
    elif field in ["stands", "research"]:
        await callback.message.answer(
            f"📝 Введите целое число (0-{MAX_BUFF}):",
            reply_markup=get_numeric_kb(decimal=False)
        )
    else:
        await callback.message.answer(
            "📝 Введите значение:",
            reply_markup=get_cancel_kb()
        )

    await state.set_state(EditState.waiting_field_value)
    await state.update_data(
        field=field,
        account_id=account_id,
        temp=""
    )
    await callback.answer()

@router.callback_query(F.data.startswith("delete_"))
async def delete_account(callback: CallbackQuery):
    try:
        account_id = int(callback.data.split("_")[1])
    except (ValueError, IndexError):
        await callback.answer("❌ Неверный ID аккаунта", show_alert=True)
        return
        
    account = db.get_account_by_id(account_id)

    if not account:
        await callback.answer("❌ Аккаунт не найден", show_alert=True)
        return

    await callback.message.edit_text(
        f"🗑️ <b>Удаление аккаунта</b>\n\n"
        f"Вы уверены, что хотите удалить {account['game_nickname']}?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да", callback_data=f"confirm_delete_{account_id}"),
                InlineKeyboardButton(text="❌ Нет", callback_data=f"select_{account_id}")
            ]
        ])
    )
    await callback.answer()

@router.callback_query(F.data.startswith("confirm_delete_"))
async def confirm_delete(callback: CallbackQuery):
    """Подтверждение удаления аккаунта пользователем"""
    print("\n" + "="*50)
    print("🗑️🗑️🗑️ confirm_delete ВЫЗВАН! 🗑️🗑️🗑️")
    print(f"   callback.data = '{callback.data}'")
    print(f"   user_id = {callback.from_user.id}")
    print("="*50)
    
    try:
        account_id = int(callback.data.split("_")[2])
        print(f"📦 account_id = {account_id}")
    except (ValueError, IndexError) as e:
        print(f"❌ Ошибка парсинга: {e}")
        await callback.answer("❌ Неверный ID аккаунта", show_alert=True)
        return
        
    account = db.get_account_by_id(account_id)
    if not account:
        print(f"❌ Аккаунт {account_id} не найден")
        await callback.answer("❌ Аккаунт не найден", show_alert=True)
        return
    
    print(f"📋 Аккаунт: {account.get('game_nickname')}")

    if db.delete_account(account_id):
        print(f"✅ Аккаунт {account_id} удален")
        db.invalidate_cache()
        
        # Проверяем остались ли аккаунты у пользователя
        remaining_accounts = db.get_user_accounts(callback.from_user.id)
        print(f"📊 Осталось аккаунтов у пользователя: {len(remaining_accounts)}")

        if remaining_accounts:
            await callback.message.edit_text(
                f"✅ Аккаунт {account['game_nickname']} удален",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📊 Мои аккаунты", callback_data="my_accounts")],
                    [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]
                ])
            )
        else:
            await callback.message.edit_text(
                f"✅ Аккаунт {account['game_nickname']} удален",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="➕ Создать новый аккаунт", callback_data="new_account")],
                    [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]
                ])
            )
    else:
        print(f"❌ Ошибка удаления аккаунта {account_id}")
        await callback.message.edit_text(
            "❌ Ошибка удаления",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"select_{account_id}")]
            ])
        )
    await callback.answer()

# ========== ОТПРАВКА В ГРУППУ ==========
@router.callback_query(F.data.startswith("send_"))
async def send_account(callback: CallbackQuery):
    if not TARGET_CHAT_ID:
        await callback.answer("❌ Отправка не настроена", show_alert=True)
        return

    try:
        account_id = int(callback.data.split("_")[1])
    except (ValueError, IndexError):
        await callback.answer("❌ Неверный ID аккаунта", show_alert=True)
        return

    account = db.get_account_by_id(account_id)

    if not account:
        await callback.answer("❌ Аккаунт не найден", show_alert=True)
        return

    text = f"📊 <b>Данные игрока:</b> {account['game_nickname']}\n\n"

    for key, name in FIELD_FULL_NAMES.items():
        if key == "nick":
            continue

        db_field = FIELD_DB_MAP.get(key, key)
        val = account.get(db_field, '')

        if val and val != '—':
            if key in ["bm", "pl1", "pl2", "pl3"]:
                if ',' in val:
                    formatted_val = val
                else:
                    formatted_val = f"{val},0"
                text += f"<b>{name}:</b> {formatted_val}\n"
            else:
                text += f"<b>{name}:</b> {val}\n"

    text += f"\n👤 От: @{callback.from_user.username or 'пользователь'}"

    try:
        if USE_TOPIC and TARGET_TOPIC_ID:
            await bot.send_message(
                chat_id=TARGET_CHAT_ID,
                message_thread_id=TARGET_TOPIC_ID,
                text=text
            )
        else:
            await bot.send_message(
                chat_id=TARGET_CHAT_ID,
                text=text
            )

        await callback.message.edit_text(
            f"✅ Отправлено: {account['game_nickname']}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🏠 Меню", callback_data="menu")]
            ])
        )
        await callback.answer("✅ Отправлено!")
    except Exception as e:
        logger.error(f"Send error: {e}")
        await callback.answer("❌ Ошибка отправки", show_alert=True)

# ========== НАВИГАЦИЯ ==========
@router.callback_query(F.data == "menu")
async def menu_cb(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await state.clear()
    user_id = callback.from_user.id
    await callback.message.edit_text(
        "🏠 Главное меню",
        reply_markup=None
    )
    await callback.message.answer(
        "🏠 Главное меню",
        reply_markup=get_main_kb(user_id)
    )
    await callback.answer()

@router.callback_query(F.data == "cancel")
async def cancel_cb(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    await callback.message.edit_text(
        "❌ Отменено",
        reply_markup=None
    )
    await callback.message.answer(
        "❌ Отменено",
        reply_markup=get_main_kb(user_id)
    )
    await callback.answer()

# ========== УПРАВЛЕНИЕ БД ==========
@router.callback_query(F.data == "db_management")
async def db_management_menu(callback: CallbackQuery):
    """Меню управления базой данных"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    await callback.answer()
    
    stats = db.get_stats()
    
    try:
        db_size = db.db_path.stat().st_size / 1024
        backups = len(list(BACKUP_DIR.glob("backup_*.db")))
        exports = len(list(EXPORT_DIR.glob("export_*.csv")))
    except Exception as e:
        logger.error(f"Ошибка получения размеров: {e}")
        db_size = backups = exports = 0
    
    text = f"""🗄️ <b>Управление базой данных</b>

📊 <b>Текущее состояние:</b>
• Размер БД: {db_size:.1f} KB
• Пользователей: {stats['unique_users']}
• Аккаунтов: {stats['total_accounts']}
• Бэкапов: {backups}
• Экспортов: {exports}

<b>Доступные действия:</b>
💾 <b>Сохранить бэкап</b> - создать копию базы данных
📥 <b>Восстановить из бэкапа на сервере</b> - выбрать ранее сохраненный бэкап
📤 <b>Загрузить с ПК</b> - отправить файл бэкапа из Telegram
🧹 <b>Очистка</b> - удалить файлы старше 14 дней
"""
    
    await safe_send(callback, text, reply_markup=get_db_management_kb())

@router.callback_query(F.data == "db_backup")
async def db_backup_handler(callback: CallbackQuery):
    """Создание бэкапа базы данных"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    await callback.answer("🔄 Создаю бэкап...")
    await callback.message.edit_text("🔄 Создание бэкапа...")
    
    try:
        path = await asyncio.to_thread(db.create_backup)
        
        if path and Path(path).exists():
            try:
                await bot.send_document(
                    chat_id=callback.from_user.id,
                    document=FSInputFile(path),
                    caption=f"💾 Бэкап от {datetime.now().strftime('%d.%m.%Y %H:%M')}"
                )
                # Возвращаемся в меню управления БД
                await db_management_menu(callback)
            except Exception as e:
                logger.error(f"Ошибка отправки файла: {e}")
                await callback.message.edit_text(
                    f"❌ Ошибка отправки файла: {e}",
                    reply_markup=get_db_management_kb()
                )
        else:
            await callback.message.edit_text(
                "❌ Ошибка создания бэкапа",
                reply_markup=get_db_management_kb()
            )
    except Exception as e:
        logger.error(f"Ошибка создания бэкапа: {e}")
        await callback.message.edit_text(
            f"❌ Ошибка: {e}",
            reply_markup=get_db_management_kb()
        )

@router.callback_query(F.data == "db_restore_menu")
async def db_restore_menu(callback: CallbackQuery):
    """Меню выбора бэкапа для восстановления"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    backups = sorted(BACKUP_DIR.glob("backup_*.db"), key=os.path.getmtime, reverse=True)
    root_backups = sorted(BASE_DIR.glob("backup_*.db"), key=os.path.getmtime, reverse=True)
    all_backups = backups + root_backups
    
    if not all_backups:
        await callback.message.edit_text(
            "❌ Нет доступных бэкапов",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="db_management")]
            ])
        )
        await callback.answer()
        return
    
    buttons = []
    for i, backup in enumerate(all_backups[:10]):
        try:
            mtime = backup.stat().st_mtime
            date_str = datetime.fromtimestamp(mtime).strftime('%d.%m.%Y %H:%M')
            location = "📁 backups" if backup.parent == BACKUP_DIR else "📁 корень"
        except:
            date_str = backup.name.replace('backup_', '').replace('.db', '')
            location = ""
        
        buttons.append([
            InlineKeyboardButton(
                text=f"📅 {date_str} ({(backup.stat().st_size / 1024):.1f} KB) {location}",
                callback_data=f"db_restore_{backup.name}"
            )
        ])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="db_management")])
    
    await callback.message.edit_text(
        "📥 <b>Восстановление из бэкапа</b>\n\n"
        "Выберите бэкап для восстановления:\n"
        "⚠️ <b>Внимание!</b> Текущая база будет заменена!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await callback.answer()

# ========== ВОССТАНОВЛЕНИЕ ИЗ БЭКАПА (СЕРВЕР) ==========
@router.callback_query(F.data.startswith("db_restore_"))
async def db_restore_unified_handler(callback: CallbackQuery, state: FSMContext):
    """Единый обработчик для всех действий с бэкапами"""
    print("\n" + "="*50)
    print("🔵🔵🔵 db_restore_unified_handler ВЫЗВАН! 🔵🔵🔵")
    print(f"   callback.data = '{callback.data}'")
    print(f"   user_id = {callback.from_user.id}")
    print("="*50)
    
    if not is_admin(callback.from_user.id):
        print("❌ ДОСТУП ЗАПРЕЩЕН")
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    # ===== ЗАГРУЗКА С ПК =====
    if callback.data == "db_restore_pc":
        print("🔴🔴🔴 ЗАГРУЗКА С ПК 🔴🔴🔴")
        await callback.answer()
        
        await callback.message.edit_text(
            "📤 <b>Загрузка бэкапа с компьютера</b>\n\n"
            "1️⃣ Нажмите на скрепку 📎\n"
            "2️⃣ Выберите 'Документ'\n"
            "3️⃣ Найдите файл .db на вашем компьютере\n"
            "4️⃣ Отправьте его\n\n"
            "⚠️ <b>Внимание!</b> Текущая база будет заменена!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Отмена", callback_data="db_management")]
            ])
        )
        
        await state.clear()
        await state.set_state(EditState.waiting_for_backup)
        await state.update_data(restore_mode="pc")
        print("✅ Режим ожидания файла установлен")
        return
    
    # ===== МЕНЮ ВЫБОРА БЭКАПА =====
    if callback.data == "db_restore_menu":
        print("📋 МЕНЮ ВЫБОРА БЭКАПА")
        backups = sorted(BACKUP_DIR.glob("backup_*.db"), key=os.path.getmtime, reverse=True)
        root_backups = sorted(BASE_DIR.glob("backup_*.db"), key=os.path.getmtime, reverse=True)
        all_backups = backups + root_backups
        
        if not all_backups:
            await callback.message.edit_text(
                "❌ Нет доступных бэкапов",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="db_management")]
                ])
            )
            await callback.answer()
            return
        
        buttons = []
        for i, backup in enumerate(all_backups[:10]):
            try:
                mtime = backup.stat().st_mtime
                date_str = datetime.fromtimestamp(mtime).strftime('%d.%m.%Y %H:%M')
                location = "📁 backups" if backup.parent == BACKUP_DIR else "📁 корень"
            except:
                date_str = backup.name.replace('backup_', '').replace('.db', '')
                location = ""
            
            buttons.append([
                InlineKeyboardButton(
                    text=f"📅 {date_str} ({(backup.stat().st_size / 1024):.1f} KB) {location}",
                    callback_data=f"db_restore_file_{backup.name}"
                )
            ])
        
        buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="db_management")])
        
        await callback.message.edit_text(
            "📥 <b>Восстановление из бэкапа</b>\n\n"
            "Выберите бэкап для восстановления:\n"
            "⚠️ <b>Внимание!</b> Текущая база будет заменена!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )
        await callback.answer()
        return
    
    # ===== ВЫБРАН КОНКРЕТНЫЙ ФАЙЛ =====
    if callback.data.startswith("db_restore_file_"):
        backup_name = callback.data.replace("db_restore_file_", "")
        print(f"📦 ВЫБРАН ФАЙЛ: {backup_name}")
        
        backup_path = BACKUP_DIR / backup_name if (BACKUP_DIR / backup_name).exists() else BASE_DIR / backup_name
        
        if not backup_path.exists():
            await callback.message.edit_text(
                "❌ Файл бэкапа не найден",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="db_restore_menu")]
                ])
            )
            await callback.answer()
            return
        
        await callback.message.edit_text(
            f"⚠️ <b>Подтверждение восстановления</b>\n\n"
            f"Файл: {backup_name}\n"
            f"Размер: {(backup_path.stat().st_size / 1024):.1f} KB\n\n"
            f"<b>ВНИМАНИЕ!</b> Текущая база данных будет полностью заменена!\n\n"
            f"Вы уверены?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Да, восстановить", callback_data=f"db_restore_confirm_{backup_name}"),
                    InlineKeyboardButton(text="❌ Нет, отмена", callback_data="db_restore_menu")
                ]
            ])
        )
        await callback.answer()
        return
    
    # ===== ПОДТВЕРЖДЕНИЕ ВОССТАНОВЛЕНИЯ =====
    if callback.data.startswith("db_restore_confirm_"):
        backup_name = callback.data.replace("db_restore_confirm_", "")
        print(f"✅ ПОДТВЕРЖДЕНИЕ ВОССТАНОВЛЕНИЯ: {backup_name}")
        
        backup_path = BACKUP_DIR / backup_name if (BACKUP_DIR / backup_name).exists() else BASE_DIR / backup_name
        await callback.message.edit_text("🔄 Восстановление...")
        
        try:
            current_backup = f"before_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            shutil.copy2(db.db_path, BACKUP_DIR / current_backup)
            
            db.close()
            shutil.copy2(backup_path, db.db_path)
            db._connect()
            
            if db.check_integrity():
                accounts = db.get_all_accounts()
                await callback.message.edit_text(
                    f"✅ База данных успешно восстановлена из {backup_name}\n\n"
                    f"📊 Загружено {len(accounts)} аккаунтов\n"
                    f"💾 Предыдущая БД сохранена как: {current_backup}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🗄️ Управление БД", callback_data="db_management")]
                    ])
                )
            else:
                shutil.copy2(BACKUP_DIR / current_backup, db.db_path)
                db._connect()
                await callback.message.edit_text(
                    "❌ Ошибка: восстановленный файл поврежден. База возвращена к предыдущему состоянию.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🗄️ Управление БД", callback_data="db_management")]
                    ])
                )
                
        except Exception as e:
            logger.error(f"Ошибка восстановления: {e}")
            await callback.message.edit_text(
                f"❌ Ошибка восстановления: {e}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="db_restore_menu")]
                ])
            )
            try:
                db._connect()
            except:
                pass
        
        await callback.answer()
        return
    
    print(f"⚠️ Неизвестный callback: {callback.data}")
    await callback.answer()

# ========== АДМИН ХЕНДЛЕРЫ ==========
@router.callback_query(F.data.startswith("admin_table_"))
async def admin_table(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    try:
        page = int(callback.data.split("_")[2])
    except:
        page = 1

    accounts = db.get_all_accounts()

    if not accounts:
        await callback.message.edit_text("📋 Нет данных", reply_markup=get_admin_kb())
        await callback.answer()
        return

    per_page = ACCOUNTS_PER_PAGE
    total = (len(accounts) + per_page - 1) // per_page
    page = max(1, min(page, total))
    start = (page - 1) * per_page
    end = min(start + per_page, len(accounts))

    text = f"📋 <b>Таблица участников</b> (стр. {page}/{total})\n\n"
    text += format_accounts_table(accounts[start:end], start)

    text += "\n<i>🔽 Нажмите кнопку ниже для удаления аккаунта</i>"

    buttons = []

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"admin_table_{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page}/{total}", callback_data="noop"))
    if page < total:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_table_{page+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([
        InlineKeyboardButton(
            text="🗑️ Удалить аккаунт",
            callback_data="admin_show_delete_menu"
        )
    ])

    buttons.append([
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"admin_table_{page}"),
        InlineKeyboardButton(text="📤 CSV", callback_data="admin_export")
    ])
    buttons.append([
        InlineKeyboardButton(text="🔍 Поиск", callback_data="admin_search"),
        InlineKeyboardButton(text="🗑️ Пакетное удаление", callback_data="admin_batch")
    ])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")])

    await safe_send(callback, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()

@router.callback_query(F.data.startswith("confirm_del_"))
async def confirm_del(callback: CallbackQuery):
    """Подтверждение удаления аккаунта админом"""
    print("\n" + "="*50)
    print("🗑️🗑️🗑️ confirm_del ВЫЗВАН! 🗑️🗑️🗑️")
    print(f"   callback.data = '{callback.data}'")
    print(f"   user_id = {callback.from_user.id}")
    print("="*50)
    
    if not is_admin(callback.from_user.id):
        print("❌ ДОСТУП ЗАПРЕЩЕН")
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    parts = callback.data.split("_")
    if len(parts) < 4:
        print(f"❌ Неверный формат: {parts}")
        await callback.answer("❌ Неверный формат данных", show_alert=True)
        return
        
    try:
        account_id = int(parts[2])
        page = int(parts[3])
        print(f"📦 account_id = {account_id}, page = {page}")
    except (ValueError, IndexError) as e:
        print(f"❌ Ошибка парсинга: {e}")
        await callback.answer("❌ Неверный ID или страница", show_alert=True)
        return

    account = db.get_account_by_id(account_id)
    if not account:
        print(f"❌ Аккаунт {account_id} не найден")
        await callback.answer("❌ Аккаунт не найден", show_alert=True)
        return
    
    print(f"📋 Аккаунт для удаления: {account.get('game_nickname')} (ID: {account_id})")

    if db.delete_account(account_id):
        print(f"✅ Аккаунт {account_id} успешно удален")
        db.invalidate_cache()
        
        remaining = db.get_all_accounts()
        print(f"📊 Осталось аккаунтов в БД: {len(remaining)}")
        
        await callback.message.edit_text(
            f"✅ Аккаунт {account['game_nickname']} (ID:{account_id}) удален",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📋 Обновить таблицу", callback_data=f"admin_table_{page}")],
                [InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin_back")]
            ])
        )
    else:
        print(f"❌ Ошибка при удалении аккаунта {account_id}")
        await callback.message.edit_text(
            "❌ Ошибка удаления",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin_table_{page}")]
            ])
        )
    
    await callback.answer()

@router.callback_query(F.data.startswith("admin_del_"))
async def admin_del_account(callback: CallbackQuery):
    """Обработка выбора аккаунта для удаления"""
    print("\n" + "="*50)
    print("🗑️🗑️🗑️ admin_del_account ВЫЗВАН! 🗑️🗑️🗑️")
    print(f"   callback.data = '{callback.data}'")
    print(f"   user_id = {callback.from_user.id}")
    print("="*50)
    
    if not is_admin(callback.from_user.id):
        print("❌ ДОСТУП ЗАПРЕЩЕН")
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    # Парсим данные: admin_del_123_1
    parts = callback.data.split("_")
    if len(parts) < 4:
        print(f"❌ Неверный формат: {parts}")
        await callback.answer("❌ Неверный формат данных", show_alert=True)
        return

    try:
        account_id = int(parts[2])
        page = int(parts[3])
        print(f"📦 account_id = {account_id}, page = {page}")
    except (ValueError, IndexError) as e:
        print(f"❌ Ошибка парсинга: {e}")
        await callback.answer("❌ Неверный ID или страница", show_alert=True)
        return

    # Получаем аккаунт
    account = db.get_account_by_id(account_id)
    if not account:
        print(f"❌ Аккаунт {account_id} не найден")
        await callback.answer("❌ Аккаунт не найден", show_alert=True)
        return

    print(f"📋 Аккаунт: {account.get('game_nickname')}")

    # Показываем подтверждение
    await callback.message.edit_text(
        f"🗑️ <b>Подтверждение удаления</b>\n\n"
        f"Аккаунт: {account['game_nickname']}\n"
        f"ID: {account_id}\n\n"
        f"Вы уверены, что хотите удалить этот аккаунт?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_del_{account_id}_{page}"),
                InlineKeyboardButton(text="❌ Нет, отмена", callback_data=f"admin_show_delete_menu_page_{page}")
            ]
        ])
    )
    await callback.answer()

# ========== ПАКЕТНОЕ УДАЛЕНИЕ С ЧЕКБОКСАМИ ==========
@router.callback_query(F.data == "admin_batch")
async def admin_batch(callback: CallbackQuery, state: FSMContext):
    """Пакетное удаление с чекбоксами"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    accounts = db.get_all_accounts()
    if not accounts:
        await callback.answer("📋 Нет аккаунтов для удаления", show_alert=True)
        return

    # Сохраняем список аккаунтов в состоянии
    await state.set_state(EditState.batch_selection)
    await state.update_data(
        batch_accounts=accounts,
        batch_selected=set(),
        batch_page=1
    )
    
    await show_batch_page(callback.message, state)
    await callback.answer()

async def show_batch_page(message: Message, state: FSMContext):
    """Показывает страницу пакетного удаления с чекбоксами"""
    data = await state.get_data()
    accounts = data.get("batch_accounts", [])
    selected = data.get("batch_selected", set())
    page = data.get("batch_page", 1)
    
    per_page = 10
    total_pages = (len(accounts) + per_page - 1) // per_page
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    end = min(start + per_page, len(accounts))
    
    # Функция проверки заполненности
    def is_incomplete(acc):
        required_fields = ['power', 'bm', 'pl1', 'pl2', 'pl3', 'dragon', 'buffs_stands', 'buffs_research']
        for field in required_fields:
            value = acc.get(field, '')
            if not value or value == '—' or value == '':
                return True
        return False
    
    text = f"🗑️ <b>Пакетное удаление</b> (стр. {page}/{total_pages})\n\n"
    text += "Отметьте аккаунты для удаления:\n\n"
    
    buttons = []
    
    # Аккаунты на текущей странице
    for i, acc in enumerate(accounts[start:end], start + 1):
        acc_id = acc.get('id')
        nick = acc.get('game_nickname', '—')
        if len(nick) > 25:
            nick = nick[:22] + '...'
        
        checkbox = "✅" if acc_id in selected else "⬜"
        warning = "⚠️ " if is_incomplete(acc) else ""
        
        buttons.append([
            InlineKeyboardButton(
                text=f"{checkbox} {i}. {warning}{nick}",
                callback_data=f"batch_toggle_{acc_id}"
            )
        ])
    
    # Навигация
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data="batch_page_prev"))
    nav_buttons.append(InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data="batch_page_next"))
    
    if nav_buttons:
        buttons.append(nav_buttons)
    
    # Кнопки управления
    buttons.append([
        InlineKeyboardButton(text="✅ Выбрать все", callback_data="batch_select_all"),
        InlineKeyboardButton(text="⬜ Снять все", callback_data="batch_deselect_all")
    ])
    
    buttons.append([
        InlineKeyboardButton(text="🗑️ Удалить выбранное", callback_data="batch_delete_selected"),
        InlineKeyboardButton(text="❌ Отмена", callback_data="admin_back")
    ])
    
    await message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("batch_toggle_"))
async def batch_toggle(callback: CallbackQuery, state: FSMContext):
    """Отметить/снять отметку с аккаунта"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    acc_id = int(callback.data.replace("batch_toggle_", ""))
    
    data = await state.get_data()
    selected = data.get("batch_selected", set())
    
    if acc_id in selected:
        selected.remove(acc_id)
    else:
        selected.add(acc_id)
    
    await state.update_data(batch_selected=selected)
    await show_batch_page(callback.message, state)
    await callback.answer()

@router.callback_query(F.data == "batch_select_all")
async def batch_select_all(callback: CallbackQuery, state: FSMContext):
    """Выбрать все аккаунты на текущей странице"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    data = await state.get_data()
    accounts = data.get("batch_accounts", [])
    selected = data.get("batch_selected", set())
    page = data.get("batch_page", 1)
    
    per_page = 10
    start = (page - 1) * per_page
    end = min(start + per_page, len(accounts))
    
    for acc in accounts[start:end]:
        selected.add(acc.get('id'))
    
    await state.update_data(batch_selected=selected)
    await show_batch_page(callback.message, state)
    await callback.answer()

@router.callback_query(F.data == "batch_deselect_all")
async def batch_deselect_all(callback: CallbackQuery, state: FSMContext):
    """Снять все отметки на текущей странице"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    data = await state.get_data()
    accounts = data.get("batch_accounts", [])
    selected = data.get("batch_selected", set())
    page = data.get("batch_page", 1)
    
    per_page = 10
    start = (page - 1) * per_page
    end = min(start + per_page, len(accounts))
    
    for acc in accounts[start:end]:
        selected.discard(acc.get('id'))
    
    await state.update_data(batch_selected=selected)
    await show_batch_page(callback.message, state)
    await callback.answer()

@router.callback_query(F.data == "batch_page_next")
async def batch_page_next(callback: CallbackQuery, state: FSMContext):
    """Следующая страница"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    data = await state.get_data()
    page = data.get("batch_page", 1)
    await state.update_data(batch_page=page + 1)
    await show_batch_page(callback.message, state)
    await callback.answer()

@router.callback_query(F.data == "batch_page_prev")
async def batch_page_prev(callback: CallbackQuery, state: FSMContext):
    """Предыдущая страница"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    data = await state.get_data()
    page = data.get("batch_page", 1)
    if page > 1:
        await state.update_data(batch_page=page - 1)
        await show_batch_page(callback.message, state)
    await callback.answer()

@router.callback_query(F.data == "batch_delete_selected")
async def batch_delete_selected(callback: CallbackQuery, state: FSMContext):
    """Удалить выбранные аккаунты"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    data = await state.get_data()
    selected = data.get("batch_selected", set())
    
    if not selected:
        await callback.answer("❌ Нет выбранных аккаунтов", show_alert=True)
        return
    
    # Показываем подтверждение
    accounts_list = []
    for acc_id in list(selected)[:5]:
        acc = db.get_account_by_id(acc_id)
        if acc:
            accounts_list.append(f"• {acc['game_nickname']} (ID:{acc_id})")
    
    text = f"🗑️ <b>Подтверждение удаления</b>\n\n"
    text += f"Выбрано аккаунтов: {len(selected)}\n\n"
    if accounts_list:
        text += "Будут удалены:\n" + "\n".join(accounts_list)
        if len(selected) > 5:
            text += f"\n...и еще {len(selected) - 5}"
    
    text += f"\n\nВы уверены?"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, удалить все", callback_data="batch_confirm_delete"),
            InlineKeyboardButton(text="❌ Нет, отмена", callback_data="admin_batch")
        ]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

@router.callback_query(F.data == "batch_confirm_delete")
async def batch_confirm_delete(callback: CallbackQuery, state: FSMContext):
    """Подтверждение массового удаления"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    data = await state.get_data()
    selected = data.get("batch_selected", set())
    
    if not selected:
        await callback.answer("❌ Нет выбранных аккаунтов", show_alert=True)
        return
    
    deleted = []
    failed = []
    incomplete = []
    
    def is_incomplete(acc):
        required_fields = ['power', 'bm', 'pl1', 'pl2', 'pl3', 'dragon', 'buffs_stands', 'buffs_research']
        for field in required_fields:
            value = acc.get(field, '')
            if not value or value == '—' or value == '':
                return True
        return False
    
    for acc_id in selected:
        acc = db.get_account_by_id(acc_id)
        if acc:
            if is_incomplete(acc):
                incomplete.append(f"{acc['game_nickname']} (ID:{acc_id})")
            
            if db.delete_account(acc_id):
                deleted.append(f"{acc['game_nickname']} (ID:{acc_id})")
            else:
                failed.append(acc_id)
    
    # Формируем отчет
    text = "🗑️ <b>Результат массового удаления</b>\n\n"
    
    if deleted:
        text += f"✅ Удалено ({len(deleted)}):\n"
        for item in deleted[:10]:
            text += f"• {item}\n"
        if len(deleted) > 10:
            text += f"...и еще {len(deleted) - 10}\n"
        text += "\n"
    
    if incomplete:
        text += f"⚠️ Среди удаленных были неполные ({len(incomplete)}):\n"
        for item in incomplete[:5]:
            text += f"• {item}\n"
        if len(incomplete) > 5:
            text += f"...и еще {len(incomplete) - 5}\n"
        text += "\n"
    
    if failed:
        text += f"❌ Не удалось удалить ({len(failed)}): {', '.join(map(str, failed[:10]))}\n\n"
    
    db.invalidate_cache()
    await state.clear()
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👑 Админ-панель", callback_data="admin_back")]
        ])
    )
    await callback.answer()

@router.callback_query(F.data == "admin_show_delete_menu")
async def admin_show_delete_menu(callback: CallbackQuery):
    """Показывает список аккаунтов для удаления"""
    print("\n" + "="*50)
    print("📋📋📋 admin_show_delete_menu ВЫЗВАН! 📋📋📋")
    print(f"   callback.data = '{callback.data}'")
    print(f"   user_id = {callback.from_user.id}")
    print("="*50)
    
    if not is_admin(callback.from_user.id):
        print("❌ ДОСТУП ЗАПРЕЩЕН")
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    accounts = db.get_all_accounts()
    if not accounts:
        print("📋 Нет аккаунтов для удаления")
        await callback.answer("📋 Нет аккаунтов для удаления", show_alert=True)
        return

    # Получаем страницу (по умолчанию 1)
    try:
        page = int(callback.data.split("_")[4]) if len(callback.data.split("_")) > 4 else 1
    except:
        page = 1

    per_page = ACCOUNTS_PER_PAGE
    total_pages = (len(accounts) + per_page - 1) // per_page
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    end = min(start + per_page, len(accounts))

    print(f"📊 Всего аккаунтов: {len(accounts)}, страница {page}/{total_pages}")

    text = f"🗑️ <b>Выберите аккаунт для удаления:</b> (стр. {page}/{total_pages})\n\n"
    buttons = []

    # Кнопки с аккаунтами
    for i, acc in enumerate(accounts[start:end], start + 1):
        nick = acc.get('game_nickname', '—')
        if len(nick) > 30:
            nick = nick[:27] + '...'
        acc_id = acc.get('id')
        if acc_id:
            callback_data = f"admin_del_{acc_id}_{page}"
            print(f"➕ Кнопка: {i}. {nick} -> {callback_data}")
            buttons.append([
                InlineKeyboardButton(
                    text=f"{i}. {nick}",
                    callback_data=callback_data
                )
            ])

    # Навигация
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"admin_show_delete_menu_page_{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_show_delete_menu_page_{page+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton(text="⬅️ Назад к таблице", callback_data="admin_table_1")])

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await callback.answer()

@router.callback_query(F.data.startswith("admin_show_delete_menu_page_"))
async def admin_show_delete_menu_page(callback: CallbackQuery):
    """Обработка переключения страниц в меню удаления"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    try:
        page = int(callback.data.split("_")[5])
        print(f"📄 Переход на страницу {page}")
    except:
        page = 1

    # Создаем объект с нужными атрибутами
    class TempCallback:
        def __init__(self, from_user, data, message, answer):
            self.from_user = from_user
            self.data = f"admin_show_delete_menu_page_{page}"
            self.message = message
            self.answer = answer

    new_callback = TempCallback(
        callback.from_user,
        f"admin_show_delete_menu_page_{page}",
        callback.message,
        callback.answer
    )

    await admin_show_delete_menu(new_callback)

@router.callback_query(F.data == "admin_export")
async def admin_export(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    await callback.message.edit_text("🔄 Создание CSV...")

    path = await asyncio.to_thread(db.export_to_csv)

    if path and Path(path).exists():
        try:
            await bot.send_document(
                chat_id=callback.from_user.id,
                document=FSInputFile(path),
                caption=f"📤 Экспорт {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
            stats = db.get_stats()
            text = f"""👑 <b>Админ-панель</b>

👥 Пользователей: {stats['unique_users']}
🎮 Аккаунтов: {stats['total_accounts']}"""
            await callback.message.edit_text(text, reply_markup=get_admin_kb())
        except Exception as e:
            logger.error(f"Ошибка отправки CSV: {e}")
            await callback.message.edit_text(f"❌ Ошибка: {e}", reply_markup=get_admin_kb())
    else:
        await callback.message.edit_text("❌ Ошибка создания файла", reply_markup=get_admin_kb())

    await callback.answer()

@router.callback_query(F.data == "admin_export_excel")
async def admin_export_excel(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    await callback.message.edit_text("🔄 Создание Excel файла...")

    path = await asyncio.to_thread(db.export_to_excel)

    if path and Path(path).exists():
        try:
            await bot.send_document(
                chat_id=callback.from_user.id,
                document=FSInputFile(path),
                caption=f"📊 Excel экспорт {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
            stats = db.get_stats()
            text = f"""👑 <b>Админ-панель</b>

👥 Пользователей: {stats['unique_users']}
🎮 Аккаунтов: {stats['total_accounts']}"""
            await callback.message.edit_text(text, reply_markup=get_admin_kb())
        except Exception as e:
            logger.error(f"Ошибка отправки Excel: {e}")
            await callback.message.edit_text(f"❌ Ошибка: {e}", reply_markup=get_admin_kb())
    else:
        await callback.message.edit_text("❌ Ошибка создания файла", reply_markup=get_admin_kb())

    await callback.answer()
    
@router.callback_query(F.data == "admin_search")
async def admin_search(callback: CallbackQuery, state: FSMContext):
    """Запуск поиска"""
    print("\n" + "="*50)
    print("🔍🔍🔍 admin_search ВЫЗВАН! 🔍🔍🔍")
    print(f"   user_id = {callback.from_user.id}")
    print(f"   is_admin = {is_admin(callback.from_user.id)}")
    
    if not is_admin(callback.from_user.id):
        print("❌ ДОСТУП ЗАПРЕЩЕН")
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    # Очищаем предыдущее состояние
    await state.clear()
    print("✅ Состояние очищено")
    
    # Устанавливаем новое состояние
    await state.set_state(EditState.waiting_search_query)
    current_state = await state.get_state()
    print(f"✅ Установлено состояние: {current_state}")
    
    # Отправляем сообщение с просьбой ввести ник
    await callback.message.edit_text(
        "🔍 <b>Поиск</b>\n\nВведите ник или ID для поиска:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_back")]
        ])
    )
    await callback.answer()
    print("✅ Сообщение отправлено")
    print("="*50)

@router.message(EditState.waiting_search_query)
async def process_search(message: Message, state: FSMContext):
    """Обработка поискового запроса"""
    print("\n" + "!"*50)
    print("🔴🔴🔴 process_search ВЫЗВАН! 🔴🔴🔴")
    print(f"   Текст: '{message.text}'")
    print(f"   User ID: {message.from_user.id}")
    print(f"   Is admin: {is_admin(message.from_user.id)}")
    print(f"   Chat ID: {message.chat.id}")
    print(f"   Message ID: {message.message_id}")
    
    # Проверяем текущее состояние
    current_state = await state.get_state()
    print(f"📊 Текущее состояние FSM: {current_state}")
    
    if not is_admin(message.from_user.id):
        print("❌ ДОСТУП ЗАПРЕЩЕН - не админ")
        await state.clear()
        return

    if current_state != EditState.waiting_search_query:
        print(f"❌ Неверное состояние: {current_state}")
        # Если состояние не то, пробуем всё равно обработать
        print("⚠️ Пробуем обработать несмотря на состояние...")

    query = message.text.strip()
    print(f"📝 Поисковый запрос: '{query}'")

    if len(query) < 2:
        print("❌ Слишком короткий запрос")
        await message.answer("❌ Минимум 2 символа для поиска")
        return

    # Получаем все аккаунты
    print("📊 Получаем все аккаунты из БД...")
    accounts = db.get_all_accounts()
    print(f"📊 Всего аккаунтов в БД: {len(accounts)}")
    
    results = []
    
    # Поиск по нику или ID
    for acc in accounts:
        nick = acc.get('game_nickname', '')
        user_id = str(acc.get('user_id', ''))
        
        if query.lower() in nick.lower() or query in user_id:
            results.append(acc)
            print(f"✅ Найдено совпадение: {nick} (ID: {user_id})")

    print(f"📊 Найдено результатов: {len(results)}")

    if not results:
        print("❌ Ничего не найдено")
        await message.answer(f"❌ Ничего не найдено по запросу: {query}")
        await state.clear()
        return

    # Формируем текст результатов
    text = f"🔍 <b>Результаты поиска:</b> {query}\n"
    text += f"Найдено: {len(results)}\n\n"
    
    # Показываем первые 10 результатов
    text += format_accounts_table(results[:10])

    if len(results) > 10:
        text += f"\n...и еще {len(results) - 10}"

    # Кнопки для быстрого удаления найденных
    buttons = []
    for acc in results[:5]:
        nick = acc.get('game_nickname', '—')
        if len(nick) > 20:
            nick = nick[:17] + '...'
        acc_id = acc.get('id')
        if acc_id:
            buttons.append([
                InlineKeyboardButton(
                    text=f"🗑️ {nick}",
                    callback_data=f"admin_del_{acc_id}_1"
                )
            ])

    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")])

    print("📤 Отправляем результаты...")
    await safe_send(message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    
    # Очищаем состояние
    await state.clear()
    print("✅ Состояние очищено")
    print("!"*50)

@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    stats = db.get_stats()

    try:
        db_size = db.db_path.stat().st_size / 1024
        exports = len(list(EXPORT_DIR.glob("export_*.csv")))
        backups = len(list(BACKUP_DIR.glob("backup_*.db")))
    except:
        db_size = exports = backups = 0

    if PSUTIL_AVAILABLE:
        try:
            memory = psutil.Process().memory_info().rss / 1024 / 1024
            cpu = psutil.Process().cpu_percent()
            mem_info = f"\n💻 Память: {memory:.1f} MB\n⚙️ CPU: {cpu}%"
        except:
            mem_info = ""
    else:
        mem_info = ""

    text = f"""📊 <b>Статистика</b>

👥 Пользователей: {stats['unique_users']}
🎮 Аккаунтов: {stats['total_accounts']}
📈 В среднем: {stats['avg_accounts_per_user']}

💾 <b>Ресурсы:</b>
📁 БД: {db_size:.1f} KB
📤 Экспортов: {exports}
💾 Бэкапов: {backups}{mem_info}

🏠 Среда: Bothost.ru"""

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_stats")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
        ])
    )
    await callback.answer()

@router.callback_query(F.data == "admin_cleanup")
async def admin_cleanup(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    db.cleanup_old_files(14)

    exports = len(list(EXPORT_DIR.glob("export_*.csv")))
    backups = len(list(BACKUP_DIR.glob("backup_*.db")))

    await callback.message.edit_text(
        f"🧹 <b>Очистка завершена</b>\n\n"
        f"📤 Экспортов: {exports}\n"
        f"💾 Бэкапов: {backups}\n\n"
        f"<i>Удалены файлы старше 14 дней</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗄️ Управление БД", callback_data="db_management")]
        ])
    )
    await callback.answer("✅ Готово")

@router.callback_query(F.data == "admin_refresh")
async def admin_refresh(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    stats = db.get_stats()
    text = f"""👑 <b>Админ-панель</b>

👥 Пользователей: {stats['unique_users']}
🎮 Аккаунтов: {stats['total_accounts']}"""

    await callback.message.edit_text(text, reply_markup=get_admin_kb())
    await callback.answer("🔄 Обновлено")

@router.callback_query(F.data == "admin_back")
async def admin_back(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    stats = db.get_stats()
    text = f"""👑 <b>Админ-панель</b>

👥 Пользователей: {stats['unique_users']}
🎮 Аккаунтов: {stats['total_accounts']}"""

    await callback.message.edit_text(text, reply_markup=get_admin_kb())
    await callback.answer()

@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()

@router.callback_query(F.data.startswith("restore_"))
async def handle_restore_choice(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора админа при запуске"""
    print(f"\n🔵🔵🔵 handle_restore_choice: {callback.data} 🔵🔵🔵")
    
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    await callback.answer()
    
    if callback.data == "restore_from_pc":
        # Загрузка с ПК
        await callback.message.edit_text(
            "📤 <b>Загрузка бэкапа с компьютера</b>\n\n"
            "1️⃣ Нажмите на скрепку 📎\n"
            "2️⃣ Выберите 'Документ'\n"
            "3️⃣ Найдите файл .db на вашем компьютере\n"
            "4️⃣ Отправьте его\n\n"
            "⚠️ <b>Внимание!</b> Текущая база будет заменена!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Отмена", callback_data="restore_cancel")]
            ])
        )
        await state.set_state(EditState.waiting_for_backup)
        await state.update_data(restore_mode="startup")
        
    elif callback.data == "restore_from_backup":
        # Восстановление из существующего бэкапа
        backups = sorted(BACKUP_DIR.glob("backup_*.db"), key=os.path.getmtime, reverse=True)
        root_backups = sorted(BASE_DIR.glob("backup_*.db"), key=os.path.getmtime, reverse=True)
        all_backups = backups + root_backups
        
        if not all_backups:
            await callback.message.edit_text(
                "❌ Нет доступных бэкапов.\n\n"
                "Будет создана новая пустая БД.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🆕 Создать новую БД", callback_data="restore_new_db")]
                ])
            )
            return
        
        buttons = []
        for i, backup in enumerate(all_backups[:5]):
            try:
                mtime = backup.stat().st_mtime
                date_str = datetime.fromtimestamp(mtime).strftime('%d.%m.%Y %H:%M')
                size = backup.stat().st_size / 1024
                buttons.append([
                    InlineKeyboardButton(
                        text=f"📅 {date_str} ({size:.1f} KB)",
                        callback_data=f"restore_backup_file_{backup.name}"
                    )
                ])
            except:
                pass
        
        buttons.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data="restore_cancel")])
        
        await callback.message.edit_text(
            "📥 <b>Выберите бэкап для восстановления:</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )
        
    elif callback.data == "restore_new_db":
        # Создание новой БД
        await callback.message.edit_text("🆕 Создаю новую пустую базу данных...")
        db._connect()
        db._create_tables()
        await callback.message.edit_text(
            "✅ Создана новая пустая база данных.\n\n"
            "Бот продолжает работу."
        )
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    "🤖 Бот готов к работе!",
                    reply_markup=get_main_kb(admin_id)
                )
            except:
                pass
                
    elif callback.data.startswith("restore_backup_file_"):
        backup_name = callback.data.replace("restore_backup_file_", "")
        backup_path = BACKUP_DIR / backup_name if (BACKUP_DIR / backup_name).exists() else BASE_DIR / backup_name
        
        await callback.message.edit_text("🔄 Восстановление...")
        
        try:
            current_backup = BACKUP_DIR / f"before_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            if db.db_path.exists():
                shutil.copy2(db.db_path, current_backup)
            
            db.close()
            shutil.copy2(backup_path, db.db_path)
            db._connect()
            
            if db.check_integrity():
                accounts = db.get_all_accounts()
                await callback.message.edit_text(
                    f"✅ База данных восстановлена из {backup_name}\n\n"
                    f"📊 Загружено {len(accounts)} аккаунтов\n\n"
                    f"🤖 Бот готов к работе!"
                )
                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(
                            admin_id,
                            "🤖 Бот готов к работе!",
                            reply_markup=get_main_kb(admin_id)
                        )
                    except:
                        pass
            else:
                await callback.message.edit_text(
                    "❌ Ошибка: восстановленный файл поврежден.\n\n"
                    "Будет создана новая пустая БД."
                )
                db._connect()
                db._create_tables()
        except Exception as e:
            await callback.message.edit_text(f"❌ Ошибка: {e}")
            db._connect()
            db._create_tables()
    
    elif callback.data == "restore_cancel":
        # Отмена - создаем новую БД
        await callback.message.edit_text("🆕 Создаю новую пустую базу данных...")
        db._connect()
        db._create_tables()
        await callback.message.edit_text(
            "✅ Создана новая пустая база данных.\n\n"
            "Бот продолжает работу."
        )
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    "🤖 Бот готов к работе!",
                    reply_markup=get_main_kb(admin_id)
                )
            except:
                pass

# ========== ПРОВЕРКА БД ПРИ ЗАПУСКЕ ==========
async def notify_admin(message: str):
    """Отправляет уведомление админам"""
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, message)
        except:
            pass

async def ask_admin_what_to_do():
    """Спрашивает админа, что делать с пустой БД"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📤 Загрузить с ПК", callback_data="restore_from_pc"),
            InlineKeyboardButton(text="💾 Из бэкапа", callback_data="restore_from_backup")
        ],
        [
            InlineKeyboardButton(text="🆕 Начать с нуля", callback_data="restore_new_db")
        ]
    ])
    
    # Отправляем сообщение всем админам
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                "⚠️ <b>База данных пуста или отсутствует!</b>\n\n"
                "Выберите действие:",
                reply_markup=keyboard
            )
        except:
            pass

async def check_database_on_startup():
    """Проверяет БД при запуске и спрашивает админа, что делать"""
    global cancel_restore
    
    # Проверяем существует ли файл БД
    db_exists = db.db_path.exists()
    db_size = db.db_path.stat().st_size if db_exists else 0
    db_empty = not db_exists or db_size == 0
    
    # Подключаемся к БД если файл существует
    if db_exists and not db_empty:
        db._connect()
    
    # Проверяем есть ли данные в текущей БД
    has_data = False
    if db_exists and not db_empty:
        try:
            accounts = db.get_all_accounts()
            has_data = len(accounts) > 0
        except:
            has_data = False
    
    # Проверяем есть ли бэкапы
    backups = sorted(BACKUP_DIR.glob("backup_*.db"), key=os.path.getmtime, reverse=True)
    has_backups = len(backups) > 0
    
    # Логируем информацию
    print(f"\n📊 ПРОВЕРКА БД:")
    print(f"   Файл существует: {db_exists}")
    print(f"   Размер: {db_size} байт")
    print(f"   Есть данные: {has_data}")
    print(f"   Бэкапов найдено: {len(backups)}")
    
    # Если БД пустая или не существует
    if not has_data:
        print("⚠️ БД пустая или отсутствует!")
        
        if ADMIN_IDS:
            # Спрашиваем админа, что делать
            print("👑 Спрашиваю админов...")
            await ask_admin_what_to_do()
            
            # Ждем ответа от админа (бот продолжит работу, а выбор обработается в колбэке)
            print("⏳ Ожидание выбора админа...")
        else:
            # Если админов нет - создаем новую БД
            print("⚠️ Админы не настроены. Создаю новую пустую БД...")
            db._connect()
            db._create_tables()
    else:
        print(f"✅ БД в порядке. Данных: {len(db.get_all_accounts())} аккаунтов")
    
    print("-" * 50)

# ========== ЗАПУСК ==========
async def main():
    print("=" * 50)
    print("🚀 ЗАПУСК БОТА НА BOTHOST.RU")
    print("=" * 50)
    print(f"💾 БД: {db.db_path}")
    print(f"👑 Админы: {ADMIN_IDS}")
    print(f"🎯 Чат: {TARGET_CHAT_ID}")
    print(f"📌 Тема: {TARGET_TOPIC_ID if USE_TOPIC else 'нет'}")
    print("-" * 50)

    # Проверяем БД при запуске
    await check_database_on_startup()

    stats = db.get_stats()
    print(f"📊 Итог: Пользователей: {stats['unique_users']}, Аккаунтов: {stats['total_accounts']}")
    print("-" * 50)

    # Очистка старых файлов
    db.cleanup_old_files(14)

    print("📡 Режим: Polling")
    
    try:
        await dp.start_polling(bot)
    finally:
        db.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        traceback.print_exc()
    finally:
        try:
            db.close()
        except:
            pass
        print("👋 Завершение работы")

