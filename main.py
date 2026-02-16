#!/usr/bin/env python3
"""
Telegram Bot для сбора игровых данных
АДАПТИРОВАНО ДЛЯ BOTHOST.RU
ИСПРАВЛЕННАЯ НАВИГАЦИЯ И ТАБЛИЦЫ
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

    try:
        from aiogram.enums import ParseMode
        PARSE_MODE = ParseMode.HTML
    except ImportError:
        PARSE_MODE = 'HTML'

    import aiogram
    if aiogram.__version__.startswith('3'):
        try:
            from aiogram.client.default import DefaultBotProperties
            bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=PARSE_MODE))
        except ImportError:
            bot = Bot(token=BOT_TOKEN, parse_mode=PARSE_MODE)
    else:
        bot = Bot(token=BOT_TOKEN, parse_mode=PARSE_MODE)

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

# ========== RATE LIMITER ==========
class RateLimiter:
    def __init__(self):
        self.requests = defaultdict(list)

    def is_limited(self, user_id: int, is_admin: bool = False) -> bool:
        now = datetime.now()
        limit = 30 if is_admin else 10
        window = timedelta(seconds=60)

        self.requests[user_id] = [t for t in self.requests[user_id] if now - t < window]

        if len(self.requests[user_id]) >= limit:
            return True

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
        self.cache_ttl = 60
        self.last_cache_update = 0
        self.change_counter = 0
        self.last_vacuum = datetime.now()

        self.conn = None
        self.cursor = None
        self._connect()

        if not self.db_path.exists():
            print(f"📁 Создана новая БД: {self.db_path}")

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
        try:
            self._execute("SELECT * FROM users WHERE id = ?", (account_id,))
            row = self.cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка get_account_by_id: {e}")
            return None

    def is_nickname_taken(self, user_id: int, nickname: str, exclude_id: int = None) -> bool:
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
        try:
            self._execute("""
            SELECT
                id, user_id, username,
                COALESCE(game_nickname, '') as nick,
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
        try:
            if not filename:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"backup_{timestamp}.db"

            filepath = BACKUP_DIR / filename

            with self.lock:
                self.conn.commit()
                shutil.copy2(self.db_path, filepath)

            logger.info(f"✅ Бэкап: {filepath}")

            backups = sorted(BACKUP_DIR.glob("backup_*.db"))
            if len(backups) > 10:
                for old in backups[:-10]:
                    old.unlink()

            return str(filepath)
        except Exception as e:
            logger.error(f"❌ Ошибка бэкапа: {e}")
            return None

    def export_to_csv(self, filename: str = None) -> Optional[str]:
        try:
            if not filename:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"export_{timestamp}.csv"

            filepath = EXPORT_DIR / filename
            accounts = self.get_all_accounts()

            if not accounts:
                return None

            with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow([
                    "№", "Ник в игре", "Эл", "БМ", "Пл 1", "Пл 2", "Пл 3",
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

                    bm = acc.get('bm', '')
                    if bm and bm != '—' and ',' not in bm:
                        bm = f"{bm},0"

                    pl1 = acc.get('pl1', '')
                    if pl1 and pl1 != '—' and ',' not in pl1:
                        pl1 = f"{pl1},0"

                    pl2 = acc.get('pl2', '')
                    if pl2 and pl2 != '—' and ',' not in pl2:
                        pl2 = f"{pl2},0"

                    pl3 = acc.get('pl3', '')
                    if pl3 and pl3 != '—' and ',' not in pl3:
                        pl3 = f"{pl3},0"

                    writer.writerow([
                        i,
                        acc.get('nick', ''),
                        acc.get('power', ''),
                        bm,
                        pl1,
                        pl2,
                        pl3,
                        acc.get('dragon', ''),
                        acc.get('buffs_stands', ''),
                        acc.get('buffs_research', ''),
                        f"@{acc.get('username', '')}" if acc.get('username') else '',
                        acc.get('user_id', ''),
                        time_str,
                        date_str
                    ])

            logger.info(f"✅ Экспорт: {filepath}")
            return str(filepath)
        except Exception as e:
            logger.error(f"❌ Ошибка экспорта: {e}")
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
        try:
            self._execute("PRAGMA integrity_check")
            return self.cursor.fetchone()[0] == "ok"
        except:
            return False

    def maybe_vacuum(self):
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
                self.conn.commit()
                self.conn.close()
        except:
            pass

db = Database()

# ========== FSM ==========
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

class EditState(StatesGroup):
    waiting_field_value = State()
    step_by_step = State()
    waiting_search_query = State()
    waiting_batch_delete = State()
    waiting_for_backup = State()
    
# ========== ОТЛАДКА ==========
# @router.callback_query()
# async def debug_all_callbacks(callback: CallbackQuery):
#     """Отлавливает ВСЕ callback для отладки"""
#     print(f"\n🔍🔍🔍 ПОЛУЧЕН CALLBACK: {callback.data} 🔍🔍🔍")
#     print(f"   От пользователя: {callback.from_user.id}")
#     print(f"   Админ? {is_admin(callback.from_user.id)}")
#     print(f"   Сообщение: {callback.message.text[:50] if callback.message.text else 'Нет текста'}")
#     await callback.answer(f"Получен callback: {callback.data}", show_alert=False)

# ========== ОБРАБОТЧИК НЕИЗВЕСТНЫХ CALLBACK ==========
# @router.callback_query()
# async def unknown_callback(callback: CallbackQuery):
#     """Обработчик неизвестных callback_data"""
#     logger.warning(f"Неизвестный callback: {callback.data}")
#     await callback.answer("❌ Неизвестная команда", show_alert=True)

# ========== КЛАВИАТУРЫ ==========
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def get_main_kb(user_id: int) -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="📊 Мои аккаунты"), KeyboardButton(text="📤 Отправить в группу")]
    ]
    if is_admin(user_id):
        kb.append([KeyboardButton(text="👑 Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_numeric_kb(decimal: bool = True) -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
        [KeyboardButton(text="4"), KeyboardButton(text="5"), KeyboardButton(text="6")],
        [KeyboardButton(text="7"), KeyboardButton(text="8"), KeyboardButton(text="9")],
        [KeyboardButton(text="0"), KeyboardButton(text=","), KeyboardButton(text="⌫")],
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
        buttons.append([InlineKeyboardButton(
            text=f"👤 {nick[:20]}",
            callback_data=f"select_{acc['id']}"
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
        buttons.append([InlineKeyboardButton(
            text=f"📤 {nick[:20]}",
            callback_data=f"send_{acc['id']}"
        )])
    buttons.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Таблица", callback_data="admin_table_1")],
        [InlineKeyboardButton(text="📤 Экспорт CSV", callback_data="admin_export")],
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
        num = min(int(val), 99)
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
        num = min(num, 999.9)
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
        num = min(num, 999.9)
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
        num = min(int(val), 99)
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
        num = min(int(val), 9)
        return str(num)
    except:
        return '—'

def format_accounts_table(accounts: List[Dict], start: int = 0) -> str:
    text = "<code>\n"
    for i, acc in enumerate(accounts, start + 1):
        nick = acc.get('game_nickname', acc.get('nick', '—'))
        if not isinstance(nick, str):
            nick = str(nick) if nick is not None else '—'
        nick = html.escape(nick)
        if len(nick) > 20:
            nick = nick[:17] + '...'

        text += f"{i:2d}. {nick}\n"
        text += f"    ⚡️{format_power(acc.get('power', '—'))} "
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

# ========== SAFE SEND ==========
async def safe_send(obj, text: str, **kwargs):
    MAX_LEN = 4096

    try:
        if len(text) <= MAX_LEN:
            if isinstance(obj, Message):
                await obj.answer(text, **kwargs)
            else:
                try:
                    await obj.message.edit_text(text, **kwargs)
                except:
                    await obj.message.answer(text, **kwargs)
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
                    except:
                        await obj.message.answer(part, **kwargs)
                else:
                    if isinstance(obj, Message):
                        await obj.answer(part, **kwargs)
                    else:
                        await obj.message.answer(part, **kwargs)
    except Exception as e:
        logger.error(f"Safe send error: {e}")

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
    await state.clear()
    await message.answer("❌ Отменено", reply_markup=get_main_kb(message.from_user.id))

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
        step_temp="",
        show_task=None
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
    elif field in ["power", "dragon", "stands", "research"]:
        hint = "💡 Вводите только целые числа (например: 1500)"

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
        prompt = f"📝 Введите целое число для поля «{name}»:"
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

    if message.text in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ","]:
        if message.text == ",":
            if "," not in step_temp:
                step_temp += ","
        else:
            step_temp += message.text
        await state.update_data(step_temp=step_temp)
        current_task = data.get("show_task")
        if current_task:
            current_task.cancel()

        async def show_value():
            await asyncio.sleep(0.5)
            new_data = await state.get_data()
            new_temp = new_data.get("step_temp", "")
            if new_temp == step_temp:
                await message.answer(f"📝 Текущее значение: {step_temp}")

        task = asyncio.create_task(show_value())
        await state.update_data(show_task=task)
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
        value = message.text.strip()

    if not value:
        await message.answer("❌ Значение не может быть пустым. Введите число или нажмите «⏭ Пропустить»")
        return

    if field in ["power", "bm", "dragon", "stands", "research", "pl1", "pl2", "pl3"]:
        value = value.replace('.', ',')

        if field in ["bm", "pl1", "pl2", "pl3"]:
            parts = value.split(',')
            if len(parts) > 2:
                await message.answer(
                    "❌ Неверный формат. Используйте: 12,5 или 15",
                    reply_markup=get_numeric_kb(decimal=True)
                )
                return
            if not parts[0].isdigit():
                await message.answer(
                    "❌ Введите число (целую часть)",
                    reply_markup=get_numeric_kb(decimal=True)
                )
                return
            if len(parts) == 2 and not parts[1].isdigit():
                await message.answer(
                    "❌ Дробная часть должна содержать только цифры",
                    reply_markup=get_numeric_kb(decimal=True)
                )
                return
        else:
            if not value.replace(',', '').isdigit():
                await message.answer(
                    "❌ Введите целое число",
                    reply_markup=get_numeric_kb(decimal=False)
                )
                return
            value = value.replace(',', '')

    step_data[field] = value
    await message.answer(f"✅ {field_name}: {value}")

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
    user_id = message.from_user.id
    username = message.from_user.username or f"user_{user_id}"
    data = await state.get_data()
    field = data.get("field")
    new = data.get("new", False)
    account_id = data.get("account_id")
    temp = data.get("temp", "")

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

    if message.text in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ","]:
        if message.text == ",":
            if "," not in temp:
                temp += ","
        else:
            temp += message.text
        await state.update_data(temp=temp)
        current_task = data.get("show_task")
        if current_task:
            current_task.cancel()

        async def show_value():
            await asyncio.sleep(0.5)
            new_data = await state.get_data()
            new_temp = new_data.get("temp", "")
            if new_temp == temp:
                await message.answer(f"📝 Текущее значение: {temp}")

        task = asyncio.create_task(show_value())
        await state.update_data(show_task=task)
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
        value = message.text.strip()

    field_name = FIELD_FULL_NAMES.get(field, field)

    if field == "nick":
        if not value:
            await message.answer("❌ Ник не может быть пустым", reply_markup=get_cancel_kb())
            return

        if len(value) < 2 or len(value) > 50:
            await message.answer("❌ Ник должен быть от 2 до 50 символов", reply_markup=get_cancel_kb())
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

    if field in ["power", "bm", "dragon", "stands", "research", "pl1", "pl2", "pl3"]:
        if value:
            value = value.replace('.', ',')

            if field in ["bm", "pl1", "pl2", "pl3"]:
                parts = value.split(',')
                if len(parts) > 2:
                    await message.answer(
                        "❌ Неверный формат. Используйте: 12,5 или 15",
                        reply_markup=get_numeric_kb(decimal=True)
                    )
                    return
                if not parts[0].isdigit():
                    await message.answer(
                        "❌ Введите число (целую часть)",
                        reply_markup=get_numeric_kb(decimal=True)
                    )
                    return
                if len(parts) == 2 and not parts[1].isdigit():
                    await message.answer(
                        "❌ Дробная часть должна содержать только цифры",
                        reply_markup=get_numeric_kb(decimal=True)
                    )
                    return
            else:
                if not value.replace(',', '').isdigit():
                    await message.answer(
                        "❌ Введите целое число",
                        reply_markup=get_numeric_kb(decimal=False)
                    )
                    return
                value = value.replace(',', '')

    if account_id:
        account = db.get_account_by_id(account_id)
        if account:
            db.create_or_update_account(user_id, username, account['game_nickname'], field, value)
            display = value if value else 'пусто'
            await message.answer(
                f"✅ {field_name}: {display}",
                reply_markup=get_main_kb(user_id)
            )

    await state.clear()

# ========== ОБРАБОТКА ФАЙЛОВ ==========
@router.message(EditState.waiting_for_backup, F.document)
async def handle_backup_file(message: Message, state: FSMContext):
    """Обработка загруженного файла бэкапа"""
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    
    if not message.document.file_name.endswith('.db'):
        await message.answer("❌ Нужен файл с расширением .db")
        await state.clear()
        return
    
    await message.answer("🔄 Загружаю и восстанавливаю бэкап...")
    
    try:
        file = await bot.get_file(message.document.file_id)
        downloaded_file = await bot.download_file(file.file_path)
        
        temp_path = BACKUP_DIR / f"restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        with open(temp_path, 'wb') as f:
            f.write(downloaded_file.getvalue())
        
        current_backup = BACKUP_DIR / f"before_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2(db.db_path, current_backup)
        
        db.close()
        shutil.copy2(temp_path, db.db_path)
        db._connect()
        
        if db.check_integrity():
            accounts = db.get_all_accounts()
            if accounts:
                await message.answer(
                    f"✅ База данных восстановлена!\n\n"
                    f"📊 Загружено {len(accounts)} аккаунтов\n"
                    f"💾 Предыдущая БД сохранена как: {current_backup.name}\n\n"
                    f"👑 Нажмите /admin для проверки"
                )
            else:
                shutil.copy2(current_backup, db.db_path)
                db._connect()
                await message.answer(
                    "❌ В загруженном файле нет данных.\n"
                    "База возвращена к предыдущему состоянию."
                )
        else:
            shutil.copy2(current_backup, db.db_path)
            db._connect()
            await message.answer(
                "❌ Загруженный файл поврежден.\n"
                "База возвращена к предыдущему состоянию."
            )
        
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        try:
            db._connect()
        except:
            pass
    finally:
        await state.clear()

# ========== ОБЩИЙ ХЕНДЛЕР ==========
@router.message(F.chat.type == "private")
async def any_message(message: Message, state: FSMContext):
    current_state = await state.get_state()
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
    await callback.message.edit_text(
        "➕ <b>Создание аккаунта</b>\n\nВведите игровой ник:"
    )
    await callback.message.answer(
        "📝 Введите ник (2-50 символов):",
        reply_markup=get_cancel_kb()
    )
    await state.set_state(EditState.waiting_field_value)
    await state.update_data(
        field="nick",
        new=True,
        first=len(db.get_user_accounts(callback.from_user.id)) == 0,
        temp="",
        show_task=None
    )
    await callback.answer()

@router.callback_query(F.data.startswith("select_"))
async def select_account(callback: CallbackQuery):
    account_id = int(callback.data.split("_")[1])
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
    account_id = int(callback.data.split("_")[2])
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
        temp="",
        show_task=None
    )
    await callback.answer()

@router.callback_query(F.data.startswith("edit_"))
async def edit_account(callback: CallbackQuery):
    account_id = int(callback.data.split("_")[1])
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
    account_id = int(parts[1])
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
            "📝 Введите число (можно с запятой):",
            reply_markup=get_numeric_kb(decimal=True)
        )
    elif field in ["power", "dragon", "stands", "research"]:
        await callback.message.answer(
            "📝 Введите целое число:",
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
        temp="",
        show_task=None
    )
    await callback.answer()

@router.callback_query(F.data.startswith("delete_"))
async def delete_account(callback: CallbackQuery):
    account_id = int(callback.data.split("_")[1])
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
    account_id = int(callback.data.split("_")[2])
    account = db.get_account_by_id(account_id)

    if not account:
        await callback.answer("❌ Аккаунт не найден", show_alert=True)
        return

    if db.delete_account(account_id):
        db.invalidate_cache()
        remaining_accounts = db.get_user_accounts(callback.from_user.id)

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

    account_id = int(callback.data.split("_")[1])
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

# ========== НОВЫЕ АДМИН ХЕНДЛЕРЫ ==========
@router.callback_query(F.data == "db_management")
async def db_management_menu(callback: CallbackQuery):
    """Меню управления базой данных"""
    print(f"\n🔴🔴🔴 НАЧАЛО db_management_menu 🔴🔴🔴")
    print(f"   Пользователь: {callback.from_user.id}")
    print(f"   Админ? {is_admin(callback.from_user.id)}")
    
    if not is_admin(callback.from_user.id):
        print(f"   ❌ ДОСТУП ЗАПРЕЩЕН")
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    print(f"   ✅ ДОСТУП РАЗРЕШЕН")
    await callback.answer("✅ Загружаю меню...")
    
    print(f"   Получаем статистику...")
    stats = db.get_stats()
    print(f"   Статистика: {stats}")
    
    try:
        db_size = db.db_path.stat().st_size / 1024
        backups = len(list(BACKUP_DIR.glob("backup_*.db")))
        exports = len(list(EXPORT_DIR.glob("export_*.csv")))
        print(f"   Размер БД: {db_size:.1f} KB, Бэкапов: {backups}, Экспортов: {exports}")
    except Exception as e:
        print(f"   ❌ Ошибка получения размеров: {e}")
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

<i>📤 Экспорт CSV находится в главном меню админки для быстрого доступа</i>
"""
    
    print(f"   Получаем клавиатуру...")
    kb = get_db_management_kb()
    
    print(f"   Кнопки в меню:")
    for i, row in enumerate(kb.inline_keyboard):
        for j, btn in enumerate(row):
            print(f"     [{i},{j}] {btn.text} : callback_data='{btn.callback_data}'")
    
    print(f"   Редактируем сообщение...")
    try:
        await callback.message.edit_text(text, reply_markup=kb)
        print(f"   ✅ Сообщение обновлено")
    except Exception as e:
        print(f"   ❌ Ошибка при edit_text: {e}")
        try:
            await callback.message.answer(text, reply_markup=kb)
            print(f"   ✅ Отправлено новое сообщение")
        except Exception as e2:
            print(f"   ❌❌ КРИТИЧЕСКАЯ ОШИБКА: {e2}")
    
    print(f"🔴🔴🔴 КОНЕЦ db_management_menu 🔴🔴🔴")
    await callback.answer()

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
    for i, backup in enumerate(all_backups[:5]):
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

@router.callback_query(F.data.startswith("db_restore_"))
async def db_restore_handler(callback: CallbackQuery):
    """Восстановление из выбранного бэкапа"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    backup_name = callback.data.replace("db_restore_", "")
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

@router.callback_query(F.data.startswith("db_restore_confirm_"))
async def db_restore_confirm(callback: CallbackQuery):
    """Подтвержденное восстановление бэкапа"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    backup_name = callback.data.replace("db_restore_confirm_", "")
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

@router.callback_query(F.data == "db_restore_pc")
async def db_restore_pc_callback(callback: CallbackQuery, state: FSMContext):
    """Обработка кнопки загрузки с ПК (работает как /restore)"""
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return
    
    await callback.answer()
    await callback.message.delete()
    
    await callback.message.answer(
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
    
    await state.set_state(EditState.waiting_for_backup)
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

    per_page = 10
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
        InlineKeyboardButton(text="🗑️ Пакетно", callback_data="admin_batch")
    ])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")])

    await safe_send(callback, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()

@router.callback_query(F.data.startswith("confirm_del_"))
async def confirm_del(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    parts = callback.data.split("_")
    account_id = int(parts[2])
    page = int(parts[3])

    account = db.get_account_by_id(account_id)

    if not account:
        await callback.answer("❌ Аккаунт не найден", show_alert=True)
        return

    if db.delete_account(account_id):
        await callback.message.edit_text(
            f"✅ Аккаунт {account['game_nickname']} (ID:{account_id}) удален",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin_table_{page}")]
            ])
        )
    else:
        await callback.message.edit_text(
            "❌ Ошибка удаления",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin_table_{page}")]
            ])
        )
    await callback.answer()

@router.callback_query(F.data == "admin_batch")
async def admin_batch(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    await callback.message.edit_text(
        "🗑️ <b>Пакетное удаление</b>\n\n"
        "Введите ID аккаунтов через запятую или пробел:\n"
        "Пример: 123, 456, 789",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_back")]
        ])
    )
    await state.set_state(EditState.waiting_batch_delete)
    await callback.answer()

@router.message(EditState.waiting_batch_delete)
async def process_batch(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return

    ids = list(set(re.findall(r'\d+', message.text)))[:20]

    if not ids:
        await message.answer("❌ ID не найдены")
        await state.clear()
        return

    deleted = []
    failed = []
    not_found = []

    for id_str in ids:
        acc_id = int(id_str)
        acc = db.get_account_by_id(acc_id)
        if acc:
            if db.delete_account(acc_id):
                deleted.append(f"{acc_id} ({acc['game_nickname']})")
            else:
                failed.append(acc_id)
        else:
            not_found.append(acc_id)

    text = "🗑️ <b>Результат</b>\n\n"

    if deleted:
        text += f"✅ Удалено ({len(deleted)}):\n" + "\n".join(deleted[:10])
        if len(deleted) > 10:
            text += f"\n...и еще {len(deleted) - 10}\n"

    if failed:
        text += f"\n❌ Ошибка ({len(failed)}): {', '.join(map(str, failed[:10]))}\n"

    if not_found:
        text += f"\n🔍 Не найдены ({len(not_found)}): {', '.join(map(str, not_found[:10]))}\n"

    await message.answer(text, reply_markup=get_admin_kb())
    await state.clear()

@router.callback_query(F.data == "admin_show_delete_menu")
async def admin_show_delete_menu(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    accounts = db.get_all_accounts()

    if not accounts:
        await callback.answer("📋 Нет аккаунтов для удаления", show_alert=True)
        return

    try:
        page = int(callback.data.split("_")[4]) if len(callback.data.split("_")) > 4 else 1
    except:
        page = 1

    per_page = 10
    total_pages = (len(accounts) + per_page - 1) // per_page
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    end = min(start + per_page, len(accounts))

    text = f"🗑️ <b>Выберите аккаунт для удаления:</b> (стр. {page}/{total_pages})\n\n"

    buttons = []

    for i, acc in enumerate(accounts[start:end], start + 1):
        nick = acc.get('nick', '—')
        if len(nick) > 30:
            nick = nick[:27] + '...'
        buttons.append([
            InlineKeyboardButton(
                text=f"{i}. {nick}",
                callback_data=f"admin_del_{acc['id']}_1"
            )
        ])

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
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    try:
        page = int(callback.data.split("_")[5])
    except:
        page = 1

    new_callback = type('obj', (object,), {
        'from_user': callback.from_user,
        'data': f"admin_show_delete_menu_page_{page}",
        'message': callback.message,
        'answer': callback.answer
    })

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
            await callback.message.edit_text(f"❌ Ошибка: {e}", reply_markup=get_admin_kb())
    else:
        await callback.message.edit_text("❌ Ошибка создания файла", reply_markup=get_admin_kb())

    await callback.answer()

@router.callback_query(F.data == "admin_search")
async def admin_search(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("🚫 Доступ запрещен", show_alert=True)
        return

    await callback.message.edit_text(
        "🔍 <b>Поиск</b>\n\nВведите ник или ID:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_back")]
        ])
    )
    await state.set_state(EditState.waiting_search_query)
    await callback.answer()

@router.message(EditState.waiting_search_query)
async def process_search(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return

    query = message.text.strip()

    if len(query) < 2:
        await message.answer("❌ Минимум 2 символа")
        return

    accounts = db.get_all_accounts()
    results = []

    for acc in accounts:
        nick = acc.get('nick', '')
        user_id = str(acc.get('user_id', ''))
        if query.lower() in nick.lower() or query in user_id:
            results.append(acc)

    if not results:
        await message.answer(f"❌ Ничего не найдено: {query}")
        await state.clear()
        return

    text = f"🔍 <b>Результаты:</b> {query}\n\n"
    text += format_accounts_table(results[:10])

    if len(results) > 10:
        text += f"\n...и еще {len(results) - 10}"

    buttons = []
    for acc in results[:5]:
        nick = acc.get('nick', '—')
        if not isinstance(nick, str):
            nick = str(nick) if nick is not None else '—'
        nick = html.escape(nick)[:20]
        buttons.append([
            InlineKeyboardButton(
                text=f"🗑️ {nick}",
                callback_data=f"admin_del_{acc['id']}_1"
            )
        ])

    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")])

    await safe_send(message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await state.clear()

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

    text = f"""📊 <b>Статистика</b>

👥 Пользователей: {stats['unique_users']}
🎮 Аккаунтов: {stats['total_accounts']}
📈 В среднем: {stats['avg_accounts_per_user']}

💾 <b>Ресурсы:</b>
📁 БД: {db_size:.1f} KB
📤 Экспортов: {exports}
💾 Бэкапов: {backups}

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

# ========== ОБРАБОТЧИК НЕИЗВЕСТНЫХ CALLBACK ==========
@router.callback_query()
async def unknown_callback(callback: CallbackQuery):
    """Обработчик неизвестных callback_data"""
    logger.warning(f"Неизвестный callback: {callback.data}")
    await callback.answer("❌ Неизвестная команда", show_alert=True)

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

    if not db.check_integrity():
        print("⚠️ Проблемы с БД, восстановление...")
        if await asyncio.to_thread(db.restore_from_backup):
            print("✅ БД восстановлена")
        else:
            print("❌ Не удалось восстановить БД")

    stats = db.get_stats()
    print(f"📊 Пользователей: {stats['unique_users']}, Аккаунтов: {stats['total_accounts']}")
    print("-" * 50)

    await asyncio.to_thread(db.cleanup_old_files, 14)

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






