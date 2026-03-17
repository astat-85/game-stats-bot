"""
Клавиатуры для раздела профиля
"""
from aiogram.types import (
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton
)

def get_profile_menu_keyboard(has_profile: bool = False) -> InlineKeyboardMarkup:
    """
    Меню профиля
    """
    if has_profile:
        buttons = [
            [InlineKeyboardButton(text="👤 Просмотр профиля", callback_data="profile_view")],
            [InlineKeyboardButton(text="✏️ Редактировать", callback_data="profile_edit")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")]
        ]
    else:
        buttons = [
            [InlineKeyboardButton(text="📝 Заполнить профиль", callback_data="profile_fill")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")]
        ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_edit_profile_keyboard() -> InlineKeyboardMarkup:
    """
    Клавиатура для выбора поля редактирования
    """
    buttons = [
        [InlineKeyboardButton(text="👤 ФИО", callback_data="edit_name")],
        [InlineKeyboardButton(text="🏙 Город", callback_data="edit_city")],
        [InlineKeyboardButton(text="📅 Дата рождения", callback_data="edit_birthday")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile_view")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_city_choice_keyboard(cities: list) -> InlineKeyboardMarkup:
    """
    Клавиатура для выбора города из нескольких вариантов
    """
    buttons = []
    for city in cities[:5]:  # максимум 5 вариантов
        text = f"{city['name']}, {city['region']['name']}"
        callback = f"city_{city['name']}_{city['region']['name']}"
        buttons.append([InlineKeyboardButton(text=text, callback_data=callback)])
    
    buttons.append([InlineKeyboardButton(text="🔍 Ввести заново", callback_data="city_retry")])
    buttons.append([InlineKeyboardButton(text="⏭ Пропустить (МСК)", callback_data="city_skip")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_skip_keyboard() -> ReplyKeyboardMarkup:
    """
    Reply клавиатура с кнопками пропуска и отмены
    """
    buttons = [
        [KeyboardButton(text="⏭ Пропустить")],
        [KeyboardButton(text="🚫 Отмена")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
