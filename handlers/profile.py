"""
Обработчики для раздела "Личные данные"
"""
import logging
from datetime import datetime

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton

from database.profile_db import ProfileDB
from utils.gender import detect_gender_by_name
from utils.date_parser import parse_birthday
from keyboards.profile import (
    get_profile_menu_keyboard,
    get_edit_profile_keyboard,
    get_city_choice_keyboard,
    get_skip_keyboard
)
from cities.city_db import CityDatabase

logger = logging.getLogger(__name__)

# Состояния FSM для заполнения профиля
class ProfileForm(StatesGroup):
    waiting_for_name = State()          # Ожидание ФИО
    waiting_for_city = State()           # Ожидание города
    waiting_for_birthday = State()       # Ожидание даты рождения
    waiting_for_confirm = State()        # Подтверждение данных

# Роутер для профиля
router = Router()
profile_db = ProfileDB()
city_db = CityDatabase()

# Вспомогательная функция для проверки подписки
async def check_subscription_wrapper(user_id: int) -> bool:
    """Обертка для проверки подписки (переиспользуем из main)"""
    # TODO: импортировать функцию check_subscription из main или передавать как параметр
    from main import check_subscription
    return await check_subscription(user_id)

@router.message(Command("profile"))
@router.message(F.text == "👤 Мой профиль")
async def cmd_profile(message: Message):
    """Показ профиля или предложение заполнить"""
    user_id = message.from_user.id
    
    # Проверяем подписку
    if not await check_subscription_wrapper(user_id):
        await message.answer(
            "❌ Для доступа к профилю нужно быть подписчиком группы",
            reply_markup=get_profile_menu_keyboard(has_profile=False)
        )
        return
    
    # Получаем данные профиля
    profile = profile_db.get_profile(user_id)
    
    if not profile:
        # Профиль не заполнен
        text = (
            "👤 <b>Мой профиль</b>\n\n"
            "У вас еще не заполнен профиль.\n"
            "Это поможет нам:\n"
            "• Обращаться к вам по имени\n"
            "• Показывать локальные предложения\n"
            "• Не беспокоить ночью\n\n"
            "Заполните профиль, это займет 1 минуту!"
        )
        await message.answer(
            text,
            reply_markup=get_profile_menu_keyboard(has_profile=False)
        )
    else:
        # Показываем заполненный профиль
        text = format_profile(profile)
        await message.answer(
            text,
            reply_markup=get_profile_menu_keyboard(has_profile=True),
            parse_mode="HTML"
        )

def format_profile(profile: dict) -> str:
    """Форматирует данные профиля для вывода"""
    # Формируем ФИО
    full_name = profile['first_name']
    if profile.get('last_name'):
        full_name = f"{profile['last_name']} {full_name}"
    if profile.get('middle_name'):
        full_name += f" {profile['middle_name']}"
    
    # Пол
    gender_text = "👨 Мужской" if profile['gender'] == 'male' else "👩 Женский" if profile['gender'] == 'female' else "—"
    
    # Дата рождения
    if profile.get('birth_day') and profile.get('birth_month'):
        birth = f"{profile['birth_day']:02d}.{profile['birth_month']:02d}"
        if profile.get('birth_year'):
            birth += f".{profile['birth_year']}"
            age = datetime.now().year - profile['birth_year']
            birth += f" ({age} лет)"
    else:
        birth = "—"
    
    # Город и часовой пояс
    location = profile.get('city', '—')
    if profile.get('region'):
        location += f", {profile['region']}"
    
    timezone_display = profile.get('timezone', 'Europe/Moscow').replace('Europe/', '').replace('Asia/', '')
    
    text = (
        f"👤 <b>Мой профиль</b>\n\n"
        f"<b>Имя:</b> {full_name}\n"
        f"<b>Пол:</b> {gender_text}\n"
        f"<b>Дата рождения:</b> {birth}\n"
        f"<b>Город:</b> {location}\n"
        f"<b>Часовой пояс:</b> {timezone_display}\n\n"
    )
    
    if profile.get('location_manually_set'):
        text += "<i>✅ Город указан вручную</i>\n"
    else:
        text += "<i>⏰ Часовой пояс: МСК (по умолчанию)</i>\n"
    
    return text

@router.callback_query(F.data == "profile_fill")
async def start_profile_fill(callback: CallbackQuery, state: FSMContext):
    """Начало заполнения профиля"""
    await callback.answer()
    
    await callback.message.edit_text(
        "📝 <b>Заполнение профиля</b>\n\n"
        "Введите ваше <b>имя</b> (обязательно) и, если хотите, "
        "фамилию и отчество.\n\n"
        "Примеры:\n"
        "• <i>Иван</i>\n"
        "• <i>Иван Петров</i>\n"
        "• <i>Иван Сергеевич Петров</i>"
    )
    await callback.message.answer(
        "📝 Введите ФИО:",
        reply_markup=get_skip_keyboard()
    )
    await state.set_state(ProfileForm.waiting_for_name)

@router.message(ProfileForm.waiting_for_name)
async def process_name(message: Message, state: FSMContext):
    """Обработка введенного ФИО"""
    text = message.text.strip()
    
    if not text or text == "🚫 Отмена":
        await message.answer("❌ Заполнение отменено", reply_markup=get_profile_menu_keyboard(has_profile=False))
        await state.clear()
        return
    
    # Разбиваем ФИО на части
    parts = text.split()
    
    data = {}
    if len(parts) == 1:
        # Только имя
        data['first_name'] = parts[0]
        data['last_name'] = None
        data['middle_name'] = None
    elif len(parts) == 2:
        # Имя + Фамилия
        data['first_name'] = parts[0]
        data['last_name'] = parts[1]
        data['middle_name'] = None
    elif len(parts) >= 3:
        # Имя + Отчество + Фамилия
        data['first_name'] = parts[0]
        data['middle_name'] = parts[1]
        data['last_name'] = ' '.join(parts[2:])
    else:
        await message.answer("❌ Слишком мало данных. Введите хотя бы имя.")
        return
    
    # Определяем пол по имени
    gender = detect_gender_by_name(data['first_name'])
    if gender:
        data['gender'] = gender
        gender_text = "мужской" if gender == 'male' else "женский"
        await message.answer(f"✅ Определен пол: {gender_text}")
    
    # Сохраняем в FSM
    await state.update_data(profile_data=data)
    
    # Переходим к вводу города
    await message.answer(
        "🏙 <b>Город</b>\n\n"
        "Введите ваш город (необязательно).\n"
        "Если укажете город, часовой пояс определится автоматически.\n\n"
        "Или нажмите <b>⏭ Пропустить</b> (будет установлен МСК)."
    )
    await state.set_state(ProfileForm.waiting_for_city)

@router.message(ProfileForm.waiting_for_city)
async def process_city(message: Message, state: FSMContext):
    """Обработка введенного города"""
    text = message.text.strip()
    
    if text == "⏭ Пропустить":
        # Пропускаем город, ставим МСК
        data = await state.get_data()
        profile_data = data.get('profile_data', {})
        profile_data['timezone'] = 'Europe/Moscow'
        profile_data['location_manually_set'] = False
        
        await state.update_data(profile_data=profile_data)
        
        await message.answer(
            "📅 <b>Дата рождения</b>\n\n"
            "Введите дату рождения (число и месяц обязательно, год по желанию).\n\n"
            "Примеры:\n"
            "• <i>15.03</i>\n"
            "• <i>15.03.1990</i>\n"
            "• <i>15 марта</i>\n"
            "• <i>15 марта 1990</i>"
        )
        await state.set_state(ProfileForm.waiting_for_birthday)
        return
    
    if text == "🚫 Отмена":
        await message.answer("❌ Заполнение отменено", reply_markup=get_profile_menu_keyboard(has_profile=False))
        await state.clear()
        return
    
    # Ищем город в справочнике
    cities = city_db.search(text)
    
    if not cities:
        # Город не найден
        await message.answer(
            f"❌ Город '{text}' не найден.\n\n"
            "Проверьте написание или нажмите <b>⏭ Пропустить</b> для МСК.",
            reply_markup=get_skip_keyboard()
        )
        return
    
    if len(cities) == 1:
        # Один город - сохраняем автоматически
        city = cities[0]
        data = await state.get_data()
        profile_data = data.get('profile_data', {})
        profile_data['city'] = city['name']
        profile_data['region'] = city['region']['name']
        profile_data['timezone'] = city['timezone']['tzid']
        profile_data['location_manually_set'] = True
        
        await state.update_data(profile_data=profile_data)
        
        await message.answer(
            f"✅ Город: {city['name']}, {city['region']['name']}\n"
            f"🕒 Часовой пояс: {city['timezone']['tzid']}"
        )
        
        # Переходим к дате рождения
        await message.answer(
            "📅 <b>Дата рождения</b>\n\n"
            "Введите дату рождения (число и месяц обязательно, год по желанию).\n\n"
            "Примеры:\n"
            "• <i>15.03</i>\n"
            "• <i>15.03.1990</i>\n"
            "• <i>15 марта</i>\n"
            "• <i>15 марта 1990</i>"
        )
        await state.set_state(ProfileForm.waiting_for_birthday)
    else:
        # Несколько городов - показываем выбор
        await message.answer(
            "🔍 Найдено несколько городов. Уточните:",
            reply_markup=get_city_choice_keyboard(cities)
        )

@router.callback_query(F.data.startswith("city_"))
async def city_choice_callback(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора города из списка"""
    await callback.answer()
    
    data = callback.data.split('_', 2)
    if len(data) < 2:
        return
    
    action = data[1]
    
    if action == "retry":
        # Ввести заново
        await callback.message.edit_text(
            "🏙 Введите название города:",
            reply_markup=get_skip_keyboard()
        )
        return
    
    if action == "skip":
        # Пропустить (МСК)
        state_data = await state.get_data()
        profile_data = state_data.get('profile_data', {})
        profile_data['timezone'] = 'Europe/Moscow'
        profile_data['location_manually_set'] = False
        
        await state.update_data(profile_data=profile_data)
        
        await callback.message.edit_text(
            "📅 <b>Дата рождения</b>\n\n"
            "Введите дату рождения (число и месяц обязательно, год по желанию).\n\n"
            "Примеры:\n"
            "• <i>15.03</i>\n"
            "• <i>15.03.1990</i>\n"
            "• <i>15 марта</i>\n"
            "• <i>15 марта 1990</i>"
        )
        await state.set_state(ProfileForm.waiting_for_birthday)
        return
    
    # Выбран конкретный город
    # Формат: city_{name}_{region}
    # Но там могут быть символы, поэтому используем join
    city_data = callback.data[5:]  # убираем "city_"
    
    # Ищем город в БД
    cities = city_db.search(city_data.split(',')[0])
    if cities:
        city = cities[0]  # берем первый (пользователь выбрал)
        
        state_data = await state.get_data()
        profile_data = state_data.get('profile_data', {})
        profile_data['city'] = city['name']
        profile_data['region'] = city['region']['name']
        profile_data['timezone'] = city['timezone']['tzid']
        profile_data['location_manually_set'] = True
        
        await state.update_data(profile_data=profile_data)
        
        await callback.message.edit_text(
            f"✅ Город: {city['name']}, {city['region']['name']}\n"
            f"🕒 Часовой пояс: {city['timezone']['tzid']}\n\n"
            f"📅 Теперь введите дату рождения:"
        )
        await state.set_state(ProfileForm.waiting_for_birthday)

@router.message(ProfileForm.waiting_for_birthday)
async def process_birthday(message: Message, state: FSMContext):
    """Обработка введенной даты рождения"""
    text = message.text.strip()
    
    if text == "⏭ Пропустить":
        # Пропускаем дату рождения
        data = await state.get_data()
        profile_data = data.get('profile_data', {})
        
        # Сохраняем профиль
        user_id = message.from_user.id
        username = message.from_user.username or f"user_{user_id}"
        
        profile_db.save_profile(user_id, username, profile_data)
        
        # Показываем готовый профиль
        profile = profile_db.get_profile(user_id)
        await message.answer(
            "✅ <b>Профиль сохранен!</b>\n\n" + format_profile(profile),
            reply_markup=get_profile_menu_keyboard(has_profile=True),
            parse_mode="HTML"
        )
        await state.clear()
        return
    
    if text == "🚫 Отмена":
        await message.answer("❌ Заполнение отменено", reply_markup=get_profile_menu_keyboard(has_profile=False))
        await state.clear()
        return
    
    # Парсим дату
    parsed = parse_birthday(text)
    
    if not parsed:
        await message.answer(
            "❌ Неверный формат даты.\n\n"
            "Примеры:\n"
            "• <i>15.03</i>\n"
            "• <i>15.03.1990</i>\n"
            "• <i>15 марта</i>\n"
            "• <i>15 марта 1990</i>",
            reply_markup=get_skip_keyboard()
        )
        return
    
    day, month, year = parsed
    
    # Сохраняем дату
    data = await state.get_data()
    profile_data = data.get('profile_data', {})
    profile_data['birth_day'] = day
    profile_data['birth_month'] = month
    if year:
        profile_data['birth_year'] = year
    
    # Сохраняем профиль в БД
    user_id = message.from_user.id
    username = message.from_user.username or f"user_{user_id}"
    
    profile_db.save_profile(user_id, username, profile_data)
    
    # Показываем готовый профиль
    profile = profile_db.get_profile(user_id)
    await message.answer(
        "✅ <b>Профиль сохранен!</b>\n\n" + format_profile(profile),
        reply_markup=get_profile_menu_keyboard(has_profile=True),
        parse_mode="HTML"
    )
    await state.clear()

@router.callback_query(F.data == "profile_view")
async def profile_view(callback: CallbackQuery):
    """Просмотр профиля"""
    await callback.answer()
    
    profile = profile_db.get_profile(callback.from_user.id)
    
    if profile:
        await callback.message.edit_text(
            format_profile(profile),
            reply_markup=get_profile_menu_keyboard(has_profile=True),
            parse_mode="HTML"
        )
    else:
        await callback.message.edit_text(
            "👤 Профиль не найден",
            reply_markup=get_profile_menu_keyboard(has_profile=False)
        )

@router.callback_query(F.data == "profile_edit")
async def profile_edit(callback: CallbackQuery, state: FSMContext):
    """Редактирование профиля"""
    await callback.answer()
    
    await callback.message.edit_text(
        "✏️ <b>Редактирование профиля</b>\n\n"
        "Что хотите изменить?",
        reply_markup=get_edit_profile_keyboard()
    )

@router.callback_query(F.data.startswith("edit_"))
async def edit_field_choice(callback: CallbackQuery, state: FSMContext):
    """Выбор поля для редактирования"""
    await callback.answer()
    
    field = callback.data.replace("edit_", "")
    
    if field == "name":
        await callback.message.edit_text(
            "✏️ Введите новое ФИО:"
        )
        await state.set_state(ProfileForm.waiting_for_name)
        await state.update_data(edit_mode=True)
    
    elif field == "city":
        await callback.message.edit_text(
            "✏️ Введите новый город:"
        )
        await state.set_state(ProfileForm.waiting_for_city)
        await state.update_data(edit_mode=True)
    
    elif field == "birthday":
        await callback.message.edit_text(
            "✏️ Введите новую дату рождения:"
        )
        await state.set_state(ProfileForm.waiting_for_birthday)
        await state.update_data(edit_mode=True)
    
    elif field == "back":
        # Назад к просмотру профиля
        await profile_view(callback)
