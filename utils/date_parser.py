"""
Парсинг даты рождения из текста
"""
import re
from datetime import datetime
from typing import Optional, Tuple

def parse_birthday(text: str) -> Optional[Tuple[int, int, Optional[int]]]:
    """
    Парсит дату рождения из текста
    Поддерживает форматы:
    - 15.03
    - 15.03.1990
    - 15 марта
    - 15 марта 1990
    - 15/03/1990
    
    Возвращает (день, месяц, год) или None
    Год может быть None, если не указан
    """
    text = text.strip().lower()
    
    # Паттерны для разных форматов
    patterns = [
        # ДД.ММ.ГГГГ или ДД.ММ
        r'^(\d{1,2})[./](\d{1,2})(?:[./](\d{4}))?$',
        
        # ДД месяц ГГГГ или ДД месяц
        r'^(\d{1,2})\s+([а-я]+)(?:\s+(\d{4}))?$',
    ]
    
    # Русские названия месяцев
    months = {
        'января': 1, 'янв': 1,
        'февраля': 2, 'фев': 2,
        'марта': 3, 'мар': 3,
        'апреля': 4, 'апр': 4,
        'мая': 5, 'май': 5,
        'июня': 6, 'июн': 6,
        'июля': 7, 'июл': 7,
        'августа': 8, 'авг': 8,
        'сентября': 9, 'сен': 9,
        'октября': 10, 'окт': 10,
        'ноября': 11, 'ноя': 11,
        'декабря': 12, 'дек': 12,
    }
    
    for pattern in patterns:
        match = re.match(pattern, text)
        if not match:
            continue
        
        groups = match.groups()
        
        if pattern == patterns[0]:  # Числовой формат
            day = int(groups[0])
            month = int(groups[1])
            year = int(groups[2]) if groups[2] else None
            
        else:  # Текстовый формат с месяцем
            day = int(groups[0])
            month_name = groups[1]
            year = int(groups[2]) if groups[2] else None
            
            # Ищем месяц в словаре
            month = None
            for name, num in months.items():
                if month_name.startswith(name[:3]):  # Сравниваем по первым 3 буквам
                    month = num
                    break
            
            if not month:
                return None
        
        # Базовая валидация
        if not (1 <= day <= 31):
            return None
        if not (1 <= month <= 12):
            return None
        if year and (year < 1900 or year > datetime.now().year):
            return None
        
        return (day, month, year)
    
    return None
