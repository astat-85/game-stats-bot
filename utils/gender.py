"""
Определение пола по имени через pymorphy2
"""
import pymorphy2

morph = pymorphy2.MorphAnalyzer(lang='ru')

def detect_gender_by_name(name: str) -> str | None:
    """
    Определяет пол по имени
    Возвращает 'male', 'female' или None, если не удалось определить
    """
    if not name or len(name) < 2:
        return None
    
    parsed = morph.parse(name)[0]
    
    # Проверяем, что это имя
    if 'Name' not in parsed.tag:
        return None
    
    if 'masc' in parsed.tag:
        return 'male'
    elif 'femn' in parsed.tag:
        return 'female'
    
    return None
