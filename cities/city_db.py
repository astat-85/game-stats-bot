"""
Работа со справочником городов России
"""
import json
import os
from typing import List, Dict, Optional
from pathlib import Path

class CityDatabase:
    def __init__(self, json_path: str = None):
        if json_path is None:
            # Ищем файл в разных местах
            possible_paths = [
                Path(__file__).parent.parent / "data" / "russia-cities.json",
                Path(__file__).parent.parent / "russia-cities.json",
                Path.cwd() / "russia-cities.json",
            ]
            
            for path in possible_paths:
                if path.exists():
                    json_path = str(path)
                    break
        
        self.json_path = json_path
        self.cities = []
        self._load()
    
    def _load(self):
        """Загружает справочник из JSON"""
        if not self.json_path or not os.path.exists(self.json_path):
            print(f"⚠️ Файл городов не найден: {self.json_path}")
            return
        
        try:
            with open(self.json_path, 'r', encoding='utf-8') as f:
                self.cities = json.load(f)
            print(f"✅ Загружено {len(self.cities)} городов из {self.json_path}")
        except Exception as e:
            print(f"❌ Ошибка загрузки городов: {e}")
    
    def search(self, query: str) -> List[Dict]:
        """
        Поиск городов по названию
        """
        if not self.cities:
            return []
        
        query = query.lower().strip().replace('ё', 'е')
        
        results = []
        for city in self.cities:
            city_name = city.get('name', '').lower().replace('ё', 'е')
            
            # Проверяем вхождение подстроки
            if query in city_name:
                results.append(city)
            
            # Если слишком много результатов, ограничиваем
            if len(results) >= 20:
                break
        
        return results
    
    def
