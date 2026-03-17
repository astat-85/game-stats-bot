"""
Работа со справочником городов России
"""
import json
import os
from typing import List, Dict, Optional, Tuple
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
    
    def get_unique_cities(self, query: str) -> Tuple[List[Dict], bool]:
        """
        Возвращает (список городов, уникальны_ли)
        Если город один - True, если несколько - False
        """
        results = self.search(query)
        return results, len(results) == 1
    
    def get_city_by_name_and_region(self, name: str, region: str) -> Optional[Dict]:
        """
        Получить конкретный город по названию и региону
        """
        name = name.lower().strip()
        region = region.lower().strip()
        
        for city in self.cities:
            city_name = city.get('name', '').lower()
            city_region = city.get('region', {}).get('name', '').lower()
            
            if city_name == name and city_region == region:
                return city
        
        return None
    
    def get_all_cities(self) -> List[Dict]:
        """
        Возвращает весь список городов
        """
        return self.cities.copy()
    
    def get_cities_by_region(self, region: str) -> List[Dict]:
        """
        Возвращает все города в указанном регионе
        """
        region = region.lower().strip()
        results = []
        
        for city in self.cities:
            city_region = city.get('region', {}).get('name', '').lower()
            if region in city_region:
                results.append(city)
        
        return results
    
    def get_timezone_for_city(self, city_name: str, region_name: str = None) -> Optional[str]:
        """
        Получает часовой пояс для города
        Если город неуникальный, требуется указать регион
        """
        if region_name:
            city = self.get_city_by_name_and_region(city_name, region_name)
            if city:
                return city.get('timezone', {}).get('tzid')
        else:
            # Ищем все города с таким названием
            cities = self.search(city_name)
            if len(cities) == 1:
                return cities[0].get('timezone', {}).get('tzid')
            elif len(cities) > 1:
                # Несколько городов - нужен регион
                return None
        
        return None
    
    def format_city_for_display(self, city: Dict) -> str:
        """
        Форматирует город для отображения пользователю
        """
        name = city.get('name', '')
        region = city.get('region', {}).get('name', '')
        timezone = city.get('timezone', {}).get('tzid', '').replace('Europe/', '').replace('Asia/', '')
        
        return f"{name}, {region} ({timezone})"
