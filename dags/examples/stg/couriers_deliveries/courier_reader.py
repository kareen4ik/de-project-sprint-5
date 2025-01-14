import requests
from datetime import datetime
from typing import List, Dict

class CourierReader:
    def __init__(self, nickname: str = "rinchen.helmut", cohort: str = "32", 
                 api_key: str = "25c27781-8fde-4b30-a22e-524044a7580f", 
                 base_url: str = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net") -> None:
        self.nickname = nickname
        self.cohort = cohort
        self.api_key = api_key
        self.base_url = base_url

    def _get_headers(self) -> dict:
        return {
            'X-Nickname': self.nickname,
            'X-Cohort': self.cohort,
            'X-API-KEY': self.api_key
        }
    def make_request(self, url, headers, params): 
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_paginated_data(self, endpoint: str, params: dict) -> List[Dict]:
        all_data = []
        offset = 0
        limit = 50
        while True:
            params['limit'] = limit
            params['offset'] = offset
            try:
                data = self.make_request(f'{self.base_url}/{endpoint}', self._get_headers(), params)
                if not data:
                    break
                all_data.extend(data)
                offset += limit
            except requests.exceptions.SSLError as e:
                self.log.error(f"SSL Error: {e}")
                break
        return all_data



    def get_couriers(self, sort_field='id', sort_direction='asc', limit=50, offset=0) -> List[Dict]:
        params = {
            'sort_field': sort_field,
            'sort_direction': sort_direction,
            'limit': limit,
            'offset': offset
        }
        return self.get_paginated_data('couriers', params)

