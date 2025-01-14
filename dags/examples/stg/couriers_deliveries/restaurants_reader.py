import requests
from typing import List, Dict


class RestaurantsReader:
    def __init__(self, nickname: str, cohort: str, api_key: str) -> None:
        self.nickname = nickname
        self.cohort = cohort
        self.api_key = api_key
        self.base_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'

    def _get_headers(self) -> dict:
        return {
            'X-Nickname': self.nickname,
            'X-Cohort': self.cohort,
            'X-API-KEY': self.api_key
        }

    def make_request(self, url: str, headers: dict, params: dict) -> List[Dict]:
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
            data = self.make_request(f'{self.base_url}/{endpoint}', self._get_headers(), params)
            if not data:
                break
            all_data.extend(data)
            offset += limit
        return all_data

    def get_restaurants(self, sort_field='id', sort_direction='asc', limit=50, offset=0) -> List[Dict]:
        params = {
            'sort_field': sort_field,
            'sort_direction': sort_direction,
            'limit': limit,
            'offset': offset
        }
        return self.get_paginated_data('restaurants', params)
