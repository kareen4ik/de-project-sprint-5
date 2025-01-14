from datetime import datetime
from typing import Any
from lib.dict_util import json2str
from psycopg import Connection


class CourierPgSaver:

    def save_object(self, conn: Connection, val: Any, updated_at: datetime):
        courier_id = val.get("_id")  # Извлекаем _id из JSON
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.api_couriers(courier_id, object_value, updated_at)
                VALUES (%(courier_id)s, %(object_value)s, %(updated_at)s)
                ON CONFLICT (courier_id) DO UPDATE
                SET
                    object_value = EXCLUDED.object_value,
                    updated_at = EXCLUDED.updated_at;
                """,
                {
                    "courier_id": courier_id,
                    "object_value": str_val,
                    "updated_at": updated_at
                }
            )
