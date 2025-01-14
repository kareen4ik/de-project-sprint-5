from datetime import datetime
from typing import Any
from lib.dict_util import json2str
from psycopg import Connection


class DeliveriesPgSaver:

    def save_object(self, conn: Connection, delivery_id: str, updated_at: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.api_deliveries (delivery_id, object_value, updated_at)
                    VALUES (%(delivery_id)s, %(object_value)s, %(updated_at)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        updated_at = EXCLUDED.updated_at;
                """,
                {
                    "delivery_id": delivery_id,
                    "object_value": str_val,
                    "updated_at": updated_at
                }
            )
