import logging
from lib import PgConnect
from lib.dict_util import str2json

class DDMCouriersLoader:
    def __init__(self, pg_dest: PgConnect, logger: logging.Logger):
        self.pg_dest = pg_dest
        self.log = logger

    def load_couriers(self):
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                self.log.info("Extracting data from stg.api_couriers...")

                cur.execute("""
                    SELECT id, courier_id, object_value::jsonb, updated_at
                    FROM stg.api_couriers;
                """)
                rows = cur.fetchall()
                self.log.info(f"Fetched {len(rows)} rows from stg.api_couriers.")

                for row in rows:
                    serial_id, courier_id, object_value, updated_at = row

                    name = object_value.get('name')

                    if not serial_id or not courier_id or not name:
                        self.log.warning(f"Skipping invalid data: {row}")
                        continue

                    cur.execute(
                        """
                        INSERT INTO dds.dm_couriers (
                            id, courier_id, name, updated_at
                        )
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (courier_id) DO UPDATE
                        SET
                            name = EXCLUDED.name,
                            updated_at = EXCLUDED.updated_at;
                        """,
                        (serial_id, courier_id, name, updated_at)
                    )

                self.log.info("Data successfully loaded into dds.dm_couriers.")
