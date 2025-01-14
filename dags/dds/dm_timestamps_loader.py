import logging
from datetime import datetime
from lib import PgConnect
from lib.dict_util import str2json


class DMTimestampsLoader:
    def __init__(self, pg_dest: PgConnect, logger: logging.Logger):
        self.pg_dest = pg_dest
        self.log = logger

    def load_timestamps(self):
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                self.log.info("Extracting data from stg.ordersystem_orders...")

                cur.execute("""
                    SELECT object_value 
                    FROM stg.ordersystem_orders
                    WHERE object_value::jsonb->>'final_status' IN ('CANCELLED', 'CLOSED');
                """)
                rows = cur.fetchall()
                self.log.info(f"Fetched {len(rows)} orders with final statuses.")

                for row in rows:
                    object_value = row[0]
                    json_data = str2json(object_value)  

                    ts_str = json_data.get("date") 
                    if not ts_str:
                        continue
                    
                    ts = datetime.fromisoformat(ts_str)
                    year = ts.year
                    month = ts.month
                    day = ts.day
                    date = ts.date()
                    time = ts.time()

                    cur.execute("""
                        INSERT INTO dds.dm_timestamps (ts, year, month, day, date, time)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (ts) DO NOTHING;
                    """, (ts, year, month, day, date, time))

                self.log.info("Data successfully loaded into dds.dm_timestamps.")