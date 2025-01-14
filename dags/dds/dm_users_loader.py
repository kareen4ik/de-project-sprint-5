import logging
from lib import PgConnect
from lib.dict_util import str2json

class DMUsersLoader:
    def __init__(self, pg_dest: PgConnect, logger: logging.Logger):
        self.pg_dest = pg_dest
        self.log = logger

    def load_users(self):
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                self.log.info("Extracting data from stg.ordersystem_users...")
                
                # Прочитать данные из stg.ordersystem_users
                cur.execute("SELECT object_id, object_value FROM stg.ordersystem_users;")
                rows = cur.fetchall()
                self.log.info(f"Fetched {len(rows)} rows from stg.ordersystem_users.")

                for row in rows:
                    object_id, object_value = row
                    json_data = str2json(object_value)  

    
                    user_id = json_data.get("_id")
                    user_name = json_data.get("name")
                    user_login = json_data.get("login")

                    if not user_id or not user_name or not user_login:
                        continue  


                    cur.execute("""
                        INSERT INTO dds.dm_users (user_id, user_name, user_login)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE
                        SET
                        user_name = EXCLUDED.user_name,
                        user_login = EXCLUDED.user_login;
                     """, (user_id, user_name, user_login))
                
                self.log.info("Data successfully loaded into dds.dm_users.")