Привет, ревьювер!

Меня зовут Карина. Я 4 года работаю продуктовым аналитиком, сейчас изучаю дата-инжиниринг для того, чтобы оптимизировать существующие витрины и выстроить эффективное хранилище данных. Буду рада комментариям и конструктивной критике.

**Задача**: построить витрину `cdm.dm_courier_ledger`, которая позволяет рассчитывать зарплату курьерам.

<h2>Этапы реализации:</h2>

1. **Загрузка данных в STG слой**
    - На этом этапе данные извлекаются из API и загружаются в STG слой «как есть» (давайте без англицизмов).
    - Таблицы: `api_couriers`, `api_restaurants`, `api_deliveries`.
    - Каждая таблица содержит данные в формате JSON. Обработкой и трансформацией данных занимаются последующие слои. Но мы предварительно дёрнем идентификаторы.

    - В папке stg/couriers_deliveries находятся модули для обработки данных из API:
	`courier_reader.py`: читает данные из API
	`courier_loader.py`: управляет процессом загрузки данных
	`courier_pg_saver.py:` сохраняет извлеченные данные в БД
	Аналогичным образом реализованы модули для deliveries, restaurants.

    **DDL STG таблиц:**
    ```sql
    CREATE TABLE stg.api_couriers (
        id SERIAL PRIMARY KEY,
        courier_id VARCHAR(255) NOT NULL,
        object_value JSONB NOT NULL,
        updated_at TIMESTAMP DEFAULT NOW()
    );
    -- Аналогично для api_restaurants и api_deliveries.
    ```

2. **Загрузка данных в DDS слой**
    - Таблицы `api_couriers` и `api_deliveries` преобразуются в формат DDS:
        - Из JSON извлекаются нужные поля.
        - Добавляются ключи для связи с другими таблицами.
    - Таблица `api_restaurants` не переносится, так как её данные дублируют существующую таблицу `dm_restaurants`.

    В папке dds находятся модули для извлечения данных из JSON и нормализации:
	`dds_couriers_loader.py`: обрабатывает данные о курьерах и наполняет таблицу dds.dm_couriers.
	`dds_deliveries_loader.py`: обрабатывает данные о доставках и создает таблицу dds.dm_deliveries.
	DAG `stg_to_dds.py` вызывает эти модули для загрузки данных из слоя STG в слой DDS.

    **DDL DDS таблиц:**
    ```sql
    CREATE TABLE dds.dm_couriers (
        id SERIAL PRIMARY KEY,
        courier_id VARCHAR(255) NOT NULL,
        name VARCHAR(255) NOT NULL,
        updated_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE dds.dm_deliveries (
        delivery_id VARCHAR(50) NOT NULL PRIMARY KEY,
        order_id INT NOT NULL REFERENCES dds.dm_orders(id),
        courier_id INT NOT NULL REFERENCES dds.dm_couriers(id),
        delivery_ts TIMESTAMP NOT NULL,
        rate INT NOT NULL,
        sum NUMERIC(10, 2) NOT NULL,
        tip_sum NUMERIC(10, 2),
        updated_at TIMESTAMP NOT NULL
    );
    ```

3. **Изменения в `dm_orders`**
    - Для связи заказов с курьерами добавляется поле `courier_id`.
    - Это позволяет использовать `dm_orders` как центральную таблицу в финальной витрине.

    **DDL изменения:**
    ```sql
    ALTER TABLE dds.dm_orders
    ADD COLUMN courier_id INT REFERENCES dds.dm_couriers(id);
    ```

4. **Построение витрины в CDM слое**
    - Витрина `cdm.dm_courier_ledger` рассчитывает ключевые метрики:
        - Количество заказов (`orders_count`).
        - Общая стоимость заказов (`orders_total_sum`).
        - Средний рейтинг курьера (`rate_avg`).
        - Суммы начислений и удержаний (`order_processing_fee`, `courier_order_sum`, `courier_reward_sum`).

    **DDL витрины:**
    ```sql
    CREATE TABLE cdm.dm_courier_ledger (
        id SERIAL PRIMARY KEY,
        courier_id VARCHAR(50) NOT NULL,
        courier_name TEXT NOT NULL,
        settlement_year INT NOT NULL CHECK (settlement_year >= 2022),
        settlement_month INT NOT NULL CHECK (settlement_month BETWEEN 1 AND 12),
        orders_count INT NOT NULL CHECK (orders_count > 0),
        orders_total_sum DECIMAL(10, 2) NOT NULL CHECK (orders_total_sum >= 0),
        rate_avg DECIMAL(3, 2) NOT NULL CHECK (rate_avg >= 0 AND rate_avg <= 5),
        order_processing_fee DECIMAL(10, 2) NOT NULL CHECK (order_processing_fee >= 0),
        courier_order_sum DECIMAL(10, 2) NOT NULL CHECK (courier_order_sum >= 0),
        courier_tips_sum DECIMAL(10, 2) NOT NULL CHECK (courier_tips_sum >= 0),
        courier_reward_sum DECIMAL(10, 2) NOT NULL CHECK (courier_reward_sum >= 0)
    );
    ```

    **Источники данных:**
    - `courier_id`, `courier_name` – из `dds.dm_couriers`.
    - `settlement_year`, `settlement_month` – из `dds.dm_timestamps`.
    - Метрики (`orders_count`, `orders_total_sum`, `rate_avg`) – из `dds.dm_orders` и `dds.dm_deliveries`.

    В папке cdm находится модуль `courier_ledger_loader.py`, который собирает данные из DDS для построения витрины cdm.dm_courier_ledger.
	DAG dds_pipeline_dag.py объединяет все этапы, начиная от наполнения слоя DDS и заканчивая CDM. Последовательность задач:

	```
	users_task >> restaurants_task >> timestamps_task >> products_task >> couriers_task >> deliveries_task >> orders_task >> sales_task >> courier_ledger_task
	```
	
<h2>Результат:</h2>
Созданная витрина обеспечивает прозрачность расчётов при расчёте зарплаты курьерам.

С учётом атрибутивного состава, готовая витрина может использоваться не только для расчёта зарплаты, но также для построения метрик customer experience, так как, к примеру, содержит данные о рейтинге курьера.
