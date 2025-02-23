import psycopg2
import time
import sqlalchemy
import pandas as pd
# Строка подключения + engine для подключения
conn_string = "Моя строка"
def get_con():
    return psycopg2.connect(
        user='Мой юзер', password='Мой пароль', host='Мой хост', port='Мой порт', database='Моя бд')
def get_time_loaded():
    # Получение времени +0
    utc_time = time.time()
    utc_struct_time = time.gmtime(utc_time)
    # Форматируем время в строку вида "yyyy-mm-dd hh:mm:ss"
    utc_time_str = time.strftime("%Y-%m-%d %H:%M:%S", utc_struct_time)
    return utc_time_str
# Функция для получения текущей даты в формате "%Y-%m-%d
def get_date_to():
    # Текущая дата
    today = time.localtime()
    # В формат yyyy-mm-dd
    date = time.strftime("%Y-%m-%d", today)
    return date
# Функция для получения текущей даты в формате "%Y-%m-%d
def get_date_from():
    # Текущая дата
    today = time.localtime()
    # Переводим текущую дату в количество секунд с 1 января 1970 года
    current_time_in_seconds = time.mktime(today)
    # Считаем количество секунд в 30 днях
    seconds_in_30_days = 30 * 24 * 60 * 60
    # Вычитаем 30 дней (в секундах)
    time_minus_30_days = current_time_in_seconds - seconds_in_30_days
    # Преобразуем полученное время обратно в структуру времени
    new_date = time.localtime(time_minus_30_days)
    # Форматируем результат в yyyy-mm-dd
    date = time.strftime("%Y-%m-%d", new_date)
    return date
def write_to_sql_with_retry(df, engine1, table, schema, method, max_retries=5, retry_interval=5):
    attempt_count = 0
    while attempt_count < max_retries:
        try:
            # Попытка записи DataFrame в таблицу
            df.to_sql(
                name=table,
                schema=schema,
                con=engine1,
                if_exists=method,
                index=False
            )
            print("Данные успешно записаны.")
            return  # Если запись успешна, выходим из функции

        except sqlalchemy.exc.OperationalError as e:
            # Если ошибка соединения, пересоздаем engine
            print(f"Ошибка соединения: {e}. Повторная попытка через {retry_interval} секунд.")
            attempt_count += 1
            engine1.dispose()  # Закрываем старое соединение
            engine1 = sqlalchemy.create_engine(conn_string)  # Создаем новое соединение
            time.sleep(retry_interval)  # Задержка перед повторной попыткой
    print("Превышено количество попыток для записи данных.")
def read_from_sql_with_retry(table, schema, max_retries=5, retry_interval=5):
    attempt_count = 0
    while attempt_count < max_retries:
        try:
            # Попытка записи DataFrame в таблицу
            df = pd.read_sql_table(table, con=conn_string, schema=schema)
            print("Данные успешно считаны.")
            return df  # Если запись успешна, выходим из функции

        except sqlalchemy.exc.OperationalError as e:
            # Если ошибка соединения, выводим ошибку и пытаемся через некоторое время получит данные
            print(f"Ошибка соединения: {e}. Повторная попытка через {retry_interval} секунд.")
            attempt_count += 1
            time.sleep(retry_interval)  # Задержка перед повторной попыткой
    print("Превышено количество попыток для записи данных.")