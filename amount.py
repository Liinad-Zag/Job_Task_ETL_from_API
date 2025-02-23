import requests
import pandas as pd
import json
import amount_constraints
import common_to_all
import sqlalchemy
def get_all_direct_amount():
    # Токены и номера кабинетов
    oauth_tokens = amount_constraints.oauth_tokens
    # Строка подключения + engine для подключения
    conn_string = common_to_all.conn_string
    # Создание движка из библиотеки sqlalchemy
    engine = sqlalchemy.create_engine(conn_string)
    def get_direct_amount(token, cabinet, curr_time):
        # URL для запроса
        url = amount_constraints.url
        # Заголовки запроса
        headers = amount_constraints.get_headers(token)
        # тело запроса
        params = amount_constraints.get_params(token, cabinet)
        while True:
            # Получаем ответ от пост-запроса
            response = requests.post(url, headers=headers, json=params)
            # Для отладки
            print('Direct ' + token)
            print(response.status_code)
            # Пока ответ не равен 200
            if response.status_code == 200:
                break
        # Получаем json ответ
        data = response.json()
        # Сохраняем тело запроса как json для записи в бд
        data_str = json.dumps(params)
        # Для отладки
        print(data_str)
        # Извлекаем данные из ответа
        account = data['data']['Accounts'][0]
        # Получаем необходимые поля
        account_id = account['AccountID']
        amount = account['Amount']
        login = account['Login']
        # Создаем список данных для записи в таблицу
        df_data = amount_constraints.get_list(account_id, amount, login, curr_time)
        # Создаем соединение и курсор
        conn = common_to_all.get_con()
        cursor = conn.cursor()
        # Получение текущей даты
        date_to_and_from = common_to_all.get_date_to()
        # Получение структуры для вставки в таблицу input
        data_list = amount_constraints.get_struct(data_str, response, login, curr_time, date_to_and_from)
        # Получение запроса для вставки в таблицу input
        insert_query = amount_constraints.insert_query
        # Выполнение запроса
        cursor.execute(insert_query, data_list)
        # Зафиксировать изменения
        conn.commit()
        # Закрыть курсор и соединение
        cursor.close()
        conn.close()
        # Преобразуем список в DataFrame
        df = pd.DataFrame(df_data)
        return df
    my_curr_time = common_to_all.get_time_loaded()
    # Датафрейм для объединения данных от всех кабинетов
    df_amount = pd.DataFrame()
    for token1, cabinet1 in oauth_tokens:
        # Получение данных от одного из кабинетов
        df_rez = get_direct_amount(token1, cabinet1, my_curr_time)
        # Добавление данных в датафреймы
        df_amount = pd.concat([df_amount, df_rez], ignore_index=True)
    print('Запись в ods начата для балансов')
    common_to_all.write_to_sql_with_retry(df_amount, engine, 'direct_balance_amount', 'ods', 'append')
    print('Запись в ods закончена для балансов')
    # Получаем все amount_campaign_performance из ods
    amount_from_db = common_to_all.read_from_sql_with_retry('direct_balance_amount', 'ods')
    # Список для уникальных записей, для ускорения записи по 1 кортежу
    amount_with_uniques = []
    # Множество для хранения уникальных ключей
    amount_keys = set()
    # Проход по всем записям из ods с конца, чтобы брать сначала более свежие версии
    for i in range(len(amount_from_db) - 1, -1, -1):
        # Получаем ключ каждой записи
        key = str(amount_from_db['Login'][i])
        # Условие на уникальность ключа
        if key not in amount_keys:
            # Добавление в множество ключей
            amount_keys.add(key)
            # Добавление уникального по ключу direct_balance_amount
            amount_with_uniques.append(amount_from_db.iloc[i])
    # Запись в датафрейм уникальных direct_balance_amount
    direct_balance_amount = pd.DataFrame(amount_with_uniques, columns=amount_from_db.columns)
    # Запись direct_balance_amount в ods_plus
    common_to_all.write_to_sql_with_retry(direct_balance_amount, engine, 'direct_balance_amount', 'ods_plus', 'replace')
    # Для отладки
    print('Запись 1 отчета окончена для ods_plus')
get_all_direct_amount()