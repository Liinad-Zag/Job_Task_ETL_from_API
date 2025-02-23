import sqlalchemy
import requests
import pandas as pd
import metrics_constants # файл с константами метрик
import json
import common_to_all # файл с общими константами
def get_all_metrics():
    # Получение даты по которую, производить загрузку
    date_to = common_to_all.get_date_to()
    # Получение текущего времени
    current_time = common_to_all.get_time_loaded()
    # Подключение для sqlalchemy
    conn_string = common_to_all.conn_string
    # Создание движка из библиотеки sqlalchemy
    engine = sqlalchemy.create_engine(conn_string)
    # Получение даты в переменную
    date_from = metrics_constants.static_load_date
    # Функция, которая получает данные с метрик get-запросами
    def get_metrics_data():
        # Получение статичных параметров из другого файла
        # Параметров
        params = metrics_constants.get_params(date_from, date_to)
        params2 = metrics_constants.get_params2(date_from, date_to)
        # Параметры в json для input
        request_body = json.dumps(params)
        request_body2 = json.dumps(params2)
        # Заголовки
        headers = metrics_constants.headers
        # Адрес api метрик
        url = metrics_constants.url
        # Выполнение запросов пока по обоим не будет код 200, может быть код 201
        while True:
            # Выполнение GET-запросов
            response = requests.get(url, headers=headers, params=params)
            # Для отладки
            # Пока ответ не равен 200
            if response.status_code == 200:
                print('Metrics1 ')
                break
        while True:
            # Выполнение GET-запросов
            response2 = requests.get(url, headers=headers, params=params2)
            # Для отладки

            # Пока ответ не равен 200
            if response2.status_code == 200:
                print('Metrics2 ')
                break
        # Сохранение ответов в json для input
        dataGeneral = response.json()
        dataSearch = response2.json()
        # Для отладки
        print(dataGeneral, dataSearch)
        # Создание подключения из библиотеки psycopg2
        connect = common_to_all.get_con()
        # Для отладки
        print("Соединение установлено успешно!")
        # Создание курсора
        cursort = connect.cursor()
        # Получение структуры для вставки в таблицу input
        data_list = metrics_constants.get_data_list(request_body, request_body2, response, response2, current_time, date_to, date_from)
        # Получение запроса для вставки в таблицу input
        insert_query = metrics_constants.insert_query
        # Выполнение запроса
        cursort.executemany(insert_query, data_list)
        # Зафиксировать изменения
        connect.commit()
        # Закрыть курсор и соединение
        cursort.close()
        connect.close()
        # Обработка данных из общего запроса
        rows = []
        for entry in dataGeneral['data']:
            row = {
                "date": entry['dimensions'][1]['name'],
                "source": entry['dimensions'][0]['name'],
                "domain": entry['dimensions'][2]['name']
            }
            metrics = entry['metrics']
            row.update({
                "visits": metrics[0],
                "visitors": metrics[1],
                "bounce_rate": metrics[2],
                "page_depth": metrics[3],
                "time_on_site": metrics[4]
            })
            rows.append(row)
        # Преобразование в DataFrame
        df = pd.DataFrame(rows)
        # Источники по которым не хотим данные
        sources_to_delete = metrics_constants.sources_to_delete
        # Удаление данных
        df_filtered = df[~df['source'].isin(sources_to_delete)]
        # Получение данных из ответа по поисковым системам
        rows2 = []
        for entry in dataSearch['data']:
            row2 = {
                "date": entry['dimensions'][1]['name'],
                "source": entry['dimensions'][0]['name'],
                "domain": entry['dimensions'][2]['name']
            }
            metrics = entry['metrics']
            row2.update({
                "visits": metrics[0],
                "visitors": metrics[1],
                "bounce_rate": metrics[2],
                "page_depth": metrics[3],
                "time_on_site": metrics[4]
            })
            rows2.append(row2)
        # Преобразование в DataFrame
        df2 = pd.DataFrame(rows2)
        # Фильтрация по источникам (Google и Яндекс)
        df_google = pd.DataFrame(columns=df2.columns)
        df_yandex = pd.DataFrame(columns=df2.columns)
        if df2.size > 0:
            # Получение срезов по Google и Яндекс
            df_google = df2[df2['source'] == 'Google'].copy()
            df_yandex = df2[df2['source'] == 'Яндекс'].copy()
            # Переименовывание источников
            df_google['source'] = 'Переходы из поисковых систем Google'
            df_yandex['source'] = 'Переходы из поисковых систем Яндекс'
        # Объединение данных по общим источникам без некоторых + Google и Яндекс
        df_combined = pd.concat([df_filtered, df_google, df_yandex], ignore_index=True)
        return df_combined
    # Получение данных от всех счетчиков
    df1 = get_metrics_data()
    # Добавление времени записи
    df1['date_loaded'] = current_time
    # Для отладки
    print('Запись метрик начата')
    # Запись в ods всех версий
    common_to_all.write_to_sql_with_retry(df1, engine, 'metrics', 'ods', 'append')
    # Для отладки
    print('Запись метрик окончена в ods')
    # Получаем все метрики из ods
    metrics_from_db = common_to_all.read_from_sql_with_retry('metrics', 'ods')
    # Список для уникальных записей, для ускорения записи по 1 кортежу
    metrics_with_uniques = []
    # Уникальные ключи
    metrics_keys = []
    # Проход по всем записям из ods с конца, чтобы брать сначала более свежие версии
    for i in range(len(metrics_from_db) - 1, -1, -1):
        # Получаем ключ каждой записи
        key = str(metrics_from_db['date'][i]) + metrics_from_db['source'][i] + metrics_from_db['domain'][i]
        # Условие на уникальность ключа
        if key not in metrics_keys:
            # Добавление в уникальные ключи
            metrics_keys.append(key)
            # Добавление уникальной по ключу метрики
            metrics_with_uniques.append(metrics_from_db.iloc[i])
    # Запись в датафрейм уникальных метрик
    metrics_2 = pd.DataFrame(metrics_with_uniques, columns=metrics_from_db.columns)
    # Запись в ods_plus только последних версий метрик
    common_to_all.write_to_sql_with_retry(metrics_2, engine, 'metrics', 'ods_plus', 'replace')
    # Для отладки
    print('Запись метрик окончена в ods_plus')
get_all_metrics()