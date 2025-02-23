import common_to_all
# Токен
oauth_token = "Мой токен"
# Айдишники счетчиков
counter_ids = [0, 0, 0]
# SQL-запрос на получение крайней даты загрузки
select_max_date = '''
select max(date_to) from "input".responses
where "source" = 'Метрики по источникам трафика'
'''
# Если в бд нет данных возвращает константу
static_load_date = '2000-01-01'
# Выбор метрик
metrics = "ym:s:visits,ym:s:users,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds"
# Выбор измерений
dimensions = "ym:s:automaticTrafficSource, ym:s:Date, ym:s:counterID"  # Для выборки по базовым источникам
dimensionsSearch = "ym:s:automaticSearchEngineRoot, ym:s:Date, ym:s:counterID"  # по поисковым системам
# Лимит записей максимальный
limit = "100000"
# URL для запроса статистики
url = "https://api-metrika.yandex.net/stat/v1/data"
# Заголовки для запроса
headers = {
    # Авторизация по токенам
    "Authorization": f"OAuth {oauth_token}",
    # Тип ответа
    "Content-Type": "application/json"
}
# Параметры для запроса по базовым источникам
def get_params(date_from, date_to):
    return {
        "ids": counter_ids,
        "metrics": metrics,
        "dimensions": dimensions,
        "date1": date_from,
        "date2": date_to,
        "limit": limit,
        "lang": "ru",
    }
# Параметры для запроса по поисковым системам
def get_params2(date_from, date_to):
    return {
        "ids": counter_ids,
        "metrics": metrics,
        "dimensions": dimensionsSearch,
        "date1": date_from,
        "date2": date_to,
        "limit": limit,
        "lang": "ru",
    }
# Заапрос для input
insert_query = '''
            INSERT INTO "input".responses (request_body, request_type, response, response_type, "source", date_loaded, date_to, date_from)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''
def get_data_list(request_body, request_body2, response, response2, curr_time, date_to, date_from):
    return [(request_body, 'get', response.text, 'json', 'Метрики по источникам трафика', curr_time, date_to, date_from),
                     (request_body2, 'get', response2.text, 'json', 'Метрики по источникам из числа поисковых систем ', curr_time, date_to, date_from)]
sources_to_delete = ['Переходы из рекомендательных систем', 'Переходы с сохранённых страниц', 'Не определено']

# Функция, которая возвращает дату последней загрузки метрик
"""def get_date_from():
    # Создание подключения из библиотеки psycopg2
    conn = common_to_all.get_con()
    # Создание курсора
    cursor = conn.cursor()
    # выполнние SQL-запроса
    cursor.execute(select_max_date)
    # Возвращает список значений
    s = cursor.fetchone()
    # Если в бд нет данных возвращает константу
    date = static_load_date
    if s[0]:
        # Выбор первого если существует дата загрузки в бд
        date_val = s[0]
        # В формат yyyy-mm-dd
        date = date_val.strftime('%Y-%m-%d')
    # Зафиксировать изменения
    conn.commit()
    # Закрыть курсор и соединение
    cursor.close()
    conn.close()
    return date"""