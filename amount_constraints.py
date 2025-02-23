# Токены и номера кабинетов
oauth_tokens = [("Мой токен", 0),
                    ("Мой токен", 0),
                    ("Мой токен", 0)]
# URL для запроса
url = "https://api.direct.yandex.ru/live/v4/json"
# Заголовки запроса
def get_headers(token):
    return {
        "Authorization": "Bearer " + token,
        "Accept-Language": "ru",
        "processingMode": "auto",
        "Content-Type": "application/json"
    }
# тело запроса
def get_params(token, id):
    return {
        "FieldNames": ["AccountID", "Amount", "Login"],
        "method": "AccountManagement",
        "token": token,
        "param":
            {
                "Action": "Get",
                "SelectionCriteria":
                {
                "AccountIDS": [id]
                }
            }
            }
# Создаем список данных для записи в таблицу
def get_list(account_id, amount, login, my_curr_time):
    return [{
    'AccountID': account_id,
    'Amount': amount,
    'Login': login,
    'date_loaded': my_curr_time  # Добавляем поле с датой загрузки
}]
# Получение структуры для вставки в таблицу input
def get_struct(data_str, response, login, curr_time, date):
    return (data_str, 'post', response.text, 'json', 'Директ баланс по ' + login, curr_time, date,
     date)
# Получение запроса для вставки в таблицу input
insert_query = """
                    INSERT INTO "input".responses (request_body, request_type, response, response_type, "source", date_loaded, date_to, date_from)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """