import sqlalchemy
import pandas as pd
import requests
import time
import json
from io import StringIO
import ydirect_constants # файл с константами директа
import common_to_all # файл с общими константами
import re
def get_all_direct():
    # Получение даты по которую, производить загрузку
    date_to = common_to_all.get_date_to()
    # Получение текущего времени
    curr_time = common_to_all.get_time_loaded()
    # Токены и названия соответствующих кабинетов
    oauth_tokens = ydirect_constants.oauth_tokens
    # Строка подключения + engine для подключения
    conn_string = common_to_all.conn_string
    # Создание движка из библиотеки sqlalchemy
    engine = sqlalchemy.create_engine(conn_string)
    def get_direct_cabinet(token, cabinet):
        # URL для запроса к отчетам
        report_url = ydirect_constants.reports_url
        # Заголовки запроса
        headers = ydirect_constants.get_headers(token)
        # Получаем дату загрузки
        date_from = common_to_all.get_date_from()
        # Получение тел запросов для 2 отчетов
        data = ydirect_constants.get_data(date_from, date_to)
        data2 = ydirect_constants.get_data2(date_from, date_to)
        # Функция, удаляющая 1 и последние 2 строки, содержащие дату запроса и статистику
        def get_text(data_given):
            # Таймер для сна при ответе не 200
            sleeping = 1
            while True:
                # Получение ответа
                response = requests.post(report_url, headers=headers, json=data_given)
                # Для отладки
                print('Direct ' + token)
                # Для отладки
                print(response.status_code)
                # Пока не равно 200
                if response.status_code == 200:
                    break
                time.sleep(sleeping)
            # Удаление первой и последних строк
            text1 = re.sub(r'^[^\n]*\n|\n[^\n]*$', '', response.text)
            return response.text, text1
        # Получаем сырой ответ и нужные строки
        raw_response, text = get_text(data)
        raw_response2, text2 = get_text(data2)
        # Тела запроса в json
        data_str = json.dumps(data)
        data_str2 = json.dumps(data2)
        # Соединение для записи в input
        conn = common_to_all.get_con()
        # Курсор
        cursor = conn.cursor()
        # Значения для input
        data_list = ydirect_constants.get_data_list(data_str, data_str2, raw_response, raw_response2, cabinet,curr_time, date_to, date_from)
        # Запрос для input
        insert_query = ydirect_constants.insert_query
        # Выполнение запроса для input со значениями
        cursor.executemany(insert_query, data_list)
        # Зафиксировать изменения
        conn.commit()
        # Закрыть курсор и соединение
        cursor.close()
        conn.close()
        # Чтение из tcv в датафрейм отчет по компаниям
        CAMPAIGN_PERFORMANCE_REPORT = pd.read_table(StringIO(text))
        # Замена нулевых значений директа на 0
        CAMPAIGN_PERFORMANCE_REPORT = CAMPAIGN_PERFORMANCE_REPORT.replace('--', '0')
        # Применение float к нужным ячейкам
        CAMPAIGN_PERFORMANCE_REPORT[['Cost', 'AvgCpc', 'CostPerConversion', 'Impressions', 'Clicks', 'Ctr',
        'AvgTrafficVolume', 'AvgPageviews', 'ConversionRate', 'Conversions', 'AvgImpressionPosition']] = CAMPAIGN_PERFORMANCE_REPORT[['Cost',
        'AvgCpc', 'CostPerConversion', 'Impressions', 'Clicks', 'Ctr',
        'AvgTrafficVolume', 'AvgPageviews', 'ConversionRate', 'Conversions', 'AvgImpressionPosition']].astype(float)
        # Делим на миллион
        CAMPAIGN_PERFORMANCE_REPORT[['Cost', 'AvgCpc', 'CostPerConversion']] = CAMPAIGN_PERFORMANCE_REPORT[
            ['Cost', 'AvgCpc', 'CostPerConversion']] / 10**6
        CAMPAIGN_PERFORMANCE_REPORT = CAMPAIGN_PERFORMANCE_REPORT.fillna(0)
        # Блок обработки 2 отчета
        # Чтение из tcv в датафрейм
        CRITERIA_PERFORMANCE_REPORT = pd.read_table(StringIO(text2))
        # Замена нулевых значений директа на 0
        CRITERIA_PERFORMANCE_REPORT = CRITERIA_PERFORMANCE_REPORT.replace('--', '0')
        # Применение float к нужным ячейкам
        CRITERIA_PERFORMANCE_REPORT[['Cost', 'AvgCpc', 'Impressions', 'Clicks', 'Ctr',
        'AvgTrafficVolume', 'ConversionRate', 'Conversions','AvgImpressionPosition']] = CRITERIA_PERFORMANCE_REPORT[['Cost',
        'AvgCpc', 'Impressions', 'Clicks', 'Ctr', 'AvgTrafficVolume', 'ConversionRate',
        'Conversions', 'AvgImpressionPosition']].astype(float)
        # Делим на миллион
        CRITERIA_PERFORMANCE_REPORT[['Cost', 'AvgCpc']] = CRITERIA_PERFORMANCE_REPORT[
            ['Cost', 'AvgCpc']] / 10**6
        CRITERIA_PERFORMANCE_REPORT = CRITERIA_PERFORMANCE_REPORT.fillna(0)
        return CAMPAIGN_PERFORMANCE_REPORT, CRITERIA_PERFORMANCE_REPORT
    # Датафреймы для объединения данных от всех кабинетов
    df_campaign1 = pd.DataFrame()
    df_criteria1 = pd.DataFrame()
    for token1, cabinet1 in oauth_tokens:
        # Получение данных от одного из кабинетов
        df_result_campaign, df_result_criteria = get_direct_cabinet(token1, cabinet1)
        # Добавление данных в датафреймы
        df_campaign1 = pd.concat([df_campaign1, df_result_campaign], ignore_index=True)
        df_criteria1 = pd.concat([df_criteria1, df_result_criteria], ignore_index=True)
    # Для отладки
    print('Запись отчетов начата для всех в ods')
    # Добавление текущего времени загрузки
    df_campaign1['date_loaded'] = curr_time
    df_criteria1['date_loaded'] = curr_time
    # Запись CAMPAIGN_PERFORMANCE_REPORT в ods всех версий
    common_to_all.write_to_sql_with_retry(df_campaign1, engine, 'direct_campaign_performance', 'ods', 'append')
    # Запись CRITERIA_PERFORMANCE_REPORT в ods всех версий
    common_to_all.write_to_sql_with_retry(df_criteria1, engine, 'direct_criteria_performance', 'ods', 'append')
    # Для отладки
    print('Запись отчетов в ods окончена для всех')
    # Получаем все direct_campaign_performance из ods
    direct_from_db = common_to_all.read_from_sql_with_retry('direct_campaign_performance', 'ods')
    # Список для уникальных записей, для ускорения записи по 1 кортежу
    direct_with_uniques = []
    # Множество для хранения уникальных ключей
    direct_keys = set()
    # Проход по всем записям из ods с конца, чтобы брать сначала более свежие версии
    for i in range(len(direct_from_db) - 1, -1, -1):
        # Получаем ключ каждой записи
        key = str(direct_from_db['Date'][i]) + direct_from_db['CampaignId'][i] + direct_from_db['ClientLogin'][i]
        # Условие на уникальность ключа
        if key not in direct_keys:
            # Добавление в множество ключей
            direct_keys.add(key)
            # Добавление уникального по ключу direct_campaign_performance
            direct_with_uniques.append(direct_from_db.iloc[i])
    # Запись в датафрейм уникальных direct_campaign_performance
    CAMPAIGN_PERFORMANCE_REPORT2 = pd.DataFrame(direct_with_uniques, columns=direct_from_db.columns)
    # Запись CAMPAIGN_PERFORMANCE_REPORT в ods_plus
    common_to_all.write_to_sql_with_retry(CAMPAIGN_PERFORMANCE_REPORT2, engine, 'direct_campaign_performance', 'ods_plus', 'replace')
    # Для отладки
    print('Запись 1 отчета окончена для ods_plus')
    # Получаем все direct_criteria_performance из ods
    criteria_from_db = common_to_all.read_from_sql_with_retry('direct_criteria_performance', 'ods')
    # Список для уникальных записей, для ускорения записи по 1 кортежу
    criteria_with_uniques = []
    # Уникальные ключи
    criteria_keys = set()
    start_time = time.time()
    # Проход по всем записям из ods с конца, чтобы брать сначала более свежие версии
    for i in range(len(criteria_from_db) - 1, -1, -1):
        # Получаем ключ каждой записи
        key = str(criteria_from_db['Date'][i]) + criteria_from_db['CriterionId'][i] + \
              criteria_from_db['CampaignId'][i] + criteria_from_db['AdGroupId'][i] + \
              criteria_from_db['ClientLogin'][i] + criteria_from_db['Criterion'][i]
        # Условие на уникальность ключа
        if key not in criteria_keys:
            # Добавление в множество ключей
            criteria_keys.add(key)
            # Добавление уникального по ключу direct_criteria_performance
            criteria_with_uniques.append(criteria_from_db.iloc[i])
    print(time.time() - start_time, 'секунд потрачено на direct_criteria_performance')
    CRITERIA_PERFORMANCE_REPORT2 = pd.DataFrame(criteria_with_uniques, columns=criteria_from_db.columns)
    # Запись CRITERIA_PERFORMANCE_REPORT в таблицу БД
    common_to_all.write_to_sql_with_retry(CRITERIA_PERFORMANCE_REPORT2, engine, 'direct_criteria_performance', 'ods_plus', 'replace')
    # Для отладки
    print('Запись 2 отчета окончена для для ods_plus')
get_all_direct()