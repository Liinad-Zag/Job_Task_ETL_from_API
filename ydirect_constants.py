import time
import random
import common_to_all
# Токены и названия соответствующих кабинетов
oauth_tokens = [("Мой токен", "Мое название"),
                    ("Мой токен", "Мое название"),
                    ("Мой токен", "Мое название")]
# URL для запроса к отчетам
reports_url = "https://api.direct.yandex.com/json/v5/reports"
# Заголовки запроса
def get_headers(token):
    return {
        "Authorization": "Bearer " + token,
        "Accept-Language": "ru",
        "processingMode": "auto"
    }
def generate_report_name():
    today = time.localtime()
    return f"Report_{time.strftime('%Y%m%d%H%M%S', today)}_{random.randint(100000, 999999)}"
def get_data(date_from, date_to):
    return {"params": {"SelectionCriteria": {"DateFrom": date_from,"DateTo": date_to},
                       "FieldNames": ["Date", "CampaignName", "CampaignId", "Impressions", "Clicks", "Ctr", "Cost",
                                      "AvgCpc", "AvgTrafficVolume", "AvgPageviews", "ConversionRate",
                                      "CostPerConversion", "Conversions", "ClientLogin", "AvgImpressionPosition"],
                       "ReportName": generate_report_name(), "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
                       "DateRangeType": "CUSTOM_DATE", "Format": "TSV", "IncludeVAT": "NO",
                       "IncludeDiscount": "NO"}}
def get_data2(date_from, date_to):
    return {"params": {"SelectionCriteria": {"DateFrom": date_from,"DateTo": date_to},
                        "FieldNames": ["Date", "ClientLogin", "Criterion", "CriterionId", "CampaignName",
                                       "CampaignId", "AdGroupName", "AdGroupId", "Impressions", "Clicks", "Ctr",
                                       "Cost", "AvgCpc", "AvgImpressionPosition", "AvgTrafficVolume",
                                       "ConversionRate", "Conversions"], "ReportName": generate_report_name(),
                        "ReportType": "CRITERIA_PERFORMANCE_REPORT", "DateRangeType": "CUSTOM_DATE",
                        "Format": "TSV", "IncludeVAT": "NO", "IncludeDiscount": "NO"}}
def get_all_data():
    return {"params": {"SelectionCriteria": {},
                       "FieldNames": ["Date", "CampaignName", "CampaignId", "Impressions", "Clicks", "Ctr", "Cost",
                                      "AvgCpc", "AvgTrafficVolume", "AvgPageviews", "ConversionRate",
                                      "CostPerConversion", "Conversions", "ClientLogin", "AvgImpressionPosition"],
                       "ReportName": generate_report_name(), "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
                       "DateRangeType": "ALL_TIME", "Format": "TSV", "IncludeVAT": "NO",
                       "IncludeDiscount": "NO"}}
def get_all_data2():
    return {"params": {"SelectionCriteria": {},
                        "FieldNames": ["Date", "ClientLogin", "Criterion", "CriterionId", "CampaignName",
                                       "CampaignId", "AdGroupName", "AdGroupId", "Impressions", "Clicks", "Ctr",
                                       "Cost", "AvgCpc", "AvgImpressionPosition", "AvgTrafficVolume",
                                       "ConversionRate", "Conversions"], "ReportName": generate_report_name(),
                        "ReportType": "CRITERIA_PERFORMANCE_REPORT", "DateRangeType": "ALL_TIME",
                        "Format": "TSV", "IncludeVAT": "NO", "IncludeDiscount": "NO"}}
# Функция, которая возвращает дату последней загрузки директа
"""def get_date_from(cabinet):
    conn = common_to_all.get_con()
    cursor = conn.cursor()
    # Запрос на получение даты
    source_string = ('Директ CAMPAIGN_PERFORMANCE_REPORT по ' + cabinet,)
    select = '''
    select max(date_to) from "input".responses
    where "source" = %s
    '''
    cursor.execute(select, source_string)
    # Возвращает список значений
    s = cursor.fetchone()
    # Если в бд нет данных возвращает константу
    date = '2024-12-01'
    if s[0]:
        # Выбор первого
        date_val = s[0]
        # В формат yyyy-mm-dd
        date = date_val.strftime('%Y-%m-%d')
    # Зафиксировать изменения
    conn.commit()
    # Закрыть курсор и соединение
    cursor.close()
    conn.close()
    return date"""
# Для указания даты выгрузки за все время
all_time_date_from = '2000-01-01'
# Значения для input
def get_data_list(data_str, data_str2, raw_response, raw_response2, cabinet, curr_time, date_to, date_from):
    return [
                (data_str, 'post', raw_response, 'tcv', 'Директ CAMPAIGN_PERFORMANCE_REPORT по ' + cabinet, curr_time, date_to, date_from),
                (data_str2, 'post', raw_response2, 'tcv', 'Директ CRITERIA_PERFORMANCE_REPORT по ' + cabinet, curr_time, date_to, date_from)]
# Запрос для input
insert_query = """
            INSERT INTO "input".responses (request_body, request_type, response, response_type, "source", date_loaded, date_to, date_from)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """