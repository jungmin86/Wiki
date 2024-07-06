import requests
from bs4 import BeautifulSoup
import pandas as pd
import pandasql as ps
import re
import logging
from datetime import datetime
import sqlite3
import json

# 로깅 환경 설정
logging.basicConfig(filename='./missions/W1/mission3/etl_project_log.txt', level=logging.INFO,
                    format='%(asctime)s  %(message)s', datefmt=datetime.now().strftime('%Y-%b-%d-%H-%M-%S')
                    )


def extract():
    logging.info("GDP 데이터 수집 시작")
    url = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    res = requests.get(url)
    html = res.text
    soup = BeautifulSoup(html, 'html.parser')

    table = soup.find('table', {'class': 'wikitable'})
    rows = table.find_all('tr')[2:]
    fields = []
    for row in rows:
        data = row.find_all('td')
        data = [d.text.strip().replace(',', '') for d in data]
        if data[0] == 'World':
            continue
        country = data[0]
        gdp = data[1]
        if gdp != '—':
            year = data[2]
        else:
            year = '-'
            continue

        fields.append([country, gdp, year])

    df_gdp = pd.DataFrame(
        fields, columns=["Country", "GDP_IN_BILLION_USD", "Year"])

    logging.info("GDP 데이터 수집 완료")

    return df_gdp


def preprocess_data(df_gdp):

    logging.info("GDP 단위 가공 시작")

    # GDP를 백만(million)에서 십억(billion) 단위로 변환
    df_gdp['GDP_IN_BILLION_USD'] = df_gdp['GDP_IN_BILLION_USD'].apply(
        lambda x: round(int(x) / 1000, 2))

    # Year 컬럼에서 [~] 내용 제외
    df_gdp['Year'] = df_gdp['Year'].apply(
        lambda x: re.sub(r"\[.*?\]", '', x).strip())

    logging.info("GDP 단위 가공 완료")

    return df_gdp


def change_country_name(df_gdp):
    logging.info("국가명 가공 시작")
    with open('./missions/W1/mission3/countries.json', 'r', encoding='utf-8') as file:
        name_dict = json.load(file)

    for old_name, new_name in name_dict.items():
        if df_gdp['Country'].str.contains(old_name).any():
            df_gdp.loc[df_gdp['Country'] == old_name, 'Country'] = new_name

    logging.info("국가명 가공 완료")
    return df_gdp


def sort_by_gdp(df_gdp):
    logging.info("GDP순 데이터 정렬 시작")
    df_gdp_sort = df_gdp.sort_values("GDP_IN_BILLION_USD", ascending=False)
    logging.info("GDP순 데이터 정렬 완료")
    return df_gdp_sort


def scrap_region_to_df():
    logging.info("region 데이터 수집 시작")
    url = 'https://en.wikipedia.org/wiki/List_of_countries_and_territories_by_the_United_Nations_geoscheme'
    res = requests.get(url)
    html = res.text
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table', {'class': 'wikitable'})

    fields = []

    for table in tables:
        rows = table.find_all('tr')[1:]

        for row in rows:
            data = row.find_all('td')
            country = re.sub(r"\[.*?\]|\(.*?\)", '',
                             data[0].text.strip()).strip()
            region = re.sub(r"\[.*?\]", '', data[3].text.strip()).strip()
            fields.append([country, region])

    df_region = pd.DataFrame(fields, columns=["Country", "Region"])
    logging.info("region 데이터 수집 시작")
    return df_region


def merge_countries_and_region(df, df_region):

    df_merged = df.merge(df_region, on='Country', how='left')
    return df_merged


def transform(df):
    df_gdp = preprocess_data(df)
    df_region = scrap_region_to_df()
    df_gdp = change_country_name(df_gdp)
    df_sort = sort_by_gdp(df_gdp)
    df_merged = df_sort.merge(df_region, on='Country', how='left')

    return df_merged


def load_to_json(df):
    logging.info("JSON 파일로 저장 시작")
    output_file = './missions/W1/mission3/Countries_by_GDP.json'

    df.to_json(output_file, orient='records')
    logging.info(f"JSON 파일로 저장 완료")


def load_to_db(df):
    logging.info("데이터베이스 연결 시작")
    table_name = 'Countries_by_GDP'
    db_path = './missions/W1/mission3/'
    db_name = 'World_Economies.db'

    conn = sqlite3.connect(db_path + db_name)
    cur = conn.cursor()
    logging.info("데이터베이스 연결 완료")

    logging.info("테이블 없으면 생성 시작")
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} 
        (Country TEXT PRIMARY KEY,
        GDP_IN_BILLION_USD REAL,
        Year TEXT,
        Region TEXT);
    """
    cur.execute(create_table_query)
    conn.commit()
    logging.info("테이블 생성 완료")

    logging.info("데이터 삽입/업데이트 시작")
    insert_query = f"INSERT OR REPLACE INTO {table_name} (Country, GDP_IN_BILLION_USD, Year, Region) VALUES (?, ?, ?, ?);"
    values = df[['Country', 'GDP_IN_BILLION_USD',
                 'Year', 'Region']].values.tolist()
    cur.executemany(insert_query, values)
    conn.commit()
    logging.info("데이터 삽입/업데이트 완료")
    conn.close()


def load(df_transformed):
    load_to_json(df_transformed)
    load_to_db(df_transformed)


def print_over_100B_USD_by_sql():
    print("-----GDP가 100B USD 이상이 되는 국가만(SQL)-----")
    table_name = 'Countries_by_GDP'
    db_path = './missions/W1/mission3/'
    db_name = 'World_Economies.db'

    conn = sqlite3.connect(db_path + db_name)
    cur = conn.cursor()
    query = f"""
    SELECT * FROM {table_name} WHERE GDP_IN_BILLION_USD >= 100
    """
    cur.execute(query)
    rows = cur.fetchall()
    fields = []
    for row in rows:
        fields.append(row)
    df = pd.DataFrame(
        fields, columns=['Country', 'GDP_IN_BILLION_USD', 'Year', 'Region'])
    print(df)
    conn.close()


def print_top5_groupby_region_by_sql():
    print("-----각 Region별로 top5 국가의 GDP 평균(SQL)-----")
    db_path = './missions/W1/mission3/'
    db_name = 'World_Economies.db'
    table_name = 'Countries_by_GDP'

    conn = sqlite3.connect(db_path + db_name)
    cur = conn.cursor()
    query = f"""
    SELECT Region, AVG(GDP_IN_BILLION_USD) FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY Region ORDER BY GDP_IN_BILLION_USD DESC) AS row_num
        FROM {table_name}
    ) WHERE row_num <= 5
    GROUP BY Region
    """

    cur.execute(query)
    rows = cur.fetchall()
    fields = []
    for row in rows:
        fields.append(row)

    df = pd.DataFrame(fields, columns=['Region', 'Average of GDP'])
    print(df)
    conn.close()


def print_over_100B_USD_by_pandasql(df):
    print("-----GDP가 100B USD 이상이 되는 국가만(Pandasql)-----")

    query = "SELECT * FROM df WHERE GDP_IN_BILLION_USD >= 100"

    # pandasql을 사용하여 쿼리 실행
    result = ps.sqldf(query)
    print(result)


def print_top5_groupby_region_by_pandasql(df):
    print("-----각 Region별로 top5 국가의 GDP 평균(Pandasql)-----")

    query = """
    SELECT Region, AVG(GDP_IN_BILLION_USD) as [Average of GDP] FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY Region ORDER BY GDP_IN_BILLION_USD DESC) AS row_num
        FROM df
    ) WHERE row_num <= 5
    GROUP BY Region
    """

    result = ps.sqldf(query)
    print(result)


def main():
    # try:
    logging.info("!!!!!ETL 프로세스 시작!!!!!")
    logging.info("-----Extract-----")

    df_gdp = extract()

    logging.info("-----Transform-----")

    df_transformed = transform(df_gdp)

    logging.info("-----Load-----")

    load(df_transformed)

    logging.info("!!!!!ETL 프로세스 완료!!!!!")

    print_over_100B_USD_by_sql()  # db에서 조회하기 때문에 df 필요없음
    print_top5_groupby_region_by_sql()  # db에서 조회하기 때문에 df 필요없음
    print_over_100B_USD_by_pandasql(df_transformed)
    print_top5_groupby_region_by_pandasql(df_transformed)

    # except Exception as e:
    #     print(f"에러 발생: {str(e)}")
    #     logging.error(f"에러 발생: {str(e)}")


if __name__ == "__main__":
    main()
