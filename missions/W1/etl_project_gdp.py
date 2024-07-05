import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import logging
from datetime import datetime
import sqlite3

# 로깅 환경 설정
logging.basicConfig(filename='etl_project_log.txt', level=logging.INFO,
                    format='%(asctime)s - %(message)s', datefmt=datetime.now().strftime('%Y-%b-%d-%H-%M-%S')
                    )


def scrap_gdp():
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
        gdp = data[1]
        if gdp != '—':
            country = data[0]
            gdp = round(int(gdp) / 1000, 2)
            year = re.sub(r"\[.*?\]", '', data[2]).strip()
        else:
            continue

        fields.append([country, gdp, year])

    df_gdp = pd.DataFrame(fields, columns=["country", "gdp", "year"])
    logging.info("GDP 데이터 수집 완료")
    return df_gdp


def change_country_name(df_gdp):
    logging.info("국가명 가공 시작")
    name_dict = {
        "United States": "United States of America",
        "United Kingdom": "United Kingdom of Great Britain and Northern Ireland",
        "Russia": "Russian Federation",
        "South Korea": "Republic of Korea",
        "Turkey": "Türkiye",
        "Taiwan": "Taiwan Province of China",
        "Vietnam": "Viet Nam",
        "Hong Kong": "China, Hong Kong Special Administrative Region",
        "Czech Republic": "Czechia",
        "Ivory Coast": "Côte d'Ivoire",
        "Tanzania": "United Republic of Tanzania",
        "DR Congo": "Congo",
        "Macau": "China, Macao Special Administrative Region",
        "Palestine": "State of Palestine",
        "Moldova": "Republic of Moldova",
        "Brunei": "Brunei Darussalam",
        "Laos": "Lao People's Democratic Republic",
        "Cape Verde": "Cabo Verde",
        "East Timor": "Timor-Leste",
        "São Tomé and Príncipe": "Sao Tome and Principe",
        # "Syria": "Syrian Arab Republic",
        # "North Korea": "Democratic People's Republic of Korea",
    }
    for old_name, new_name in name_dict.items():
        if df_gdp['country'].str.contains(old_name).any():
            df_gdp.loc[df_gdp['country'] == old_name, 'country'] = new_name

    logging.info("국가명 가공 완료")
    return df_gdp


def sort_by_gdp(df_gdp):
    logging.info("GDP순 데이터 정렬 시작")
    df_gdp_sort = df_gdp.sort_values("gdp", ascending=False)
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

    df_region = pd.DataFrame(fields, columns=["country", "region"])
    logging.info("region 데이터 수집 시작")
    return df_region


def extract_to_json(df):
    logging.info("JSON 파일로 저장 시작")
    output_file = 'Countries_by_GDP.json'

    df.to_json(output_file, orient='records')
    logging.info(f"JSON 파일로 저장 완료")


def extract_to_db(df):
    logging.info("데이터베이스 연결 시작")
    table_name = 'Countries_by_GDP'
    db_name = 'World_Economies.db'

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    logging.info("데이터베이스 연결 완료")

    logging.info("테이블 없으면 생성 시작")
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} 
        (Country TEXT PRIMARY KEY,
        GDP_USD_billion REAL,
        Region TEXT);
    """
    cur.execute(create_table_query)
    conn.commit()
    logging.info("테이블 생성 완료")

    logging.info("데이터 삽입/업데이트 시작")
    insert_query = f"INSERT OR REPLACE INTO {table_name} (Country, GDP_USD_billion, Region) VALUES (?, ?, ?);"
    values = df[['country', 'gdp', 'region']].values.tolist()
    cur.executemany(insert_query, values)
    conn.commit()
    logging.info("데이터 삽입/업데이트 완료")
    conn.close()


def print_over_100B_USD(df):
    print("-----GDP가 100B USD이상이 되는 국가만-----")
    print(df.loc[df['gdp'] >= 100])
    logging.info("-----GDP가 100B USD이상이 되는 국가만-----")
    logging.info(df.loc[df['gdp'] >= 100])


def print_top5_groupby_region(df):
    print("-----각 Region별로 top5 국가의 GDP 평균-----")
    grouped_df = df.groupby('region')

    for region, group in grouped_df:
        top5_avg_gdp = group.nlargest(5, 'gdp')['gdp'].mean()
        print(f"({region}, {top5_avg_gdp})")

    logging.info("-----각 Region별로 top5 국가의 GDP 평균-----")
    logging.info("\n".join(
        f"({region}, {group.nlargest(5, 'gdp')['gdp'].mean()})" for region, group in grouped_df))


def print_over_100B_USD_by_sql():
    print("-----GDP가 100B USD 이상이 되는 국가만(SQL)-----")
    table_name = 'Countries_by_GDP'
    db_name = 'World_Economies.db'

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    query = f"""
    SELECT * FROM {table_name} WHERE GDP_USD_billion >= 100
    """
    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.close()


def print_top5_groupby_region_by_sql():
    print("-----각 Region별로 top5 국가의 GDP 평균(SQL)-----")
    table_name = 'Countries_by_GDP'
    db_name = 'World_Economies.db'

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    query = f"""
    SELECT Region, AVG(GDP_USD_billion) FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY region ORDER BY GDP_USD_billion DESC) AS row_num
        FROM {table_name}
    ) WHERE row_num <= 5
    GROUP BY Region
    """

    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.close()


def main():
    # try:
    logging.info("!!!!!ETL 프로세스 시작!!!!!")
    logging.info("-----Extract-----")

    df_gdp = scrap_gdp()
    df_region = scrap_region_to_df()

    logging.info("-----Transform-----")

    df_gdp = change_country_name(df_gdp)
    df_sort = sort_by_gdp(df_gdp)
    df_merged = df_sort.merge(df_region, on='country', how='left')

    logging.info("-----Load-----")

    extract_to_json(df_merged)
    extract_to_db(df_merged)

    logging.info("!!!!!ETL 프로세스 완료!!!!!")

    print_over_100B_USD(df_merged)
    print_top5_groupby_region(df_merged)
    print_over_100B_USD_by_sql()
    print_top5_groupby_region_by_sql()

    # except Exception as e:
    #     print(f"에러 발생: {str(e)}")
    #     logging.error(f"에러 발생: {str(e)}")


if __name__ == "__main__":
    main()