import requests
import pandas as pd
from bs4 import BeautifulSoup

url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
timestamp = '2024-01-19 10:09'

def scrape_html(url):
    response = requests.get(url)
    assert response.status_code == 200
    return response.content

def filter_uri(content, timestamp):
    soup = BeautifulSoup(content, "html.parser")
    table = soup.find("table")
    rows = table.find_all("tr")[3:-1]

    for row in rows:
        cols = row.find_all('td')

        if len(cols) >= 4 and cols[1].text.strip() == timestamp:
            target_uri = cols[0].find('a')['href']

            return target_uri
        
def main():
    content = scrape_html(url)
    target_path = filter_uri(content, timestamp)
    
    file_url = url + target_path
    df = pd.read_csv(file_url)
    max_value = df['HourlyDryBulbTemperature'].max()
    row_with_max_value = df[df['HourlyDryBulbTemperature'] == max_value]
    print(row_with_max_value)


if __name__ == "__main__":
    main()
