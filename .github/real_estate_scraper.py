import requests
import csv
from datetime import datetime

# Government real estate data sources
GOV_SOURCES = {
    "US": "https://catalog.data.gov/dataset/real-estate-sales-data.csv",
    "UK": "https://www.gov.uk/government/uploads/system/price-paid-data.csv",
    "Canada": "https://creastats.crea.ca/en-CA/historical-sales.csv",
    "Australia": "https://data.gov.au/dataset/property-sales-data.csv",
    "New Zealand": "https://www.linz.govt.nz/data/land-records/property-sales-data.csv",
    "Germany": "https://www.govdata.de/dataset/real-estate-sales.csv",
    "France": "https://www.data.gouv.fr/fr/datasets/transactions-immobilieres",
    "Spain": "https://datos.gob.es/en/catalogo/real-estate-sales-prices",
    "Italy": "https://www.agenziaentrate.gov.it/portale/web/guest/osservatorio-del-mercato-immobiliare",
    "Netherlands": "https://data.overheid.nl/en/dataset/real-estate-sales",
    "Sweden": "https://www.scb.se/en/finding-statistics/statistics-by-subject-area/housing-construction-and-building/housing-market/real-estate-prices-and-registrations/",
    "Norway": "https://www.ssb.no/en/statbank/list/realestate",
    "Denmark": "https://www.dst.dk/en/Statistik/emner/ejendomme-og-boliger/ejendomspriser",
    "Finland": "https://www.stat.fi/en/real-estate-transactions",
    "Japan": "https://www.reinetdb.mlit.go.jp/en/real-estate-prices",
    "South Korea": "https://www.data.go.kr/en/dataset/real-estate-transactions",
    "Singapore": "https://data.gov.sg/dataset/resale-flat-prices",
    "Hong Kong": "https://data.gov.hk/en-data/dataset/housing-price-transactions",
    "China": "https://data.stats.gov.cn/english/easyquery.htm?cn=E0105",
    "India": "https://data.gov.in/catalog/all-india-property-transactions",
    "Brazil": "https://dados.gov.br/dataset/real-estate-sales-data",
    "Mexico": "https://datos.gob.mx/dataset/ventas-inmobiliarias",
    "South Africa": "https://data.gov.za/dataset/real-estate-sales",
    "Argentina": "https://datos.gob.ar/dataset/ventas-inmobiliarias",
    "Russia": "https://data.gov.ru/en/opendata/real-estate-sales",
    "UAE": "https://bayanat.ae/en/dataset/property-sales",
    "Saudi Arabia": "https://data.gov.sa/Data/en/real-estate-transactions",
    "Turkey": "https://acikveri.tkgm.gov.tr/dataset/real-estate-sales",
    "Malaysia": "https://www.data.gov.my/data/en_US/dataset/real-estate-transactions",
    "Indonesia": "https://data.go.id/dataset/real-estate-sales",
    "Thailand": "https://data.go.th/dataset/real-estate-transactions",
    "Philippines": "https://data.gov.ph/dataset/real-estate-sales",
    "Vietnam": "https://data.gov.vn/dataset/real-estate-transactions",
    "Chile": "https://datos.gob.cl/dataset/real-estate-transactions",
    "Colombia": "https://www.datos.gov.co/dataset/real-estate-sales",
    "Peru": "https://www.datosabiertos.gob.pe/dataset/real-estate-transactions",
    "Switzerland": "https://data.sbb.ch/explore/dataset/real-estate-sales/",
    "Belgium": "https://data.gov.be/en/dataset/real-estate-transactions",
    "Portugal": "https://dados.gov.pt/en/datasets/real-estate-sales/",
    "Greece": "https://www.data.gov.gr/dataset/real-estate-transactions/",
    "Ireland": "https://data.gov.ie/dataset/property-price-register",
    "Poland": "https://dane.gov.pl/dataset/real-estate-sales-data",
    "Czech Republic": "https://data.gov.cz/en/dataset/real-estate-transactions",
    "Hungary": "https://data.gov.hu/dataset/real-estate-prices",
    "Romania": "https://data.gov.ro/dataset/real-estate-sales",
    "Ukraine": "https://data.gov.ua/dataset/real-estate-sales-transactions",
    "Austria": "https://www.data.gv.at/katalog/dataset/immobilienpreisspiegel",
    "Belgium": "https://statbel.fgov.be/en/themes/construction-housing/house-prices",
    "Bulgaria": "https://data.egov.bg/data/resourceView/RealEstateSales",
    "Croatia": "https://data.gov.hr/dataset/real-estate-transactions",
    "Cyprus": "https://www.data.gov.cy/dataset/real-estate-sales",
    "Estonia": "https://www.eesti.ee/en/real-estate-transactions",
    "Latvia": "https://data.gov.lv/dati/lv/dataset/nekustama-ipasuma-darijumi",
    "Lithuania": "https://data.gov.lt/dataset/nekilnojamojo-turto-sandoriai",
    "Luxembourg": "https://data.public.lu/en/datasets/ventes-immobilieres/",
    "Malta": "https://data.gov.mt/dataset/property-sales",
    "Slovakia": "https://data.gov.sk/dataset/real-estate-transactions",
    "Slovenia": "https://podatki.gov.si/dataset/nepremicninski-posli"
}


# FastAPI Endpoint
API_URL = "https://worldworth.onrender.com/add-price"

# Function to download and process CSV data
def fetch_real_estate_data(country):
    if country not in GOV_SOURCES:
        print("Country data source not available.")
        return []
    
    response = requests.get(GOV_SOURCES[country])
    if response.status_code != 200:
        print(f"Failed to download data for {country}")
        return []
    
    data_lines = response.text.splitlines()
    csv_reader = csv.reader(data_lines)
    
    price_data = []
    next(csv_reader)  # Skip header
    
    for row in csv_reader:
        try:
            property_id = row[0]
            location = row[1]
            price = float(row[2].replace("$", "").replace(",", ""))
            date_sold = datetime.strptime(row[3], "%Y-%m-%d").date()
            
            price_data.append({
                "property_id": property_id,
                "location": location,
                "price": price,
                "date_sold": date_sold
            })
        except (IndexError, ValueError):
            continue  # Skip invalid rows
    
    return price_data

# Function to send scraped data to FastAPI
def push_to_api(data):
    for record in data:
        response = requests.post(API_URL, json=record)
        if response.status_code == 200:
            print(f"Uploaded: {record}")
        else:
            print(f"Failed to upload {record}: {response.text}")

if __name__ == "__main__":
    country = "US"  # Change to any country from GOV_SOURCES
    data = fetch_real_estate_data(country)
    if data:
        push_to_api(data)
