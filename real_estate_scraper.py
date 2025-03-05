import requests
import csv
from datetime import datetime

# Government real estate data sources
GOV_SOURCES = {
    "US": "https://catalog.data.gov/dataset/real-estate-sales-data.csv",
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
