from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, Column, Integer, Float, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from datetime import date
import os

# Read DATABASE_URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set!")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define the table for property price history
class PropertyPriceHistory(Base):
    __tablename__ = "property_price_history"
    id = Column(Integer, primary_key=True, index=True)
    property_id = Column(String, index=True)
    location = Column(String, index=True)
    price = Column(Float)
    date_sold = Column(Date)

# Create the database table if it doesn’t exist
Base.metadata.create_all(bind=engine)

app = FastAPI()

# Dependency to get database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def home():
    return {"message": "Real Estate API is running!"}

# Define the request model for inserting price data
class PriceHistoryCreate(BaseModel):
    property_id: str
    location: str
    price: float
    date_sold: date

# Endpoint to insert a new price record
@app.post("/add-price")
def add_price(price_data: PriceHistoryCreate, db: Session = Depends(get_db)):
    new_record = PropertyPriceHistory(
        property_id=price_data.property_id,
        location=price_data.location,
        price=price_data.price,
        date_sold=price_data.date_sold
    )
    db.add(new_record)
    db.commit()
    return {"message": "Price history added successfully!"}

# Endpoint to fetch price history by property ID
@app.get("/price-history/{property_id}")
def get_price_history(property_id: str, db: Session = Depends(get_db)):
    records = db.query(PropertyPriceHistory).filter(PropertyPriceHistory.property_id == property_id).all()
    return {"property_id": property_id, "price_history": records}
