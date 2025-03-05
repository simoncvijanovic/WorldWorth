from fastapi import FastAPI, Depends 
from sqlalchemy import create_engine, Column, Integer, Float, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import os


# Read DATABASE_URL from the environment
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set!")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class PropertyPriceHistory(Base):
    __tablename__ = "property_price_history"
    id = Column(Integer, primary_key=True, index=True)
    property_id = Column(String, index=True)
    location = Column(String, index=True)
    price = Column(Float)
    date_sold = Column(Date)

Base.metadata.create_all(bind=engine)

app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def home():
    return {"message": "Real Estate API is running!"}

@app.get("/price-history/{property_id}")
def get_price_history(property_id: str, db: Session = Depends(get_db)):
    records = db.query(PropertyPriceHistory).filter(PropertyPriceHistory.property_id == property_id).all()
    return {"property_id": property_id, "price_history": records}
