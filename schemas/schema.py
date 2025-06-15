from sqlalchemy import Column, Integer,Float,Boolean, DateTime, String
from sqlalchemy.orm import declarative_base


Base=declarative_base()

#The order table created to insert into the database
class Order(Base):
    __tablename__="order"
    id=Column(Integer, primary_key=True, autoincrement=True)
    order_id=Column(Integer)
    order_date=Column(DateTime)
    customer_id=Column(String)
    customer_name=Column(String)
    email=Column(String)
    product=Column(String)
    product_category=Column(String)
    quantity=Column(Float)
    price_usd=Column(Float)
    country=Column(String)
    state=Column(String)
    invalid_email=Column(Boolean)
    clv=Column(Float)
    new_or_returning=Column(String)

    def __repr__(Self):
        return {Self.order_id}