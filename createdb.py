from dbconn import engine, session
from models import Base, Users, Orders, OrderProds, Products

Base.metadata.create_all(engine)

session.add_all([
	Users(name = "eric1", gender = 1, age = 20), 
	Users(name = "eric2", gender = 0, age = 45), 
	Users(name = "eric3", gender = 1, age = 22), 
	Users(name = "eric4", gender = 1, age = 29)])

session.add_all([
	Orders(user_id = 2),
	Orders(user_id = 1),
	Orders(user_id = 1),
	Orders(user_id = 2)])

session.add_all([
	OrderProds(order_id = 1, product_id = 3),
	OrderProds(order_id = 1, product_id = 1),
	OrderProds(order_id = 2, product_id = 2)
	OrderProds(order_id = 4, product_id = 3)])

session.add_all([
	Products(name = "prod1", price = 100),
	Products(name = "prod2", price = 500),
	Products(name = "prod3", price = 200),
	Products(name = "prod4", price = 600)])

session.commit()

# drop_all()