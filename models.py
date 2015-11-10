from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

Base = declarative_base()

class Users(Base):
	__tablename__ = 'users'
	id = Column(Integer, primary_key = True)
	name = Column(String)
	gender = Column(Integer)
	age = Column(Integer)

	orders = relationship("Orders", backref = "users")

	def __repr__(self):
		return "<Users(name ='%s')>" % self.name

class Orders(Base):
	__tablename__ = 'orders'
	id = Column(Integer, primary_key = True)
	user_id = Column(Integer, ForeignKey('users.id'))

	orderprods = relationship("OrderProds", backref = "orders")

	def __repr__(self):
		return "<Orders(id = %d, user_id = %d)>" % (self.id, self.user_id)

class OrderProds(Base):
	__tablename__ = 'orderprods'
	id = Column(Integer, primary_key = True)
	order_id = Column(Integer, ForeignKey('orders.id'))
	product_id = Column(Integer, ForeignKey('products.id'))

	def __repr__(self):
		return "<OrderProds(id = %d, order_id = %d, product_id = %d)>" % (self.id, self.order_id, self.product_id)

class Products(Base):
	__tablename__ = 'products'
	id = Column(Integer, primary_key = True)
	name = Column(String)
	price = Column(Integer)

	orderprods = relationship("OrderProds", backref = "products")

	def __repr__(self):
		return "<Products(name = '%s', price = %d)>" % (self.name, self.price)