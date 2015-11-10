from sqlalchemy import *
from models import *
from dbconn import session

back_rels = {
	Users: [Orders],
	Orders: [OrderProds],
	Products: [OrderProds]}

forward_rels = {
	OrderProds: [Orders, Products],
	Orders: [Users]}

def rfeat(tab, target_tab):
	return

def dfeat(tab, target_tab):
	return

def make_features(tab, visited = set(), depth = 0, max_depth = -1):
	visited = visited.union({tab})

	back_tabs, forward_tabs = [], []
	if tab in back_rels: 
		back_tabs = back_rels[tab]
	if tab in forward_rels:
		forward_tabs = forward_rels[tab]

	for target_tab in back_tabs:
		if depth == max_depth:
			continue
		make_features(target_tab, visited, depth + 1, max_depth)
		rfeat(tab, target_tab)

	for target_tab in forward_tabs:
		if target_tab in visited or depth == max_depth:
			continue
		make_features(target_tab, visited, depth + 1, max_depth)
		dfeat(tab, target_tab)

	print("%s at depth:%d - visited: %s" % (str(tab.__table__).upper(), depth, [str(visit.__table__).upper() for visit in visited]))

# make_features(Products)

# print(Orders.__table__.foreign_keys)
# print(dir(Orders.__table__))
# print(Orders.__table__.)

print(inspect(Orders).relationships.items())

for k, v in inspect(Orders).relationships.items():
	# print(v, dir(v))
	# print(v, dir(v.target))
	print(v, v.target.name)
