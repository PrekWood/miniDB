from database import Database

db = Database("smdb", True)
db.select("instructor", "*")
print("department ok")
print("-----------------------------")
print("Create hash index")
db.create_index('instructor', index_name="hashindex", index_type="HashIndex")
print("-----------------------------")
print("Search with hash index")
db.select('instructor', '*', 'ID==12121')