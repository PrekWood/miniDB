from database import Database

db = Database("smdb", True)
db.select("course", "*")
print("course ok")
print("-----------------------------")
print("Create hash index")
# db.create_index('course', index_name='course_id', column_name='course_id', index_type="HashIndex")
# db.create_index('course', index_name='course_id', column_name='course_id', index_type="Btree")
db.create_index('course', index_name='course_title', column_name='title', index_type="HashIndex")
# print("-----------------------------")
# print("Search with hash index for course: ")
# db.select('department', '*', 'dept_name==Finance')
# print("-----------------------------")
# print("Create hash index")
# db.create_index('course', index_type="HashIndex")
# print("-----------------------------")
# print("Search with hash index for course: ")
db.select('course', '*', 'title==Genetics')
# db.unlock_table('course')
# db.select('course', '*', 'course_id==BIO-301')
