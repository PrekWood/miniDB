from database import Database

db = Database("smdb", True)
db.select("teaches", "*")
print("course ok")
print("-----------------------------")
print("Create hash index")
# db.create_index('course', index_name='course_course_id', column_name='course_id', index_type="HashIndex")
# db.create_index('teaches', index_name='teaches_course_id', column_name='course_id', index_type="HashIndex")
# db.create_index('course', index_name='course_id', column_name='course_id', index_type="Btree")
# db.create_index('course', index_name='course_title', column_name='title', index_type="HashIndex")
# print("-----------------------------")
# print("Search w/ith hash index for course: ")
# db.select('department', '*', 'dept_name==Finance')
# print("-----------------------------")
# print("Create hash index")
# db.create_index('course', index_type="HashIndex")
# print("-----------------------------")
# print("Search with hash index for course: ")
# db.select('course', '*', 'title==Genetics')
# db.unlock_table('course')
# db.select('course', '*', 'course_id==CS-315')
# db.inner_join('teaches', 'course', 'course_id==course_id ')
# db.inner_join('course', 'teaches', 'course_id==course_id ')
# if True:
#     for i in range(100):
#         if 2 is i:
#             break
#     for y in range(100):
#         if 2 is y:
#             break
a = '''
## teaches_join_course ##
  teaches_ID (str)  teaches_course_id (str)      teaches_sec_id (str)  teaches_semester (str)      teaches_year (int)  course_course_id (str)    course_title (str)          course_dept_name (str)      course_credits (int)
------------------  -------------------------  ----------------------  ------------------------  --------------------  ------------------------  --------------------------  ------------------------  ----------------------
             10101  CS-101                                          1  Fall                                      2009  CS-101                    Intro. to Computer Science  Comp. Sci.                                     4
             10101  CS-315                                          1  Spring                                    2010  CS-315                    Robotics                    Comp. Sci.                                     3
             10101  CS-347                                          1  Fall                                      2009  CS-347                    Database System Concepts    Comp. Sci.                                     3
             12121  FIN-201                                         1  Spring                                    2010  FIN-201                   Investment Banking          Finance                                        3
             15151  MU-199                                          1  Spring                                    2010  MU-199                    Music Video Production      Music                                          3
             22222  PHY-101                                         1  Fall                                      2009  PHY-101                   Physical Principles         Physics                                        4
             32343  HIS-351                                         1  Spring                                    2010  HIS-351                   World History               History                                        3
             45565  CS-101                                          1  Spring                                    2010  CS-101                    Intro. to Computer Science  Comp. Sci.                                     4
             45565  CS-319                                          1  Spring                                    2010  CS-319                    Image Processing            Comp. Sci.                                     3
             76766  BIO-101                                         1  Summer                                    2009  BIO-101                   Intro. to Biology           Biology                                        4
             76766  BIO-301                                         1  Summer                                    2010  BIO-301                   Genetics                    Biology                                        4
             83821  CS-190                                          1  Spring                                    2009  CS-190                    Game Design                 Comp. Sci.                                     4
             83821  CS-190                                          2  Spring                                    2009  CS-190                    Game Design                 Comp. Sci.                                     4
             83821  CS-319                                          2  Spring                                    2010  CS-319                    Image Processing            Comp. Sci.                                     3
             98345  EE-181                                          1  Spring                                    2009  EE-181                    Intro. to Digital Systems   Elec. Eng.                                     3

'''
b = '''
## teaches_join_course ##
  teaches_ID (str)  teaches_course_id (str)      teaches_sec_id (str)  teaches_semester (str)      teaches_year (int)  course_course_id (str)    course_title (str)          course_dept_name (str)      course_credits (int)
------------------  -------------------------  ----------------------  ------------------------  --------------------  ------------------------  --------------------------  ------------------------  ----------------------
             10101  CS-101                                          1  Fall                                      2009  CS-101                    Intro. to Computer Science  Comp. Sci.                                     4
             10101  CS-315                                          1  Spring                                    2010  CS-315                    Robotics                    Comp. Sci.                                     3
             10101  CS-347                                          1  Fall                                      2009  CS-347                    Database System Concepts    Comp. Sci.                                     3
             12121  FIN-201                                         1  Spring                                    2010  FIN-201                   Investment Banking          Finance                                        3
             15151  MU-199                                          1  Spring                                    2010  MU-199                    Music Video Production      Music                                          3
             22222  PHY-101                                         1  Fall                                      2009  PHY-101                   Physical Principles         Physics                                        4
             32343  HIS-351                                         1  Spring                                    2010  HIS-351                   World History               History                                        3
             45565  CS-101                                          1  Spring                                    2010  CS-101                    Intro. to Computer Science  Comp. Sci.                                     4
             45565  CS-319                                          1  Spring                                    2010  CS-319                    Image Processing            Comp. Sci.                                     3
             76766  BIO-101                                         1  Summer                                    2009  BIO-101                   Intro. to Biology           Biology                                        4
             76766  BIO-301                                         1  Summer                                    2010  BIO-301                   Genetics                    Biology                                        4
             83821  CS-190                                          1  Spring                                    2009  CS-190                    Game Design                 Comp. Sci.                                     4
             83821  CS-190                                          2  Spring                                    2009  CS-190                    Game Design                 Comp. Sci.                                     4
             83821  CS-319                                          2  Spring                                    2010  CS-319                    Image Processing            Comp. Sci.                                     3
             98345  EE-181                                          1  Spring                                    2009  EE-181                    Intro. to Digital Systems   Elec. Eng.                                     3

'''
c = '''
## course_join_teaches ##
course_course_id (str)    course_title (str)          course_dept_name (str)      course_credits (int)    teaches_ID (str)  teaches_course_id (str)      teaches_sec_id (str)  teaches_semester (str)      teaches_year (int)
------------------------  --------------------------  ------------------------  ----------------------  ------------------  -------------------------  ----------------------  ------------------------  --------------------
BIO-101                   Intro. to Biology           Biology                                        4               76766  BIO-101                                         1  Summer                                    2009
BIO-301                   Genetics                    Biology                                        4               76766  BIO-301                                         1  Summer                                    2010
CS-101                    Intro. to Computer Science  Comp. Sci.                                     4               10101  CS-101                                          1  Fall                                      2009
CS-101                    Intro. to Computer Science  Comp. Sci.                                     4               45565  CS-101                                          1  Spring                                    2010
CS-190                    Game Design                 Comp. Sci.                                     4               83821  CS-190                                          1  Spring                                    2009
CS-190                    Game Design                 Comp. Sci.                                     4               83821  CS-190                                          2  Spring                                    2009
CS-315                    Robotics                    Comp. Sci.                                     3               10101  CS-315                                          1  Spring                                    2010
CS-319                    Image Processing            Comp. Sci.                                     3               45565  CS-319                                          1  Spring                                    2010
CS-319                    Image Processing            Comp. Sci.                                     3               83821  CS-319                                          2  Spring                                    2010
CS-347                    Database System Concepts    Comp. Sci.                                     3               10101  CS-347                                          1  Fall                                      2009
EE-181                    Intro. to Digital Systems   Elec. Eng.                                     3               98345  EE-181                                          1  Spring                                    2009
FIN-201                   Investment Banking          Finance                                        3               12121  FIN-201                                         1  Spring                                    2010
HIS-351                   World History               History                                        3               32343  HIS-351                                         1  Spring                                    2010
MU-199                    Music Video Production      Music                                          3               15151  MU-199                                          1  Spring                                    2010
PHY-101                   Physical Principles         Physics                                        4               22222  PHY-101                                         1  Fall                                      2009

'''
d = '''
## course_join_teaches ##
course_course_id (str)    course_title (str)          course_dept_name (str)      course_credits (int)    teaches_ID (str)  teaches_course_id (str)      teaches_sec_id (str)  teaches_semester (str)      teaches_year (int)
------------------------  --------------------------  ------------------------  ----------------------  ------------------  -------------------------  ----------------------  ------------------------  --------------------
BIO-101                   Intro. to Biology           Biology                                        4               76766  BIO-101                                         1  Summer                                    2009
BIO-301                   Genetics                    Biology                                        4               76766  BIO-301                                         1  Summer                                    2010
CS-101                    Intro. to Computer Science  Comp. Sci.                                     4               10101  CS-101                                          1  Fall                                      2009
CS-101                    Intro. to Computer Science  Comp. Sci.                                     4               45565  CS-101                                          1  Spring                                    2010
CS-190                    Game Design                 Comp. Sci.                                     4               83821  CS-190                                          1  Spring                                    2009
CS-190                    Game Design                 Comp. Sci.                                     4               83821  CS-190                                          2  Spring                                    2009
CS-315                    Robotics                    Comp. Sci.                                     3               10101  CS-315                                          1  Spring                                    2010
CS-319                    Image Processing            Comp. Sci.                                     3               45565  CS-319                                          1  Spring                                    2010
CS-319                    Image Processing            Comp. Sci.                                     3               83821  CS-319                                          2  Spring                                    2010
CS-347                    Database System Concepts    Comp. Sci.                                     3               10101  CS-347                                          1  Fall                                      2009
EE-181                    Intro. to Digital Systems   Elec. Eng.                                     3               98345  EE-181                                          1  Spring                                    2009
FIN-201                   Investment Banking          Finance                                        3               12121  FIN-201                                         1  Spring                                    2010
HIS-351                   World History               History                                        3               32343  HIS-351                                         1  Spring                                    2010
MU-199                    Music Video Production      Music                                          3               15151  MU-199                                          1  Spring                                    2010
PHY-101                   Physical Principles         Physics                                        4               22222  PHY-101                                         1  Fall                                      2009

'''
if c == d:
    print("gamas re malaka")
else:
    print("malakia ekanes")