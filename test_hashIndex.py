from database import Database

db = Database("test1", False)
db.create_table('department', ['dept_name', 'building', 'budget'], [str, str, int], primary_key='dept_name')
db.insert('department', ['Civil Eng.', 'Chandler', 255041.46])
db.insert('department', ['Biology', 'Candlestick', 647610.55])
db.insert('department', ['History', 'Taylor', 699140.86])
db.insert('department', ['Physics', 'Wrigley', 942162.76])
db.insert('department', ['Marketing', 'Lambeau', 210627.58])
db.insert('department', ['Pol. Sci.', 'Whitman', 573745.09])
db.insert('department', ['English', 'Palmer', 611042.66])
db.insert('department', ['Accounting', 'Saucon', 441840.92])
db.insert('department', ['Comp. Sci.', 'Lamberton', 106378.69])
db.insert('department', ['Languages', 'Linderman', 601283.60])
db.insert('department', ['Finance', 'Candlestick', 866831.75])
db.insert('department', ['Geology', 'Palmer', 406557.93])
db.insert('department', ['Cybernetics', 'Mercer', 794541.46])
db.insert('department', ['Astronomy', 'Taylor', 617253.94])
db.insert('department', ['Athletics', 'Bronfman', 734550.70])
db.insert('department', ['Statistics', 'Taylor', 395051.74])
db.insert('department', ['Psychology', 'Thompson', 848175.04])
db.insert('department', ['Math', 'Brodhead', 777605.11])
db.insert('department', ['Elec. Eng.', 'Main', 276527.61])
db.insert('department', ['Mech. Eng.', 'Rauch', 520350.65])
print("department ok")
print("-----------------------------")
print("Create hash index")
db.create_index('department', index_name="hashindex", index_type="HashIndex")
print("-----------------------------")
print("Search with hash index")
db.select('department', '*', 'dept_name==Math')