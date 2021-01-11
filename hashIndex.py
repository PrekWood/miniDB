import hashlib


def hasfuct(i):
    return i % 2


hash_table = [[] for _ in range(2)]

data = {'25': 'USA',
        '20': 'India',
        '10': 'Nepal',
        '22': 'Greece'}

for i in data:
    bucket = hasfuct(int(i))
    if len(hash_table[bucket]) == 2:
        print("bucket :", bucket, " is full")
    else:
        hash_table[bucket].append(i)
print(hash_table)
