import hashlib


class Hashlisted:
    def __init__(self, b):
        '''
        The tree abstraction.
        '''
        self.b = b # branching factor
        self.nodes = [] # list of nodes. Every new node is appended here
        self.root = None # the index of the root node



class Node:
    def __init__(self, b, id=[], ptrs=[]):
        self.b = b # branching factor
        self.id = id # Values (the data from the pk column)
        self.ptrs = ptrs # ptrs (the indexes of each datapoint or the index of another bucket)







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
