import hashlib


class Hashlisted:
    def __init__(self):
        self.headval = None


class Bucket:
    def __init__(self, value = None):
        self.value = None
        self.next = None
        self.hashfact = None



list1 = SLinkedList()
list1.headval = Node("Mon")
e2 = Node("Tue")
e3 = Node("Wed")
# Link first Node to second node
list1.headval.nextval = e2

# Link second Node to third node
e2.nextval = e3



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


# https://www.tutorialspoint.com/python_data_structure/python_linked_lists.htm
