import hashlib


def hasfuct(i):
    return i % 2


class Hashlisted:
    def __init__(self):
        self.headval = None  # root bucket


class NodeBucket:
    # every bucket has a list for the values and a list for the next buckets
    def __init__(self, id, size=2, hashfact=3, value=None):
        self.id = id                # id to recognize each bucket
        self.value = value          # a list with indexes to the DB file
        self.size = size            # how many id for the DB can handle
        self.next = []              # for next linked bucket, this is for the overflow situation
        self.hashfact = hashfact    # hash fuction for the next linked buckets

    def find(self, value, hashfuct):
        bucket = value % hashfuct
        if value in self.next[bucket].value:
            print("find it in bucket ", self.next[bucket].id)


# make the list
list1 = Hashlisted()
# rout bucket is the node with id -1
list1.headval = NodeBucket(-1)

# create first bucket layer
bucket1 = NodeBucket('1st', value=[3, 6])
bucket2 = NodeBucket('2st', value=[1, 4])
bucket3 = NodeBucket('3st', value=[2, 5])
# connect these buckets to the root bucket with id = -1
list1.headval.next.append(bucket1)
list1.headval.next.append(bucket2)
list1.headval.next.append(bucket3)
i = list1.headval.next
# try to create the find faction
list1.headval.find(5, list1.headval.hashfact)
# https://www.tutorialspoint.com/python_data_structure/python_linked_lists.htm
