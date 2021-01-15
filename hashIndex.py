from database import Database


class Hashlisted:
    def __init__(self):
        self.headval = None  # root bucket


class NodeBucket:
    # every bucket has a list for the values and a list for the next buckets
    def __init__(self, size=5, data=[]):
        self.data = data  # a list with indexes to the DB file
        self.size = size  # how many id for the DB can handle
        self.next = []

    def insert(self, value):
        if self.next == []:
            self.next.append(NodeBucket())
        exp = len(self.next) - 1
        if self.next[int(value % (2 ** exp))].size > len(self.next[value % (2 ** exp)].data):
            self.next[value % (2 ** exp)].data.append(value)
            print(len(self.next[value % (2 ** exp)].data))
        else:
            exp += 1
            templist = []
            for y in self.next:
                for k in y.data:
                    templist.append(k)
            templist.append(value)
            self.next.append(NodeBucket())
            for y in self.next:
                y.data.clear()
            for y in templist:
                self.next[y % (2 ** exp)].data.append(y)
            for i in self.next:
                print(i.data)


# create a small table
id = 0
name = "prekas"
row = []
for i in range(100):
    row.append([id + i, name + str(i)])
l = Hashlisted()
l.headval = NodeBucket()
l.headval.insert(row[0][0])
l.headval.insert(row[1][0])
l.headval.insert(row[2][0])
l.headval.insert(row[3][0])
l.headval.insert(row[4][0])
l.headval.insert(row[5][0])
'''
    def __hashfuction__(self, value):
        return value % self.hashfact

    def find(self, value):
        bucket = self.__hashfuction__(value)
        current_bucket = self.next[bucket]
        # find the bucket tha is a leaf or the attribute next == []
        # this loop will be stop when we find the bucket - leaf
        while not current_bucket.isleaf:
            current_bucket = current_bucket.next[current_bucket.__hashfuction__(value)]
        # if the value is inside the list bucket.value returns the bucket id
        if value in current_bucket.value:
            print("find it in bucket ", current_bucket.id)

    def insert(self, value):
        bucket = self.__hashfuction__(value)
        # if this bucket is not full
        current_bucket = self.next[bucket]
        while not current_bucket.isleaf:
            current_bucket = current_bucket.next[current_bucket.__hashfuction__(value)]
        if len(current_bucket.value) < current_bucket.size:
            current_bucket.value.append(value)
        else:
            current_bucket.hashfaction = 2
            current_bucket.isleaf = False
            newbuckets1 = NodeBucket("newbuckets1")
            newbuckets2 = NodeBucket("newbuckets2")
            current_bucket.next = [newbuckets1, newbuckets2]
        print("ok")


def testfind():
    # make the list
    list1 = Hashlisted()
    # rout bucket is the node with id root
    list1.headval = NodeBucket("root")

    # ------------
    # test for find faction
    # create first bucket layer
    bucket1 = NodeBucket('1st', value=[3, 6])
    bucket2 = NodeBucket('2st', value=[1, 4])
    bucket3 = NodeBucket("3st", hashfact=2, isleaf=False)
    # make a second layer bucket
    bucket3i = NodeBucket('3st_i', value=[2, 8])
    bucket3ii = NodeBucket('3st_ii', value=[5])
    bucket3.next.append(bucket3i)
    bucket3.next.append(bucket3ii)

    # connect these buckets to the root bucket with id root
    list1.headval.next.append(bucket1)
    list1.headval.next.append(bucket2)
    list1.headval.next.append(bucket3)
    list1.headval.find(5)
    # -------------


def testinstert():
    # make the list
    list1 = Hashlisted()
    # rout bucket is the node with id root
    list1.headval = NodeBucket("root")
    # test for insert faction
    for i in range(3):
        list1.headval.next.append(NodeBucket(i))
    list1.headval.insert(1)
    list1.headval.insert(2)
    list1.headval.insert(3)
    list1.headval.insert(4)
    list1.headval.insert(5)
    list1.headval.insert(6)
    list1.headval.insert(8)
    # --------------
    # try to create the find faction
    list1.headval.find(5)

# testfind()
testinstert()
# https://www.tutorialspoint.com/python_data_structure/python_linked_lists.htm
'''
