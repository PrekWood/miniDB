from database import Database


class HashIndex:

    def __init__(self, db, max_bucket_size=5):
        self.db = db
        self.max_bucket_size = max_bucket_size
        self.bucketlist = [[]]
        self.row = []
        self.count_buckets = 0

    def create_hashtable(self, data):
        # loop through tables and create hash indexes
        # we insert a list with the data for the hashing and the pointer
        # to the db
        for i in range(len(data)):
            self.insert([data,i])

    def __balancebuckets__(self, value):
        # the templist has all the values from the bucket
        self.count_buckets += 1
        templist = []
        for bucket in self.bucketlist:
            for data in bucket:
                templist.append(data)

        # we add the value (this was the reason for overflow)
        templist.append(value)

        # we clear the buckets
        self.bucketlist.clear()

        # we create new buckets
        for i in range(2 ** self.count_buckets):
            self.bucketlist.append([])

        # we insert all the elements
        # we are sure that we dont have overflow situation
        for value in templist:
            self.bucketlist[value[0] % (2 ** self.count_buckets)].append(value)
        return self.bucketlist

    def insert(self, value):
        # if the bucket is not full insert the element
        try:
            if len(self.bucketlist[value[0] % (2 ** self.count_buckets)]) < self.max_bucket_size:
                self.bucketlist[value[0] % (2 ** self.count_buckets)].append(value)
            else:
                # if is full create new buckets and balance the elements
                self.bucketlist = self.__balancebuckets__(value)
            return self.bucketlist
        except:
            print("Error with insert")

    def find(self, value):
        if self.bucketlist == [[]]:
            print("Hash Table is empty, create first")
            return
        if value in self.bucketlist[value % len(self.bucketlist)][0]:
            return self.row[value]


db = Database("test_db", True)
h = HashIndex("asd")
# db.create_index()
h.create_hashtable([db.select("users", "id", "*")])

print("~~BUCKET LIST~~")
print(h.bucketlist)
# insert a value
value = 10
print("find the value ", value, " is the row", h.find(value))
