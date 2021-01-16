class HashIndex():

    def __init__(self, db, max_bucket_size=10):
        self.db = db
        self.max_bucket_size = max_bucket_size
        self.bucketlist = [[]]
        self.row = []
        self.count_buckets = 0
        # create a small table
        id = 0
        name = "prekas"
        # hashindex= HashIndex()

        for i in range(2000):
            self.row.append([id + i, name + str(i)])
        for i in range(2000):
            self.bucketlist = self.insert(self.row[i][0])

        # #loop through tables and create hash indexes
        # for table_index in self.db.tables:
        #     table = self.db.tables[table_index]

    def balancebuckets(self, value):
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
            self.bucketlist[value % (2 ** self.count_buckets)].append(value)
        return self.bucketlist

    def insert(self, value):
        # if the bucket is not full insert the element
        if len(self.bucketlist[value % (2 ** self.count_buckets)]) < self.max_bucket_size:
            self.bucketlist[value % (2 ** self.count_buckets)].append(value)
        else:
            # if is full create new buckets and balance the elements
            self.bucketlist = self.balancebuckets(value)
        return self.bucketlist

    def find(self, value):
        if value in self.bucketlist[value % len(self.bucketlist)]:
            return self.row[value]


h =HashIndex("asd")
print("~~BUCKET LIST~~")
print(h.bucketlist)

# insert a value
value = 10
print("find the value ", value, " is the row", h.find(value))
