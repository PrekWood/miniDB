from database import Database
import sys
import copy

class Bucket:
    def __init__(self, max_bucket_size=64, data=None):
        if(data == None):
            self.data = []
        else:
            self.data = data
        self.max_bucket_size = max_bucket_size
        self.size = sys.getsizeof(self.data)

    def insert(self, row):
        self.data.append(row)
        self.size = sys.getsizeof(self.data)

class HashIndex:
    def __init__(self, db, max_bucket_size=64):
        # todo: rename count_buckets σε κάτι σαν hashfunctionN γιατι δεν ειναι όντως το count των buckets αλλα το %2^n της hashing function
        # todo: address table συζητηση

        if type(db) is not Database:
            raise Exception('db parameter must be an object of the Database class')

        self.db = db
        self.max_bucket_size = max_bucket_size
        self.bucket_list = []
        self.row = []
        self.count_buckets = 0

    def hashFunction(self, input):
        return input % (2 ** self.count_buckets)

    def createHashtable(self, table, column=0):
        #todo: to be called by constuctor on primary key

        # loop through tables and create hash indexes
        # we insert a list with the data for the hashing and the pointer
        # to the db
        table_rows = self.db.tables[table].data
        for row_position in range(len(table_rows)):
            hash_key = table_rows[row_position][column]
            self.insert([hash_key, row_position])

    def _balanceBuckets(self, value):
        # todo: overflow after balance

        # the templist has all the values from the bucket

        self.count_buckets += 1

        templist = []
        for bucket in self.bucket_list:
            for row in bucket.data:
                templist.append(row)

        # we add the value (this was the reason for overflow)
        templist.append(value)

        # we clear the buckets
        self.bucket_list.clear()

        # we create new buckets
        for i in range(2 ** self.count_buckets):
            self.bucket_list.append(Bucket(self.max_bucket_size))

        # we insert all the elements
        # we are sure that we dont have overflow situation
        for value in templist:
            hash_value = self.hashFunction(value[0])
            self.bucket_list[hash_value].insert(value)

        return self.bucket_list


    def insert(self, value):
        # if the bucket is not full insert the element
        try:
            if self.bucket_list == []:
                self.bucket_list.append(Bucket(self.max_bucket_size))

            bucket_to_insert = self.bucket_list[self.hashFunction(value[0])]
            # we use deepcopy because python's simple copying makes it so that both variables point to the same
            # collection of memory. So this calculation would have been imposible without changing the object's data property
            bucket_data = copy.deepcopy(bucket_to_insert.data)
            bucket_data.append(value)
            bucket_size_after_insertion = sys.getsizeof(bucket_data)

            if(bucket_size_after_insertion <= bucket_to_insert.max_bucket_size):
                bucket_to_insert.insert(value)
            else:
                self.bucket_list = self._balanceBuckets(value)

        except:
            print("Error with insert")

    def search(self, value):
        if self.bucket_list == [[]]:
            print("Hash Table is empty, create first")
            return

        bucket = self.bucket_list[self.hashFunction(value)]

        for row in bucket.data:
            if row[0] == value:
                return row[1]



db = Database("test_db", True)
print(db.unlock_table('users'))
h = HashIndex(db, 64)
h.createHashtable("users")

# print(h.count_buckets)
# print(len(h.bucketlist))
# for bucket in h.bucketlist:
#     print(bucket, bucket.data)

# print("~~BUCKET LIST~~")
# print(h.bucketlist)
# insert a value
value = 10
print("find the value ", value, " is the row", h.search(value))
