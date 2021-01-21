def convert_str_to_int(value):
    try:
        return int(value)
    except:
        sum = 0
        for i in value:
            sum += ord(i)
        return sum


class Bucket:
    def __init__(self, data=None):
        if data is None:
            self.data = []

    def insert(self, row):
        self.data.append(row)


class HashIndex:

    def __init__(self, max_bucket_size=5):
        self.max_bucket_size = max_bucket_size
        self.bucket_list = [Bucket()]
        self.row = []
        self.count_buckets = 0
        self.hashfuction_exp = 0

    def hashFunction(self, input):
        return input % (2 ** self.hashfuction_exp)

    def __balancebuckets__(self, value, idx):

        # the templist has all the values from the bucket
        self.hashfuction_exp += 1

        templist = []
        for bucket in self.bucket_list:
            for row in bucket.data:
                templist.append(row)

        # we add the value (this was the reason for overflow)
        templist.append([value,idx])

        # we clear the buckets
        self.bucket_list.clear()

        # we create new buckets
        for i in range(2 ** self.hashfuction_exp):
            self.bucket_list.append(Bucket())

        # we insert all the elements
        # we are sure that we dont have overflow situation
        for value in templist:
            h =self.hashFunction(value[0])
            self.bucket_list[h].insert(value)
        return self.bucket_list

    def insert(self, value, idx):
        # if the value is string
        if type(value) == str:
            value = convert_str_to_int(value)
        # if the bucket is not full insert the element
        try:
            if len(self.bucket_list[self.hashFunction(value)].data) < self.max_bucket_size:
                self.bucket_list[value % (2 ** self.hashfuction_exp)].insert([value, idx])
            else:
                # if is full create new buckets and balance the elements
                self.bucket_list = self.__balancebuckets__(value, idx)
            return self.bucket_list
        except:
            print("Error with insert")

    def find(self, value):
        if self.bucket_list == [[]]:
            print("Hash Table is empty, create first")
            return
        else:
            if type(value) == str:
                value = convert_str_to_int(value)
            # finds the right bucket and search every column-list
            idx = []
            for data in self.bucket_list[self.hashFunction(value)].data:
                if value in data:
                    idx.append(data[1])
            return idx
