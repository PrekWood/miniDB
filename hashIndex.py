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
        self.bucket_list = []
        self.hashfuction_exp = 0

    def hashFunction(self, input):
        return input % (2 ** self.hashfuction_exp)

    def _balanceBuckets(self, value, idx):

        self.hashfuction_exp += 1
        # the templist has all the values from the bucket
        templist = []
        for bucket in self.bucket_list:
            for row in bucket.data:
                templist.append(row)

        # we add the value (this was the reason of overflow)
        templist.append([value, idx])

        # we clear the buckets
        self.bucket_list.clear()

        # we create new buckets
        for i in range(2 ** self.hashfuction_exp):
            self.bucket_list.append(Bucket())

        # we insert all the elements
        # we are sure that we dont have overflow situation
        for value in templist:
            self.bucket_list[self.hashFunction(value[0])].insert(value)
        return self.bucket_list

    def insert(self, value, idx):

        if self.bucket_list == []:
            self.bucket_list = [Bucket()]

        # if the value is string
        if type(value) == str:
            value = convert_str_to_int(value)
        # if the bucket is not full insert the element
        try:
            if len(self.bucket_list[self.hashFunction(value)].data) < self.max_bucket_size:
                self.bucket_list[value % (2 ** self.hashfuction_exp)].insert([value, idx])
            else:
                # if is full create new buckets and balance the elements
                self.bucket_list = self._balanceBuckets(value, idx)
            return self.bucket_list
        except:
            print("Error with insert")

    def find(self, value):
        if len(self.bucket_list) == 1 and self.bucket_list[0].data == []:
            print("Hash Table is empty, create first")
            return
        else:
            if type(value) == str:
                value = convert_str_to_int(value)
            idx = []
            for data in self.bucket_list[self.hashFunction(value)].data:
                if value in data:
                    idx.append(data[1])
            return idx

    def delete(self, value):
        if self.bucket_list == []:
            print("Hash Table is empty, create first")
            return
        else:
            if type(value) == str:
                value = convert_str_to_int(value)
            for data in self.bucket_list[self.hashFunction(value)].data:
                if value in data:
                    self.bucket_list[self.hashFunction(value)].data.remove(data)
