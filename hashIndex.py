def convert_str_to_int(value):
    sum = 0
    for i in value:
        sum += ord(i)
    return sum


class HashIndex:

    def __init__(self, max_bucket_size=5):
        self.max_bucket_size = max_bucket_size
        self.bucketlist = [[]]
        self.row = []
        self.count_buckets = 0

    def __balancebuckets__(self, value, idx):
        # the templist has all the values from the bucket
        self.count_buckets += 1
        templist = []
        for bucket in self.bucketlist:
            for data in bucket:
                templist.append(data)

        # we add the value (this was the reason for overflow)
        templist.append([value, idx])

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

    def insert(self, value, idx):
        # if the value is string
        if type(value) == str:
            value = convert_str_to_int(value)
        # if the bucket is not full insert the element
        try:
            if len(self.bucketlist[value % (2 ** self.count_buckets)]) < self.max_bucket_size:
                self.bucketlist[value % (2 ** self.count_buckets)].append([value, idx])
            else:
                # if is full create new buckets and balance the elements
                self.bucketlist = self.__balancebuckets__(value, idx)
            print(self.bucketlist)
            return self.bucketlist
        except:
            print("Error with insert")

    def find(self, value):
        if self.bucketlist == [[]]:
            print("Hash Table is empty, create first")
            return
        else:
            if type(value) == str:
                value = convert_str_to_int(value)
            # finds the right bucket and search every column-list
            for data in self.bucketlist[value % len(self.bucketlist)]:
                if value in data:
                    return [data[1]]
