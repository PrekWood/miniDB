class HashBucket:
    def __init__(self, id, _max_input_size, data = []):
        self.id = id
        self._max_input_size = _max_input_size
        self.data = data
    # data = [[id,value],[]]
    def addItem(self, row):
        # print(self.id, row)
        self.data.append(row)

    # def redirect(self):
    #     redirect_bucket = HashBucketRedirect(self.id)


# class HashBucketRedirect:
#     '''
#         breakes a bucket into two and redirects the data that was send to be written here to the children buckets
#     '''
#     def __init__(self, id):
#         self.id = id
#         self.buckets = [
#
#         ]
#
#     def addItem(self, row):
#         self.data.append(row)

class HashIndex:
    def __init__(self, db, max_bucket_size=10):
        self.db = db
        self.max_bucket_size = max_bucket_size
        self.buckets = {}
        self.bucket_count = 0

        #loop through tables and create hash indexes
        for table_index in self.db.tables:
            table = self.db.tables[table_index]
            if table_index == "users":
                self.buckets[table_index] = [
                    self.addBucket(),
                    self.addBucket()
                ]
                self.createTableIndex(table_index, table.data)

                # print(self.buckets)

                for b in self.buckets[table_index]:
                    print(b)
                    print(b.data)


    def addBucket(self):
        bucket = HashBucket(self.bucket_count+1, self.max_bucket_size)
        self.bucket_count += 1
        return bucket

    def createTableIndex(self, table_index, data):
        for row in data:
            #Hash function returns the selected bucket
            hash_key = row[0] #todo: primary key
            bucket_id = self.hashFunction(hash_key)

            #bug: Τα rows γράφονται και στα δύο objects
            bucket_selected = self.buckets[table_index][bucket_id]
            bucket_selected.data.append(row)
            # bucket_selected.addItem(row)

    def hashFunction(self, hash_key):
        '''
        mod2^i hash function
        '''
        binary_hash_key = '{0:07b}'.format(hash_key) #todo: δυναμική επιλογή αριθού bits

        if(binary_hash_key[0] == "0"):
            bucket_id = 0
        else:
            bucket_id = 1
        return bucket_id


