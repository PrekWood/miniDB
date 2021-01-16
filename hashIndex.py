count_buckets = 0
bucketlist = [[]]
maxbucketsize = 5
row = []


def balancebuckets(value, bucketlist):
    global count_buckets
    # the templist has all the values from the bucket
    count_buckets += 1
    templist = []
    for bucket in bucketlist:
        for data in bucket:
            templist.append(data)

    # we add the value (this was the reason for overflow)
    templist.append(value)

    # we clear the buckets
    bucketlist.clear()

    # we create new buckets
    for i in range(2 ** count_buckets):
        bucketlist.append([])

    # we insert all the elements
    # we are sure that we dont have overflow situation
    for value in templist:
        bucketlist[value % (2 ** count_buckets)].append(value)
    return bucketlist


def insert(value):
    global bucketlist
    # if the bucket is not full insert the element
    if len(bucketlist[value % (2 ** count_buckets)]) < maxbucketsize:
        bucketlist[value % (2 ** count_buckets)].append(value)
    else:
        # if is full create new buckets and balance the elements
        bucketlist = balancebuckets(value, bucketlist)
    return bucketlist


def find(value):
    if value in bucketlist[value % len(bucketlist)]:
        return row[value]


# create a small table
id = 0
name = "prekas"
for i in range(2000):
    row.append([id + i, name + str(i)])
for i in range(2000):
    bucketlist = insert(row[i][0])
print("~~BUCKET LIST~~")
print(bucketlist)

# insert a value
value = 10
print("find the value ", value, " is the row",find(value))