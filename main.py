from database import Database
from hash_index import HashIndex
def main():
    db = Database("test_db", True)
    # db.create_table("users", ["id","name"], [int,str])

    # id = 1
    # name = "prekas"
    # row  = [id, name]
    #
    # for i in range(100):
    #     db.insert("users", row)
    # db.save()

    db.select("teachers", "*")


    h = HashIndex(db)


if __name__ == "__main__":
    main()




# rootBucket.next
# [[]]),Bucket(),Bucket(),Bucket(),Bucket()]
# Bucket() := data,maxelemet

bucketlist = []

[[],[],[],[]]