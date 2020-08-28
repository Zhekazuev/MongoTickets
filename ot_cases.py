import pymongo
import config
import csv

mongo_ip = config.mongo_ip
mongo_db = config.mongo_db
mongo_collection = config.mongo_collection


def read_file():
    with open('OT Tickets.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:


def update():
    pass


def insert():
    pass


def main():
    cloud_file = read_file()
    myclient = pymongo.MongoClient(mongo_ip)
    mydb = myclient[mongo_db]
    mycol = mydb[mongo_collection]
    records = mycol.find({"RT №": "90103287"})
    for record in records:
        if record:
            update()
        else:
            insert()


if __name__ == '__main__':
    main()


