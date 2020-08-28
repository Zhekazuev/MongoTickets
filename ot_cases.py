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
    return csv_reader


def update(mycol, row):
    if row["Ответ MTS"] == "Открыт":
        status = "Opened"
    else:
        status = "Closed"
    myquery = {"RT №": row["№ кейса в RT"]}
    newvalues = {"$set": {"Device_type": row["Оборудование"],
                          "Priority": row["Приоритет"],
                          "Started_date": row["Дата (открыт)"],
                          "Temporary_solution_date": row["Дата (должен быть закрыт)"],
                          "Closing_date": row["Дата (закрыт)"],
                          "Status": status}}

    x = mycol.update_one(myquery, newvalues)
    print(x)


def insert(mycol, row):
    if row["Ответ MTS"] == "Открыт":
        status = "Opened"
    else:
        status = "Closed"

    mydict = {
        "RT №": row["№ кейса в RT"],
        "Case_title": row["Краткое описание"],
        "Device_type": row["Оборудование"],
        "S/W_version": "",
        "Priority": row["Приоритет"],
        "Description": "",
        "Started_date": row["Дата (открыт)"],
        "Temporary_solution_date": row["Дата (должен быть закрыт)"],
        "Closing_date": row["Дата (закрыт)"],
        "Contract_number": "1410-18/ЗП",
        "Comments": "",
        "Status": status,
        "Opener": ""}
    x = mycol.insert_one(mydict)
    print(x)


def delete(mycol, id):
    mycol.delete_one({"_id": id})


def main():
    with open('OT Tickets.csv', encoding="utf8") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')

        myclient = pymongo.MongoClient(mongo_ip)
        mydb = myclient[mongo_db]
        mycol = mydb[mongo_collection]

        for row in csv_reader:
            records = mycol.find({"RT №": row["№ кейса в RT"]})
            if records.count() == 0:
                insert(mycol, row)
            elif records.count() == 1:
                update(mycol, row)
            elif records.count() == 2:
                delete(mycol, records[0]["_id"])
                update(mycol, row)


if __name__ == '__main__':
    main()


