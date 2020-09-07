"""
Перенос заявок OpenTechnologies в БД
http://jira/browse/PSSER-2527
"""
import pymongo
import config
import csv

mongo_ip = config.mongo_ip
mongo_db = config.mongo_db
mongo_collection = config.mongo_collection_ot


def file(csv_file):
    """
     Read a CSV file using csv.DictReader
    """
    csv_reader = csv.DictReader(csv_file, delimiter=',')
    return csv_reader


def cloud_file():
    """
     Read a CSV file using Google SpreadSheet API
    """
    pass


def update(mycol, row):
    """
    Update one document in DB
    """
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

    results = mycol.update_one(myquery, newvalues)
    return results


def insert(mycol, row):
    """
    Insert one document to DB
    """
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
    results = mycol.insert_one(mydict)
    return results


def delete(mycol, id):
    """
    Delete one document in DB
    """
    results = mycol.delete_one({"_id": id})
    return results


def main():
    with open('OT Tickets.csv', encoding="utf8") as csv_file:
        csv_reader = file(csv_file)

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
