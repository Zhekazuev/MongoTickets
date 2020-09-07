"""
Перенос заявок ТП Связьком в БД
http://jira/browse/PSSER-2586
"""
import pymongo
import config
import csv


mongo_ip = config.mongo_ip
mongo_db = config.mongo_db
mongo_collection = config.mongo_collection_swiazcom


def file(csv_file):
    """
     Read a CSV file using csv.DictReader
    """
    csv_reader = csv.DictReader(csv_file, delimiter=',')
    return csv_reader


def update(mycol, row):
    """
    Update one document in DB
    """

    myquery = {"RT №": row["RT"]}
    newvalues = {"$set": {"Case_title": row["Технологическая услуга"],
                          "Device_type": row["Device_type"],
                          "Priority": row["Priority"],
                          "Description": row["Description"],
                          "Started_date": row["Started_date"],
                          "Closing_date": row["Closing_date"],
                          "Contract_number": row["Contract_number"],
                          "Comments": row["Comments"],
                          "Status": row["Status"],
                          "Opener": row["Opener"]}}

    results = mycol.update_one(myquery, newvalues)
    return results


def insert(mycol, row):
    """
    Insert one document to DB
    """
    mydict = {
        "RT №": row["RT"],
        "Case_title": row["Технологическая услуга"],
        "Device_type": row["Device_type"],
        "Priority": row["Priority"],
        "Description": row["Description"],
        "Started_date": row["Started_date"],
        "Closing_date": row["Closing_date"],
        "Contract_number": row["Contract_number"],
        "Comments": row["Comments"],
        "Status": row["Status"],
        "Opener": row["Opener"]}
    results = mycol.insert_one(mydict)
    return results


def delete(mycol, id):
    """
    Delete one document in DB
    """
    results = mycol.delete_one({"_id": id})
    return results


def main():
    with open('SwiazCom Tickets.csv', encoding="utf8") as csv_file:
        csv_reader = file(csv_file)

        myclient = pymongo.MongoClient(mongo_ip)
        mydb = myclient[mongo_db]
        mycol = mydb[mongo_collection]

        for row in csv_reader:
            records = mycol.find({"RT №": row["RT"]})
            count = mycol.count_documents({"RT №": row["RT"]})
            if count == 0:
                insert(mycol, row)
            elif count == 1:
                update(mycol, row)
            elif count == 2:
                delete(mycol, records[0]["_id"])
                update(mycol, row)


if __name__ == '__main__':
    main()
