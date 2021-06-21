from pymongo import MongoClient



def cursor():
    # Connect INFO:
    client = MongoClient('mongodb+srv://felipecid:Fyrfdt53@cluster0.n3hgr.mongodb.net')
    db = client.get_database('inside_market')
    collection = db['stock_stock']

    return collection
