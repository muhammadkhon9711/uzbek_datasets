from tinydb import TinyDB, Query
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from tinydb_smartcache import SmartCacheTable

# TODO: o'zizga moslang
def __dbpath(file):
    return f"./datasets/database/{file}.json"


# ! TEGMANG
def connect(name: str) -> TinyDB:
    return TinyDB(__dbpath(name),  storage=CachingMiddleware(JSONStorage))

# ! TEGMANG
def close(db: TinyDB):
    if db is None:
        return
    db.close()
    db = None

def check(datasetname, database=None):
    print("proccessing ...")
    # !TODO o'zizga moslab oling
    if database is None:
        database = "./datasets/database/wikitinydb.json"
    dbpath = database
    db = connect(dbpath)
    db.table_class = SmartCacheTable
    try:
        # !TODO agar ma'lumotlarni o'chirib yuborish kerak bo'lsa
        # db.drop_table(datasetname)
        table = db.table(datasetname)
        length = len(table)
        q = Query()
        r = table.search(q.i == 100)
        print(length, len(r[0]["tr"]))
    except Exception as ex:
        print("ERROR")
        print(ex)
        print(ex.with_traceback())
        close(db)
        return
    except KeyboardInterrupt:
        print("program stopped")
        close(db)
        return
    finally:
        close(db)

check("wikipedia-list-20", "wikidb")