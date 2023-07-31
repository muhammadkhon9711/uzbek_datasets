from tinydb import TinyDB, Query
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from tinydb_smartcache import SmartCacheTable
import re

# TODO: o'zizga moslang
def __dbpath(file):
    return f"./datasets/database/{file}.json"


def __textpath(file):
    return f"./datasets/text/{file}.txt"

# ! TEGMANG
def connect(name: str) -> TinyDB:
    return TinyDB(__dbpath(name),  storage=CachingMiddleware(JSONStorage))

# ! TEGMANG
def close(db: TinyDB):
    if db is None:
        return
    db.close()
    db = None

def db2text(datasetname, databasename):
    db = connect(databasename)
    db.table_class = SmartCacheTable
    try:
        # !TODO agar ma'lumotlarni o'chirib yuborish kerak bo'lsa
        # db.drop_table(datasetname)
        table = db.table(datasetname)
        length = len(table)
        q = Query()
        r = table.search(q.i == length-1)
        print(length)
        s = ""
        print(r[0]["text"][0:200])
        print(r[0]["tr"])
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
        print(length)
        with open(__textpath(f"{datasetname}-en"), "w") as fen, open(__textpath(f"{datasetname}-uz"), "w") as fuz:
            for r in table.all():
                ens = r["en"].split("\n")
                uzs = r["uz"].split("\n")
                for en in ens:
                    en = en.strip()
                    if len(en) <= 1:
                        continue
                    fen.write(en + "\n")
                for uz in uzs:
                    uz = uz.strip()
                    if len(uz) <= 1:
                        continue
                    fuz.write(uz + "\n")
            
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

check("wikipedia-list-1", "wikidb-1")
print("done.")