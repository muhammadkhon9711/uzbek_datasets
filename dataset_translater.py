import googletrans as gtr
import pickle
import sys
from tinydb import TinyDB, Query
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from tinydb_smartcache import SmartCacheTable

# ! shu paketlarni yuklash kerak
# * pip install tinydb tinydb_smartcache googletrans==3.1.0a0


# !TODO o'zizdagi pickle pathga moslab oling!


def __picklepath(file):
    return f"./datasets/pickle/{file}.pickle"

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




# ! TEGMANG
def load_pickle(filename: str):
    with open(__picklepath(filename), "rb") as file:
        data = file.read()
    raw = pickle.loads(data)
    return raw

# * Bu yerda berilgan matnni kichik qismlarga ajratadi


def _split(text: str) -> list:
    texts = text.split("\n")
    new = ""
    news = []
    for t in texts:
        if len(new + t) > 4999:
            news.append(new)
            new = ""
        new += t
    return news

# * Tarjima qilish logikasi


def translate(text):
    translator = gtr.Translator()
    # * XATOLIK MATNI
    # ! TEGMANG
    error = "([ERROR])"
    # * Agar matn 5000 ko'p bo'lsa maydalab tarjima qilib yana juftlaydi
    if len(text) > 5000:
        trs = []
        for partial_text in _split(text):
            tr = translator.translate(
                text=partial_text, src="en", dest="uz").text
            if tr is None:
                tr = error
            trs.append(tr)
        return "\n ".join(trs)
    tr = translator.translate(text=text, src="en", dest="uz").text
    if tr is None:
        tr = error
    return tr


def main(datasetname, database=None):
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
        dataset = load_pickle(datasetname)
        end = len(dataset)
        table = db.table(datasetname)
        start = len(table)
        for i in range(start, end):
            text = dataset[i]
            translated = translate(text)
            table.insert({"text": text, "tr": translated, "i": i})
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
        start = len(table)
        q = Query()
        r = table.search(q.i == 100)
        print(start, len(r[0]["tr"]))
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


# * Ctrl + C da xavfsiz dastur ishni yakunlaydi
# source ./.venv/bin/activate
# python dataset_translater.py wikipedia-list-20 wikidb-20
print("Use Ctrl + C for safe stop program")
datasetname = sys.argv[1]
dbname = sys.argv[2]
main(datasetname, dbname)
print("all done.")
