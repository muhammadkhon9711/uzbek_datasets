import googletrans as gtr
import pickle
import sys
from tinydb import TinyDB, Query
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from tinydb_smartcache import SmartCacheTable
import time
import re

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
def qcointer(text):
    qc = 0
    rc = 0
    ec = 0
    gc = 0
    for c in text:
        if c == "(": 
            qc += 1
            continue
        if c == ")":
            qc -= 1
            continue
        if c == "[":
            rc += 1
            continue
        if c == "]":
            rc -= 1
        if c == "{":
            ec += 1
            continue
        if c == "}":
            ec -= 1
            continue
        if c == "\"":
            gc += 1
    return qc == 0 and rc == 0 and ec == 0 and (gc % 2 == 0)

def _split_dot(text:str) -> str:
    texts = text.split(".")
    new = ""
    dots = []
    for t in texts:
        t = t.strip()
        if len(t) == 0:
            continue
        new += t + "."
        if qcointer(new) == True:
            dots.append(new)
            new = ""
    return "\n".join(dots)

def _split_enter(text: str) -> list:
    texts = text.split("\n")
    new = ""
    news = []
    for t in texts:
        if len(new + t) > 4999:
            news.append(new)
            new = ""
        new += t + "\n"
    news.append(new)
    return news


# * Tarjima qilish logikasi

exp = re.compile(r"\n{1,}")
def translate(text):
    translator = gtr.Translator()
    # * XATOLIK MATNI
    # ! TEGMANG
    error = "([ERROR])"
    # * Agar matn 5000 ko'p bo'lsa maydalab tarjima qilib yana juftlaydi
    if len(text) > 5000:
        trs = []
        for partial_text in _split_enter(text):
            tr = translator.translate(
                text=partial_text, src="en", dest="uz").text
            if tr is None:
                tr = error
            trs.append(tr)
        return "\n".join(trs)
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
            text = _split_dot(re.sub(exp, ".", dataset[i]))
            translated = translate(text)
            table.insert({"en": text, "uz": translated, "i": i})
    except Exception as ex:
        print("ERROR")
        print(ex)
        close(db)
        return -1
    except KeyboardInterrupt:
        print("program stopped")
        close(db)
        return 0
    finally:
        close(db)
    return 0


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
"""
source ./.venv/bin/activate && python dataset_translater.py wikipedia-list-1 wikidb-1

source ./.venv/bin/activate && python dataset_translater.py wikipedia-list-2 wikidb-2

source ./.venv/bin/activate && python dataset_translater.py wikipedia-list-3 wikidb-3

source ./.venv/bin/activate && python dataset_translater.py wikipedia-list-4 wikidb-4

source ./.venv/bin/activate && python dataset_translater.py wikipedia-list-5 wikidb-5

source ./.venv/bin/activate && python dataset_translater.py wikipedia-list-6 wikidb-6

source ./.venv/bin/activate && python dataset_translater.py wikipedia-list-7 wikidb-7

source ./.venv/bin/activate && python dataset_translater.py wikipedia-list-8 wikidb-8

source ./.venv/bin/activate && python dataset_translater.py wikipedia-list-9 wikidb-9

source ./.venv/bin/activate && python dataset_translater.py wikipedia-list-10 wikidb-10
"""
print("Use Ctrl + C for safe stop program")
datasetname = sys.argv[1]
dbname = sys.argv[2]
code = 1
try: 
    while (code != 0):
        code = main(datasetname, dbname)
        print("program returned code:", code, " wait 5 seconds")
        time.sleep(5)
except KeyboardInterrupt:
    print("Program stopped")
print("all done.")
