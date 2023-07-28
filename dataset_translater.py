import googletrans as gtr
import pickle
from tinydb import TinyDB
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage
from tinydb_smartcache import SmartCacheTable

# ! shu paketlarni yuklash kerak
# * pip install tinydb tinydb_smartcache googletrans==3.1.0a0

# ! TEGMANG
def connect(dbpath: str) -> TinyDB:
    return TinyDB(dbpath,  storage=CachingMiddleware(JSONStorage))

# ! TEGMANG
def close(db: TinyDB):
    if db is None:
        return
    db.close()
    db = None

# !TODO o'zizdagi pickle pathga moslab oling!
def __picklepath(file):
    return f"./datasets/pickle/{file}.pickle"


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
            tr = translator.translate(text=partial_text, src="en", dest="uz").text
            if tr is None:
                tr = error
            trs.append(tr)
        return "\n ".join(trs)
    tr = translator.translate(text=text, src="en", dest="uz").text
    if tr is None:
        tr = error
    return tr


def main():
    print("proccessing ...")
    # !TODO tarjima qilish kerak bo'lgan fayllar
    pickle_names = [
        "wikipedia-list-1",
        # .....
        "wikipedia-list-66",
    ]

    # !TODO o'zizga moslab oling
    dbpath = "./datasets/database/tinydb.json"
    db = connect(dbpath)
    db.table_class = SmartCacheTable
    
    try:
        for name in pickle_names:
            # !TODO agar ma'lumotlarni o'chirib yuborish kerak bo'lsa
            # db.drop_table(name)
            dataset = load_pickle(name)
            end = len(dataset)
            table = db.table(name)
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

# * Ctrl + C da xavfsiz dastur ishni yakunlaydi
print("Use Ctrl + C for safe stop program")
main()
print("all done.")
