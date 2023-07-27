from core import dataset as dt
import datasets as dts

def huggingface_abstracts():
    dataset = dts.load_dataset("wikipedia", "20220301.en", cache_dir="./datasets/downloaded")
    data = dt.PickleDataSet()
    data.set(dataset["train"], "en")
    data.save("wikkipedia")

def dict2text(data:dict):
    text = ""
    for key in data.keys():
        if not data[key].endswith("."):
            data[key] += "."
        if data[key] is not None:
            text += data[key] + "\n"
    return text

def cleanup(data:dict):
    uselesskeys = [
        "id",
        "doi",
        "report-no",
        "categories",
        "license",
        "versions",
        "update_date",
        "authors_parsed",
        "url"
    ]

    for key in uselesskeys:
        if key in data:
            data.pop(key)
    return data

def split(file, data:list, step):
    print(f"{file} prossesing..")
    size = len(data)
    c = 0
    start = 0
    raw = dt.PickleDataSet(language="en") 
    while start + step < size:
        raw.set(data[start:start+step])
        raw.save(f"{file}-{c}")
        start += step
        c += 1
    raw.set(data[start:size])
    raw.save(f"{file}-{c}")
    print(f"{file} split done.")

def proccess(file):
    print(f"{file}, proccessing ...")
    dataset = dt.PickleDataSet(language="en")
    dataset.load(file)
    rows = []
    for data in dataset.data():
        row = dict2text(cleanup(data))
        rows.append(row)
    dataset.set(rows, "en")
    dataset.save(file+"-list")
    print(f"{file}, done.")

def main():
    proccess("abstracts-large")

def main_split():
    dataset = dt.PickleDataSet()
    dataset.load("abstracts-large-list")
    print(len(dataset.data()))
    split("abstracts-100K", dataset.data(), 100000)

main()
main_split()
d = dt.PickleDataSet("abstracts-100K-0", "en")
for i in range(10):
    print(d.data()[i])