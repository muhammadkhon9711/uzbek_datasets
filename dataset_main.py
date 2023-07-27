from core import dataset as dt
import datasets as dts

def huggingface_abstracts():
    dataset = dts.load_dataset("universeTBD/arxiv-abstracts-large", cache_dir="./datasets/downloaded")
    data = []
    for raw in dataset["train"]:
        data.append(dict2text(cleanup(raw)))
    dt.save_pickle(data, "abstracts-list")


def huggingface_wikkipedia():
    dataset = dts.load_dataset("wikipedia", "20220301.en", cache_dir="./datasets/downloaded")
    data = []
    for raw in dataset["train"]:
        data.append(dict2text(cleanup(raw)))
    dt.save_pickle(data, "wikkipedia-list")



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
        "url",
        "journal-ref",
        "comments",
        "authors",
        "submitter"
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
    while start + step < size:
        raw = data[start:start+step]
        dt.save_pickle(raw, f"{file}-{c}")
        start += step
        c += 1
    raw = (data[start:size])
    dt.save_pickle(raw, f"{file}-{c}")
    print(f"{file} split done.")

def main_split():
    dataset = dt.load_pickle("abstracts-list")
    print(len(dataset))
    split("abstracts-100K", dataset, 100000)

#huggingface_abstracts()
#main_split()

huggingface_wikkipedia()