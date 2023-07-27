from core import database as db, dataset as dt
import asyncio

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

async def proccess(file):
    print(f"{file}, proccessing ...")
    dataset = dt.PickleDataSet(language="en")
    dataset.load(file)
    rows = []
    for data in dataset.data():
        row = dict2text(cleanup(data))
        rows.append(row)
    dataset.set(rows, "en")
    dataset.save(file+"-list")
    await asyncio.sleep(0.01)
    print(f"{file}, done.")

async def main(loop):
    params = [
        #("abstracts-2021", "en_uz_small_abstracts"),
        ("abstracts-large", "en_uz_large_abstracts"),
        #("wikkipedia", "en_uz_large_wikkipedia"),
    ]
    for i in params:
        loop.create_task(proccess(i[0]))
loop = asyncio.get_event_loop()
loop.run_until_complete(main(loop))