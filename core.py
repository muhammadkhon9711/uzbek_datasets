import asyncio as aio
import aiofiles as aiof
import pickle
import sqlite3 as sql
import python_translator as pytr
import datasets as dts

datasets_path = "./datasets"

def make_dataset(languages):
    return {
        "languages": languages,
        "count": 0
    }

async def load_pickle(filename):
    async with aiof.open(f"{datasets_path}/pickle/{filename}.pickle", "rb") as file:
        data = await file.read()
        return pickle.loads(data)


async def save_pickle(filename, dataset):
    async with aiof.open(f"{datasets_path}/pickle/{filename}.pickle", "wb") as file:
        data = pickle.dumps(dataset)
        await file.write(data)


async def dataset2pickle(name:str, translation:str, outputfile:str):
    print(f"dataset2pickle {name}({translation})...")
    data = dts.load_dataset(name, translation)
    languages = translation.split("-")
    dataset = make_dataset(languages)
    count = 0
    for language in languages:
        dataset[language] = []
    for branch in data.keys():
        print(data[branch][0])
        for sentence in data[branch]:
            count += 1
            for language in languages:
                text = str(sentence['translation'][language]).replace("\n", " ")
                dataset[language].append(text)
    dataset["count"] = count
    await save_pickle(outputfile, dataset)
    for language in languages:
        print(language, len(dataset[language]))

    print(f"dataset2pickle {str}({dataset['count']}) done.")



async def pickle2text(inputfile: str, outputfile: str):
    print(f"pickle2text {inputfile}...")
    dataset = await load_pickle(inputfile)
    async with aiof.open(f"{datasets_path}/text/{outputfile}.txt", "w") as file:
        languages = dataset["languages"]

        size = dataset["count"]
        for i in range(size):
            sentences = []
            for language in languages:
                sentences.append(dataset[language][i])
            await file.write("\t\t".join(sentences)+"\n")
    print(f"pickle2text {inputfile} done.")


async def text2pickle(inputfile: str, outputfile: str, languages: tuple):
    print(f"text2pickle {inputfile}...")
    dataset = make_dataset(languages)
    count = 1
    for language in languages:
        dataset[language] = []
    async with aiof.open(inputfile, "r") as file:
        async for line in file:
            count += 1
            if line.endswith("\n"):
                line = line[:-1]
            text = line.split("\t\t")
            for index in range(len(languages)):
                dataset[languages[index]].append(text[index])
    dataset["count"] = count
    await save_pickle(outputfile, dataset)
    print(f"text2pickle ({count}) done.")
