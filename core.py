import aiofiles as aiof
import pickle
import aiosqlite as aiosql
import python_translator as pytr
import datasets as dts

datasets_path = "./datasets"
database = "./datasets/database/sentences.db"

async def create_tables():
    print("create_table ...")
    commands = [
      "drop table if exists en_ru_sentences;",
      "drop table if exists en_tr_sentences;",
      "drop table if exists en_uz_sentences;",
      """create table if not exists en_ru_sentences (
        id integer primary key autoincrement,
        dest text,
        src text,
        tr text
      );""",
      """create table if not exists en_tr_sentences (
        id integer primary key autoincrement,
        dest text,
        src text,
        tr text
      );""",
      """create table if not exists en_uz_sentences (
        id integer primary key autoincrement,
        dest text,
        src text,
        tr text
      );"""
    ]
    async with aiosql.connect(database) as db:
        for sql in commands:
            await db.execute(sql)
        await db.commit()
    print("create_table done.")

async def connect():
    return await aiosql.connect(database)

async def pickle2db(db, dataset, tablename, dest, src):
    print(f"pickle2db {tablename}({dataset['count']}) ...")

    escape = str.maketrans({"'":"''", '"':'""'})

    dest_list = dataset[dest]
    src_list = dataset[src]
    size = dataset["count"]
    try:
        for i in range(size):
            ds = dest_list[i].translate(escape)
            sr = src_list[i].translate(escape)
            command = f"insert or ignore into {tablename}(dest, src) values('{ds}', '{sr}');"
            await db.execute(command)
    except aiosql.Error as ex:
        print(ex)
        print(ex.args)
        print(dest_list[i], src_list[i])
    print(f"pickle2db {tablename}({dataset['count']}) done.")

async def translate2text(dataset, output, language_from, language_to):
    print(f"translate2text {output}({dataset['count']})...")
    path = f"{datasets_path}/translated"
    translator = pytr.Translator()
    size = dataset["count"]
    async with aiof.open(f"{path}/{output}-{language_from}.txt", "a") as file_from, aiof.open(f"{path}/{output}-{language_to}.txt", "a") as file_to, aiof.open(f"{datasets_path}/logs/{output}.txt", "a") as file_logs:
        for i in range(size):
            text_from = dataset[language_from][i]
            try:
                tr = str(translator.translate(text=text_from, source_language=language_from, target_language=language_to))
                if tr == None or tr == "":
                    await file_logs.write(f"{i},")
                    continue
            except:
                await file_logs.write(f"{i},")
            await file_from.write(text_from + "\n")
            await file_to.write(tr + "\n")
    print(f"translate2text {output} done.")
            



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
