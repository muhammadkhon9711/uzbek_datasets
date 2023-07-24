import aiofiles as aiof
import pickle
import aiosqlite as aiosql
import aiomysql as aiomysql
import python_translator as pytr
import datasets as dts

datasets_path = "./datasets"
database = "./datasets/database/sentences_joined.db"

async def create_mysql_tables():
    print("create_mysql_tables ...")
    commands = [
        "drop database if exists sentences_nlp;",
        "create database if not exists sentences_nlp;",
        "use sentences_nlp",
        "drop table if exists en_ru_sentences;",
        "drop table if exists en_tr_sentences;",
        "drop table if exists en_uz_sentences;",
        """create table if not exists en_ru_sentences (
        id integer primary key auto_increment,
        dest text,
        src text,
        tr text
      );""",
        """create table if not exists en_tr_sentences (
        id integer primary key auto_increment,
        dest text,
        src text,
        tr text
      );""",
        """create table if not exists en_uz_sentences (
        id integer primary key auto_increment,
        dest text,
        src text,
        tr text
      );"""
    ]

    host, user, password, _, port = mysql_config()
    async with await aiomysql.connect(host, user, password, port=port) as db:
        curr = await db.cursor()
        for sql in commands:
            await curr.execute(sql)
        await db.commit()
    print("create_mysql_tables done.")

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
    async with await connect() as db:
        for sql in commands:
            await db.execute(sql)
        await db.commit()
    print("create_table done.")

def mysql_config():
    host = "localhost"
    user = "root"
    password = "9711"
    database = "sentences_nlp"
    port = 3306
    return (host, user, password, database, port)

async def connect_mysql(loop):
    host, user, password, database, port = mysql_config()
    return await aiomysql.create_pool(host=host, user=user, password=password, db=database, port=port, loop=loop)

async def close_mysql(connpool):
    connpool.close()
    await connpool.wait_closed()


async def connect():
    return await aiosql.connect(database)

async def pickle2mydbf(curr, dataset, tablename, dest, src):
    print(f"pickle2mydbf {tablename}({dataset['count']}) ...")

    #escape = str.maketrans({"'": "''", '"': '""'})

    dest_list = dataset[dest]
    src_list = dataset[src]
    size = dataset["count"]
    try:
        dss = ""
        srs = ""
        for i in range(size):
            ds = dest_list[i]#.translate(escape)
            sr = src_list[i]#.translate(escape)
            if len(srs + sr) >= 5000:
                command = f"insert ignore into {tablename}(dest, src) value('{dss}', '{srs}');"
                await curr.execute(command)
                dss = ""
                srs = ""
            dss += ds + "\n"
            srs += sr + "\n"
    except aiomysql.Error as ex:
        print(ex)
        print(ex.args)
        print("---------------------------------")
        print(dest_list[i], src_list[i])
        print(tablename, "------------------------------")
    print(f"pickle2db {tablename}({dataset['count']}) done.")

async def pickle2dbf(db, dataset, tablename, dest, src):
    print(f"pickle2mydbf {tablename}({dataset['count']}) ...")

    escape = str.maketrans({"'": "''", '"': '""'})

    dest_list = dataset[dest]
    src_list = dataset[src]
    size = dataset["count"]
    try:
        dss = ""
        srs = ""
        for i in range(size):
            ds = dest_list[i].translate(escape)
            sr = src_list[i].translate(escape)
            if len(srs + sr) >= 5000:
                command = f"insert or ignore into {tablename}(dest, src) values('{dss}', '{srs}');"
                await db.execute(command)
                dss = ""
                srs = ""
            dss += ds + "\n"
            srs += sr + "\n"
    except aiosql.Error as ex:
        print(ex)
        print(ex.args)
        print(dest_list[i], src_list[i])
    print(f"pickle2db {tablename}({dataset['count']}) done.")

async def pickle2mydb(db:aiomysql.Connection, dataset, tablename, dest, src):
    print(f"pickle2mydb {tablename}({dataset['count']}) ...")

    escape = str.maketrans({"'": "''", '"': '""'})
    curr = db.cursor()
    dest_list = dataset[dest]
    src_list = dataset[src]
    size = dataset["count"]
    try:
        for i in range(size):
            ds = dest_list[i].translate(escape)
            sr = src_list[i].translate(escape)
            command = f"insert or ignore into {tablename}(dest, src) values('{ds}', '{sr}');"
            await curr.execute(command)

    except aiomysql.Error as ex:
        print(ex)
        print(ex.args)
        print(dest_list[i], src_list[i])
    print(f"pickle2mydb {tablename}({dataset['count']}) done.")

async def pickle2db(db, dataset, tablename, dest, src):
    print(f"pickle2db {tablename}({dataset['count']}) ...")

    escape = str.maketrans({"'": "''", '"': '""'})

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


async def text2english(dataset, output, count):
    print(f"text2english {output}({dataset['count']})...")
    path = f"{datasets_path}/translated"
    size = count
    async with aiof.open(f"{path}/{output}-en.txt", "a") as file:
        for i in range(size):
            text = dataset["en"][i]
            await file.write(text + "\n")
    print(f"text2english {output} done.")

async def translate2text(dataset, output, language_from, language_to):
    print(f"translate2text {output}({dataset['count']})...")
    path = f"{datasets_path}/translated"
    translator = pytr.Translator()
    size = dataset["count"]
    async with aiof.open(f"{path}/{output}-{language_from}.txt", "a") as file_from, aiof.open(f"{path}/{output}-{language_to}.txt", "a") as file_to, aiof.open(f"{datasets_path}/logs/{output}.txt", "a") as file_logs:
        for i in range(size):
            text_from = dataset[language_from][i]
            try:
                tr = str(translator.translate(
                    text=text_from, source_language=language_from, target_language=language_to))
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


async def dataset2pickle(name: str, translation: str, outputfile: str):
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
                text = str(sentence['translation']
                           [language]).replace("\n", " ")
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
