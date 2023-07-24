import aiofiles as aiof
import pickle
import aiosqlite as aiosql
import aiomysql as aiomysql
import googletrans as pytr
import datasets as dts
import os
import asyncio
import datetime
import codecs


class DataSet:
    datasets_path = "./datasets"

    def mkdir(self, path):
        if not os.path.exists(path):
            os.mkdir(path)

    def __init__(self):
        self.mkdir(self.datasets_path)
        self.mkdir(f"{self.datasets_path}/pickle")
        self.mkdir(f"{self.datasets_path}/text")
        self.mkdir(f"{self.datasets_path}/translated")
        self.mkdir(f"{self.datasets_path}/logs")
        self.mkdir(f"{self.datasets_path}/database")

    def empty_dataset(self, languages):
        return {
            "languages": languages,
            "count": 0
        }

    async def load_pickle(self, filename):
        async with aiof.open(f"{self.datasets_path}/pickle/{filename}.pickle", "rb") as file:
            data = await file.read()
            return pickle.loads(data)

    async def save_pickle(self, filename, dataset):
        async with aiof.open(f"{self.datasets_path}/pickle/{filename}.pickle", "wb") as file:
            data = pickle.dumps(dataset)
            await file.write(data)

    async def dataset2pickle(self, name: str, translation: str, outputfile: str):
        print(f"dataset2pickle {name}({translation})...")
        data = dts.load_dataset(name, translation)
        languages = translation.split("-")
        dataset = self.empty_dataset(languages)
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
        await self.save_pickle(outputfile, dataset)
        for language in languages:
            print(language, len(dataset[language]))

        print(f"dataset2pickle {str}({dataset['count']}) done.")

    async def pickle2text(self, inputfile: str, outputfile: str):
        print(f"pickle2text {inputfile}...")
        dataset = await self.load_pickle(inputfile)
        async with aiof.open(f"{self.datasets_path}/text/{outputfile}.txt", "w") as file:
            languages = dataset["languages"]

            size = dataset["count"]
            for i in range(size):
                sentences = []
                for language in languages:
                    sentences.append(dataset[language][i])
                await file.write("\t\t".join(sentences)+"\n")
        print(f"pickle2text {inputfile} done.")

    async def text2pickle(self, inputfile: str, outputfile: str, languages: tuple):
        print(f"text2pickle {inputfile}...")
        dataset = self.empty_dataset(languages)
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
        await self.save_pickle(outputfile, dataset)
        print(f"text2pickle ({count}) done.")


class MySQL:
    def __init__(self):
        self.config()
        pool = None

    async def drop_tables(self):
        commands = [
            "drop table if exists en_ru_sentences;",
            "drop table if exists en_tr_sentences;",
            "drop table if exists en_uz_sentences;",
        ]

        async with await self.connect() as db:
            curr = await db.cursor()
            for sql in commands:
                await curr.execute(sql)
            await db.commit()
        print("drop_tables done.")

    async def log(self, file, func, msg):
        async with aiof.open(f"{file}.logs", "a") as f:
            await f.write(rf"{func}: {msg}, ({datetime.datetime.now()})")

    async def create_tables(self):
        commands = [
            """create table if not exists en_ru_sentences (
            id integer primary key auto_increment,
            dest longtext,
            src longtext,
            tr longtext
          );""",
            """create table if not exists en_tr_sentences (
            id integer primary key auto_increment,
            dest longtext,
            src longtext,
            tr longtext
          );""",
            """create table if not exists en_uz_sentences (
            id integer primary key auto_increment,
            dest longtext,
            src longtext,
            tr longtext
          );"""
        ]

        async with await self.connect() as db:
            curr = await db.cursor()
            for sql in commands:
                await curr.execute(sql)
            await db.commit()
        print("create_tables done.")

    def config(self):
        self.host = "localhost"
        self.user = "root"
        self.password = "9711"
        self.database = "sentences_nlp"
        self.port = 3306

    async def connect(self) -> aiomysql.Connection:
        return await aiomysql.connect(host=self.host, user=self.user, password=self.password, db=self.database, port=self.port)

    async def connectpool(self, loop) -> aiomysql.Pool:
        self.pool = await aiomysql.create_pool(host=self.host, user=self.user, password=self.password, db=self.database, port=self.port, loop=loop)
        return self.pool

    async def close(self):
        self.pool.close()
        await self.pool.wait_closed()

    async def select(self, pool: aiomysql.Pool, table: str, count: int):
        sql = rf"SELECT id, dest, src, tr from {table} where tr is null or tr = '' limit {count};"
        try:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as curr:
                    await curr.execute(sql)
                    return curr.fetchall()
        except aiomysql.Error as err:
            await self.log("mysql", "select", str(err))

    async def updatetr(self, pool: aiomysql.Pool, table: str, records: list):
        sql = rf"update ignore {table} set tr=%s where id=%s"
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as curr:
                    await curr.executemany(sql, records)
                await conn.commit()
        except aiomysql.Error as err:
            await self.log("mysql", "updatetr", str(err))

    async def insertmany(self, pool: aiomysql.Pool, table: str, records: list):
        sql = rf'INSERT IGNORE {table}(dest, src, tr) value("?", "?", "?")'
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as curr:
                    await curr.executemany(sql, records)
                await conn.commit()
        except aiomysql.Error as err:
            await self.log("mysql", "insertmany", str(err))

    async def insert(self, pool: aiomysql.Pool, table: str, record: dict):
        sql = rf'INSERT IGNORE {table}(dest, src, tr) value("{record["dest"]}", "{record["src"]}", "{record["tr"]}")'
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as curr:
                    await curr.execute(sql)
                await conn.commit()
        except aiomysql.Error as err:
            await self.log("mysql", "insert", str(err))
            print(str(err))

    async def insertpickle(self, pool: aiomysql.Pool, dataset: dict, tablename: str, dest: str, src: str):
        print(f"pickle2mydbf {tablename}({dataset['count']}) ...")
        # escape = str.maketrans({"'": "''", '"': '""'})

        dest_list = dataset[dest]
        src_list = dataset[src]
        size = dataset["count"]
        records = []
        try:
            dss = ""
            srs = ""
            for i in range(size):
                ds = dest_list[i]  # .translate(escape)
                sr = src_list[i]  # .translate(escape)
                if len(srs + sr) >= 5000:
                    records.append((str(dss), str(srs)))
                    dss = ""
                    srs = ""
                dss += ds + "\n"
                srs += sr + "\n"
            command = rf'insert ignore into {tablename}(dest, src) values(%s, %s);'
            async with pool.acquire() as conn:
                async with conn.cursor() as curr:
                    await curr.executemany(command, records)
                await conn.commit()
        except aiomysql.Error as ex:
            print(ex)
            print(ex.args)
            print("---------------------------------")
            print(dest_list[i], src_list[i])
            print(tablename, "------------------------------")
        print(f"pickle2db {tablename}({dataset['count']}) done.")


class Worker:
    def __init__(self):
        self.dataset = DataSet()
        self.mysql = MySQL()

    def translate(self, text:str, langfrom):
        translator = pytr.Translator()
        tr = translator.translate(text=text, dest="uz", src= langfrom)
        if tr == None:
            return tr
        return tr.text
    def main(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.translatemysql(loop))

    async def pickle2mysql(self, loop: asyncio.AbstractEventLoop):
        await self.mysql.drop_tables()
        await self.mysql.create_tables()
        files = [
            ("opus_infopankki-en-ru", "en_ru_sentences", "en", "ru"),  # -
            ("opus_wikipedia-en-ru", "en_ru_sentences", "en", "ru"),  # -
            ("opus100-en-ru", "en_ru_sentences", "en", "ru"),  # -
            ("wmt16-en-ru", "en_ru_sentences", "en", "ru"),  # +
            ("opus_infopankki-en-tr", "en_tr_sentences", "en", "tr"),
            ("opus100-en-tr", "en_tr_sentences", "en", "tr"),
            ("opus100-en-uz", "en_uz_sentences", "en", "uz"),
            ("wmt16-en-tr", "en_tr_sentences", "en", "tr"),
        ]
        tasks = []
        pool = await self.mysql.connectpool(loop)
        for i in files:
            dataset = await self.dataset.load_pickle(i[0])
            tasks.append(loop.create_task(
                self.mysql.insertpickle(pool, dataset, i[1], i[2], i[3])))
        await asyncio.wait(tasks)

    async def translatemysql(self, loop):
        # await self.mysql.drop_tables()
        # await self.mysql.create_tables()
        pool = await self.mysql.connectpool(loop)
        table = "en_ru_sentences"
        future = await self.mysql.select(pool, table, 10)
        words = future.result()
        result = []
        for word in words:
            print(len(word["dest"]), len(word["src"]))
            tr = self.translate(word["src"], "ru")
            print(word["id"], tr)
            if tr == None:
              continue
            result.append((tr, word["id"]))
            #await self.mysql.updatetr(pool, table, result)
        


worker = Worker()
worker.main()
