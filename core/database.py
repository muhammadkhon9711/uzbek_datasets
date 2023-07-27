import aiomysql
import datetime
import aiosqlite

class SQLDatabase:
    conn = None
    path = "./datasets/database/"

    def __init__(self, **kwargs):
        self.database = self.path + kwargs["database"]

    async def __runcommands(self, commands:list):
        conn = self.conn
        try:
            for sql in commands:
                await conn.execute(sql)
        except aiomysql.Error as err:
            self.log(f"exec({sql})", str(err))
            await conn.rollback()
        finally:
            await conn.commit()

    def tables(self):
        return ("en_uz_sentences", "en_uz_small_abstracts", "en_uz_large_abstracts", "en_uz_large_wikkipedia")

    async def drop_tables(self):
        commands = [
            "drop table if exists en_uz_sentences;",
            "drop table if exists en_uz_small_abstracts;",
            "drop table if exists en_uz_large_abstracts;",
            "drop table if exists en_uz_large_wikkipedia;",
        ]
        await self.__runcommands(commands)
        

    async def create_tables(self):
        commands = [
            "create table if not exists en_uz_sentences (id bigint unsigned primary key autoincrement, en longtext not null, tr longtext null);",
            "create table if not exists en_uz_small_abstracts (id bigint unsigned primary key autoincrement, en longtext not null, tr longtext null);",
            "create table if not exists en_uz_large_abstracts (id bigint unsigned primary key autoincrement, en longtext not null, tr longtext null);",
            "create table if not exists en_uz_large_wikkipedia (id bigint unsigned primary key autoincrement, en longtext not null, tr longtext null);",
        ]
        await self.__runcommands(commands)

    def log(self, func, msg):
        with open("sqlite_logs.txt", "a", encoding="utf8") as file:
            file.write(f"{func}({datetime.datetime.now()}): {msg}\n")

    async def connect(self) -> aiosqlite.Connection:
        if self.conn is not None:
            print("coonection created!")
        self.conn = await aiosqlite.connect(database=self.database, cursorclass=aiomysql.SSCursor, charset="utf8")
        return self.conn

    async def disconnect(self):
        if self.conn is not None:
            await self.conn.close()
            self.conn = None

    async def __execute(self, sql: str, records: list):
        conn = self.conn
        conn.begin()
        try:
            await conn.executemany(sql, records)
        except aiomysql.Error as err:
            self.log(f"exec({sql})", str(err))
            await conn.rollback()
        finally:
            await conn.commit()
    
    async def __select(self, sql:str):
        conn = self.conn
        try:
            
            curr = await conn.execute(sql)
            return await curr.fetchall()
        except aiomysql.Error as err:
            self.log(f"exec({sql})", str(err))
        return None

    async def insert(self, table: str, columns: list, records: list):
        marks = ", ".join("%s" * len(columns))
        sql = rf"INSERT IGNORE {table}({', '.join(columns)}) VALUES ({marks})"
        await self.__execute(sql, records)

    async def update(self, table: str, columns: str, records: list):
        sql = rf"UPDATE IGNORE {table} SET {columns} WHERE id = %s;"
        await self.__execute(sql, records)

    async def selectemptytr(self, table: str, columns: str, limits: int):
        sql = rf"SELECT {columns} FROM {table} WHERE tr is null or tr = '' LIMIT {limits}"
        return await self.__select(sql)
    
    async def select(self, table: str, columns: str, limits=0):
        sql = rf"SELECT {columns} FROM {table} "
        if limits != 0:
            sql += rf"LIMIT {limits}"
        return await self.__select(sql)

class MySQLDatabase:
    loop = None
    pool = None

    def __init__(self, **kwargs):
        self.database = kwargs["database"]
        self.host = kwargs["host"]
        self.username = kwargs["username"]
        self.password = kwargs["password"]
        self.port = kwargs["port"]
        self.loop = kwargs["loop"]

    def setloop(self, loop):
        self.loop = loop

    async def __runcommands(self, commands:list):
        async with self.pool.acquire() as conn:
            conn.begin()
            try:
                async with conn.cursor() as curr:
                    for sql in commands:
                        await curr.execute(sql)
            except aiomysql.Error as err:
                self.log(f"exec({sql})", str(err))
                await conn.rollback()
            finally:
                await conn.commit()

    async def drop_tables(self):
        commands = [
            "drop table if exists en_uz_sentences;",
            "drop table if exists en_uz_small_abstracts;",
            "drop table if exists en_uz_large_abstracts;",
        ]
        await self.__runcommands(commands)
        

    async def create_tables(self):
        commands = [
            "create table if not exists en_uz_sentences (id bigint unsigned primary key autoincrement, en longtext not null, tr longtext null);",
            "create table if not exists en_uz_small_abstracts (id bigint unsigned primary key autoincrement, en longtext not null, tr longtext null);",
            "create table if not exists en_uz_large_abstracts (id bigint unsigned primary key autoincrement, en longtext not null, tr longtext null);",
        ]
        await self.__runcommands(commands)

    def log(self, func, msg):
        with open("mysql_logs.txt", "a", encoding="utf8") as file:
            file.write(f"{func}({datetime.datetime.now()}): {msg}\n")

    async def connect(self) -> aiomysql.Pool:
        if self.pool is not None:
            print("pool created!")
        pool = await aiomysql.create_pool(maxsize=24, host=self.host, user=self.username, password=self.password, db=self.database, port=self.port, loop=self.loop, cursorclass=aiomysql.SSCursor, charset="utf8")
        self.pool = pool
        return self.pool

    async def disconnect(self):
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()

    async def __execute(self, sql: str, records: list):
        async with self.pool.acquire() as conn:
            conn.begin()
            try:
                async with conn.cursor() as curr:
                    await curr.executemany(sql, records)
            except aiomysql.Error as err:
                self.log(f"exec({sql})", str(err))
                await conn.rollback()
            finally:
                await conn.commit()
    
    async def __select(self, sql:str):
        async with self.pool.acquire() as conn:
            try:
                async with conn.cursor() as curr:
                    await curr.execute(sql)
                    return await curr.fetchall()
            except aiomysql.Error as err:
                self.log(f"exec({sql})", str(err))
        return None

    async def insert(self, table: str, columns: list, records: list):
        marks = ", ".join("%s" * len(columns))
        sql = rf"INSERT IGNORE {table}({', '.join(columns)}) VALUES ({marks})"
        await self.__execute(sql, records)

    async def update(self, table: str, columns: str, records: list):
        sql = rf"UPDATE IGNORE {table} SET {columns} WHERE id = %s;"
        await self.__execute(sql, records)

    async def selectemptytr(self, table: str, columns: str, limits: int):
        sql = rf"SELECT {columns} FROM {table} WHERE tr is null or tr = '' LIMIT {limits}"
        return await self.__select(sql)
    
    async def select(self, table: str, columns: str, limits=0):
        sql = rf"SELECT {columns} FROM {table} "
        if limits != 0:
            sql += rf"LIMIT {limits}"
        return await self.__select(sql)
        