import asyncio, core

async def insert(db, file, table, dest, src):
    print(f"{file} prossesing...")
    dataset = await core.load_pickle(file)
    await core.pickle2db(db, dataset, table, dest, src) 
    print(f"{file} done.")

async def main():
    await core.create_tables()
    files = [
        ("opus_infopankki-en-ru", "en_ru_sentences", "en", "ru"), # -
        ("opus_wikipedia-en-ru", "en_ru_sentences", "en", "ru"), # -
        ("opus100-en-ru", "en_ru_sentences", "en", "ru"), # -
        ("wmt16-en-ru", "en_ru_sentences", "en", "ru"), # +
        ("opus_infopankki-en-tr", "en_tr_sentences", "en", "tr"),
        ("opus100-en-tr", "en_tr_sentences", "en", "tr"),
        ("opus100-en-uz", "en_uz_sentences", "en", "uz"),
        ("wmt16-en-tr", "en_tr_sentences", "en", "tr"),
    ]

    tasks = []
    db = await core.connect()
    for i in files:
        tasks.append(asyncio.create_task(insert(db, i[0], i[1], i[2], i[3])))
    await asyncio.wait(tasks)
    await db.commit()
    await db.close()

asyncio.run(main())