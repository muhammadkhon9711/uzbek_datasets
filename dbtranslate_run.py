import core, asyncio

async def main():
    files = [
        ("opus_infopankki-en-ru", "ru", "uz"), # -
        ("opus_wikipedia-en-ru", "ru", "uz"), # -
        ("opus100-en-ru", "ru", "uz"), # -
        ("wmt16-en-ru",  "ru", "uz"), # +
    ]
    tasks = []
    for i in files:
        dataset = await core.load_pickle(i[0])
        tasks.append(asyncio.create_task(core.translate2text(dataset, i[0], i[1], i[2])))
    await asyncio.wait(tasks)

asyncio.run(main())