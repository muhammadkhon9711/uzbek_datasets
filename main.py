import core, asyncio


async def download_datasets():
    params = [
        ("wmt16", "ru-en", "wmt16-en-ru"),
        ("wmt16", "tr-en", "wmt16-en-tr"),
        ("opus100", "en-ru", "opus100-en-ru"),
        ("opus100", "en-tr", "opus100-en-tr"),
        ("opus100", "en-uz", "opus100-en-uz"),
        ("opus_infopankki", "en-ru", "opus_infopankki-en-ru"),
        ("opus_infopankki", "en-tr", "opus_infopankki-en-tr"),
        ("opus_wikipedia", "en-ru", "opus_wikipedia-en-ru"),
    ]

    tasks = []
    for i in params:
        tasks.append(asyncio.create_task(core.dataset2pickle(i[0], i[1], i[2])))
    await asyncio.wait(tasks)

asyncio.run(download_datasets())
            