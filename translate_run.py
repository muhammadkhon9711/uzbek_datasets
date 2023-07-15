import core, asyncio

async def main():
    dataset = await core.load_pickle("opus100-en-ru")
    await core.translate2text(dataset, "opus100-en-ru", "ru", "uz")

asyncio.run(main())