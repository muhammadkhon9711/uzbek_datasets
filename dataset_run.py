import datasets, asyncio

def download_datasets():
    params = [
        ("wmt16", "ru-en", "./datasets/downloaded"),
        ("opus100", "en-ru", "./datasets/downloaded"),
        ("opus_infopankki", "en-ru", "./datasets/downloaded"),  
    ]

    texts = [
        ("gfissore/arxiv-abstracts-2021","./datasets/downloaded"),
        ("aalksii/ml-arxiv-papers","./datasets/downloaded"),
        ("common_gen","./datasets/downloaded"),
    ]

    for i in params:
        datasets.load_dataset(i[0], i[1], cache_dir=i[2] )
    for i in texts:
        datasets.load_dataset(i[0], cache_dir=i[1] )

download_datasets()
            