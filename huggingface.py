import datasets as dt
from core import dataset as cdt

#universeTBD/arxiv-abstracts-large
#gfissore/arxiv-abstracts-2021
"""
dataset = dt.load_dataset("universeTBD/arxiv-abstracts-large", cache_dir="./datasets/downloaded")
data = cdt.PickleDataSet()

data.set(dataset["train"], "en")
data.save("abstracts-2021")

dataset = dt.load_dataset("gfissore/arxiv-abstracts-2021", cache_dir="./datasets/downloaded")
data = cdt.PickleDataSet()
data.set(dataset["train"], "en")
data.save("abstracts-large")
"""
dataset = dt.load_dataset("wikipedia", "20220301.en", cache_dir="./datasets/downloaded")
data = cdt.PickleDataSet()
data.set(dataset["train"], "en")
data.save("wikkipedia")