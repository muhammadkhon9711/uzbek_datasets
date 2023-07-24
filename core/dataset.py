import os
import pickle


def mkdir(path):
    if not os.path.exists(path):
        os.mkdir(path)


class DataSet:
    path = ",/datasets"
    ext = ""
    lang = ""
    raw = ""

    def makeempty(self):
        self.raw = []
        return self.raw

    def set(self, data, language=""):
        self.raw = data
        self.lang = language

    def data(self):
        return self.raw
    
    def language(self):
        return self.lang

    def _fullpath(self, filename:str):
        return rf"{self.path}/{filename}.{self.ext}"

    def load(self, filename:str):
        pass

    def save(self, filename:str):
        pass


class TextDataSet(DataSet):
    path = "./datasets/text"
    ext = ".txt"

    def __init__(self, filename=None, language=""):
        mkdir(self.path)
        self.lang = language
        if filename is not None:
            self.load(filename)

    def load(self, filename:str):
        raw = []
        with open(self._fullpath(filename), "r", encoding="utf8") as file:
            for line in file:
                raw.append(line.strip())

    def save(self, filename:str):
        with open(self._fullpath(filename), "w", encoding="utf8") as file:
            for line in self.raw:
                file.write(line + "\n")


class PickleDataSet(DataSet):
    path = "./datasets/pickle"
    ext = ".pickle"

    def __init__(self, filename=None, language=""):
        mkdir(self.path)
        self.lang = language
        if filename is not None:
            self.load(filename)

    def load(self, filename:str):
        with open(self._fullpath(filename), "rb") as file:
            data = file.read()
        self.raw = pickle.loads(data)
        return self.raw

    def save(self, filename:str):
        data = pickle.dumps(self.raw)
        with open(self._fullpath(filename), "wb") as file:
            file.write(data)