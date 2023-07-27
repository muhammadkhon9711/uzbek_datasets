import googletrans as gtr

def _split(text:str)->list:
    texts = text.split(".")
    new = ""
    news = []
    for t in texts:
        if len(new + t) > 4999:
            news.append(new)
            new = ""
        new += t + "."
    return news


def translate(text):
    translator = gtr.Translator()
    if len(text) > 5000:
        trs = []
        for t in _split(text):
            tr = translator.translate(text=t, src="en", dest="uz").text
            if tr is None:
                tr = "([ERROR])"
            trs.append(tr)
        return ". ".join(trs)
    tr = translator.translate(text=t, src="en", dest="uz").text
    if tr is None:
        tr = "([ERROR])"
    return tr