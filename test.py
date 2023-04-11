import requests

url = "https://api.freecurrencyapi.com/v1/latest?apikey=wrlJlPAi3so0k2GZQN19QaZx1Svtds9g43lQD8bL"

r = requests.get(url)

print(r.text)


def mm():
    s = 4

def rr():
    print(s)

mm()
rr()