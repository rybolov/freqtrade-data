#!/usr/bin/env python3.6

"""This script generates a json list of tickers from bittrex"""
import json
from os import path
import urllib.request
import ssl


OUTPUT_DIR = path.dirname(path.realpath(__file__))
TICKER_FILENAME = OUTPUT_DIR + "/tickers.json"


print("Checking for tickers file", TICKER_FILENAME)
if path.isfile(TICKER_FILENAME):
    with open(TICKER_FILENAME, "rt") as fp:
        old_tickers = json.load(fp)
    print("Existing file has", len(old_tickers), "tickers in total.")
    #print(old_tickers)
else:
    print("No existing tickers file.")

tickers = []
query = 'https://bittrex.com/api/v1.1/public/getmarkets'
print("Sending query:", query)

for looper in range(5):
    try:
        req = urllib.request.urlopen(url=query, timeout=60, context=ssl._create_unverified_context())
        out_data = json.loads(req.read())
        break
    except urllib.request.URLError:
        print("Timed out. Resending....")
        time.sleep(looper * 10)
else:
    print("\n======Connection timed out.  Aborting....======\n")

out_data = out_data['result']
print("Done!")
print(len(out_data), "total tickers retrieved, including non-BTC ones.")

for ticker_result in out_data:
    if ticker_result['BaseCurrency'] == "BTC":
        tickers.append(ticker_result['MarketName'])

tickers = sorted(tickers)
print("New file has", len(tickers), "BTC-based tickers available.")
#print(tickers)

with open(TICKER_FILENAME, "wt") as fp:
    json.dump(tickers, fp, indent=4)