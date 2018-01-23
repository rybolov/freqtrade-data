#!/usr/bin/env python3.6

"""This script generate json data from bittrex"""
import json
from os import path
import urllib.request
import ssl
import gzip
import time

PAIRS = ['USDT-BTC', 'BTC-ADA', 'BTC-ARDR', 'BTC-ARK', 'BTC-BAT', 'BTC-BCC', 'BTC-BTG', 'BTC-CVC', 'BTC-DASH', 'BTC-DGB',
         'BTC-EDG', 'BTC-EMC2', 'BTC-ETC', 'BTC-ETH', 'BTC-FUN', 'BTC-GNT', 'BTC-LSK',
         'BTC-LTC', 'BTC-KMD', 'BTC-LSK', 'BTC-MANA', 'BTC-MCO', 'BTC-MER', 'BTC-MYST', 'BTC-NBT', 'BTC-NEO', 'BTC-NXT',
         'BTC-OK', 'BTC-OMG', 'BTC-PAY', 'BTC-PIVX', 'BTC-POWR', 'BTC-QRL',
         'BTC-QTUM', 'BTC-RDD', 'BTC-SALT', 'BTC-SBD', 'BTC-SC', 'BTC-SNT', 'BTC-SPHR', 'BTC-STEEM', 'BTC-STORJ', 'BTC-STRAT', 'BTC-VOX', 'BTC-VTC', 'BTC-WAVES', 'BTC-XCP',
         'BTC-XEM', 'BTC-XLM', 'BTC-XMR', 'BTC-XMY', 'BTC-XDN', 'BTC-XRP', 'BTC-XVG', 'BTC-XZC', 'BTC-ZCL', 'BTC-ZEC']

#PAIRS = ['BTC-BCC']
INTERVALS = {
    "1": {"query_interval": "oneMin"},
    "5": {"query_interval": "fiveMin"}
}

OUTPUT_DIR = path.dirname(path.realpath(__file__))
#interval = "1"

def main():
    for pair in PAIRS:
        for interval in INTERVALS:
            print('========== Generating', pair, 'Interval:', interval, ' ==========')
            filepair = pair.replace("-", "_")
            filename = path.join(OUTPUT_DIR, '{}-{}.json.gz'.format(
                filepair,
                interval,
            ))

            filename = filename.replace('USDT_BTC', 'BTC_FAKEBULL')

            print(filename)

            if path.isfile(filename):
                with gzip.open(filename, "rt") as fp:
                    data = json.load(fp)
                print("Current Start:", data[1]['T'])
                print("Current End: ", data[-1:][0]['T'])
                print(len(data), "tickers in total.")
            else:
                data = []
                print("Current Start: None")
                print("Current End: None")
            new_data = get_ticker(pair, interval)

            #largest ticker set seen is 14500 rows.
            #Just using a subset of data works to speed up the comparison below
            if len(data) < 200001:
                last_data = data
            else:
                last_data=data[-20000:]

            #working in reverse order (newest ticker first)
            #When we hit 1000 duplicates, assume that we've seen all of the tickers.
            #This speeds everything up considerably.
            print("Working on finding new and unique tickers....")
            bottom_data=[]
            duplicates=0
            for row in reversed(new_data):
                if row not in data:
                    bottom_data.append(row)
                else:
                    duplicates += 1
                if duplicates >= 1000:
                    print("Found all the new tickers and reached 1000 duplicates, so we're done here.")
                    break

            print(len(bottom_data), "tickers are new.")
            #bottom_data = sorted(bottom_data, key=lambda data: data['T'])
            print("Merging old and new and sorting everything.....")
            data = sorted([*data, *bottom_data], key=lambda data: data['T'])
            print("Done!")


            print("New Start:", data[1]['T'])
            print("New End: ", data[-1:][0]['T'])
            print(len(data), "tickers in aggregate after the merge.")

            #with gzip.open(filename, "wt") as fp:
            #    json.dump(data, fp)
            #time.sleep(5)



def get_ticker(ticker, interval):
    query = 'https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName=' + ticker + '&tickInterval=' + INTERVALS[interval]["query_interval"]
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
    print(len(out_data), "tickers retrieved.")
    return out_data



if __name__ == '__main__':
    main()
