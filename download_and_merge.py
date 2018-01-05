#!/usr/bin/env python3

"""This script generate json data from bittrex"""
import json
from os import path
import urllib.request
import ssl
import gzip
import time


PAIRS = ['USDT-BTC', 'BTC-BCC', 'BTC-BTG', 'BTC-DASH', 'BTC-EDG',
         'BTC_EMC2', 'BTC-ETC', 'BTC-ETH', 'BTC_LSK', 'BTC-LTC',
         'BTC_MCO', 'BTC_MER', 'BTC-MTL', 'BTC-NEO', 'BTC-OK',
         'BTC-OMG', 'BTC-PAY', 'BTC-PIVX', 'BTC_POWR', 'BTC-QTUM',
         'BTC-SNT', 'BTC_STRAT', 'BTC_VTC', 'BTC_WAVES', 'BTC_XLM',
         'BTC-XMR', 'BTC-XRP', 'BTC-XZC', 'BTC-ZEC']
#PAIRS = ['BTC-BCC']
INTERVALS = {
    "1": {"query_interval": "oneMin"},
    "5": {"query_interval": "fiveMin"}
}

OUTPUT_DIR = path.dirname(path.realpath(__file__))
interval = "1"

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
            else:
                data = []
                print("Current Start: None")
                print("Current End: None")
            new_data = get_ticker(pair, interval)
            for row in new_data:
                if row not in data:
                    data.append(row)
            print("New Start:", data[1]['T'])
            print("New End: ", data[-1:][0]['T'])
            data = sorted(data, key=lambda data: data['T'])

            with gzip.open(filename, "wt") as fp:
                json.dump(data, fp)
            time.sleep(5)



def get_ticker(ticker, interval):
    query = 'https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName=' + ticker + '&tickInterval=' + INTERVALS[interval]["query_interval"]
    print("Sending query:", query)
    req = urllib.request.urlopen(url=query, timeout=60, context=ssl._create_unverified_context())
    out_data = json.loads(req.read())
    out_data = out_data['result']
    return out_data



if __name__ == '__main__':
    main()
