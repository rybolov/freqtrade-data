#!/usr/bin/env python3.6

"""This script generate json data from bittrex"""
import json
from os import path
import urllib.request
import ssl
import gzip
import time


PAIRS = [
        'BTC-1ST',
        'BTC-2GIVE',
        'BTC-ABY',
        'BTC-ADA',
        'BTC-ADT',
        'BTC-ADX',
        'BTC-AEON',
        'BTC-AGRS',
        'BTC-AMP',
        'BTC-ANT',
        'BTC-APX',
        'BTC-ARDR',
        'BTC-ARK',
        'BTC-AUR',
        'BTC-BAT',
        'BTC-BAY',
        'BTC-BCC',
        'BTC-BCY',
        'BTC-BITB',
        'BTC-BLITZ',
        'BTC-BLK',
        'BTC-BLOCK',
        'BTC-BNT',
        'BTC-BRK',
        'BTC-BRX',
        'BTC-BSD',
        'BTC-BTCD',
        'BTC-BTG',
        'BTC-BURST',
        'BTC-BYC',
        'BTC-CANN',
        'BTC-CFI',
        'BTC-CLAM',
        'BTC-CLOAK',
        'BTC-CLUB',
        'BTC-COVAL',
        'BTC-CPC',
        'BTC-CRB',
        'BTC-CRW',
        'BTC-CURE',
        'BTC-CVC',
        'BTC-DASH',
        'BTC-DCR',
        'BTC-DCT',
        'BTC-DGB',
        'BTC-DMD',
        'BTC-DNT',
        'BTC-DOGE',
        'BTC-DOPE',
        'BTC-DTB',
        'BTC-DYN',
        'BTC-EBST',
        'BTC-EDG',
        'BTC-EFL',
        'BTC-EGC',
        'BTC-EMC',
        'BTC-EMC2',
        'BTC-ENG',
        'BTC-ENRG',
        'BTC-ERC',
        'BTC-ETC',
        'BTC-ETH',
        'BTC-EXCL',
        'BTC-EXP',
        'BTC-FAIR',
        'BTC-FCT',
        'BTC-FLDC',
        'BTC-FLO',
        'BTC-FTC',
        'BTC-FUN',
        'BTC-GAM',
        'BTC-GAME',
        'BTC-GBG',
        'BTC-GBYTE',
        'BTC-GCR',
        'BTC-GEO',
        'BTC-GLD',
        'BTC-GNO',
        'BTC-GNT',
        'BTC-GOLOS',
        'BTC-GRC',
        'BTC-GRS',
        'BTC-GUP',
        'BTC-HMQ',
        'BTC-INCNT',
        'BTC-INFX',
        'BTC-IOC',
        'BTC-ION',
        'BTC-IOP',
        'BTC-KMD',
        'BTC-KORE',
        'BTC-LBC',
        'BTC-LGD',
        'BTC-LMC',
        'BTC-LSK',
        'BTC-LTC',
        'BTC-LUN',
        'BTC-MAID',
        'BTC-MANA',
        'BTC-MCO',
        'BTC-MEME',
        'BTC-MER',
        'BTC-MLN',
        'BTC-MONA',
        'BTC-MUE',
        'BTC-MUSIC',
        'BTC-MYST',
        'BTC-NAV',
        'BTC-NBT',
        'BTC-NEO',
        'BTC-NEOS',
        'BTC-NLG',
        'BTC-NMR',
        'BTC-NXC',
        'BTC-NXS',
        'BTC-NXT',
        'BTC-OK',
        'BTC-OMG',
        'BTC-OMNI',
        'BTC-PART',
        'BTC-PAY',
        'BTC-PDC',
        'BTC-PINK',
        'BTC-PIVX',
        'BTC-PKB',
        'BTC-POT',
        'BTC-POWR',
        'BTC-PPC',
        'BTC-PTC',
        'BTC-PTOY',
        'BTC-QRL',
        'BTC-QTUM',
        'BTC-QWARK',
        'BTC-RADS',
        'BTC-RBY',
        'BTC-RCN',
        'BTC-RDD',
        'BTC-REP',
        'BTC-RISE',
        'BTC-RLC',
        'BTC-SALT',
        'BTC-SBD',
        'BTC-SC',
        'BTC-SEQ',
        'BTC-SHIFT',
        'BTC-SIB',
        'BTC-SLR',
        'BTC-SLS',
        'BTC-SNRG',
        'BTC-SNT',
        'BTC-SPHR',
        'BTC-SPR',
        'BTC-START',
        'BTC-STEEM',
        'BTC-STORJ',
        'BTC-STRAT',
        #'BTC-SWIFT',
        'BTC-SWT',
        'BTC-SYNX',
        'BTC-SYS',
        'BTC-THC',
        'BTC-TIX',
        'BTC-TKS',
        'BTC-TRST',
        'BTC-TRUST',
        'BTC-TX',
        'BTC-UBQ',
        'BTC-UKG',
        'BTC-UNB',
        'BTC-VIA',
        'BTC-VIB',
        'BTC-VOX',
        'BTC-VRC VRM',
        'BTC-VTC',
        'BTC-VTR',
        'BTC-WAVES',
        'BTC-WINGS',
        'BTC-XCP',
        'BTC-XDN',
        'BTC-XEL',
        'BTC-XEM',
        'BTC-XLM',
        'BTC-XMG',
        'BTC-XMR',
        'BTC-XMY',
        'BTC-XRP',
        'BTC-XST',
        'BTC-XVC',
        'BTC-XVG',
        'BTC-XWC',
        'BTC-XZC',
        'BTC-ZCL',
        'BTC-ZEC',
        'BTC-ZEN'
         ]

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

            if len(data) > 0:
                print("New Start:", data[1]['T'])
                print("New End: ", data[-1:][0]['T'])
            else:
                print ("Data set is empty: no start or end.  Check if ticker exists.")
            print(len(data), "tickers in aggregate after the merge.")

            with gzip.open(filename, "wt") as fp:
                json.dump(data, fp)



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
