import datetime
import os
import os.path
import random
import sqlite3
import threading
import time
import ujson
import json
import sys
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import requests
from retrying import retry

from requests.packages.urllib3.exceptions import InsecureRequestWarning
from modules.ProxyProvider import ProxyProvider
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class Crawler:
    def __init__(self):
        self.cityname = sys.argv[1].upper();
        self.start_time = datetime.datetime.now()
        self.csv_path = "./db/"+self.cityname+"/" + datetime.datetime.now().strftime("%Y%m%d")
        os.makedirs(self.csv_path, exist_ok=True)
        self.csv_name = self.csv_path + "/" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S") + '.csv'
        self.db_name = "./"+self.cityname+".db"
        self.lock = threading.Lock()
        self.proxyProvider = ProxyProvider()
        self.total = 0
        self.done = 0
        count = 0
    def get_nearby_bikes(self, args):
        print(datetime.datetime.now())
        try:
            url = "https://mwx.mobike.com/mobike-api/rent/nearbyBikesInfo.do"

            payload = "latitude=%s&longitude=%s&errMsg=getMapCenterLocation" % (args[0], args[1])

            headers = {
                'charset': "utf-8",
                'platform': "4",
                "referer":"https://servicewechat.com/wx40f112341ae33edb/1/",
                'content-type': "application/x-www-form-urlencoded",
                'user-agent': "MicroMessenger/6.5.4.1000 NetType/WIFI Language/zh_CN",
                'host': "mwx.mobike.com",
                'connection': "Keep-Alive",
                'accept-encoding': "gzip",
                'cache-control': "no-cache"
            }

            self.request(headers, payload, args, url)
        except Exception as ex:
            print(ex)

    def request(self, headers, payload, args, url):
        while True:
            proxy = self.proxyProvider.pick()
            try:
                response = requests.request(
                    "POST", url, data=payload, headers=headers,
                    proxies={"https": proxy.url},
                    timeout=5,verify=False
                )

                with self.lock:
                    with sqlite3.connect(self.db_name) as c:
                        try:
                            #print(response.text)
                            decoded = ujson.decode(response.text)['object']
                            self.done += 1
                            for x in decoded:
                                c.execute("INSERT INTO mobike VALUES (%d,'%s',%d,%d,%s,%s,%f,%f)" % (
                                    int(time.time()) * 1000, x['bikeIds'], int(x['biketype']), int(x['distId']),
                                    x['distNum'], x['type'], x['distX'],
                                    x['distY']))

                            timespend = datetime.datetime.now() - self.start_time
                            percent = self.done / self.total
                            total = timespend / percent
                            if(percent>count):
                                print(percent * 100)
                                count += 1
                        except Exception as ex:
                            pass#print (ex)
                            #traceback.print_exc()
                    break
            except Exception as ex:
                proxy.fatal_error()

    def start(self):

        with open('./city_list.json', 'r') as f:
            data = json.load(f)
        try:
            top = float(data[self.cityname]['top'])
            left = float(data[self.cityname]['left'])
            right = float(data[self.cityname]['right'])
            bottom = float(data[self.cityname]['bottom'])
            offset = 0.002
            print (top)
            print(left)
            print(right)
            print(bottom)
        except Exception as ex:
            os.removedirs(self.csv_path)
            os._exit(0)
        if os.path.isfile(self.db_name):
            os.remove(self.db_name)

        try:
            with sqlite3.connect(self.db_name) as c:
                c.execute('''CREATE TABLE mobike
                    (Time DATETIME, bikeIds VARCHAR(12), bikeType TINYINT,distId INTEGER,distNum TINYINT, type TINYINT, x DOUBLE, y DOUBLE)''')
        except Exception as ex:
            pass

        executor = ThreadPoolExecutor(max_workers=250)
        print("Start")
        self.total = 0
        lat_range = np.arange(top, bottom, -offset)
        for lat in lat_range:
            lon_range = np.arange(left, right, offset)
            for lon in lon_range:
                self.total += 1

                executor.submit(self.get_nearby_bikes, (lat, lon))

        executor.shutdown()
        self.group_data()

    def group_data(self):
        print("Creating group data")
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        f = open(self.csv_name, "w")
        for row in cursor.execute('''SELECT * FROM mobike'''):
            timestamp, bikeIds, bikeType, distId, distNumber, type, lon, lat = row
            f.write("%s,%s,%s,%s,%s,%s,%s,%s\n" % (
                datetime.datetime.fromtimestamp(int(timestamp) / 1000).isoformat(), bikeIds, bikeType, distId, distNumber, type, lon, lat))
        f.flush()
        f.close()

        os.system("gzip -9 " + self.csv_name)


Crawler().start()
