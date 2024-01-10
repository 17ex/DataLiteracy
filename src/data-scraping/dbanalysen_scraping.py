import os
import random
from time import sleep
import csv
import logging
import datetime
import itertools
import sys
import requests

"""
Intended to be run from the directory this script is placed in
(along with valid cookie data in the cookie files and
the wp user id in the corresponding file, for which you need
a paid account on bahn-analysen).
This script is purposefully slow and has delays between every
request and keeps the request sizes small in order
to not overload/create problems for the database servers
behind www.bahn-analysen.de.

Scrapes all of the data from bahn-analysen.de from the start
of records until yesterday,
for the origins and destinations specified in the
stations_start.txt and stations_end.txt files.
"""

DOMAIN = 'https://www.bahn-analysen.de'
API_ENDPOINT = 'https://www.bahn-analysen.de/wp-content/plugins/simple-table/includes/load-data.php'
REFERER = DOMAIN + '/historische-verbindungen/'
ORIGIN = DOMAIN
ACCEPT_LANGUAGE = 'en-US,en;q=0.5'
ACCEPT = 'application/json, text/javascript, */*; q=0.01'
ACCEPT_ENCODING = 'gzip, deflate, br'
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0"
LOGIN_COOKIE = "./cookie_login.txt"
SEC_COOKIE = "./cookie_sec.txt"
WP_USER_ID = "./wp_id.txt"
STATIONS_START = "./stations_start.txt"
STATIONS_END = "./stations_end.txt"
OUTPUT_DIR = "../../dat/scraped/"
FILE_PREFIX_INCOMING = OUTPUT_DIR + "scraped_incoming"
FILE_PREFIX_OUTGOING = OUTPUT_DIR + "scraped_outgoing"
FILE_STATION_SEP = "___"
DATE_BEGIN = "01.01.2021"
DATE_END = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%d.%m.%Y")
SLEEP_MIN = 1
SLEEP_MAX = 4
DATE_CHUNK_LEN = 90
MAX_ROWS_PER_REQUEST = 500


def create_headers():
    return {
            "Accept-Language": ACCEPT_LANGUAGE,
            "Origin": ORIGIN,
            "Referer": REFERER,
            "User-Agent": USER_AGENT,
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            }


def random_delay():
    sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))


def get_cookies():
    with open(LOGIN_COOKIE, "r") as login_cookie_file:
        login_cookie = login_cookie_file.readline().strip('\n')

    with open(SEC_COOKIE, "r") as sec_cookie_file:
        sec_cookie = sec_cookie_file.readline().strip('\n')

    with open(WP_USER_ID, "r") as id_file:
        wp_id = id_file.readline().strip('\n')

    return {
            "wordpress_logged_in_" + wp_id: login_cookie,
            "wordpress_sec_" + wp_id: sec_cookie,
            }


HEADERS = create_headers()
COOKIES = get_cookies()




def format_station_name(station):
    return station.replace(" ", "+") \
            .replace("ä", "%C3%A4") \
            .replace("ö", "%C3%B6") \
            .replace("ü", "%C3%BC") \
            .replace("ß", "%C3%9F")


def format_station_name_file(station):
    return station.replace(" ", "_") \
            .replace("(", "PARO") \
            .replace(")", "PARC") \
            .replace("/", "SLASH")


def format_request_payload(payload):
    return str(payload) \
            .replace("'", "") \
            .replace(" ", "") \
            .replace(":", "=") \
            .replace(",", "&") \
            .replace("[", "%5B") \
            .replace("]", "%5D") \
            .replace("{", "") \
            .replace("}", "")



def create_request_payload(request_num, date_begin, date_end, origin, dest):
    return {
            "draw": request_num + 1,
            "columns[0][data]": 0,
            "columns[0][name]": "",
            "columns[0][searchable]": "true",
            "columns[0][orderable]": "true",
            "columns[0][search][value]": "",
            "columns[0][search][regex]": "false",
            "columns[1][data]": 1,
            "columns[1][name]": "",
            "columns[1][searchable]": "true",
            "columns[1][orderable]": "true",
            "columns[1][search][value]": "",
            "columns[1][search][regex]": "false",
            "columns[2][data]": 2,
            "columns[2][name]": "",
            "columns[2][searchable]": "true",
            "columns[2][orderable]": "true",
            "columns[2][search][value]": "",
            "columns[2][search][regex]": "false",
            "columns[3][data]": 3,
            "columns[3][name]": "",
            "columns[3][searchable]": "true",
            "columns[3][orderable]": "true",
            "columns[3][search][value]": "",
            "columns[3][search][regex]": "false",
            "columns[4][data]": 4,
            "columns[4][name]": "",
            "columns[4][searchable]": "true",
            "columns[4][orderable]": "true",
            "columns[4][search][value]": "",
            "columns[4][search][regex]": "false",
            "columns[5][data]": 5,
            "columns[5][name]": "",
            "columns[5][searchable]": "true",
            "columns[5][orderable]": "true",
            "columns[5][search][value]": "",
            "columns[5][search][regex]": "false",
            "order[0][column]": 0,
            "order[0][dir]": "asc",
            "start": request_num * MAX_ROWS_PER_REQUEST,
            "length": MAX_ROWS_PER_REQUEST,
            "search[value]": "",
            "search[regex]": "false",
            "date_input": date_begin,
            "date_end_input": date_end,
            "start_input": origin,
            "end_input": dest,
            "delay_cluster": "-",
            "cancel": "-",
            }


def get_data(request_num, date_begin, date_end, origin, dest):
    r = requests.post(API_ENDPOINT,
                      data=format_request_payload(
                          create_request_payload(request_num,
                                                 date_begin,
                                                 date_end,
                                                 format_station_name(origin),
                                                 format_station_name(dest))),
                      headers=HEADERS,
                      cookies=COOKIES)
    r.raise_for_status()
    return r.json()


def scrape_connection(date_begin, date_end, origin, dest, file_prefix):
    done = False
    request_num = 0
    filename = file_prefix \
        + FILE_STATION_SEP + format_station_name_file(origin) \
        + FILE_STATION_SEP + format_station_name_file(dest) + ".csv"
    if os.path.exists(filename):
        os.remove(filename)
    # num_done = 0
    try:
        while not done:
            response = get_data(request_num, date_begin, date_end, origin, dest)
            connection_data = response['data']
            if (len(connection_data) == 0):
                done = True
            else:
                cd_with_stations = [ [origin, dest] + c for c in connection_data ]
                with open(filename, 'a') as f:
                    write = csv.writer(f)
                    write.writerows(cd_with_stations)
                request_num += 1
                random_delay()
    except Exception as e:
        logging.error("Origin: " + origin + ", Dest: " + dest)
        # raise Exception from e


def scrape_all(origins, dests, file_prefix):
    num_total = len(origins) * len(dests)
    current = 0
    for (origin, dest) in itertools.product(origins, dests):
        logging.info("Progress: " + str(current / num_total * 100) + "%")
        scrape_connection(DATE_BEGIN, DATE_END, origin, dest, file_prefix)
        current += 1


def get_station_names(file):
    with open(file, "r") as f:
        stations = f.readlines()
    return [ s.strip('\n') for s in stations ]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    origins = get_station_names(STATIONS_START)
    dests = get_station_names(STATIONS_END)
    if len(sys.argv) == 2:
        if sys.argv[1] == "incoming":
            scrape_all(origins, dests, FILE_PREFIX_INCOMING)
        elif sys.argv[1] == "outgoing":
            scrape_all(dests, origins, FILE_PREFIX_OUTGOING)
        else:
            print("Wrong usage: must pass either 'incoming' or 'outgoing' as argument")
    else:
        print("Wrong usage: must pass either 'incoming' or 'outgoing' as argument")


# def scrape_connection_all(origin, dest):
    # for (date_begin, date_end) in date_ranges():
        # scrape_connection(date_begin, date_end, origin, dest)
