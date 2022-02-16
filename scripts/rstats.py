#!/usr/bin/env python3
#!coding:utf-8

# Copyright 2022 TiKV Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import re
import json
import argparse

def main():
    pattern = r'.*SlowLog.*'
    slowstr = "SlowLog:"
    grpc_pattern = "gRPC tikvpb.Tikv"
    backoff_pattern = "backoff "

    args = parse_args()
    items = []
    with open(args.slowlog, encoding = 'utf-8') as f:
        for line in f.readlines():
            matched = re.match(pattern, line, re.M|re.I)
            if matched is not None:
                log = json.loads(line[(line.index(slowstr) + len(slowstr)):])
                item = {
                    'req': log['func'],
                    'start': log['start'],
                    'tot_lat': int(log['duration'][:len(log['duration'])-2]),
                    'tot_grpc': 0,
                    'tot_bo': 0,
                }
                items.append(item)
                for span in log['spans']:
                    if grpc_pattern in span['name'] and span['duration'] != 'N/A':
                        item['tot_grpc'] += int(span['duration'][:len(span['duration'])-2])
                    elif backoff_pattern in span['name'] and span['duration'] != 'N/A':
                        item['tot_bo'] += int(span['duration'][:len(span['duration'])-2])

        if args.order == "total":
            items = sorted(items, key=lambda d: d['tot_lat'], reverse=True)
        elif args.order == "grpc":
            items = sorted(items, key=lambda d: d['tot_grpc'], reverse=True)
        elif args.order == "backoff":
            items = sorted(items, key=lambda d: d['tot_bo'], reverse=True)
        else:
            print("unsupported order option, use default value: total")
            items = sorted(items, key=lambda d: d['tot_lat'], reverse=True)

    fmtStr = "{:<12} {:<14} {:<14} {:<20} {:<20}"
    print(fmtStr.format("Request", "Start", "Total Lat(ms)", "Total gRPC Lat(ms)", "Total Backoff Lat(ms)"))
    for item in items:
        print(fmtStr.format(item['req'], item['start'], item['tot_lat'], item['tot_grpc'], item['tot_bo']))

def parse_args():
    parser = argparse.ArgumentParser(description="rstats: A TiKV Java Client Request Stats Analyzer")
    parser.add_argument("--order", dest="order", default="total", help="order the output, default: total. accepted value: total, grpc, backoff")
    parser.add_argument("slowlog", help="slow log file")
    return parser.parse_args()

if __name__ == '__main__':
    main()

