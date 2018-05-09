# Data Generator for LAIN Datasets

import ijson
import pandas as pd
import argparse
import json
import os
import random
import string
import csv
import datetime
import sys



def shape_logs(args, extension, config):
    print("Shaping File")
    selected_cols = list(config['columns'])
    trans_cols = filter(lambda x: 'transform' in config['columns'][x], selected_cols)
    with open(args.input, 'rb') as f:
        if extension == ".JSON": df = pd.read_json(args.input)
        if extension == ".CSV": df = pd.read_csv(args.input, engine='python')
        print("Data Loaded in DF. Preforming Column Transform")
        print(list(df))
        df = df[[x for x in selected_cols if x in list(df)]] #This line needs to be changed to add the transformed column names.
        for tc in [x for x in list(df) if x in trans_cols]:
            print("Transform {}".format(tc))
            df[tc] = df[tc].apply(eval(config['columns'][tc]['transform']))
        return df

def generate(args):
    '''
    (scan data) host_id, os_detection, host_up, 21, 22, 23, 25, 53, 80, 111, 135, 137, 139, 443, 445
    (scan_results) 21_scan, 22_scan, 23_scan, 25_scan, 53_scan, 80443_scan, 111_scan, 135_scan, 137_scan, 139_scan, 443_scan, 445_scan
    
    (state data) last_action, last_result
    '''
    if random.randint(0,10) > 2:
        hostnames = ['abstract', 'merlyn', 'champion', 'tenor', 'bearface', 'ameer', 'dom']


        inbound = random.choice([True, False])
        weird_port = True if (random.randint(0, 10) == 9) else False
        return [
            random.choice(hostnames), #hostnames
            random.randint(0,23), #hour
            random.randint(0,5), #load
            str(random.uniform(0.0,1.0))[:4], #memutil
            str(random.uniform(0.0,1.0))[:4]
        ]
    else:
        return [
            0,0,0,"",""
            ]


def run(config):

    try:
        if args.input:
            input_name,input_extension = os.path.splitext(args.input)
            df = shape_logs(args, input_extension.upper(), config)

        elif args.generate:
            df = generate(args)

        if not df:
            raise Exception("Dataframe not generated. Specify --generate or --input")
        
    except Exception as e:
        print("Input Error")
        print(e)
        sys.exit(0)
    

    try:
        if args.output:
            output_name,output_extension = os.path.splitext(args.output)
            if output_extension.upper() == ".CSV":
                df.to_csv(args.output, sep='\t')
            elif output_extension.upper() == ".JSON":
                with open(args.output, 'w') as f:
                    f.write(df.to_json())
            else:
                print(df)
        else:
            print(df)
            
    except Exception as e:
        print("Dataframe Error, or No Output Specified.")
        print(e)

if __name__ == '__main__':
    prog = "gen_dataset"
    descr = "Generate or convert datasets"
    parser = argparse.ArgumentParser(prog=prog, description=descr)
    parser.add_argument("--input", type=str, default=None, help="Input Dataset")
    parser.add_argument("--output", type=str, help="Output Dataset")
    parser.add_argument("--config", type=str, default="config.json", help="Config file")
    parser.add_argument("--generate", action='store_true', help="Generate new data")
    args = parser.parse_args()
    config = json.load(open(args.config)) if args.config else {"columns": ""}

    run(config)
