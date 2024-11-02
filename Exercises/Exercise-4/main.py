import glob
import json, csv
import os

DATA_PATH = "./data"

def explore_path():
    for root, dirs, files in os.walk(DATA_PATH):
        for file in files:
            if file.endswith(".json"):
                json_to_csv(root, file)

def json_to_csv(folder, file):
    print(folder, file)

    with open(f"{folder}/{file}", "r") as json_file:
        json_obj = json.load(json_file)
        json_obj = flatten_json(json_obj)

        columns = [x for x in json_obj]
        values = [json_obj[x] for x in json_obj]

        csv_filename = file.replace('.json', '.csv')
        print(csv_filename)

        with open(f"{folder}/{csv_filename}", "w") as csv_file:
            csv_w = csv.writer(csv_file)
            csv_w.writerows(columns)
            csv_w.writerow(values)
        
def flatten_json(json):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + "_")
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    
    flatten(json)
    return out

def main():
    explore_path()

if __name__ == "__main__":
    main()
