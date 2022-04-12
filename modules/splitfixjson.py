import json
lines_per_file = 1000
smallfile = None
with open('../files/reddit/RC_2018-01-01') as bigfile:
    for lineno, line in enumerate(bigfile):
        if lineno % lines_per_file == 0:
            json_processed = json.loads(line)
            print(json_processed.keys())
            if smallfile:
                smallfile.close()
            small_filename = '../files/reddit/split/small_file_{}.txt'.format(lineno + lines_per_file)
            smallfile = open(small_filename, "w")
        smallfile.write(line)
    if smallfile:
        smallfile.close()

