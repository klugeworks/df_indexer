#!/usr/bin/env python
import sys
import redis
import string
from collections import Counter

def main():
    if len(sys.argv) != 2:
        print "usage: " + sys.argv[0] + " df_file"
        sys.exit(-1)

    dfs = Counter()
    print "Reading data..."
    with open(sys.argv[1], 'r') as fh:
        docs = int(fh.readline())
        print "Docs: " + str(docs)
        for line in fh:
            line_in = line.split(" ")
            count = int(line_in[0].strip())
            token = line_in[1].strip()
            subtokens = token.split('_')
            for subtoken in subtokens:
                dfs.update({subtoken.translate(string.maketrans("", ""), string.punctuation).lower(): count})
    print "Pushing to redis..."
    r = redis.StrictRedis(host='redis.marathon.mesos', port=6379, db=0)
    r.hmset('kluge:stt:df:english:static', dict(dfs))
    print "Done."

if __name__ == '__main__':
    main()
