# -*- coding: utf-8 -*-

import sys
for line in sys.stdin:
    line = line.strip()
    words = line.split()
    v = float(words[-1])
    for i in range(0,len(words)-1):
        print "%d\t%.10f" % (i+1, v*float(words[i]))
