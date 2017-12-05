import os
import time

def SortKey(s):
    line = s.strip()
    k2, value2 = line.split()
    return k2

def SortKey2(s):
    line = s.strip()
    k2, value2 = line.split()
    return value2

alpha = 0.85

input_file = "PageRankMatrix_100"
N = 5
for j in range(0, N):
    print "Turn number is: %d/%d" % (j+1, N)
    os.system("rm -R ./HadoopMapReduceResult")
    command = "./HadoopMapReduce.sh pr_mapper.py pr_reducer.py " + input_file + " HadoopMapReduceResult"
    os.system(command)
    fr = open("./HadoopMapReduceResult/part-00000", 'r')
    pairs = fr.readlines()
    sortedResult = sorted(pairs, key=SortKey)
    fr.close()
    f_input_r = open(input_file, 'r')
    f_input_w = open("MidFile_"+str(j), 'wb')
    lines = f_input_r.readlines()
    for i in range(0,len(lines)):
        line = lines[i]
        strLine = line.strip()
        words = strLine.split()
        result = sortedResult[i]
        result = result.strip()
        k, v = result.split()
        newV = alpha*float(v)+(1-alpha)*float(words[-1])
        words[-1] = "%.10f" % newV
        writeLine = " ".join(words) + "\n"
        f_input_w.write(writeLine)
        f_input_w.flush()
    f_input_r.close()
    f_input_w.close()
    input_file = "MidFile_"+str(j)

fr = open("./HadoopMapReduceResult/part-00000", 'r')
pairs = fr.readlines()
sortedResult = sorted(pairs, key=SortKey2, reverse=True)
fr.close()
fw = open("PageRankResult", 'wb')
for line in sortedResult:
    fw.write(line)
    fw.flush()
fw.close()
os.system("rm MidFil*")
