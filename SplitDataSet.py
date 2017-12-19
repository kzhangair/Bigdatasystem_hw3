fo = open("PageRankDataSet", 'r')
fw = open("PageRankDataSet_1M", 'wb')
lines = fo.readlines()
fo.close()
length = len(lines)
splitPoint = length/10000
for i in range(0, splitPoint):
    fw.write(lines[i])
    fw.flush()
fw.close()