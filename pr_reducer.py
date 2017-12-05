# -*- coding: utf-8 -*-
import sys

current_word = None
current_count = 0.0
word = None

for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1)
    word = int(word)
    count = float(count)
    if current_word == word:
        current_count += count
    else:
        if current_word:
            print "%d\t%.10f" % (current_word, current_count)
        current_count = count
        current_word = word

if word == current_word:  #不要忘记最后的输出
    print "%d\t%.10f" % (current_word, current_count)