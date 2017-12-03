#-*- coding: utf-8 -*-

import socket
import sys
import struct
import os
import threading
import time

import name_node

HEAD_STRUCT = 'I128sI'

def DownloadFile(addr, port, file_name):
    try:
        buffer_size = 1024

        #Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        #Connect the socket to the port where the server is listening
        server_address = (addr, port)
        print >>sys.stderr, 'connecting to %s port %s' % server_address
        sock.connect(server_address)

        #Send file_name to data_node
        print >>sys.stderr, 'Get %s from data_node' % file_name
        sent_info = struct.pack(HEAD_STRUCT, 0, file_name, len(file_name)) #DownloadFile = 0
        sock.send(sent_info)

        #Look for the response
        fopen = open(file_name, 'wb')
        one_slice = sock.recv(buffer_size)
        while one_slice :
            fopen.write(one_slice)
            fopen.flush()
            one_slice = sock.recv(buffer_size)
        fopen.close()
        print >>sys.stderr, 'closing socket'
        sock.close()
        return True
    except:
        print  "There is some error!"
        return False

def UploadFile(addr, port, file_name):
    try:
        #Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        #Connect the socket to the port where the server is listening
        server_address = (addr, port)
        print >>sys.stderr, 'connecting to %s port %s' % server_address
        sock.connect(server_address)
        # Send file_name to data_node
        print >> sys.stderr, 'Upload %s to data_node' % file_name
        sent_info = struct.pack(HEAD_STRUCT, 1, file_name, len(file_name))
        sock.send(sent_info)

        fopen = open(file_name, 'rb')
        print >> sys.stderr, 'Begin to send file...'
        for slice in fopen:
            sock.send(slice)
        print >> sys.stderr, 'sent...'
        fopen.close()
        print >>sys.stderr, 'closing socket'
        sock.close()
        return True
    except:
        print  "There is some error!"
        return False

def RunRemotePyScript(addr, port, script_name, script_input, script_result):
    buffer_size = 1024
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (addr, port)
        sock.connect(server_address)
        #request type: 2 run remote py script
        file_name = script_name+"&"+script_input
        sent_info = struct.pack(HEAD_STRUCT, 2, file_name, len(file_name))
        sock.send(sent_info)
        #receive script result and save it to "script_result"
        fopen = open(script_result, 'wb')
        one_slice = sock.recv(buffer_size)
        while one_slice:
            fopen.write(one_slice)
            fopen.flush()
            one_slice = sock.recv(buffer_size)
        fopen.close()
        sock.close()
        return True
    except:
        print "There is some errors!"
        return False

def SortKey(s):
    line = s.strip()
    k2, value2 = line.split()
    return k2

class RunRemotePyScriptThread (threading.Thread):   #继承父类threading.Thread
    def __init__(self, name, addr, port, script_name, script_input, script_result):
        threading.Thread.__init__(self)
        self.name = name
        self.addr = addr
        self.port = port
        self.script_name = script_name
        self.script_input = script_input
        self.script_result = script_result
    def run(self):                   #把要执行的代码写到run函数里面 线程在创建后会直接运行run函数
        print "Starting thread " + self.name
        RunRemotePyScript(self.addr, self.port, self.script_name, self.script_input, self.script_result)
        print "Exiting thread " + self.name

class UploadFileThread(threading.Thread):
    def __init__(self, name, addr, port, file_name):
        threading.Thread.__init__(self)
        self.name = name
        self.addr = addr
        self.port = port
        self.file_name = file_name
    def run(self):
        print "Starting thread " + self.name
        UploadFile(self.addr, self.port, self.file_name)
        print "Exiting thread " + self.name

def MyMapReduce(mapper, reducer, DFSInputFile, DFSOutputFile):
    start = time.clock()
    #prepare mapper input file
    #UploadFile("thumm02", 31728, mapper)
    #UploadFile("thumm03", 31728, mapper)
    #UploadFile("thumm04", 31728, mapper)
    upload_t2 = UploadFileThread("upload_t2", "thumm02", 31728, mapper)
    upload_t3 = UploadFileThread("upload_t3", "thumm03", 31728, mapper)
    upload_t4 = UploadFileThread("upload_t4", "thumm04", 31728, mapper)
    upload_t2.start()
    upload_t3.start()
    upload_t4.start()
    upload_t2.join()
    upload_t3.join()
    upload_t4.join()

    print "mapper send complete"
    #UploadFile("thumm02", 31728, reducer)
    #UploadFile("thumm03", 31728, reducer)
    #UploadFile("thumm04", 31728, reducer)
    upload_t2 = UploadFileThread("upload_t2", "thumm02", 31728, reducer)
    upload_t3 = UploadFileThread("upload_t3", "thumm03", 31728, reducer)
    upload_t4 = UploadFileThread("upload_t4", "thumm04", 31728, reducer)
    upload_t2.start()
    upload_t3.start()
    upload_t4.start()
    upload_t2.join()
    upload_t3.join()
    upload_t4.join()

    print "reducer send complete"
    name_node.DFSLoad(DFSInputFile, "MapInput")
    finput = open("MapInput", 'r')
    finput_02 = open("MapInput_02", 'wb')
    finput_03 = open("MapInput_03", 'wb')
    finput_04 = open("MapInput_04", 'wb')
    lines = finput.readlines()
    for i in range(0, len(lines)):
        if i < len(lines)/3:
            finput_02.write(lines[i])
            finput_02.flush()
        elif i >= len(lines)/3 and i < len(lines)*2/3:
            finput_03.write(lines[i])
            finput_03.flush()
        elif i >= len(lines)*2/3 and i < len(lines):
            finput_04.write(lines[i])
            finput_04.flush()
    finput.close()
    finput_02.close()
    finput_03.close()
    finput_04.close()
    print "begin send MapInput"
    #UploadFile("thumm02", 31728, "MapInput_02")
    #UploadFile("thumm03", 31728, "MapInput_03")
    #UploadFile("thumm04", 31728, "MapInput_04")
    upload_t2 = UploadFileThread("upload_t2", "thumm02", 31728, "MapInput_02")
    upload_t3 = UploadFileThread("upload_t3", "thumm03", 31728, "MapInput_03")
    upload_t4 = UploadFileThread("upload_t4", "thumm04", 31728, "MapInput_04")
    upload_t2.start()
    upload_t3.start()
    upload_t4.start()
    upload_t2.join()
    upload_t3.join()
    upload_t4.join()

    os.remove("MapInput_02")
    os.remove("MapInput_03")
    os.remove("MapInput_04")
    print "MapInput send complete"
    #run Mapper
    #RunRemotePyScript("thumm02", 31728, mapper, "MapInput_02", "MapResult_02")
    #RunRemotePyScript("thumm03", 31728, mapper, "MapInput_03", "MapResult_03")
    #RunRemotePyScript("thumm04", 31728, mapper, "MapInput_04", "MapResult_04")
    mapper_t2 = RunRemotePyScriptThread("mapper_t2", "thumm02", 31728, mapper, "MapInput_02", "MapResult_02")
    mapper_t3 = RunRemotePyScriptThread("mapper_t3", "thumm03", 31728, mapper, "MapInput_03", "MapResult_03")
    mapper_t4 = RunRemotePyScriptThread("mapper_t4", "thumm04", 31728, mapper, "MapInput_04", "MapResult_04")
    mapper_t2.start()
    mapper_t3.start()
    mapper_t4.start()
    mapper_t2.join()
    mapper_t3.join()
    mapper_t4.join()

    os.system("cat MapResult_02 MapResult_03 MapResult_04 > MapMidResult")
    os.remove("MapResult_02")
    os.remove("MapResult_03")
    os.remove("MapResult_04")
    #prepare reducer input file
    fr = open("MapMidResult", 'r')
    pairs = fr.readlines()
    mapSortedResult = sorted(pairs, key=SortKey)
    length = len(mapSortedResult)
    line = mapSortedResult[length/3]
    stripedLine = line.strip()
    key2, value2 = stripedLine.split()
    count = 0
    key = key2
    while(key == key2):
        count = count + 1
        line = mapSortedResult[length/3+count]
        stripedLine = line.strip()
        key2, value2 = stripedLine.split()
    splitCount_2 = length/3+count

    line = mapSortedResult[length*2/3]
    stripedLine = line.strip()
    key2, value2 = stripedLine.split()
    count = 0
    key = key2
    while(key == key2):
        count = count + 1
        line = mapSortedResult[length*2/3+count]
        stripedLine = line.strip()
        key2, value2 = stripedLine.split()
    splitCount_3 = length*2/3+count

    fw2 = open("ReduceInput_02", 'wb')
    fw3 = open("ReduceInput_03", 'wb')
    fw4 = open("ReduceInput_04", 'wb')
    for i in range(0, length):
        if i < splitCount_2:
            fw2.write(mapSortedResult[i])
            fw2.flush()
        elif i >= splitCount_2 and i < splitCount_3:
            fw3.write(mapSortedResult[i])
            fw3.flush()
        elif i >= splitCount_3:
            fw4.write(mapSortedResult[i])
            fw4.flush()
    fw2.close()
    fw3.close()
    fw4.close()
    fr.close()
    print "begin send ReduceInput"
    #UploadFile("thumm02", 31728, "ReduceInput_02")
    #UploadFile("thumm03", 31728, "ReduceInput_03")
    #UploadFile("thumm04", 31728, "ReduceInput_04")
    upload_t2 = UploadFileThread("upload_t2", "thumm02", 31728, "ReduceInput_02")
    upload_t3 = UploadFileThread("upload_t3", "thumm03", 31728, "ReduceInput_03")
    upload_t4 = UploadFileThread("upload_t4", "thumm04", 31728, "ReduceInput_04")
    upload_t2.start()
    upload_t3.start()
    upload_t4.start()
    upload_t2.join()
    upload_t3.join()
    upload_t4.join()
    os.remove("ReduceInput_02")
    os.remove("ReduceInput_03")
    os.remove("ReduceInput_04")
    print "ReduceInput send complete"
    #run reducer
    #RunRemotePyScript("thumm02", 31728, reducer, "ReduceInput_02", "ReduceResult_02")
    #RunRemotePyScript("thumm03", 31728, reducer, "ReduceInput_03", "ReduceResult_03")
    #RunRemotePyScript("thumm04", 31728, reducer, "ReduceInput_04", "ReduceResult_04")
    reducer_t2 = RunRemotePyScriptThread("reducer_t2", "thumm02", 31728, reducer, "ReduceInput_02", "ReduceResult_02")
    reducer_t3 = RunRemotePyScriptThread("reducer_t3", "thumm03", 31728, reducer, "ReduceInput_03", "ReduceResult_03")
    reducer_t4 = RunRemotePyScriptThread("reducer_t4", "thumm04", 31728, reducer, "ReduceInput_04", "ReduceResult_04")
    reducer_t2.start()
    reducer_t3.start()
    reducer_t4.start()
    reducer_t2.join()
    reducer_t3.join()
    reducer_t4.join()
    os.system("cat ReduceResult_02 ReduceResult_03 ReduceResult_04 > ReduceResult")
    os.remove("ReduceResult_02")
    os.remove("ReduceResult_03")
    os.remove("ReduceResult_04")
    elapsed = (time.clock() - start)
    print "MyMapReduce Time used: %d seconds" % elapsed
