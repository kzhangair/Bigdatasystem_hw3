#-*-coding:utf-8-*-

import socket
import sys
import struct
import os

HEAD_STRUCT = 'I128sI'
info_size = struct.calcsize(HEAD_STRUCT)
buffer_size = 1024
#addr = '219.223.181.197'
addr = '192.168.0.103' #thumm03
port = 31728

#Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#Bind the socket to the port
server_address = (addr, port)
print >>sys.stderr, 'starting up on %s port %s' % server_address
sock.bind(server_address)
#Listen for incoming connections
sock.listen(1)

while True :
    #Wait for a connection
    print >>sys.stderr, 'waiting for a connection'
    connection, client_address = sock.accept()
    try:
        print >>sys.stderr, 'connection from', client_address

        file_info = connection.recv(info_size)
        request_type, file_name, file_name_len = struct.unpack(HEAD_STRUCT, file_info)
        #Download File
        if request_type == 0 :
            file_name = file_name[:file_name_len]
            print 'Requested File Name is: %s' % file_name
            fopen = open(file_name, 'rb')
            count = 0
            print 'Begin to send file...'
            for slice in fopen:
                connection.send(slice)
            print >>sys.stderr, 'File send complete...'
            fopen.close()
        #Upload File
        elif request_type == 1:
            file_name = file_name[:file_name_len]
            print 'Upload File Name is: %s' % file_name
            fopen = open(file_name, 'wb')
            print 'Begin to receive file...'
            one_slice = connection.recv(buffer_size)
            while one_slice:
                fopen.write(one_slice)
                fopen.flush()
                one_slice = connection.recv(buffer_size)
            print 'File receive complete...'
            fopen.close()
        #Run Remote Python Script
        elif request_type == 2:
            file_name = file_name[:file_name_len]
            script_name, script_input = file_name.split('&', 1)
            print 'Run Python Script %s' % script_name
            command = "python " + script_name + " < " + script_input + " > MapResult"
            os.system(command)

            fopen = open("MapResult", 'rb')
            count = 0
            print 'Begin to send MapResult'
            for slice in fopen:
                connection.send(slice)
            print >>sys.stderr, 'File send complete...'
            fopen.close()
    except socket.errno, e:
        print "Socket error: %s" % str(e)
    finally:
        #Clean up the connection
        connection.close()
