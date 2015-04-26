
import os
import sqlite3
import gdbm
import cStringIO

class RabinCarp:

    def __init__(self):
        """
        Initialize variables you want to use.
        """
        self.defaultLength = 1028


    def ComputeHash(self, data, ph=0, pc=None):
        """
        Computes hash Value of data
        """
        return 0

    def MatchHashValues(self, dataBuf, blocks):
        """
        Get the data buffer and perform rolling hash on the data.
        1. For each sliding window of defaultLength, find hash of data[start...finish]
        2. check if the hash is already present in blocks.
        2.1 if it is present then add alreadypresent = 1 in output with its value and length
        3. if it is not present then check if the start - startpointer = defaultLength
        3.1 if yes then store the hash of data[startpointer...start-1] in output with alreadypresent = 0
        3.2 store new hashValue calculated above in self.blocks
        3.3 if not then just increment start and finish
        :return: output{blockNbr:(alreadyPresent(0/1), hashValue, length)}
        """
        output = {}
        blockNbr, startPtr = 0, 0
        start, finish = 0, 0
        #totalLength = self.__getSizeOfBuffer(dataBuf)
        dataBuf.seek(0)
        data = dataBuf.read()
        totalLength = len(data)
        previousHash = 0
        previousChar = None
        while finish < totalLength:
            if startPtr - start == self.defaultLength:
                hashValue = self.ComputeHash(data[startPtr:start-1])
                blocks[hashValue] = data[startPtr:start-1]
                output[blockNbr] = (0, hashValue, self.defaultLength)
                blockNbr += 1
                startPtr = start
            else:
                previousHash = self.ComputeHash(data[start:finish], ph=previousHash, pc=previousChar)
                previousChar = data[start]
                if previousHash in blocks:
                    if start != startPtr:
                        hashValue = self.ComputeHash(data[startPtr:start-1])
                        blocks[hashValue] = data[startPtr:start-1]
                        output[blockNbr] = (0, hashValue, start-startPtr)
                        blockNbr += 1
                    output[blockNbr] = (1, previousHash, self.defaultLength)
                    blockNbr += 1
                    start = finish + 1
                    finish = start + self.defaultLength - 1
                    startPtr = start
                    previousHash = 0
                    previousChar = None
                else:
                    start += 1
                    finish += 1
        return output




    def __getSizeOfBuffer(self, buf):
        """
        Get the length of the buffer.
        """
        startPosition = buf.tell()     #get the current position of cursor
        buf.seek(0, os.SEEK_END)       #go to the last position in the buffer. 0th position respective to END of buffer
        length = buf.tell()            #get the length by current position of cursor
        buf.seek(startPosition, os.SEEK_SET) #Go back to starting position.
        return length
