
import os
from Crypto.Cipher import DES
import hashlib.md5


EncryptionKey = "wolfpack"

class RabinKarp:

    def __init__(self):
        """
        Initialize variables you want to use.
        """
        self.defaultLength = 1024
        self.base = 256  #base used for hash calculation
        self.primary_number = 5915587277  # random long prime number
        self.RM = 1 # to calculate new hash from previous hash
        self.__computeRM()

    def __computeRM(self):
        """
         calculate R^(length-1) for assisting in hash calculation
        """
        for i in range(self.defaultLength-1):
             self.RM = (self.base * self.RM) % self.primary_number
    

    def ComputeHash(self, data, ph=None, pc=None):
        """
          This function will calculate hash value of of data block. As Rabin Karp algorithm uses rolling hash mechanism, we
          are using previous hash and first character of previous data block to calculate new hash
        """
        #self.__write_log("ComputeHash", "called for length:" + str(len(data)))

        new_hash_value = 0
        if not ph:
            for index in range(len(data)):
                #self.__write_log("ComputeHash", "index=" + str(index))
                new_hash_value = (self.base * new_hash_value + ord(data[index])) % self.primary_number
            #self.__write_log("ComputeHash", "ended. return:"+str(new_hash_value))
            return str(new_hash_value)
        else:
            ph = int(ph)
            new_hash_value = (ph + self.primary_number - (self.RM * ord(pc))%self.primary_number)%self.primary_number
            new_hash_value = (new_hash_value*self.base + ord(data[len(data)-1]))%self.primary_number
            #self.__write_log("ComputeHash", "ended. return:"+str(new_hash_value))
            return str(new_hash_value)


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
        start, finish = 0, self.defaultLength
        #totalLength = self.__getSizeOfBuffer(dataBuf)
        dataBuf.seek(0)
        data = dataBuf.read()
        totalLength = len(data)
        #self.__write_log("MatchHashValues", "data:"+data + " totallength:" + str(totalLength))
        previousHash = None
        previousChar = None
        self.__write_log("MatchHashValues", "called. totallength:"+str(totalLength))
        if finish > totalLength:
            #self.__write_log("Block size less than default block size", "Not doing dedupe")
            #self.__write_log("MatchHashValues", "ended with:" + str(len(output)))
            return output

        DESObj = DES.new(EncryptionKey, DES.MODE_ECB)

        while finish <= totalLength and finish != start:
            #self.__write_log("start:" +str(start) + " finish:" + str(finish), "startptr:"+ str(startPtr))
            if start - startPtr == self.defaultLength:
                #self.__write_log("Inside first if", "")
                hashValue = self.ComputeHash(data[startPtr:start])
                self.__EncryptAndInsertData(blocks, hashValue, data[startPtr:start])
                #blocks[hashValue] = data[startPtr:start]
                output[blockNbr] = (0, hashValue, self.defaultLength)
                self.__write_log("Added 1", "from start:" + str(startPtr) + " to finish:" + str(start-1) + " at block:" + str(blockNbr))
                blockNbr += 1
                startPtr = start
            else:
                #self.__write_log("Inside first else", "")
                previousHash = self.ComputeHash(data[start:finish], ph=previousHash, pc=previousChar)
                previousChar = data[start]
                #self.__write_log("After ComputeHash", "previousChar:" + previousChar)
                if previousHash in blocks:
                    if start != startPtr:
                        hashValue = self.ComputeHash(data[startPtr:start])
                        self.__EncryptAndInsertData(blocks, hashValue, data[startPtr:start])
                        #blocks[hashValue] = data[startPtr:start]
                        if hashValue in blocks:
                            output[blockNbr] = (1, hashValue, start-startPtr)
                        else:
                            output[blockNbr] = (0, hashValue, start-startPtr)
                        self.__write_log("Added 2", "from start:" + str(startPtr) + " to finish:" + str(start-1) + " at block:" + str(blockNbr))
                        blockNbr += 1
                    self.__write_log("Added 3", "from start:" + str(start) + " to finish:" + str(finish-1) + " at block:" + str(blockNbr))
                    output[blockNbr] = (1, previousHash, finish-start)
                    blockNbr += 1
                    start = finish
                    if start + self.defaultLength > totalLength:
                        finish = totalLength
                    else:
                        finish = start + self.defaultLength
                    startPtr = start
                    previousHash = None
                    previousChar = None
                else:
                    if finish == totalLength:
                        if start != startPtr:
                            #Adding starting part of defaultLength
                            start = startPtr + self.defaultLength
                            hashValue = self.ComputeHash(data[startPtr:start])
                            if hashValue in blocks:
                                output[blockNbr] = (1, hashValue, self.defaultLength)
                            else:
                                output[blockNbr] = (0, hashValue, self.defaultLength)
                            self.__EncryptAndInsertData(blocks, hashValue, data[startPtr:start])
                            #blocks[hashValue] = data[startPtr:start]
                            self.__write_log("Added 4", "from start:" + str(startPtr) + " to finish:" + str(start-1) + " at block:" + str(blockNbr))
                            blockNbr += 1

                            #Adding trailing part upto finish
                            hashValue = self.ComputeHash(data[start:finish])
                            if hashValue in blocks:
                                output[blockNbr] = (1, hashValue, finish-start)
                            else:
                                output[blockNbr] = (0, hashValue, finish-start)
                            self.__EncryptAndInsertData(blocks, hashValue, data[start:finish])
                            #blocks[hashValue] = data[start:finish]
                            self.__write_log("Added 5", "from start:" + str(start) + " to finish:" + str(finish-1) + " at block:" + str(blockNbr))
                            blockNbr += 1
                        else:
                            self.__write_log("Added 6", "from start:" + str(start) + " to finish:" + str(finish-1) + " at block:" + str(blockNbr))
                            hashValue = self.ComputeHash(data[start:finish])
                            self.__EncryptAndInsertData(blocks, hashValue, data[start:finish])
                            #blocks[previousHash] = data[start:finish]
                            output[blockNbr] = (0, previousHash, finish-start)
                            blockNbr += 1
                        break
                    start += 1
                    finish += 1
        self.__write_log("MatchHashValues", "ended with:" + str(len(output)))
        return output


    def __EncryptAndInsertData(self, blocks, hashValue, data):
        """
        Encrypts given data of given length
        """
        length = len(data)
        paddingLength = 8 - (length % 8)
        for i in range(paddingLength):
            data = data + 'X'

        DESObj = DES.new(EncryptionKey, DES.MODE_ECB)
        blocks[hashValue] = DESObj.encrypt(data)


    def __getSizeOfBuffer(self, buf):
        """
        Get the length of the buffer.
        """
        startPosition = buf.tell()     #get the current position of cursor
        buf.seek(0, os.SEEK_END)       #go to the last position in the buffer. 0th position respective to END of buffer
        length = buf.tell()            #get the length by current position of cursor
        buf.seek(startPosition, os.SEEK_SET) #Go back to starting position.
        return length


    def __write_log(self,function_name,message="",exception=None):
        f = open(os.path.expanduser('~/log.txt'),"a")
        if not exception:
            f.write(function_name +"   " + message + "\n")
        else:
            f.write(function_name +"   " + message + " " + exception.message + "\n")
        f.close()
