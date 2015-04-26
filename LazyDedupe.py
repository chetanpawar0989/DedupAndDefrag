
import sys
import os
import sqlite3
import gdbm
import cStringIO
from RabinCarp import RabinCarp

class LazyDedupe:

    def __init__(self):
        """
        Log that lazy dedupe has started
        """
        self.blockDatabase = '~/.datastore.db'
        self.metaDatabase = '~/.metadata.sqlite3'
        self.blockDBPath = os.path.expanduser(self.blockDatabase)
        self.metaDBPath = os.path.expanduser(self.metaDatabase)
        self.conn = sqlite3.connect(self.metaDBPath, isolation_level=None)
        self.conn.row_factory = sqlite3.Row   #named attributes
        self.conn.text_factory = str    #to return regular strings
        self.conn.execute('PRAGMA locking_mode = EXCLUSIVE')
        self.blocks = gdbm.open(self.blockDBPath, 'cs')
        self.rbkObj = RabinCarp()


    def clearCache(self):
        """
        Removes unnecessary inode and hashValues entries from metadata tables.
        Removes unused blocks from gdbm self.blocks
        """
        try:
            self.__write_log("clearCache in LazyDedupe","called")
            # Removing unused entries from fileFolderNames table
            self.conn.execute('DELETE FROM fileFolderNames WHERE fnameId NOT IN (SELECT fnameId FROM hierarchy)')

            # Removing unused inodes
            self.conn.execute('DELETE FROM inodes WHERE nlink <= 0')

            # Removing unused hashBlock entries
            hashKeys = self.conn.execute('SELECT hashValue FROM hashValues WHERE refCount <= 0').fetchall()
            for hk in hashKeys:
                if hk[0] in self.blocks:
                    del self.blocks[hk[0]]

            self.conn.execute('DELETE FROM hashValues WHERE refCount <= 0')
            self.conn.commit()
            self.__write_log("clearCache in LazyDedupe","ended")
        except Exception, e:
            self.conn.rollback()
            self.__write_log("clearCache()","Exception", e)


    def startDedupe(self):
        """
        1. Get inodes from logs table which are modified on which lazy dedupe will work.
        2. For each file changed, get total block of data from fileBlocks table in totalBuf
            2.1. Send totalBuf to RabinCarp to get output{blockNbr:(alreadyPresent(0/1), hashValue, length)}
            2.2 for each item in output:
                2.2.1 insert in hashValues(hashId, hashValue, length)
                2.2.2 insert in fileBlocks(inodeNum, last_insert_rowid(), blockNbr)
        3 delete all inode entries from logs table.
        """

        #1. Get inodes from logs table which are modified on which lazy dedupe will work.
        try:
            self.__write_log("startDedupe in LazyDedupe","called")
            query = 'SELECT inodeNum, path, isNewlyCreated FROM logs'
            results = self.conn.execute(query).fetchall()

            for row in results:
                inodeNum = row[0]
                #2. For each file changed, get total block of data from fileBlocks table in totalBuf
                dataBuf = self.__get_data_buffer(inodeNum)
                #2.1. Send totalBuf to RabinCarp to get output{blockNbr:(hashValue, length)}
                output = self.rbkObj.MatchHashValues(dataBuf, self.blocks)
                self.__delete_old_fileBlockEntries(inodeNum)

                for blockNbr, hashValue in output:
                    hashKeyForDB = sqlite3.Binary(hashValue[1])
                    #2.2.1 insert in hashValues(hashId, hashValue, length)
                    if not hashValue[0]:
                        query = 'INSERT INTO hashValues(hashId, hashValue, refCount, length) VALUES (NULL, ?, 1, ?)'
                        self.conn.execute(query, (hashKeyForDB, hashValue[2],))
                        hashId = self.conn.execute('SELECT last_insert_rowid()').fetchone()[0]
                    else:
                        query = 'UPDATE hashValues SET refCount = refCount + 1 WHERE hashValue = ?'
                        self.conn.execute(query, (hashKeyForDB,))
                        hashId = self.conn.execute('SELECT hashId FROM hashValues WHERE hashValue = ?', (hashKeyForDB,)).fetchone()[0]
                    #2.2.2 insert in fileBlocks(inodeNum, last_insert_rowid(), blockNbr)
                    query = 'INSERT INTO fileBlocks(inodeNum, hashId, blockOrder) VALUES (?, ?, ?)'
                    self.conn.execute(query, (inodeNum, hashId, blockNbr))

            #3 delete all inode entries from logs table.
            query = 'DELETE FROM logs'
            self.conn.execute(query)
            self.__write_log("startDedupe in LazyDedupe","ended")
            self.conn.commit()
        except Exception, e:
            self.conn.rollback()
            self.__write_log("startDedupe()","Exception", e)



    def __delete_old_fileBlockEntries(self, inodeNum):
        self.__write_log("__delete_old_fileBlockEntries in LazyDedupe","called")
        query = 'DELETE FROM fileBlocks WHERE inodeNum = ?'
        self.conn.execute(query, (inodeNum,))
        self.__write_log("__delete_old_fileBlockEntries in LazyDedupe","ended")


    def __get_data_buffer(self, inodeNum):
        self.__write_log("get_data_buffer in LazyDedupe","called")

        dataBuf = cStringIO.StringIO()

        query = """SELECT h.hashValue from hashValues h, fileBlocks f WHERE f.inodeNum = ?
                  AND f.hashId = h.hashId ORDER BY f.blockOrder ASC"""
        resultList = self.conn.execute(query, (inodeNum,)).fetchall()
        for row in resultList:
            dataBuf.write(self.blocks[row[0]])

        self.__write_log("get_data_buffer  in LazyDedupe","ended")
        return dataBuf



    def __write_log(self,function_name,message="",exception=None):
        f = open('/home/chetanpawar0989/log.txt','a')
        if not exception:
            f.write(function_name +"   " + message + "\n")
        else:
            f.write(function_name +"   " + message + " " + exception.message + "\n")
        f.close()


if __name__ == '__main__':
    dedupeObj = LazyDedupe()
    if len(sys.argv) == 1 or len(sys.argv) > 2:
        print "Please call only one function."
    funct = sys.argv[1]
    if hasattr(dedupeObj, funct):
        methodCall = getattr(dedupeObj, funct)
        methodCall()
    else:
        print "Invalid function name"


