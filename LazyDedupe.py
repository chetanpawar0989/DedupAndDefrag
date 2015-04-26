
import sys
import os
import sqlite3
import gdbm
import cStringIO
from RabinKarp import RabinKarp

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
        self.rbkObj = RabinKarp()


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
            2.1. Send totalBuf to RabinKarp to get output{blockNbr:(alreadyPresent(0/1), hashValue, length)}
            2.2 for each item in output:
                2.2.1 insert in hashValues(hashId, hashValue, length)
                2.2.2 insert in fileBlocks(inodeNum, last_insert_rowid(), blockNbr)
        3 delete all inode entries from logs table.
        """

        #1. Get inodes from logs table which are modified on which lazy dedupe will work.
        try:
            self.__write_log("startDedupe in LazyDedupe","called")
            query = 'SELECT inodeNum, path FROM logs'
            results = self.conn.execute(query).fetchall()

            for row in results:
                inodeNum = row[0]
                self.__write_log("running for file:","" + str(row[0]) + " " + str(row[1]))
                #2. For each file changed, get total block of data from fileBlocks table in totalBuf
                dataBuf = self.__get_data_buffer(inodeNum)

                #2.1. Send totalBuf to RabinKarp to get output{blockNbr:(hashValue, length)}
                output = self.rbkObj.MatchHashValues(dataBuf, self.blocks)

                self.__delete_old_fileBlockEntries(inodeNum)
                self.__write_log("After deleting old entries","")
                for blockNbr, hashValue in output.iteritems():
                    self.__write_log("Block no:" + str(blockNbr), " value:(" + str(hashValue[0]) + "," + str(hashValue[1]) + "," + str(hashValue[2]) + ")")
                    #2.2.1 insert in hashValues(hashId, hashValue, length)
                    if not hashValue[0]:
                        query = 'INSERT INTO hashValues(hashId, hashValue, refCount, length) VALUES (NULL, ?, 1, ?)'
                        self.conn.execute(query, (hashValue[1], hashValue[2],))
                        hashId = self.conn.execute('SELECT last_insert_rowid()').fetchone()[0]
                    else:
                        hashId = self.conn.execute('SELECT hashId FROM hashValues WHERE hashValue = ?', (hashValue[1],)).fetchone()
                        if not hashId:
                            query = 'INSERT INTO hashValues(hashId, hashValue, refCount, length) VALUES (NULL, ?, 1, ?)'
                            self.conn.execute(query, (hashValue[1], hashValue[2],))
                            hashId = self.conn.execute('SELECT last_insert_rowid()').fetchone()[0]
                        else:
                            query = 'UPDATE hashValues SET refCount = refCount + 1 WHERE hashValue = ?'
                            self.conn.execute(query, (hashValue[1],))
                            hashId = hashId[0]

                    #2.2.2 insert in fileBlocks(inodeNum, last_insert_rowid(), blockNbr)
                    query = 'INSERT INTO fileBlocks(inodeNum, hashId, blockOrder) VALUES (?, ?, ?)'
                    self.conn.execute(query, (inodeNum, hashId, blockNbr))

            self.__write_log("After adding new entries, deleting from logs","")
            #3 delete all inode entries from logs table.
            query = 'DELETE FROM logs'
            self.conn.execute(query)
            self.__write_log("startDedupe in LazyDedupe","ended")
            self.conn.commit()
            self.__closeConnections()
        except Exception, e:
            self.conn.rollback()
            self.__write_log("startDedupe()","Exception", e)




    def __delete_old_fileBlockEntries(self, inodeNum):
        try:
            self.__write_log("__delete_old_fileBlockEntries in LazyDedupe","called")
            query = 'SELECT hashId FROM fileBlocks WHERE inodeNum = ?'
            results = self.conn.execute(query, (inodeNum,)).fetchall()
            inClause = '('
            for row in results:
                inClause = inClause + str(row[0]) + ','
            inClause = inClause[:-1] + ')'

            query = 'UPDATE hashValues SET refCount = refCount - 1 WHERE hashId in ' + inClause
            self.__write_log("in __delete_old_fileBlockEntries query=" + query,"")
            self.conn.execute(query)

            query = 'DELETE FROM fileBlocks WHERE inodeNum = ?'
            self.conn.execute(query, (inodeNum,))
            self.__write_log("__delete_old_fileBlockEntries in LazyDedupe","ended")

        except Exception, e:
            raise


    def __get_data_buffer(self, inodeNum):
        self.__write_log("get_data_buffer in LazyDedupe","called")

        dataBuf = cStringIO.StringIO()

        query = """SELECT h.hashValue from hashValues h, fileBlocks f WHERE f.inodeNum = ?
                  AND f.hashId = h.hashId ORDER BY f.blockOrder ASC"""
        resultList = self.conn.execute(query, (inodeNum,)).fetchall()
        for row in resultList:
            self.__write_log("","data:" + self.blocks[row[0]] + " data ended")
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


    def printLogs(self):
        query = 'SELECT * FROM logs'
        results = self.conn.execute(query).fetchall()
        print "inode\tpath"
        for row in results:
            print str(row[0]) + "\t" + str(row[1])

    def printFileBlocks(self):
        query = """SELECT f.inodeNum, h.hashValue, h.refCount, h.length, f.blockOrder  FROM fileBlocks f, hashValues h
                WHERE h.hashId = f.hashId order by f.blockOrder"""
        results = self.conn.execute(query).fetchall()
        print "inode\thashkey\tRefCount\tLength\tBlockOrder"
        for row in results:
            print str(row[0]) + "\t" + str(row[1]) + "\t" + str(row[2]) + "\t" + str(row[3]) + "\t" + str(row[4])

    def printHashes(self):
        query = 'SELECT * FROM hashValues'
        results = self.conn.execute(query).fetchall()
        print "hashId\thashkey\tRefCount\tLength"
        for row in results:
            print str(row[0]) + "\t" + str(row[1]) + "\t" + str(row[2]) + "\t" + str(row[3])

    def __closeConnections(self):
        self.conn.commit()
        self.conn.close()
        self.blocks.close()


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


