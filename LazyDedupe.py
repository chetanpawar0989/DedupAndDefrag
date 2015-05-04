
import sys
import os
import sqlite3
import gdbm
import cStringIO
import time
from datetime import datetime
from RabinKarp import RabinKarp
from collections import namedtuple
from Crypto.Cipher import DES

EncryptionKey = "wolfpack"

def namedtuple_factory(cursor, row):
        """
        Usage:
        con.row_factory = namedtuple_factory
        """
        fields = [col[0] for col in cursor.description]
        Row = namedtuple("Row", fields)
        return Row(*row)

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
        self.conn.row_factory = namedtuple_factory   #named attributes
        self.conn.text_factory = str    #to return regular strings
        self.conn.execute('PRAGMA locking_mode = EXCLUSIVE')
        self.blocks = gdbm.open(self.blockDBPath, 'cs')
        self.rbkObj = RabinKarp()


    def clearCache(self, calledWithin=False):
        """
        Removes unnecessary inode and hashValues entries from metadata tables.
        Removes unused blocks from gdbm self.blocks
        """
        try:
            self.__write_log("clearCache in LazyDedupe","called")
            # Removing unused entries from fileFolderNames table
            #self.conn.execute('DELETE FROM fileFolderNames WHERE fnameId NOT IN (SELECT fnameId FROM hierarchy)')

            # Decreasing refCount of blocks for unused inodes
            query = """UPDATE hashValues SET refCount = refCount-1 WHERE hashValue in
                    (SELECT hashValue FROM fileBlocks WHERE inodeNum in
                    (SELECT inodeNum FROM inodes WHERE nlink <= 0))"""
            self.conn.execute(query)
            self.__write_log("clearCache query",query)


            # Removing unused inodes
            self.conn.execute('DELETE FROM inodes WHERE nlink <= 0')

            # Removing unused hashBlock entries
            query = """SELECT h.hashValue FROM hashValues h WHERE h.refCount <= 0
                       AND h.hashId not in (SELECT ha.hashId from hashValuesArch ha)"""
            hashKeys = self.conn.execute(query).fetchall()
            for hk in hashKeys:
                if hk[0] in self.blocks:
                    self.__write_log("Deleted hashkey " + str(hk[0]),"from datastore")
                    del self.blocks[hk[0]]

            self.blocks.reorganize()
            self.conn.execute('DELETE FROM hashValues WHERE refCount <= 0')
            self.conn.commit()
            if not calledWithin:
                self.__closeConnections()
            self.__write_log("clearCache in LazyDedupe","ended")
        except Exception, e:
            self.conn.rollback()
            self.__write_log("clearCache()","Exception", e)
            if calledWithin:
                raise


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

            # starting deduplication on file which are modified
            query = 'SELECT inodeNum, path FROM logs'
            results = self.conn.execute(query).fetchall()

            if len(results) <= 0:
                self.__write_log("Nothing to dedupe in logs","called")
                self.__write_log("startDedupe in LazyDedupe","ended")
                return

            for row in results:
                inodeNum = row[0]
                self.__write_log("running for file:","" + str(row[0]) + " " + str(row[1]))
                #2. For each file changed, get total block of data from fileBlocks table in totalBuf
                dataBuf = self.__get_data_buffer(inodeNum)

                #2.1. Send totalBuf to RabinKarp to get output{blockNbr:(hashValue, length)}
                output = self.rbkObj.MatchHashValues(dataBuf, self.blocks)
                if len(output) > 0:
                    self.__delete_old_fileBlockEntries(inodeNum)
                    self.__write_log("After deleting old entries","")
                for blockNbr, hashValue in output.iteritems():
                    self.__write_log("Block no:" + str(blockNbr), " value:(" + str(hashValue[0]) + "," + str(hashValue[1]) + "," + str(hashValue[2]) + ")")
                    #2.2.1 insert in hashValues(hashId, hashValue, length)
                    if not hashValue[0]:
                        query = 'INSERT INTO hashValues(hashId, hashValue, refCount, length) VALUES (NULL, ?, 1, ?)'
                        self.conn.execute(query, (hashValue[1], hashValue[2],))
                        hashId = self.conn.execute('SELECT last_insert_rowid() as hashId').fetchone()[0]
                    else:
                        hashId = self.conn.execute('SELECT hashId FROM hashValues WHERE hashValue = ?', (hashValue[1],)).fetchone()
                        if not hashId:
                            query = 'INSERT INTO hashValues(hashId, hashValue, refCount, length) VALUES (NULL, ?, 1, ?)'
                            self.conn.execute(query, (hashValue[1], hashValue[2],))
                            hashId = self.conn.execute('SELECT last_insert_rowid() as hashId').fetchone()[0]
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
            self.__closeConnections()
        except Exception, e:
            self.conn.rollback()
            self.__write_log("startDedupe()","Exception", e)


    def __delete_old_fileBlockEntries(self, inodeNum):
        try:
            self.__write_log("__delete_old_fileBlockEntries in LazyDedupe","called")
            query = 'SELECT hashId FROM fileBlocks WHERE inodeNum = ?'
            results = self.conn.execute(query, (inodeNum,)).fetchall()
            if len(results) > 0:
                inClause = '('
                for row in results:
                    inClause = inClause + str(row[0]) + ','
                inClause = inClause[:-1] + ')'

                query = 'UPDATE hashValues SET refCount = refCount - 1 WHERE hashId in ' + inClause
                self.__write_log("query:",query)
                self.conn.execute(query)

            query = 'DELETE FROM fileBlocks WHERE inodeNum = ?'
            self.conn.execute(query, (inodeNum,))
            self.__write_log("__delete_old_fileBlockEntries in LazyDedupe","ended")
        except Exception, e:
            raise


    def __get_data_buffer(self, inodeNum):
        self.__write_log("get_data_buffer in LazyDedupe","called")

        dataBuf = cStringIO.StringIO()

        query = """SELECT h.hashValue, h.length from hashValues h, fileBlocks f WHERE f.inodeNum = ?
                  AND f.hashId = h.hashId ORDER BY f.blockOrder ASC"""
        resultList = self.conn.execute(query, (inodeNum,)).fetchall()
        DESObj = DES.new(EncryptionKey, DES.MODE_ECB)

        data = ""
        for row in resultList:
            tempData = DESObj.decrypt(self.blocks[row[0]])
            tempData = tempData[0:row[1]]
            data = data + tempData

        dataBuf.write(data)
        self.__write_log("get_data_buffer  in LazyDedupe","ended")
        return dataBuf


    def createSnapshot(self):
        """
        This method creates a snapshot of current file system.
        """
        try:
            self.__write_log("createSnapshot","called")
            # Creating snapshots
            t = time.time()
            query = 'INSERT INTO snapshots(snapId, startTime, endTime) VALUES (NULL, ?, ?)'
            self.conn.execute(query, (t, t,))
            snapId = self.conn.execute('SELECT last_insert_rowid() as snapId').fetchone()[0]

            self.backUpTables(snapId)

            #Updating end time for snapshot creation
            t = time.time()
            query = 'UPDATE snapshots SET endTime = ? WHERE snapId = ?'
            self.conn.execute(query, (snapId, t,))
            self.conn.commit()
            self.__closeConnections()
            self.__write_log("createSnapshot","ended")
            print "Snapshot " + str(snapId) + " is created."
        except Exception, e:
            self.conn.rollback()
            self.__write_log("createSnapshot()","Exception", e)


    def backUpTables(self, snapId):
        try:
            self.__write_log("backUpTables","called")
            results = self.conn.execute('SELECT ? as snapId, i.* FROM inodes i', (snapId,)).fetchall()
            if len(results) > 0:
                query = 'INSERT INTO inodesArch VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                self.conn.executemany(query, results)

            results = self.conn.execute('SELECT ? as snapId, h.* FROM hierarchy h', (snapId,)).fetchall()
            if len(results) > 0:
                query = 'INSERT INTO hierarchyArch VALUES (?, ?, ?, ?, ?)'
                self.conn.executemany(query, results)

            results = self.conn.execute('SELECT ? as snapId, s.* FROM softlinks s', (snapId,)).fetchall()
            if len(results) > 0:
                query = 'INSERT INTO softlinksArch VALUES (?, ?, ?)'
                self.conn.executemany(query, results)

            results = self.conn.execute('SELECT ? as snapId, h.* FROM hashValues h', (snapId,)).fetchall()
            if len(results) > 0:
                query = 'INSERT INTO hashValuesArch VALUES (?, ?, ?, ?, ?)'
                self.conn.executemany(query, results)

            results = self.conn.execute('SELECT ? as snapId, f.* FROM fileBlocks f', (snapId,)).fetchall()
            if len(results) > 0:
                query = 'INSERT INTO fileBlocksArch VALUES (?, ?, ?, ?)'
                self.conn.executemany(query, results)

            results = self.conn.execute('SELECT ? as snapId, l.* FROM logs l', (snapId,)).fetchall()
            if len(results) > 0:
                query = 'INSERT INTO logsArch VALUES (?, ?, ?)'
                self.conn.executemany(query, results)

            self.conn.commit()
            self.__write_log("backUpTables","ended")
        except Exception, e:
            self.conn.rollback()
            raise


    def deleteSnapshot(self, snapId):
        """
        This function deletes all the metadata from the archive table for particular snapshot.
        Deletes its corresponding blocks from the gdbm database.
        """
        try:
            self.__write_log("deleteSnapshot", "called for snapshotid:" + str(snapId))
            query = "DELETE FROM inodesArch WHERE snapId = ?"
            self.conn.execute(query, (snapId,))
            self.__write_log("", "Records from inodesArch deleted")

            query = "DELETE FROM hierarchyArch WHERE snapId = ?"
            self.conn.execute(query, (snapId,))
            self.__write_log("", "Records from hierarchyArch deleted")

            query = "DELETE FROM softlinksArch WHERE snapId = ?"
            self.conn.execute(query, (snapId,))
            self.__write_log("", "Records from softlinksArch deleted")

            query = "DELETE FROM hashValuesArch WHERE snapId = ?"
            self.conn.execute(query, (snapId,))
            self.__write_log("", "Records from hashValuesArch deleted")

            query = "DELETE FROM fileBlocksArch WHERE snapId = ?"
            self.conn.execute(query, (snapId,))
            self.__write_log("", "Records from fileBlocksArch deleted")

            query = "DELETE FROM logsArch WHERE snapId = ?"
            self.conn.execute(query, (snapId,))
            self.__write_log("", "Records from logsArch deleted")

            query = "DELETE FROM snapshots WHERE snapId = ?"
            self.conn.execute(query, (snapId,))
            self.__write_log("", "Record from snapshots deleted")

            self.conn.commit()
            self.clearCache(calledWithin=True)
            self.__closeConnections()
            self.__write_log("deleteSnapshot", "ended")
        except Exception, e:
            self.conn.rollback()
            self.__write_log("deleteSnapshot", "exception", e)


    def restoreToSnapshot(self, snapId):
        """
        Copies the particular snapId tables to current metadata and restores it to particular snapshot.
        """
        #Check if snapshotId is correct.
        self.__write_log("restoreToSnapshot", "called")
        row = self.conn.execute('SELECT COUNT(*) as r FROM snapshots').fetchall()
        if not row:
            print 'Invalid snapshot id. Please choose the correct id from below  snapshots'
            self.printSnapshots()
            return
        try:
            self.__delete_current_data()
            self.__restoreTables(snapId)
            self.conn.commit()
            self.__write_log("restoreToSnapshot", "ended")
            self.__closeConnections()
        except Exception, e:
            self.conn.rollback()
            self.__write_log("restoreToSnapshot", "exception", e)

    def __delete_current_data(self):
        """
        Deletes all current metatdata
        """
        self.conn.execute('DELETE FROM inodes')
        self.conn.execute('DELETE FROM hierarchy')
        self.conn.execute('DELETE FROM softlinks')
        self.conn.execute('DELETE FROM hashValues')
        self.conn.execute('DELETE FROM fileBlocks')
        self.conn.execute('DELETE FROM logs')

    def __restoreTables(self, snapId):
        """
        Copies given snapId tables from archive back to original metadata tables.
        """
        try:
            self.__write_log("__restoreTables","called")
            query = """SELECT inodeNum, nlink, mode, uid, gid, dev, size, atime, mtime, ctime FROM inodesArch
                       WHERE snapId = ?"""
            results = self.conn.execute(query, (snapId,)).fetchall()
            if len(results) > 0:
                query = 'INSERT INTO inodes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                self.conn.executemany(query, results)

            query = """SELECT hid, parenthid, fnameId, inodeNum FROM hierarchyArch
                       WHERE snapId = ?"""
            results = self.conn.execute(query, (snapId,)).fetchall()
            if len(results) > 0:
                query = 'INSERT INTO hierarchy VALUES (?, ?, ?, ?)'
                self.conn.executemany(query, results)

            query = """SELECT inodeNum, target FROM softlinksArch
                       WHERE snapId = ?"""
            results = self.conn.execute(query, (snapId,)).fetchall()
            if len(results) > 0:
                query = 'INSERT INTO softlinks VALUES (?, ?, ?)'
                self.conn.executemany(query, results)

            query = """SELECT hashId, hashValue, refCount, length FROM hashValuesArch
                       WHERE snapId = ?"""
            results = self.conn.execute(query, (snapId,)).fetchall()
            if len(results) > 0:
                query = 'INSERT INTO hashValues VALUES (?, ?, ?, ?)'
                self.conn.executemany(query, results)

            query = """SELECT inodeNum, hashId, blockOrder FROM fileBlocksArch
                       WHERE snapId = ?"""
            results = self.conn.execute(query, (snapId,)).fetchall()
            if len(results) > 0:
                query = 'INSERT INTO fileBlocks VALUES (?, ?, ?)'
                self.conn.executemany(query, results)

            query = """SELECT inodeNum, path FROM logsArch
                       WHERE snapId = ?"""
            results = self.conn.execute(query, (snapId,)).fetchall()
            if len(results) > 0:
                query = 'INSERT INTO logs VALUES (?, ?)'
                self.conn.executemany(query, results)

            self.conn.commit()
            self.__write_log("__restoreTables","ended")
        except Exception, e:
            self.conn.rollback()
            self.__write_log("__restoreTables", "exception", e)
            raise


    def printSnapshots(self):
        query = 'SELECT * FROM snapshots'
        results = self.conn.execute(query).fetchall()
        print "Id\tstartTime\t\t\tendTime"
        for row in results:
            print str(row[0]) + "\t" + str(datetime.fromtimestamp(float(row[1]))) + "\t" + str(datetime.fromtimestamp(float(row[2])))
        self.__closeConnections()


    def printLogs(self):
        query = 'SELECT * FROM logs'
        results = self.conn.execute(query).fetchall()
        print "inode\tpath"
        for row in results:
            print str(row[0]) + "\t" + str(row[1])
        self.__closeConnections()


    def printFileBlocks(self, inodeNum=None):
        query1 = """SELECT f.inodeNum, h.hashValue, h.refCount, h.length, f.blockOrder  FROM fileBlocks f, hashValues h
                WHERE h.hashId = f.hashId """
        query2 = " AND f.inodeNum = '" + str(inodeNum) + "'"
        query3 = " order by f.inodeNum, f.blockOrder """
        if inodeNum == None:
            query = query1 + query3
        else:
            query = query1 + query2 + query3
        results = self.conn.execute(query).fetchall()
        print "inode\thashkey \tRefCount\tLength\tBlockOrder"
        for row in results:
            print str(row[0]) + "\t" + str(row[1]) + "\t" + str(row[2]) + "\t\t" + str(row[3]) + "\t" + str(row[4])
        self.__closeConnections()


    def printHashes(self):
        query = 'SELECT * FROM hashValues'
        results = self.conn.execute(query).fetchall()
        print "hashId\thashkey \tRefCount\tLength"
        for row in results:
            print str(row[0]) + "\t" + str(row[1]) + "\t" + str(row[2]) + "\t\t" + str(row[3])
        self.__closeConnections()


    def printInodes(self):
        query = 'SELECT inodeNum, nlink, size, atime, mtime, ctime FROM inodes'
        results = self.conn.execute(query).fetchall()
        print "inode\tnlink \tsize\tatime"
        for row in results:
            print str(row[0]) + "\t" + str(row[1]) + "\t" + str(row[2]) + \
                  "\t" + str(datetime.fromtimestamp(float(row[3])))
        self.__closeConnections()


    def printFSStats(self):
        """
        prints the file system statistics.
        """
        #print number of inodes used.
        query = 'SELECT COUNT(inodeNum) as i FROM inodes WHERE nlink > 0'
        ninodes = self.conn.execute(query).fetchone()[0]
        print "Number of inodes used: " + str(ninodes)

        #print total number of individual blocks
        query = 'SELECT COUNT(hashId) as b FROM hashValues WHERE refCount > 0'
        nblocks = self.conn.execute(query).fetchone()[0]
        print "Number of variable sized blocks used: " + str(nblocks)

        #Get actual size of database without deduplication
        query = 'SELECT SUM(x) as y FROM (SELECT refCount * length as x FROM hashValues WHERE refCount > 0)'
        apparentSize = self.conn.execute(query).fetchone()[0]
        print "Apparant size without deduplication: " + str(apparentSize) + " bytes"

        #Get size of database after deduplication
        query = 'SELECT SUM(length) as l FROM hashValues WHERE refCount > 0'
        actualSize = self.conn.execute(query).fetchone()[0]
        print "Actual size with deduplication: " + str(actualSize) + " bytes"

        #Get size of database after deduplication + snapshots data
        #datavfs = os.stat(self.blockDBPath)
        #tsize = datavfs.st_size
        #totalSize = (datavfs.f_blocks -datavfs.f_bfree) * datavfs.f_frsize
        #totalSize1 = (datavfs.f_blocks -datavfs.f_bfree) * datavfs.f_bsize
        #print "Actual size of gdbm database object: " + str(tsize) + " bytes"
        #print "Actual size after deduplication + snapshots data1: " + str(totalSize1) + " bytes"

        self.__closeConnections()


    def __closeConnections(self):
        self.conn.commit()
        self.conn.close()
        self.blocks.close()
        import gc
        gc.collect()


    def printHelp(self):
        msg = """Help: python LazyDedupe <function-name> <args>
                 function-names:
                 1. printLogs - To see logs
                 2. printInodes - To print inode information
                 3. printFileBlocks <fileInodes> - To print inode information of all files/particular file.
                 4. printHashes - To print all hashKeys stored in hashValues table.
                 5. startDedupe - To start Lazy deduplication process
                 6. clearCache - To shrink database size after deduplication and clearing unnecessary tables.
                 7. printSnapshots - To list all the available snapshots.
                 8. createSnapshot - To create new snapshot
                 9. deleteSnapshot <snapShotId> - To delete particular snapshot entries
                 10.restoreToSnapshot <snapShotId> - To restore file system to particular snapshot.
                 11.printFSStats - To print file system statistics
                 12.printHelp - To see this help menu.
              """
        print msg


    def __write_log(self,function_name,message="",exception=None):
        f = open(os.path.expanduser('~/log.txt'),"a")
        if not exception:
            f.write(function_name +"   " + message + "\n")
        else:
            f.write(function_name +"   " + message + " " + exception.message + "\n")
        f.close()


    def printArch(self, snapId):
        #printing inodes
        query = 'SELECT inodeNum, nlink, size, atime, mtime, ctime FROM inodesArch WHERE snapId = ?'
        results = self.conn.execute(query, (snapId,)).fetchall()
        print "inode\tnlink \tsize\tatime"
        for row in results:
            print str(row[0]) + "\t" + str(row[1]) + "\t" + str(row[2]) + \
                  "\t" + str(datetime.fromtimestamp(float(row[3])))

        #printing fileblocks
        query = """SELECT f.inodeNum, h.hashValue, h.refCount, h.length, f.blockOrder  FROM fileBlocksArch f, hashValuesArch h
                WHERE h.hashId = f.hashId AND h.snapId = ? order by f.inodeNum, f.blockOrder"""
        results = self.conn.execute(query, (snapId,)).fetchall()
        print "inode\thashkey \tRefCount\tLength\tBlockOrder"
        for row in results:
            print str(row[0]) + "\t" + str(row[1]) + "\t" + str(row[2]) + "\t\t" + str(row[3]) + "\t" + str(row[4])

        #printing hashes
        query = 'SELECT hashId, hashValue, refCount, length FROM hashValuesArch WHERE snapId = ?'
        results = self.conn.execute(query, (snapId,)).fetchall()
        print "hashId\thashkey \tRefCount\tLength"
        for row in results:
            print str(row[0]) + "\t" + str(row[1]) + "\t" + str(row[2]) + "\t" + str(row[3])

        query = 'SELECT * FROM logsArch'
        results = self.conn.execute(query).fetchall()
        print "inode\tpath"
        for row in results:
            print str(row[1]) + "\t" + str(row[2])



if __name__ == '__main__':
    dedupeObj = LazyDedupe()
    if len(sys.argv) < 2:
        dedupeObj.printHelp()
        sys.exit(1)
    funct = sys.argv[1]
    if hasattr(dedupeObj, funct):
        methodCall = getattr(dedupeObj, funct)
        if funct in ('printLogs', 'printInodes', 'printFileBlocks', 'printHashes', 'clearCache',
                     'printHelp', 'printFSStats', 'printSnapshots', 'createSnapshot', 'startDedupe'):
            if funct == 'printFileBlocks' and len(sys.argv) == 3:
                    methodCall(sys.argv[2])
            else:
                methodCall()
        else:
            if len(sys.argv) == 3:
                arg = sys.argv[2]
                methodCall(arg)
            else:
                dedupeObj.printHelp()
    else:
        print "Invalid function name"
        dedupeObj.printHelp()



