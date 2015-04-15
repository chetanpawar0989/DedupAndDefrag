

import sys

try:
  import fuse
except ImportError:
  sys.stderr.write("Fuse not installed")
  sys.exit(1)
  
import logging
import os
import time
import datetime
from errno import *

SUCCSS, FAIL = 0, -1

fuse.fuse_python_api = (0, 2)

def main():
   """
   Starting point which initilizes the dedupe class object which in turn calls FUSE
   to create file system.
   """
   server = dedupeEncFS()

   arguments = server.parse(['-o', 'use_ino,default_permissions,fsname=dedupfs'] + sys.argv[1:])

   """
   if server.fuse_args.mount_expected() and not arguments.mountpoint:
     server.parse('-h')
   el
   """
   if arguments.mountpoint or not server.fuse_args.mount_expected():     
     dfs.main()		#calling fuse.main() indirectly
     

class dedupeEncFS(fuse.Fuse):
  
  
  def __init__(self, *args, **kw):
    """
    Constructor to initialize all the required objects, creating metadata and actual database.
    """
    try: 
      fuse.Fuse.__init__(self, *args, **kw)
      
      self.blockSize = 1024 * 128     #setting default block size
      self.dataBuffer = {}    #contains key-value pair of {path:data} of all blocks
      self.blockDatabase = '~/.datastore.db'
      self.metaDatabase = '~/.metadata.sqlite3'
      self.logFile = '~/FS.log'
      self.dedupeLogFile = '~/dedupeFileList.txt'
      self.link_mode = stat.S_IFLNK | 0777
      self.nodes = {}
      self.__NODE_KEY = 0

      #creating logs of activities
      self.logger = logging.getLogger('dedupeEncFS')
      self.logger.setLevel(logging.INFO)
      self.logger.addHandler(logging.StreamHandler(sys.stderr))
      
    except Exception, e:
      #ToDo: To write exception in console and in log.
      sys.exit(1)

  #####################################################
  #API methods: public and implementation of FUSE API
  #
  #####################################################

  def fsinit(self):
    """
    Called initializing the filesystem. It does
    - Setting up block database path
    - Setting up metadatabase path
    - Setting up log file and adding it as handler
    - Creating block database for storing different blocks
    - Creating metadatabase connection and tables for storing metadata.
    - Creating file to store list of files that are not deduplicated.
    """
    try:
      tstart = datetime.datetime.now()
      self.hashFunction = 'sha1'
      self.encryptionMethod = ''    #Todo
      self.blockDBPath = os.path.expanduser(self.blockDatabase)
      self.metaDBPath = os.path.expanduser(self.metaDatabase)
      self.logFilePath = os.path.expanduser(self.logFile)
      self.dedupeLogPath = os.path.expanduser(self.dedupeLogFile)
      

      #adding a log file to log messages
      logFileHandler = logging.StreamHandler(os.open(self.logFilePath, 'w'))
      self.logger.addHandler(logFileHandler)

      #creating database for storing blocks and metadata
      msg = "Creating database :\n"
      msg += "Block database file: " + self.blockDBPath + "\n"
      msg += "Metadata DB file: " + self.metaDBPath + "\n"
      self.__logMessage(msg)
      
      self.__createBlockDatabase()
      self.__createMetaDatabase()
      self.__insertMetaDataTables()

      #creating dedupe log for storing list of files for lazy deduplication
      os.open(self.dedupeLogPath, 'w')
      

      tend = datetime.datetime.now()      
      self.__logRunningTime('fsinit()', tstart, tend)
      
    except Exception, e
      sys.exit(1)

      
  def access(self, path, mode):
    """
    check if user have access to particular path with given flag.
    """
    try:
      msg = "access() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)
      hid, inodeNum = self.__getHidAndInode(path)
      if mode != os.F_OK and not self.__checkAccessInMetaData(path):
        return -EACCES

      return SUCCESS
    except Exception, e:
      #ToDo: To write exception in console and in log.
      sys.exit(1)


  def chmod(self, path, mode):
    """
    Change the file access mode.
    """
    try:
      msg = "chmod() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)
      hid, inodeNum = self.__getHidAndInode(path)

      query = 'UPDATE inodes SET mode = ? WHERE inode = ?'
      conn.execute(query, (mode, inodeNum, uid))
      return SUCCESS
      
    except Exception, e:
      #ToDo: To write exception in console and in log.
      sys.exit(1)


  def chown(self, path, uid, gid):
    """
    Change the file user id and group id.
    """
    try:
      msg = "chown() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)
      hid, inodeNum = self.__getHidAndInode(path)

      query = 'UPDATE inodes SET uid = ?, gid = ? WHERE inode = ?'
      conn.execute(query, (uid, gid, inodeNum))
      return SUCCESS
      
    except Exception, e:
      #ToDo: To write exception in console and in log.
      sys.exit(1)


  def open(self, path, flags, inode=None):
    try:
      msg = "open() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      inodeNum = inode or self.__getHidAndInode(path)[1]

      if self.__checkAccessInMetaData(inodeNum, flags):
        return -EACCES

      return SUCCESS
    except Exception, e:
      #ToDo: To write exception in console and in log.
      sys.exit(1)



  def create(self, path, flags, mode):
    try:
      msg = "create() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      if not os.access(path, os.F_OK):
        inodeNum, parentINodeNum = self.__insertNewNode(path, mode, 0)
        res = self.open(path, flags, inodeNum)

      if res == SUCCSS:
        self.conn.commit()
        return SUCCESS
      else:
        msg = "Error in create() method. Rolling back the changes.")
        self.conn.rollback()
        return FAIL
      
    except Exception, e:
      #ToDo: To write exception in console and in log.
      sys.exit(1)


  def fsdestroy(self):
    try:
      msg = "fsdestroy() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)
      #committing and closing metadatabase connection
      self.conn.commit()
      self.conn.close()

      #closing gdbm DB object
      self.blocks.close()

      #deleting cached nods
      del self.nodes

      #garbage collection
      import gc
      gc.collect()

      return SUCCESS
      
    except Exception, e:
      #ToDo: To write exception in console and in log.
      sys.exit(1)


  
  
      
    
  #####################################################
  #Helper methods: private and called internally
  #
  #####################################################

  def __insertNewNode(self, path, mode, size, dev=0):
    """
    - To insert to new file/folder in metadata. Consists of three steps:
    - 1. Insert in inodes table.
    - 2. Insert in fileFolderNames table
    - 3. Link file/folder with parent by inserting in hierarchy table.
    - returns insertedInode and parent_inode
    """
    parent, child = os.path.split(path)
    parent_hid, parent_inodeNum = self.__getHidAndInode(parent)
    if mode & stat.S_IFDIR:
      nlink = 2     #2 links '.' and '..' by default for folder
    else:
      nlink = 1     #1 link by default for other file types
    t = time.time()
    context = fuse.FuseGetContext()
    query = 'INSERT INTO inodes (nlink, mode, uid, gid, dev, size, atime, mtime, ctime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
    self.conn.execute(query, (nlink, mode, context['uid'], context['gid'], size, t, t, t))
    insertedInodeNum = self.conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    insertedFnameId = self.__GetFnameIdFromName(child)
    query = 'INSERT INTO hierarchy (parenthid, fnameId, inodeNum) VALUES (?, ?, ?)'
    self.conn.execute(query, (parent_hid, insertedFnameId, insertedInodeNum))
    return insertedInodeNum, parent_inodeNum



  def __GetFnameIdFromName(self, fname):
    """
    - Returns fileNameId from fileFolderNames table.
    - If file/folder not present then insert new record and return the inserted fnameid.
    """
    fname = sqlite3.Binary(fname)
    fnameId = self.conn.execute('SELECT fnameId FROM fileFolderNames WHERE fname = ?', (fname)).fetchone()
    if not fileId:    #file/folder not present. insert new one.
      self.conn.execute('INSERT INTO fileFolderNames (fnameId, fname) VALUES (NULL, ?)', (fname))
      fnameId = self.conn.execute('SELECT last_insert_rowid()').fetchone()
    return int(fnameId[0])

    

  def __checkAccessInMetaData(self, inodeNum, flags):
    """
    Check if the access stored in metadata for this user is consistent with flags
    """
    #get uid and gid from fuse method
    context = fuse.FuseGetContext()
    #get values stored in database.
    query = 'SELECT mode, uid, gid FROM inodes WHERE inodeNum = ?'
    result = self.conn.execute(query, (inodeNum)).fetchone();

    o = context['uid'] == result['uid']    #check if owner
    g = context['gid'] == result['gid'] and not u   #check if same group but not owner
    w = not (o or g)    #implies neither owner nor beloging to same group
    m = result['mode']

    output = (not (flags & os.R_OK) or ((o and (m & 0400)) or (g and (m & 0040)) or (w and (m & 0004)))) \
              and (not (flags & os.W_OK) or ((o and (m & 0200)) or (g and (m & 0020)) or (w and (m & 0002)))) \
              and (not (flags & os.X_OK) or ((o and (m & 0100)) or (g and (m & 0010)) or (w and (m & 0001))))

    return output
               
    

    

  def __createMetaDatabase(self):
    """
    Creates sqlite3 database for storing metadata
    """
    self.conn = sqlite3.connect(self.metaDBPath, isolation_level=None)
    self.conn.row_factory = sqlite3.Row   #named attributes
    self.conn.text_factory = str    #to return regular strings
    self.conn.execute('PRAGMA locking_mode = EXCLUSIVE')


  def __insertMetaDataTables(self):
    """
    - Creates basic tables for storing metadata like inodes, filenames, hashes etc.
    - insert default rows for root folder.
    """
    
    uid = os.getuid()   #get current user id
    gid = os.getgid()   #get current user group id
    root_mode = stat.S_IFDIR | 0755
    t = time.time()
    script = """
        --Creating the tables
        CREATE TABLE IF NOT EXISTS fileFolderNames(fnameId INTEGER PRIMARY KEY, fname BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS inodes(inodeNum INTEGER PRIMARY KEY, nlink INTEGER NOT NULL, mode INTEGER NOT NULL, uid INTEGER, gid INTEGER, dev INTEGER, size INTEGER, atime INTEGER, mtime INTEGER, ctime INTEGER);
        CREATE TABLE IF NOT EXISTS hierarchy(hid INTEGER PRIMARY KEY, parenthid INTEGER, fnameId INTEGER NOT NULL, inodeNum INTEGER, UNIQUE(parent_hid, fnameId));
        CREATE TABLE IF NOT EXISTS softlinks(inodeNum INTEGER, target BLOB NOT NULL)
        CREATE TABLE IF NOT EXISTS hashValues(hashId INTEGER PRIMARY KEY, hashValue BLOB NOT NULL UNIQUE, dedupeDone INTEGER NOT NULL)
        CREATE TABLE IF NOT EXISTS fileBlocks(inodeNum INTEGER, hashId INTEGER, blockOrder INTEGER NOT NULL, PRIMARY KEY(inodeNum, hashId, blockOrder))

        -- Insert default rows for root folder.
        INSERT INTO fileFolderNames (fnameId, fname) VALUES(1, '');
        INSERT INTO inodes(nlink, mode, uid, gid, dev, size, atime, mtime, ctime) VALUES (2, %i, %i, %i, 0, 4096, %f, %f, %f);
        INSERT INTO hierarchy (hid, parenthid, fnameId, inodeNum) VALUES (1, NULL, 1, 1);
        
        """ % (root_mode, uid, gid, t, t, t)
    
    self.conn.execute(script)
    
    

  def __createBlockDatabase(self):
    """
    Create GDBM database for storing different blocks
    """
    import gdbm
    self.blocks = gdbm.open(self.blockDBPath, 'cs')
    


  def __getHidAndInode(self, path):
    """
    Returns hierachy id and inode for path.
    """
    hid, inodeNum = 1, 1    #default for root folder
    if path == '/':
      return hid, inodeNum
    parenthid = hid
    names = path.split('/')
    names = [n for n in names if name != '']      #removing empty path at beginning
    tempNodes = self.nodes      #getting cached nodes so far
    
    for fname in names:
      if fname in tempNodes:
        tempNodes = tempNodes[fname]
        hid, inodeNum = tempNodes[self.__NODE_KEY]
      else:
        query = 'SELET h.hid, h.inodeNum from hierarchy h, fileFolderNames f WHERE h.parenthid = ? AND h.fnameid=f.fnameid AND f.fname = ?'
        row = self.conn.execute(query, (parenthid, sqlite3.Binary(fname))).fetchone()
        if row == None:
          raise OSError     #ToDo raise custom exception showing path does not exists
        hid, inodeNum = row
        tempNodes[fname] = {self.__NODE_KEY:(hid, inodeNum)}
        tempNodes = tempNodes[fname]
      parenthid = hid

    return hid, inodeNum
        
    
  

  def __logRunningTime(functionName, tstart, tend):
    """
    To log the running time of function.
    """
    msg = ''
    time_diff = tend - tstart;
    seconds = time_diff.total_seconds();

    msg = functionName + " called:" + str(tstart) + " to " \
            + str(tend) " --> " + str(seconds) + " seconds."      
      
    self.__logMessage(msg)


  def __logMessage(message):
    """
    To log debug message.
    """
    self.logger.debug(message)



      
    
    
    



if __name__ == '__main__':
   main()
