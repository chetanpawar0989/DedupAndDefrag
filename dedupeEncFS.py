

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
import cStringIO
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
      
      self.DefaultBlockSize = 1024 * 128     #setting default block size
      self.dataBuffer = {}    #contains key-value pair of {path:buffer data} of all blocks
      self.blockDatabase = '~/.datastore.db'
      self.metaDatabase = '~/.metadata.sqlite3'
      self.logFile = '~/FS.log'
      self.dedupeLogFile = '~/dedupeFileList.txt'
      self.link_mode = stat.S_IFLNK | 0777
      self.nodes = {}
      self.cache_key = 0
      self.defaulFolderSize = 1024 * 4    #Default folder size = 4kb when created
      self.defaultFileSize = 0            #Default file size = 0kb when created

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
        inodeNum, parentINodeNum = self.__insertNewNode(path, mode, self.defaultFileSize)
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


  
  def getattr(sef, path):
    """
    Called when stat, fstat, lstat done
    """
    try:
      msg = "getattr() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      inodeNum = self.__getHidAndInode(path)[1]
      query = 'SELECT inodeNum, nlink, mode, uid, gid, dev, size, atime, mtime, ctime FROM inodes WHERE inode = ?'
      res = self.conn.execute(query, (inode,)).fetchone();
      output = Stat(st_ino = res['inodeNum'],
                  st_nlink = res['nlink'],
                  st_mode = res['mode'],
                  st_uid = res['uid'],
                  st_gid = res['gid'],
                  st_dev = res['dev'],
                  st_nlink = res['nlink'],
                  st_size = res['size'],
                  st_atime = res['atime'],
                  st_mtime = res['mtime'],
                  st_ctime = res['ctime'])
      
      return output
    except Exception, e:
      #ToDo: To write exception in console and in log.
      return -ENOENT


  def link(self, old_file_path, new_file_path):
    """
    Creating hard links.
    - First create the new_file in new_file_parent, if it does not exists by calling __getfnameidFromName method
    - We need not create new inode, Insert record in hierarchy table with old_file inode.
    - Update inode table to increase the link count.
    - 
    """
    try:
      msg = "link() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)
      new_file_parent, new_file_name = os.path.split(new_file_path);
      new_file_parent_hid, new_file_inode = self.__getHidAndInode(new_file_parent)
      old_file_inode = self.__getHidAndInode(old_file_path)[1]

      #create a new_file in fileFolderNames table.
      new_file_nameId = self.__GetFnameIdFromName(new_file_name)

      #insert new record in hierarchy related to same inode but a new file in new_file_parent
      query = 'INSERT INTO hierarchy (parenthid, fnameId, inodeNum) VALUES (?, ?, ?)'
      self.conn.execute(query, (new_file_parent_hid, new_file_nameId, old_file_inode))

      #update inode table to increase the link count
      query = 'UPDATE inodes SET nlink = nlink + 1 WHERE inodeNum = ?'
      self.conn.execute(query, (old_file_inode,))

      self.conn.commit()
      return SUCCESS
      
    except Exception, e:
      #ToDo: To write exception in console and in log.
      self.conn.rollback()
      return FAIL
    

  def symlink(self, target, new_link):
    """
    To create symbolic/soft links.
    
    """
    try:
      msg = "symlink() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      #creating new link
      new_link_Inode, parent_folder_inode = self.__insertNewNode(new_link, stat.S_IFLNK | 0777, len(target))

      #keep track by inserting in softlinks table.
      query = 'INSERT INTO softlinks (inodeNum, target) VALUES (?, ?)'
      self.conn.execute(query, (new_link_Inode, sqlite3.Binary(target)))
      
      self.conn.commit()
      return SUCCSS
      
    except Exception, e:
      #ToDo: To write exception in console and in log.
      self.conn.rollback()
      return FAIL


  def mkdir(self, path, mode):
    """
    Called while creating a new directory.
    - Create a folder by inserting in inode, fileAndFolderNames and Hirarchy by calling __insertNewNode() method
    - Increase the nlink count of parent in which this folder is created.
    """
    try:
      msg = "mkdir() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      #creating new folder.
      new_Folder_Inode, parent_Folder_Inode = self.__insertNewNode(path, mode | stat.S_IFDIR, self.defaultFolderSize)

      #Increasing the link count of parent folder.
      query = 'UPDATE inodes SET nlink = nlink + 1 WHERE inodeNum = ?'
      self.conn.execute(query, (parent_Folder_Inode,))
      
      self.conn.commit()
      return SUCCESS
    
    except Exception, e:
      #ToDo: To write exception in console and in log.
      self.conn.rollback()
      return FAIL


  def mknod(self, path, mode, dev):
    """
    Called while creating a filesystem node (file, device special file or named pipe)
    - Create new file by inserting in inode, fileAndFolderNames and Hierarchy by calling __insertNewNode() method
    - Need not increase the nlink count of parent.
    """
    try:
      msg = "mknod() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      self.__insertNewNode(path, mode, self.defaultFolderSize, dev)
      
      self.conn.commit()
      return SUCCESS
    
    execpt Exception, e:
      #ToDo: To write exception in console and in log.
      self.conn.rollback()
      return FAIL


  def read(self, path, length, offset):
    """
    Called when reading a particular file.
    - Get the total buffer from self.buffer by calling helper method __get_data_buffer()
    - read buffer from offset to length
    """
    try:
      msg = "read() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)
      
      buf = self.__get_data_buffer(path)
      buf.seek(offset)
      data = buf.read(length)
      return data
    except Exception, e:
      #ToDo: To write exception in console and in log.
      return -EIO   #input output error


  def readdir(self, path, offset):
    """
    Get the directory entries.
    """
    try:
      msg = "readdir() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      path_hid, path_inode = self.__getHidAndInode(path)     
      
      #Default directory pointers adding to direntry of fuse
      yield fuse.Direntry('.', ino = inode)   
      yield fuse.Direntry('..')

      #get all the files and folders inside path by querying hierarchy table
      query = 'SELECT h.inodeNum, f.fname FROM hierarchy h, fileFolderNames f WHERE h.parenthid = ? AND h.fnameId = f.fnameId'
      resultList = self.conn.execute(query, (parent_hid)).fetchall()

      for file in resultList:
        yeild fuse.Direntry(str(file[1]), ino=file[0])

    except Exception, e:
      #ToDo: To write exception in console and in log.
      return -EIO   #input output error


  def readlink(self, path):
    """
    Called while reading a link.
    """
    try:
      msg = "readlink() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      linkInode = self.__getHidAndInode(path)[1]

      #Get the link target from softlinks table.
      query = 'SELECT target FROM softlinks WHERE inodeNum = ?'
      result = self.conn.execute(query, (linkInode,)).fetchone()

      targetpath = result[0]
      return str(targetpath)    
    except Exception, e:
      #ToDo: To write exception in console and in log.
      return -ENOENT   #No path exists


  def unlink(self, path):
    """
    Called while removing the link.
    """
    try:
      self.__removeFileOrFolder(path)
      self.conn.commit()
    except Exception, e:
      self.conn.rollback()
      #ToDo: To write exception in console and in log.
      return FAIL
    

  def rename(self, oldPath, toPath):
    """
    Change the name and location of file.
    """
    try:
      msg = "rename() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)
      
      try:
        #if toPath directory is different that fromPath directory then check if it not empty before deleting
        self.__removeFileOrFolder(toPath, emptyCheck=True)
      except OSError, e:
        if e.errno != errno.ENOENT: raise   

      #creating new hard link --> equivalent to creating the new file
      self.link(oldPath, toPath)

      #deleting old file
      self.unlink(oldPath)

      self.conn.commit()
      return SUCCESS
    except Exception, e:
      #ToDo: To write exception in console and in log.
      return FAIL


  def rmdir(self, path):
    """
    Called when directory is removed.
    """
    try:
      msg = "rmdir() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      #check if it is not empty before deleting
      self.__removeFileOrFolder(toPath, emptyCheck=True)

      self.conn.commit()
      return SUCCESS
    except Exception, e:
      #ToDo: To write exception in console and in log.
      return FAIL


  def statfs(self):
    """
    statistics about the filesystem. We will perform statvfs on metadata file, which holds the inodes
    and can determine the number of blocks available to user etc.
    """
    try:
      msg = "statfs() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      metavfs = os.statvfs()
      #Total number of blocks
      totalBlocks = (metavfs.f_blocks * metavfs.f_frsize) / self.defaultBlockSize

      #Total number of free blocks in file system
      totalFree = (metavfs.f_bfree * metavfs.f_frsize) / self.defaultBlockSize

      #Total number of free blocks that are available
      totalFreeAvail = (metavfs.f_bavail * metavfs.f_frsize) / self.defaultBlockSize

      output = StatVfs(f_bsize = self.defaultBlockSize,
                       f_frsize = self.defaultBlockSize,
                       f_blocks = totalBlocks,
                       f_bfree = totalFree,
                       f_bavail = totalFreeAvail,
                       f_files = 0,
                       f_ffree = 0,
                       f_favail = 0,
                       f_flag = 0,
                       f_namemax = metavfs.f_namemax)
      return output
      
    except Exception, e:
      #ToDo: To write exception in console and in log.
      return -EIO


  def truncate(self, path, size):
    """
    Truncate or extend the given file so that it is precisely size bytes long.
    - Get last block from size/blockSize after which we have to truncate
    - delete all the blocks 
    """
    try:
      msg = "truncate() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      lastBlock = size / self.defaultBlockSize
      if not lastBlock:   #size is less than defaultBlockSize
        return SUCCESS
      
      #get inodeNume whose block entries are to be removed from fileBlocks table.
      inodeNum = self.__getHidAndInode(path)[1]

      query = 'DELETE FROM fileBlocks WHERE inodeNum = ? AND block_nr > ?'
      self.conn.execute(query, (inodeNum, lastBlock,))

      query = 'UPDATE inodes SET size = ? WHERE inodeNum = ?'
      self.conn.execute(query, (size, inodeNum,))

      self.conn.commit()
      return SUCCESS
    except Exception, e:
      #ToDo: To write exception in console and in log.
      self.conn.rollback()
      return FAIL


  def write(self, path, data, offset):
    """
    write the data to path at particular offset.
    - Get data buffer from database
    - seek to particular offset
    - write data at the offset
    """
    try:
      msg = "write() called at " + str(datetime.datetime.now() + " on file " + str(path))
      self.__logMessage(msg)
      length = len(data)
      
      buf = __get_data_buffer(path)
      buf.seek(offset)
      buf.write(data)
      
      return length
    except Exception, e:
      #ToDo: To write exception in console and in log.
      self.conn.rollback()
      return -EIO


  def release(self, path, flags):
    try:
      
    except Exception, e:
      #ToDo: To write exception in console and in log.
      self.conn.rollback()
      return -EIO

    
      
    
  #####################################################
  #Helper methods: private and called internally
  #
  #####################################################

  def __get_data_buffer(self, path):
    if path in self.dataBuffer:
      return self.dataBuffer[path]

    dataBuf = cString.StringIO()
    fileInode = self.__getHidAndInode(path)[1]
    query = """SELECT h.hashValue from hashValues h, fileBlocks f WHERE f.inodeNum = ?
              AND f.hashId = h.hashId ORDER BY f.blockOrder ASC"""
    resultList = self.conn.execute(query, (fileInode,)).fetchall()
    for row in resultList:
      buf.write(self.blocks[row[0]])
    self.dataBuffer[path] = buf       #storing in dataBuffer for quick access later
    return buf
    

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

  def __removeFileOrFolder(self, path, emptyCheck=False):
    """
    removes file/folder from hierarchy table and decrement the nlink in inodes table.
    - if emptyCheck is true then we can't allow removing directory if directory is not empty.
    - else remove from hierarchy table and decremenet nlink in inodes table.
    - if deleted item is folder then decrement the parent folder nlink as well.
    """
    hid, inodeNum = self.__getHidAndInode(path)
    if emptyCheck:
      query = """SELECT count(h.hid) FROM hierarchy h, inodes i WHERE h.parenthid = ?
                 AND h.inodeNum = i.inodeNum AND i.nlink > 0"""
      results = self.conn.execute(query, (hid)).fetchone()
      linkCount = result[0]
      if linkCount:
        raise OSError, (errno.ENOTEMPTY, os.strerror(errno.ENOTEMPTY), path)
    
    del self.dataBuffer[path]
    # Delete from hierarchy table.
    query = 'DELETE FROM hierarchy WHERE hid = ?'
    self.conn.execute(query, (hid,))

    # Decrement the link in inodes table.
    query = 'UPDATE inodes SET nlink = nlink - 1 WHERE inodeNum = ?'
    self.conn.execute(query, (inodeNum,))

    # Find if currently deleted item is folder from mode.
    query = 'SELECT mode from inodes WHERE inodeNum = ?'
    mode = self.conn.execute(query, (inodeNum,)).fetchone()[0]
    if mode & stat.S_IFDIR:
      parent_path = os.path.split(path)[0]
      parent_inode = self.__getHidAndInode(parent_path)
      query = 'UPDATE inodes SET nlink = nlink - 1 WHERE inodeNum = ?'
      self.conn.execute(query, (parent_inode,))
      
    
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
        hid, inodeNum = tempNodes[self.cache_key]
      else:
        query = 'SELET h.hid, h.inodeNum from hierarchy h, fileFolderNames f WHERE h.parenthid = ? AND h.fnameid=f.fnameid AND f.fname = ?'
        row = self.conn.execute(query, (parenthid, sqlite3.Binary(fname))).fetchone()
        if row == None:
          raise OSError     #ToDo raise custom exception showing path does not exists
        hid, inodeNum = row
        tempNodes[fname] = {self.cache_key:(hid, inodeNum)}
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

  def getLength(buf):
      """
      Get the length of the buffer.
      - Get the current position in the buffer as start position
      - Go to last position
      - Length = last position - start position
      - Go back to start position
      """
      startPosition = buf.tell()     #get the current position of cursor
      buf.seek(0, os.SEEK_END)       #go to the last position in the buffer. 0th position respective to END of buffer
      length = buf.tell()            #get the length by current position of cursor
      buf.seek(startPosition, os.SEEK_SET) #Go back to starting position.
      return length

  def utime(self, path, times)
       """
       - update inode access, modified time 
       """
       try:
         inodeNum =  __getHidAndInode(path)[1]
         atime, mtime = times
         self.conn.execute('UPDATE inodes SET atime = ?, mtime = ? WHERE inode = ?' (atime, mtime, inodeNum))
         return 0
       except Exception e:
         #ToDo: To write exception in console and in log.
         sys.exit(1)

  def utimens(self, path, a_time, m_time)      
       """
       - update inode access, modified time in nano seconds
       """
    try:
      inodeNum =  __getHidAndInode(path)[1]
      atime_ns = a_time.tv_sec + (a_time.tv_nsec / 1000000.0) #convert access time in nano seconds
      mtime_ns = m_time.tv_sec + (m_time.tv_nsec / 1000000.0) #convert modified time in nano seconds
      self.conn.execute('UPDATE inodes SET atime = ?, mtime = ? WHERE inode = ?', (atime_ns, mtime_ns, inode))           
      return 0
    except Exception e:
       #ToDo: To write exception in console and in log.
        sys.exit(1)      

  def unlink(self, path, inner_call)
       """
       - Unlink and update inode and hierarchy details
       """   
    try:
      self.__remove(path)
      self.conn.commit()
    except Exception, e:
      self.__rollback_changes(nested)
      if inner_call: raise
      #ToDo: To write exception in console and in log.
      sys.exit(1)

  def __remove(self, path, check)    
       """
       - update Hierarchy and Inode tables
       """  
     hid, inodeNum = self.__getHidAndInode(path)
     query = ' SELECT COUNT(h.hid) FROM hierarchy, indoes i WHERE \
               h.parenthid = ? AND  i.inodeNum AND i.nlink > 0'
      if self.conn.execute(query, values).fetchone()[0] > 0 #if we are deleting a directory but it is not empty
         \\ ToDo Raise an error
      self.conn.execute('DELETE FROM hierarchy where hid= ?',(hid,))
      self.conn.execute(UPDATE indoes SET nlink = nlink - 1 WHERE inodeNum = ?', (inodeNum,))   

      # Reduce links for parent by 1
      mode = self.conn.execute('select mode FROM inodes WHERE inodeNum = ?',inodeNum)
      if mode:
        parentHid, parentINodeNum = self.__getHidAndInode(os.path.split(path)[0])
        self.conn.execute('UPDATE inodes SET nlink = nlink -1 WHERE inodeNum = ?', (parentINodeNum,))






if __name__ == '__main__':
   main()
