

import sys

try:
  import fuse
except ImportError:
  sys.stderr.write("Fuse not installed")
  sys.exit(1)
  
import logging
import os
import stat
import time
import datetime
import sqlite3
import cStringIO
from errno import *
import hashlib
import threading

SUCCESS, FAIL = 0, -1




def main():
   """
   Starting point which initilize the dedupe class object which in turn calls FUSE
   to create file system.
   """
   server = dedupeEncFS()

   arguments = server.parse(['-o', 'use_ino,default_permissions,fsname=dedupeEncfs'] + sys.argv[1:])

   ###
   #if server.fuse_args.mount_expected() and not arguments.mountpoint:
   #  server.parse('-h')
   #
   ###

   #print "Came inside"

   if arguments.mountpoint or not server.fuse_args.mount_expected():
    msg = "Filesystem initialized. mountpt = " + arguments.mountpoint
    print msg
    print arguments
    server.main()		#calling fuse.main() indirectly
    print "Everything ok"
     

class dedupeEncFS(fuse.Fuse):

  
  def __init__(self, *args, **kw):
    """
    Constructor to initialize all the required objects, creating metadata and actual database.
    """
    try:


      fuse.fuse_python_api = (0, 2)

      fuse.Fuse.__init__(self, *args, **kw)

      self.flags = 0
      self.multithreaded = 0
      
      self.defaultBlockSize = 1024 * 128     #setting default block size
      self.dataBuffer = {}    #contains key-value pair of {path:buffer data} of all blocks
      self.dirtyPaths = {}    #contains key-value pair of {path:isFileChanged}
      self.blockDatabase = '~/.datastore.db'
      self.metaDatabase = '~/.metadata.sqlite3'
      self.logFile = '~/FS.log'
      #self.dedupeLogFile = '~/dedupeFileList.txt'
      self.link_mode = stat.S_IFLNK | 0777
      self.nodes = {}
      self.cache_key = 0
      self.defaulFolderSize = 1024 * 4    #Default folder size = 4kb when created
      self.defaultFileSize = 0            #Default file size = 0kb when created
      self.hashFunction = getattr(hashlib, "md5")

      #creating logs of activities
      self.logger = logging.getLogger('dedupeEncFS')
      self.logger.setLevel(logging.INFO)
      self.logger.addHandler(logging.StreamHandler(sys.stderr))
      
    except Exception, e:
      #ToDo: To write exception in console and in log.
      print 'Exception in __init__() method'
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
      self.__write_log("Fsinit","called")
      tstart = datetime.datetime.now()
      self.encryptionMethod = ''    #Todo
      self.blockDBPath = os.path.expanduser(self.blockDatabase)
      self.metaDBPath = os.path.expanduser(self.metaDatabase)
      self.logFilePath = os.path.expanduser(self.logFile)
      #self.dedupeLogPath = os.path.expanduser(self.dedupeLogFile)
      

      #adding a log file to log messages
      #logFileHandler = logging.StreamHandler(os.open(self.logFilePath, 'w'))
      #self.logger.addHandler(logFileHandler)

      #creating database for storing blocks and metadata
      msg = "Creating database :\n"
      msg += "Block database file: " + self.blockDBPath + "\n"
      msg += "Metadata DB file: " + self.metaDBPath + "\n"
      self.__logMessage(msg)
      f = open('/home/chetanpawar0989/log.txt','a')
      f.write('Before create block\n')
      f.close()
      try:
        self.__createBlockDatabase()
      except Exception, e:
        f = open('/home/chetanpawar0989/log.txt','a')
        f.write('After create block\n' + e.message + "\n")
        f.close()

      metaDBAlreadyPresent = False
      if os.path.exists(self.metaDBPath):
          metaDBAlreadyPresent = True

      try:
        self.__createMetaDatabase()
      except Exception, e:
        f = open('/home/chetanpawar0989/log.txt','a')
        f.write('after create meta data\n' + e.message + "\n")
        f.close()

      try:
        if not metaDBAlreadyPresent:
            self.__insertMetaDataTables()
      except Exception, e:
        f = open('/home/chetanpawar0989/log.txt','a')
        f.write('after insert meta data\n' + e.message + "\n")
        f.close()

      #creating dedupe log for storing list of files for lazy deduplication
      #__logRunningTime(os.open(self.dedupeLogPath, 'w')
      

      tend = datetime.datetime.now()      
      self.__logRunningTime('fsinit()', tstart, tend)

      self.conn.commit()
      self.__write_log("Fsinit","Ended")
    except Exception, e:
      self.__write_log("Fsinit","Exception",e)
      sys.exit(1)

      
  def access(self, path, mode):
    """
    check if user have access to particular path with given flag.
    """
    try:
      self.__write_log("Access","called")
      msg = "access() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)
                       
      hid, inodeNum = self.__getHidAndInode(path)
      if mode != os.F_OK and not self.__checkAccessInMetaData(inodeNum, mode):
        return -EACCES
      self.__write_log("Access","Ended")
      return SUCCESS
    except Exception, e:
      self.__write_log("Access","Exception")
      #ToDo: To write exception in console and in log.
      sys.exit(1)


  def chmod(self, path, mode):
    """
    Change the file access mode.
    """
    try:
      self.__write_log("Chmod ","called")
      msg = "chmod() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)
      hid, inodeNum = self.__getHidAndInode(path)

      query = 'UPDATE inodes SET mode = ? WHERE inode = ?'
      self.conn.execute(query, (mode, inodeNum,))

      self.conn.commit()
      self.__write_log("Chmod","ended")
      return SUCCESS
      
    except Exception, e:
      self.__write_log("Chmod","Exception",e)
      #ToDo: To write exception in console and in log.
      self.conn.rollback()
      sys.exit(1)


  def chown(self, path, uid, gid):
    """
    Change the file user id and group id.
    """
    try:
      self.__write_log("Chown ","called")
      msg = "chown() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)
      hid, inodeNum = self.__getHidAndInode(path)

      query = 'UPDATE inodes SET uid = ?, gid = ? WHERE inode = ?'
      self.conn.execute(query, (uid, gid, inodeNum))

      self.conn.commit()
      self.__write_log("Chown","ended")
      return SUCCESS
      
    except Exception, e:
      #ToDo: To write exception in console and in log.
      self.__write_log("Chown","Exception",e)
      self.conn.rollback()
      sys.exit(1)

  def utime(self, path, times):
    """
    - update inode access, modified time
    """
    try:
      self.__write_log("utime","called")
      inodeNum = self.__getHidAndInode(path)[1]
      atime, mtime = times
      self.conn.execute('UPDATE inodes SET atime = ?, mtime = ? WHERE inode = ?' (atime, mtime, inodeNum))
      self.conn.commit()
      self.__write_log("utime","ended")
      return 0
    except Exception, e:
      self.__write_log("utime","Exception", e)
      #ToDo: To write exception in console and in log.
      self.conn.rollback()
      sys.exit(1)


  def utimens(self, path, a_time, m_time):
    """
    - update inode access, modified time in nano seconds
    """
    try:
      self.__write_log("utimens","called")
      inodeNum = self.__getHidAndInode(path)[1]
      atime_ns = a_time.tv_sec + (a_time.tv_nsec / 1000000.0) #convert access time in nano seconds
      mtime_ns = m_time.tv_sec + (m_time.tv_nsec / 1000000.0) #convert modified time in nano seconds
      self.conn.execute('UPDATE inodes SET atime = ?, mtime = ? WHERE inode = ?', (atime_ns, mtime_ns, inodeNum))
      self.__write_log("utimens","ended")
      return 0
    except Exception, e:
      self.__write_log("utimens","Exception", e)
      #ToDo: To write exception in console and in log.
      sys.exit(1)


  def open(self, path, flags, inode=None):
    """
    Called while opening a file. inode attribute will be passed when called from create() method.
    """
    try:
      self.__write_log("Open","called")
      msg = "open() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      if inode == None:
        inodeNum = self.__getHidAndInode(path)[1]
      else:
        inodeNum = inode

      if self.__checkAccessInMetaData(inodeNum, flags):
        return -EACCES
      self.__write_log("Open","ended")
      return SUCCESS
    except Exception, e:
      self.__write_log("Open","Exception",e)
      #ToDo: To write exception in console and in log.
      sys.exit(1)



  def create(self, path, flags, mode):
    """
    Called while creating new file.
    """
    try:
      self.__write_log("Create","called")
      msg = "create() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      if not os.access(path, os.F_OK):
        inodeNum, parentINodeNum = self.__insertNewNode(path, mode, self.defaultFileSize)
      
      res = self.open(path, flags, inodeNum)

      self.__write_log("Create","ended")
      if res == SUCCESS:
        self.conn.commit()
        return SUCCESS      
      else:
        msg = "Error in create() method. Rolling back the changes."
        self.conn.rollback()
        return FAIL
      
    except Exception, e:
      self.__write_log("Create","Exception",e)
      #ToDo: To write exception in console and in log.
      self.conn.rollback()
      sys.exit(1)


  def fsdestroy(self):
    """
    Called when filesystem is unmounted.
    """
    try:
      self.__write_log("Fsdestroy","called")
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

      f = open('/home/chetanpawar0989/log.txt','a')
      f.write('fsdestroy successful,\n')
      f.close()
      self.__write_log("Fsdestroy","ended")
      return SUCCESS
      
    except Exception, e:

      self.__write_log("Fsdestroy","Exception",e)
      #ToDo: To write exception in console and in log.
      sys.exit(1)


  
  def getattr(self, path):
    """
    Called when stat, fstat, lstat done
    """
    try:
      self.__write_log("getattr","called")

      msg = "getattr() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)
      print msg

      inodeNum = self.__getHidAndInode(path)[1]
      query = 'SELECT inodeNum, nlink, mode, uid, gid, dev, size, atime, mtime, ctime FROM inodes WHERE inodeNum = ?'
      res = self.conn.execute(query, (inodeNum,)).fetchone();
      output = fuse.Stat(st_ino = res['inodeNum'],
                  st_mode = res['mode'],
                  st_uid = res['uid'],
                  st_gid = res['gid'],
                  st_dev = res['dev'],
                  st_nlink = res['nlink'],
                  st_size = res['size'],
                  st_atime = res['atime'],
                  st_mtime = res['mtime'],
                  st_ctime = res['ctime'])
      self.__write_log("getattr","ended")
      return output
    except Exception, e:
      self.__write_log("getattr","Exception",e)
      #ToDo: To write exception in console and in log.
      return -ENOENT


  def link(self, old_file_path, new_file_path):
    """
    Creating hard links.
    - First create the new_file in new_file_parent, if it does not exists by calling __getfnameidFromName method
    - We need not create new inode, Insert record in hierarchy table with old_file inode.
    - Update inode table to increase the link count.
    """
    try:
      self.__write_log("Link","called")
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
      self.__write_log("Link","ended")
      return SUCCESS
      
    except Exception, e:
      #ToDo: To write exception in console and in log.
      self.__write_log("Link","Exception",e)
      self.conn.rollback()
      return FAIL
    

  def symlink(self, target, new_link):
    """
    To create symbolic/soft links.    
    """
    try:
      self.__write_log("Symlink","called")
      msg = "symlink() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      #creating new link
      new_link_Inode, parent_folder_inode = self.__insertNewNode(new_link, stat.S_IFLNK | 0777, len(target), isSoftLink=True)

      #keep track by inserting in softlinks table.
      query = 'INSERT INTO softlinks (inodeNum, target) VALUES (?, ?)'
      self.conn.execute(query, (new_link_Inode, sqlite3.Binary(target)))
      
      self.conn.commit()
      self.__write_log("Symlink","ended")
      return SUCCESS
      
    except Exception, e:
      #ToDo: To write exception in console and in log.
      self.__write_log("Symlink","Exception",e)
      self.conn.rollback()
      return FAIL



  def mkdir(self, path, mode):
    """
    Called while creating a new directory.
    - Create a folder by inserting in inode, fileAndFolderNames and Hirarchy by calling __insertNewNode() method
    - Increase the nlink count of parent in which this folder is created.
    """
    try:
      self.__write_log("mkdir","called")
      msg = "mkdir() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      #creating new folder.
      new_Folder_Inode, parent_Folder_Inode = self.__insertNewNode(path, mode | stat.S_IFDIR, self.defaultFolderSize)

      #Increasing the link count of parent folder.
      query = 'UPDATE inodes SET nlink = nlink + 1 WHERE inodeNum = ?'
      self.conn.execute(query, (parent_Folder_Inode,))
      
      self.conn.commit()
      self.__write_log("mkdir","ended")
      return SUCCESS
    
    except Exception, e:
      self.__write_log("Mkdir","Exception",e)
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
      self.__write_log("Mknod","called")
      msg = "mknod() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      self.__insertNewNode(path, mode, self.defaultFolderSize, dev)
      
      self.conn.commit()
      self.__write_log("Mknod","Ended")
      return SUCCESS
    
    except Exception, e:
      self.__write_log("Mknoe","Exception",e)
      #todo: To write exception in console and in log.
      self.conn.rollback()
      return FAIL


  def read(self, path, length, offset):
    """
    Called when reading a particular file.
    - Get the total buffer from self.buffer by calling helper method __get_data_buffer()
    - read buffer from offset to length
    """
    try:
      self.__write_log("Read","called")
      msg = "read() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)
      
      buf = self.__get_data_buffer(path)
      buf.seek(offset)
      data = buf.read(length)
      self.__write_log("read","ended")
      return data
    except Exception, e:
      #ToDo: To write exception in console and in log.
      self.__write_log("Read","Exception",e)
      return -EIO   #input output error


  def readdir(self, path, offset):
    """
    Get the directory entries.
    """
    try:
      self.__write_log("readdir","called")
      msg = "readdir() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)
      print 'Readdir() called.'

      path_hid, path_inode = self.__getHidAndInode(path)     
      
      #Default directory pointers adding to direntry of fuse
      yield fuse.Direntry('.', ino = path_inode)
      yield fuse.Direntry('..')

      #get all the files and folders inside path by querying hierarchy table
      query = 'SELECT h.inodeNum, f.fname FROM hierarchy h, fileFolderNames f WHERE h.parenthid = ? AND h.fnameId = f.fnameId'
      resultList = self.conn.execute(query, (path_hid)).fetchall()

      for file in resultList:
        yield fuse.Direntry(str(file[1]), ino=file[0])
      self.__write_log("readdir","ended")

    except Exception, e:
      #ToDo: To write exception in console and in log.
      self.__write_log("readdir","exception",e)
      pass


  def readlink(self, path):
    """
    Called while reading a link.
    """
    try:
      self.__write_log("readlink","called")
      msg = "readlink() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      linkInode = self.__getHidAndInode(path)[1]

      #Get the link target from softlinks table.
      query = 'SELECT target FROM softlinks WHERE inodeNum = ?'
      result = self.conn.execute(query, (linkInode,)).fetchone()

      targetpath = result[0]
      self.__write_log("readlink","ended")
      return str(targetpath)    
    except Exception, e:
      self.__write_log("readlink","exception",e)
      #ToDo: To write exception in console and in log.
      return -ENOENT   #No path exists


  def unlink(self, path):
    """
    Called while removing the link.
    """
    try:
      self.__write_log("unlink","called")
      self.__removeFileOrFolder(path)
      self.conn.commit()
      self.__write_log("unlink","ended")
      return SUCCESS

    except Exception, e:
      self.__write_log("unlink","exception",e)
      self.conn.rollback()
      #ToDo: To write exception in console and in log.
      return FAIL
    

  def rename(self, oldPath, toPath):
    """
    Change the name and location of file.
    """
    try:
      self.__write_log("rename","called")
      msg = "rename() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)
      
      try:
        #if toPath directory is different that fromPath directory then check if it not empty before deleting
        self.__removeFileOrFolder(toPath, emptyCheck=True)
      except OSError, e:
        self.__write_log("rename","exception",e)
        if e.errno != ENOENT: raise

      #creating new hard link --> equivalent to creating the new file
      self.link(oldPath, toPath)

      #deleting old file
      self.unlink(oldPath)

      self.conn.commit()
      self.__write_log("rename","ended")
      return SUCCESS
    except Exception, e:
      self.__write_log("rename","exception",e)
      #ToDo: To write exception in console and in log.
      self.conn.rollback()
      return FAIL


  def rmdir(self, path):
    """
    Called when directory is removed.
    """
    try:
      self.__write_log("rmdir","called")
      msg = "rmdir() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      #check if it is not empty before deleting
      self.__removeFileOrFolder(path, emptyCheck=True)

      self.conn.commit()
      self.__write_log("rmdir","ended")
      return SUCCESS
    except Exception, e:
      self.__write_log("rmdir","exception",e)
      #ToDo: To write exception in console and in log.
      return FAIL


  def statfs(self):
    """
    statistics about the filesystem. We will perform statvfs on metadata file, which holds the inodes
    and can determine the number of blocks available to user etc.
    """
    try:
      self.__write_log("statfs","called")
      msg = "statfs() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      metavfs = os.statvfs()
      #Total number of blocks
      totalBlocks = (metavfs.f_blocks * metavfs.f_frsize) / self.defaultBlockSize

      #Total number of free blocks in file system
      totalFree = (metavfs.f_bfree * metavfs.f_frsize) / self.defaultBlockSize

      #Total number of free blocks that are available
      totalFreeAvail = (metavfs.f_bavail * metavfs.f_frsize) / self.defaultBlockSize

      output = fuse.StatVfs(f_bsize = self.defaultBlockSize,
                       f_frsize = self.defaultBlockSize,
                       f_blocks = totalBlocks,
                       f_bfree = totalFree,
                       f_bavail = totalFreeAvail,
                       f_files = 0,
                       f_ffree = 0,
                       f_favail = 0,
                       f_flag = 0,
                       f_namemax = metavfs.f_namemax)
      self.__write_log("statfs","ended")
      return output
      
    except Exception, e:
      self.__write_log("statfs","exception",e)
      #ToDo: To write exception in console and in log.
      return -EIO


  def truncate(self, path, size):
    """
    Truncate or extend the given file so that it is precisely size bytes long.
    - Get last block from size/blockSize after which we have to truncate
    - delete all the blocks 
    """
    try:
      self.__write_log("truncate","called")
      msg = "truncate() called at " + str(datetime.datetime.now())
      self.__logMessage(msg)

      t = time.time()
      #get inodeNume whose block entries are to be removed from fileBlocks table.
      inodeNum = self.__getHidAndInode(path)[1]
      query = 'UPDATE inodes SET size = ?, mtime=? WHERE inodeNum = ?'
      self.conn.execute(query, (size, t, inodeNum,))
      
      self.conn.commit()
      self.__write_log("truncate","ended")
      return SUCCESS
    except Exception, e:
      self.__write_log("truncate","exception",e)
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
      self.__write_log("write","called")
      msg = "write() called at " + str(datetime.datetime.now() + " on file " + str(path))
      self.__logMessage(msg)
      length = len(data)
      
      buf = self.__get_data_buffer(path)
      buf.seek(offset)
      buf.write(data)
      self.dirtyPaths[path] = True
      self.__write_log("write","ended")
      return length
    except Exception, e:
      self.__write_log("write","exception",e)
      #ToDo: To write exception in console and in log.
      self.conn.rollback()
      return -EIO


  def release(self, path, flags):
    try:
      self.__write_log("release","called")
      buf = self.dataBuffer[path]
      # If buffer is not modified then return success.
      if path not in self.dirtyPaths or not self.dirtyPaths[path]:
        buf.close()
        return SUCCESS

      #else we need to update the new buffer in metadata as a new file.
      inodeNum = self.__getHidAndInode(path)[1]      
      size = self.__getSizeOfBuffer(buf)
      try:
        self.__store_blocks(path, inodeNum, buf, size)
        self.conn.commit()
      except Exception, e:
        self.__write_log("release","exception",e)
        self.conn.rollback()
        raise
      
      buf.close()
      if path in self.dataBuffer:
        del self.dataBuffer[path]

      if path in self.dirtyPaths:
        del self.dirtyPaths[path]
      self.__write_log("release","ended")
      return SUCCESS
    except Exception, e:
      self.__write_log("release","exception",e)
      #ToDo: To write exception in console and in log.      
      return -EIO

    
      
    
  #####################################################
  #Helper methods: private and called internally
  #
  #####################################################

  def __store_blocks(self, path, inodeNum, buf, size):
    """
    This method updates the fileBlocks table to store the buffer as single block.
    Since we are following lazy deduplication, we will check for duplicate blocks later.    
    """
    # Delete old entries in fileBlocks table for corresponding inodeNum
    self.__write_log("store_blocks","called")
    query = 'DELETE FROM fileBlocks WHERE inodeNum = ?'
    self.conn.execute(query, (inodeNum,))

    # Store new block as a single block.
    buf.seek(0, os.SEEK_SET)
    data = buf.read(size)
    #rpk = RabinKarp()
    #hashKey = rpk.getHashKey(data)
    temp = self.hashFunction()
    temp.update(data)
    hashKey = temp.hexdigest()
    hashKeyForDB = sqlite3.Binary(hashKey)

    self.blocks[hashKey] = data

    newBuf = cStringIO.StringIO()
    newBuf.write(data)
    self.dataBuffer[path] = newBuf
    self.dirtyPaths[path] = True    

    # Insert new hashKey in hashValues table with refCount = 1 (default)
    query = 'INSERT INTO hashValues(hashId, hashValue, refCount) VALUES (NULL, ?, 1)'
    self.conn.execute(query, (hashKeyForDB,))

    # Insert records in fileBlocks for referencing inode with corresponding hashKey just inserted.
    query = 'INSERT INTO fileBlocks(inodeNum, hashId, blockOrder) VALUES (?, last_insert_rowid(), 0)'
    self.conn.execute(query, (inodeNum,))    

    #update size and modified time in inodes table.
    t = time.time()
    query = 'UPDATE inodes SET size = ?, mtime = ? WHERE inodeNum = ?'
    self.conn.execute(query, (size, inodeNum))
    
    #if file is modified then insert record in logs table for lazy deduplication to pickup.
    query = 'SELECT isNewlyCreated FROM logs WHERE inodeNum = ?'
    rowCount = self.conn.execute(query, (inodeNum,)).fetchone()    
    # insert in logs only if its not already there. Create() function might have inserted it already.
    if not rowCount:
      query = 'INSERT INTO logs(inodeNum, isNewlyCreated) VALUES (?, 0)'
      self.conn.execute(query, (inodeNum,))
    self.__write_log("store_blocks","ended")

      


  def __get_data_buffer(self, path):
    self.__write_log("get_data_buffer","called")
    if path in self.dataBuffer:
      return self.dataBuffer[path]

    dataBuf = cStringIO.StringIO()
    fileInode = self.__getHidAndInode(path)[1]
    
    query = """SELECT h.hashValue from hashValues h, fileBlocks f WHERE f.inodeNum = ?
              AND f.hashId = h.hashId ORDER BY f.blockOrder ASC"""
    resultList = self.conn.execute(query, (fileInode,)).fetchall()
    for row in resultList:
      dataBuf.write(self.blocks[row[0]])
      
    self.dataBuffer[path] = dataBuf       #storing in dataBuffer for quick access later
    self.dirtyPaths[path] = False     #storing that buffer is not modified currently.
    self.__write_log("get_data_buffer","ended")
    return dataBuf
    

  def __insertNewNode(self, path, mode, size, dev=0, isSoftLink=False):
    """
    - To insert to new file/folder in metadata. Consists of three steps:
    - 1. Insert in inodes table.
    - 2. Insert in fileFolderNames table
    - 3. Link file/folder with parent by inserting in hierarchy table.
    - returns insertedInode and parent_inode
    """
    self.__write_log("insertnewnode","called")
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

    #if new file is created then insert record in logs table for lazy deduplication to pickup.
    if nlink == 1 and not isSoftLink:
      query = 'INSERT INTO logs(inodeNum, isNewlyCreated) VALUES (?, 1)'
      self.conn.execute(query, (insertedInodeNum,))
    self.__write_log("insertnewnode","ended")
    return insertedInodeNum, parent_inodeNum


  def __GetFnameIdFromName(self, fname):
    """
    - Returns fileNameId from fileFolderNames table.
    - If file/folder not present then insert new record and return the inserted fnameid.
    """
    self.__write_log("getFnameIdFromName","called")
    fname = sqlite3.Binary(fname)
    fnameId = self.conn.execute('SELECT fnameId FROM fileFolderNames WHERE fname = ?', (fname)).fetchone()
    if not fnameId:    #file/folder not present. insert new one.
      self.conn.execute('INSERT INTO fileFolderNames (fnameId, fname) VALUES (NULL, ?)', (fname))
      fnameId = self.conn.execute('SELECT last_insert_rowid()').fetchone()
    self.__write_log("getFnameIdFromName","ended")
    return int(fnameId[0])

  def __removeFileOrFolder(self, path, emptyCheck=False):
    """
    removes file/folder from hierarchy table and decrement the nlink in inodes table.
    - if emptyCheck is true then we can't allow removing directory if directory is not empty.
    - else remove from hierarchy table and decremenet nlink in inodes table.
    - if deleted item is folder then decrement the parent folder nlink as well.
    """
    self.__write_log("removeFileorFolder","called")
    hid, inodeNum = self.__getHidAndInode(path)
    if emptyCheck:
      query = """SELECT count(h.hid) FROM hierarchy h, inodes i WHERE h.parenthid = ?
                 AND h.inodeNum = i.inodeNum AND i.nlink > 0"""
      result = self.conn.execute(query, (hid)).fetchone()
      linkCount = result[0]
      if linkCount:
        raise OSError, (ENOTEMPTY, os.strerror(ENOTEMPTY), path)

    if path in self.dataBuffer:
      del self.dataBuffer[path]

    if path in self.dirtyPaths:
      del self.dirtyPaths[path]
    
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
    self.__write_log("RemoveFileorFolder","ended")
      
    
  def __checkAccessInMetaData(self, inodeNum, flags):
    """
    Check if the access stored in metadata for this user is consistent with flags
    """
    self.__write_log("CheckAccessInMetadata","called")
    #get uid and gid from fuse method
    context = fuse.FuseGetContext()
    #get values stored in database.
    query = 'SELECT mode, uid, gid FROM inodes WHERE inodeNum = ?'
    result = self.conn.execute(query, (inodeNum)).fetchone();

    o = context['uid'] == result['uid']    #check if owner
    g = context['gid'] == result['gid'] and not o   #check if same group but not owner
    w = not (o or g)    #implies neither owner nor beloging to same group
    m = result['mode']

    output = (not (flags & os.R_OK) or ((o and (m & 0400)) or (g and (m & 0040)) or (w and (m & 0004)))) \
              and (not (flags & os.W_OK) or ((o and (m & 0200)) or (g and (m & 0020)) or (w and (m & 0002)))) \
              and (not (flags & os.X_OK) or ((o and (m & 0100)) or (g and (m & 0010)) or (w and (m & 0001))))
    self.__write_log("checkAccessinMetadata","called")
    return output
               

  def __createMetaDatabase(self):
    """
    Creates sqlite3 database for storing metadata
    """
    self.__write_log("createMetaDatase","called")
    self.conn = sqlite3.connect(self.metaDBPath, isolation_level=None)
    self.conn.row_factory = sqlite3.Row   #named attributes
    self.conn.text_factory = str    #to return regular strings
    self.conn.execute('PRAGMA locking_mode = EXCLUSIVE')
    self.__write_log("createMetaDatase","ended")


  def __insertMetaDataTables(self):
    """
    - Creates basic tables for storing metadata like inodes, filenames, hashes etc.
    - insert default rows for root folder.
    """
    self.__write_log("insertMetadaTables","called")
    uid = os.getuid()   #get current user id
    gid = os.getgid()   #get current user group id
    root_mode = stat.S_IFDIR | 0755
    t = time.time()
    script = """
        --Creating the tables
        CREATE TABLE IF NOT EXISTS fileFolderNames(fnameId INTEGER PRIMARY KEY, fname BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS inodes(inodeNum INTEGER PRIMARY KEY, nlink INTEGER NOT NULL, mode INTEGER NOT NULL, uid INTEGER, gid INTEGER, dev INTEGER, size INTEGER, atime INTEGER, mtime INTEGER, ctime INTEGER);
        CREATE TABLE IF NOT EXISTS hierarchy(hid INTEGER PRIMARY KEY, parenthid INTEGER, fnameId INTEGER NOT NULL, inodeNum INTEGER, UNIQUE(parenthid, fnameId));
        CREATE TABLE IF NOT EXISTS softlinks(inodeNum INTEGER, target BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS hashValues(hashId INTEGER PRIMARY KEY, hashValue BLOB NOT NULL UNIQUE, refCount INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS fileBlocks(inodeNum INTEGER, hashId INTEGER, blockOrder INTEGER NOT NULL, PRIMARY KEY(inodeNum, hashId, blockOrder));
        CREATE TABLE IF NOT EXISTS logs(inodeNum INTEGER NOT NULL, isNewlyCreated INTEGER);

        -- Insert default rows for root folder.
        INSERT INTO fileFolderNames (fnameId, fname) VALUES(1, '');
        INSERT INTO inodes(nlink, mode, uid, gid, dev, size, atime, mtime, ctime) VALUES (2, %i, %i, %i, 0, 4096, %f, %f, %f);
        INSERT INTO hierarchy (hid, parenthid, fnameId, inodeNum) VALUES (1, NULL, 1, 1);
        
        """ % (root_mode, uid, gid, t, t, t)
    
    self.conn.executescript(script)
    self.__write_log("insertMetadaTables","ended")
    

  def __createBlockDatabase(self):
    """
    Create GDBM database for storing different blocks
    """
    self.__write_log("__createBlockDatabase","called")
    import gdbm
    self.blocks = gdbm.open(self.blockDBPath, 'cs')
    self.__write_log("__createBlockDatabase","ended")


  def __getHidAndInode(self, path):
    """
    Returns hierachy id and inode for path.
    """
    self.__write_log("__getHidAndInode","called")
    hid, inodeNum = 1, 1    #default for root folder
    if path == '/':
      return hid, inodeNum
    
    parenthid = hid
    names = path.split('/')
    names = [n for n in names if n != '']      #removing empty path at beginning
    tempNodes = self.nodes      #getting cached nodes so far
    
    for fname in names:
      if fname in tempNodes:
        tempNodes = tempNodes[fname]
        hid, inodeNum = tempNodes[self.cache_key]
      else:
        query = 'SELECT h.hid, h.inodeNum from hierarchy h, fileFolderNames f WHERE h.parenthid = ? AND h.fnameid=f.fnameid AND f.fname = ?'
        row = self.conn.execute(query, (parenthid, sqlite3.Binary(fname))).fetchone()
        if row == None:
          raise OSError     #ToDo raise custom exception showing path does not exists
        hid, inodeNum = row
        tempNodes[fname] = {self.cache_key:(hid, inodeNum)}
        tempNodes = tempNodes[fname]
      parenthid = hid
    self.__write_log("__getHidAndInode","ended")
    return hid, inodeNum
  

  def __logRunningTime(self, functionName, tstart, tend):
    """
    To log the running time of function.
    """
    self.__write_log("__logRunningTime","called")
    msg = ''
    time_diff = tend - tstart
    seconds = time_diff.total_seconds()

    msg = functionName + " called:" + str(tstart) + " to " \
            + str(tend) + " --> " + str(seconds) + " seconds."
      
    self.__logMessage(msg)
    self.__write_log("__logRunningTime","ended")

  def __logMessage(self, message):
    """
    To log debug message.
    """
    self.__write_log("__logMessage","called")
    self.logger.debug(message)
    self.__write_log("__logMessage","ended")

  def __getSizeOfBuffer(self, buf):
      """
      Get the length of the buffer.
      - Get the current position in the buffer as start position
      - Go to last position
      - Length = last position - start position
      - Go back to start position
      """
      self.__write_log("__getSizeOfBuffer","called")
      startPosition = buf.tell()     #get the current position of cursor
      buf.seek(0, os.SEEK_END)       #go to the last position in the buffer. 0th position respective to END of buffer
      length = buf.tell()            #get the length by current position of cursor
      buf.seek(startPosition, os.SEEK_SET) #Go back to starting position.
      self.__write_log("__getSizeOfBuffer","ended")
      return length

  def __write_log(self,function_name,message="",exception=""):
      f = open('/home/chetanpawar0989/log.txt','a')
      if(exception == ""):
        f.write(function_name +"   " + message + "\n")
      else:
        f.write(function_name +"   " + message + " " + exception.message + "\n")
      f.close()


if __name__ == '__main__':
   main()
