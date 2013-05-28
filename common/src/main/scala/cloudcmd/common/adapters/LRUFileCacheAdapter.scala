package cloudcmd.common.adapters

import cloudcmd.common.BlockContext
import java.io.{FileInputStream, File, InputStream}
import java.net.URI
import org.apache.log4j.Logger
import cloudcmd.common.util.{StreamUtil, FileWalker}
import cloudcmd.common.util.FileWalker.FileHandler
import collection.mutable
import java.util.Date

class LRUFileCacheAdapter(underlying: DirectAdapter) extends DirectAdapter {

  private val log = Logger.getLogger(classOf[IndexFilterAdapter])

  protected var _rootPath: String = null
  protected var _cacheDir: String = null
  protected var _cacheDirFile: File = null

  protected var _maxCacheSize = (8L * 1024L * 1024L * 1024L) // 8GB TODO: load from configURI
  protected var _cacheSize: Long = 0
  protected var _cacheMap = new mutable.HashMap[String, FileInfo] with mutable.SynchronizedMap[String, FileInfo]

  val lock : AnyRef = new Object()

  override def init(configDir: String, tier: Int, adapterType: String, acceptsTags: Set[String], configURI: URI) {
    underlying.init(configDir, tier, adapterType, acceptsTags, configURI)

    _rootPath = configURI.getPath
    if (_rootPath.length == 0) _rootPath = configDir
    val rootPathDir: File = new File(_rootPath)
    rootPathDir.mkdirs
    _isOnline = rootPathDir.exists
    if (_isOnline) {
      _cacheDir = _rootPath + File.separator + "cache"
      _configDir = _cacheDir
      _cacheDirFile = new File(_cacheDir)
      _cacheDirFile.mkdirs()

      bootstrap(_cacheDir)
    }
  }

  def shutdown() {
    underlying.shutdown()
  }

  /*
    Enumerate all files in the cache and sort by file age.
   */
  protected def bootstrap(cacheDir: String) {
    _cacheSize = loadCache(cacheDir, _cacheMap)
    pruneFiles()
  }

  case class FileInfo(name: String, size: Long, var date: Long)

  def loadCache(cacheDir: String, fileMap: mutable.HashMap[String, FileInfo]): Long = {
    var cacheSize = 0L
    FileWalker.enumerateFolders(cacheDir, new FileHandler {
      def skipDir(file: File): Boolean = false
      def process(file: File) {
        val fileInfo = FileInfo(file.getName, file.length(), file.lastModified())
        fileMap.put(fileInfo.name, fileInfo)
        cacheSize = cacheSize + file.length()
      }
    })
    cacheSize
  }

  def pruneFiles() {
    lock.synchronized {
      val files = _cacheMap.values.toList.sortBy(_.date).toList
      var idx = files.size - 1
      while (_cacheSize > (_maxCacheSize / 2) && idx >= 0) {
        val file = files(idx)
        _cacheMap.remove(file.name)
        _cacheSize -= file.size
        new File(_cacheDir + File.separator + file.name).delete()
        idx -= 1
      }
      _cacheSize
    }
  }

  def fileFromParts(cacheDir: String, name: String): File = {
    new File(cacheDir + File.separator + name)
  }

  /** *
    * Gets if the CAS contains the specified blocks.
    * @param ctxs
    * @return
    */
  def containsAll(ctxs: Set[BlockContext]): Map[BlockContext, Boolean] = underlying.containsAll(ctxs)

  /** *
    * Removes the specified blocks.
    * @param ctxs
    * @return
    */
  def removeAll(ctxs: Set[BlockContext]): Map[BlockContext, Boolean] = underlying.removeAll(ctxs)

  /** *
    * Ensure block level consistency with respect to the CAS implementation
    * @param ctxs
    * @param blockLevelCheck
    * @return
    */
  def ensureAll(ctxs: Set[BlockContext], blockLevelCheck: Boolean): Map[BlockContext, Boolean] = underlying.ensureAll(ctxs)

  /** *
    * Store the specified block in accordance with the CAS implementation.
    * @param ctx
    * @param is
    * @return
    */
  def store(ctx: BlockContext, is: InputStream) {
    underlying.store(ctx, is)
  }

  /** *
    * Load the specified block from the CAS.
    * @param ctx
    * @return
    */
  def load(ctx: BlockContext): (InputStream, Int) = {
    val file = _cacheMap.get(ctx.getId) match {
      case Some(fileInfo) => {
        fileInfo.date = new Date().getTime
        fileFromParts(_cacheDir, fileInfo.name)
      }
      case None => {
        // spool file to cache
        // verify hash
        // add reference to top of file list
        // add reference to map
        val (is, size) = underlying.load(ctx)
        if ((_cacheSize + size) > _maxCacheSize) {
          pruneFiles() // TODO: do async
        }
        val (hash, tmpFile) = StreamUtil.spoolStream(is, _cacheDirFile)
        val realFile = fileFromParts(_cacheDir, ctx.getId)
        val success = tmpFile.renameTo(realFile)
        if (!success) {
          log.warn("failed to rename file: " + realFile.getAbsolutePath)
        }
        val fileInfo = FileInfo(ctx.getId, realFile.length(), realFile.lastModified())
        _cacheMap.put(fileInfo.name, fileInfo)
        tmpFile
      }
    }

    if (!file.exists) throw new DataNotFoundException(ctx)
    (new FileInputStream(file), file.length.toInt)
  }

  /** *
    * List all hashes stored in the CAS without regard to block context.  There may be hashes stored in the CAS which are
    * not returned in describe(), so this method can help identify unreferenced blocks.
    * @return
    */
  def describe(ownerId: Option[String] = None): Set[String] = {
    underlying.describe()
  }
}
