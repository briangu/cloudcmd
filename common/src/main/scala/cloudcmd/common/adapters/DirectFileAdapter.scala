package cloudcmd.common.adapters

import cloudcmd.common._
import util.{CryptoUtil, FileWalker}
import java.io._
import java.net.URI
import collection.mutable

//     "file:///tmp/storage?tier=1&tags=image,movie,vacation"

class DirectFileAdapter extends DirectAdapter {

  val MIN_FREE_STORAGE_SIZE: Int = 1024 * 1024 * 128 // ensure 128MB free

  private var _dataDir: String = null

  override def init(configDir: String, tier: Int, adapterType: String, tags: Set[String], config: URI) {
    super.init(config.getPath, tier, adapterType, tags, config)
    _dataDir = _configDir + File.separator + "data"
    val rootPathDir = new File(_configDir)
    rootPathDir.mkdirs
    _isOnline = rootPathDir.exists
    if (IsOnLine) {
      bootstrap(_dataDir)
    }
  }

  def shutdown() {}

  override def IsFull: Boolean = {
    new File(_dataDir).getUsableSpace < MIN_FREE_STORAGE_SIZE
  }

  def containsAll(ctxs: Set[BlockContext]): Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap( ctx => Map(ctx -> new File(getDataFileFromHash(ctx.hash)).exists()))
  }

  // TODO: use FileLock: this operation is not thread safe.
  def removeAll(ctxs: Set[BlockContext]): Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap{ ctx =>
      val file = new File(getDataFileFromHash(ctx.hash))
      if (file.exists) FileUtil.delete(file)
      Map(ctx -> true)
    }
  }

  // TODO: use FileLock: this operation is not thread safe.
  override def ensure(ctx: BlockContext, blockLevelCheck: Boolean) : Boolean = {
    val file = new File(getDataFileFromHash(ctx.hash))
    val valid = if (blockLevelCheck) {
      if (file.exists) {
        val idx = ctx.hash.lastIndexOf(".")
        val testHash = if (idx >= 0) ctx.hash.substring(0, idx) else ctx.hash
        CryptoUtil.computeHashAsString(file) == testHash
      } else {
        false
      }
    } else {
      file.exists()
    }
    valid
  }

  def ensureAll(ctxs: Set[BlockContext], blockLevelCheck: Boolean): Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap{ ctx => Map(ctx -> ensure(ctx, blockLevelCheck)) }
  }

  // TODO: use FileLock: this operation is not thread safe.
  //       If two stores of the same context co-occur, they will collide causing a fail (probably of both)
  def store(ctx: BlockContext, is: InputStream) {
    val writeHash = FileUtil.writeFileAndComputeHash(is, new File(getDataFileFromHash(ctx.hash)))
    val success = (writeHash == getHashFromDataFile(ctx.hash))
    if (!success) {
      FileUtil.delete(new File(getDataFileFromHash(ctx.hash)))
      throw new RuntimeException("failed to store data: expected %s got %s".format(ctx.hash, writeHash))
    }
  }

  // TODO: use FileLock: this operation is not thread safe.
  def load(ctx: BlockContext): (InputStream, Int) = {
    val file = new File(getDataFileFromHash(ctx.hash))
    if (!file.exists) throw new DataNotFoundException(ctx)
    (RandomAccessFileInputStream.create(file), file.length.toInt)
  }

  def describe(): Set[String] = {
    val hashes = new mutable.HashSet[String] with mutable.SynchronizedSet[String]
    FileWalker.enumerateFolders(_dataDir, new FileWalker.FileHandler {
      def skipDir(file: File): Boolean = false
      def process(file: File) {
        hashes.add(file.getName)
      }
    })
    hashes.toSet
  }

  private def bootstrap(dataPath: String) {
    val file: File = new File(dataPath)
    if (!file.exists) {
      initSubDirs(dataPath)
    }
  }

  private def initSubDirs(rootPath: String) {
    (0 until 0x100).par.foreach(i => new File(rootPath + File.separator + "%02x".format(i)).mkdirs)
  }

  private def getPathFromHash(hash: String): String = _dataDir + File.separator + hash.substring(0, 2)

  private def getDataFileFromHash(hash: String): String = getPathFromHash(hash) + File.separator + hash

  private def getHashFromDataFile(hash: String): String = {
    val idx = hash.lastIndexOf(".")
    if (idx >= 0) hash.substring(0, idx) else hash
  }
}