package cloudcmd.common.adapters

import cloudcmd.common._
import util.{JsonUtil, CryptoUtil, FileWalker}
import java.io._
import java.net.URI
import collection.mutable
import org.apache.log4j.Logger

//     "file:///tmp/storage?tier=1&tags=image,movie,vacation"

class DirectFileAdapter extends Adapter {

  private val log: Logger = Logger.getLogger(classOf[DirectFileAdapter])

  val MIN_FREE_STORAGE_SIZE: Int = 1024 * 1024
  val LARGE_FILE_CUTOFF: Int = 128 * 1024 * 1024

  private var _rootPath: String = null
  private var _dbDir: String = null
  private var _dataDir: String = null

  override def init(configDir: String, tier: Int, adapterType: String, tags: Set[String], config: URI) {
    super.init(configDir, tier, adapterType, tags, config)
    _rootPath = URI.getPath
    _dbDir = _rootPath + File.separator + "db"
    ConfigDir = _dbDir
    _dataDir = _rootPath + File.separator + "data"
    val rootPathDir = new File(_rootPath)
    rootPathDir.mkdirs
    _isOnline = rootPathDir.exists
    if (IsOnLine()) bootstrap(_dataDir)
  }

  def shutdown {}

  override def IsFull(): Boolean = new File(_rootPath).getUsableSpace < MIN_FREE_STORAGE_SIZE

  def refreshCache {}

  def containsAll(ctxs: Set[BlockContext]): Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap( ctx => Map(ctx -> new File(getDataFileFromHash(ctx.hash)).exists()))
  }

  def removeAll(ctxs: Set[BlockContext]): Map[BlockContext, Boolean] = {
    Map() ++ ctxs.par.flatMap{ ctx =>
      val file = new File(getDataFileFromHash(ctx.hash))
      if (file.exists) FileUtil.delete(file)
      Map(ctx -> true)
    }
  }

  override
  def ensure(ctx: BlockContext, blockLevelCheck: Boolean) : Boolean = {
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

  def store(ctx: BlockContext, is: InputStream) = {
    val writeHash = FileUtil.writeFileAndComputeHash(is, new File(getDataFileFromHash(ctx.hash)))
    val success = (writeHash == getHashFromDataFile(ctx.hash))
    if (!success) {
      FileUtil.delete(new File(getDataFileFromHash(ctx.hash)))
      throw new RuntimeException("failed to store data: expected %s got %s".format(ctx.hash, writeHash))
    }
  }

  def load(ctx: BlockContext): (InputStream, Int) = {
    val file = new File(getDataFileFromHash(ctx.hash))
    if (!file.exists) throw new DataNotFoundException(ctx)
    (new FileInputStream(file), file.length.toInt)
  }

  def describe: Set[BlockContext] = {
    val ctxs = new mutable.HashSet[BlockContext] with mutable.SynchronizedSet[BlockContext]

    FileWalker.enumerateFolders(_dataDir, new FileWalker.FileHandler {
      def skipDir(file: File): Boolean = false
      def process(file: File) = {
        val hash = file.getName
        if (hash.endsWith(".meta")) {
          val fis = new FileInputStream(file)
          try {
            val fmd = FileMetaData.create(hash, JsonUtil.loadJson(fis))
            ctxs.add(fmd.createBlockContext)

            val thumbHash = fmd.getThumbHash
            if (thumbHash != null) {
              ctxs.add(fmd.createBlockContext(thumbHash))
            }

            val blockHashes = fmd.getBlockHashes
            (0 until blockHashes.length()).foreach{i =>
              val blockHash = blockHashes.getString(i)
              if (new File(getDataFileFromHash(blockHash)).exists) {
                ctxs.add(fmd.createBlockContext(blockHash))
              }
            }
          } finally {
            FileUtil.SafeClose(fis)
          }
        }
      }
    })

    ctxs.toSet
  }

  def describeHashes: Set[String] = {
    val hashes = new mutable.HashSet[String] with mutable.SynchronizedSet[String]
    FileWalker.enumerateFolders(_dataDir, new FileWalker.FileHandler {
      def skipDir(file: File): Boolean = false
      def process(file: File) = hashes.add(file.getName)
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