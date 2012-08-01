package cloudcmd.common.adapters

import cloudcmd.common._
import util.{CryptoUtil, FileWalker}
import org.jboss.netty.buffer.ChannelBuffer
import java.io._
import java.net.URI
import collection.mutable
import org.apache.commons.io.FileUtils

//     "file:///tmp/storage?tier=1&tags=image,movie,vacation"

class DirectFileAdapter extends Adapter {

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

  private def bootstrap(dataPath: String) {
    val file: File = new File(dataPath)
    if (!file.exists) {
      initSubDirs(dataPath)
    }
  }

  private def initSubDirs(rootPath: String) {
    (0 until 0x100).par.foreach(i => new File(rootPath + File.separator + "%02x".format(i)).mkdirs)
  }

  override def IsFull(): Boolean = new File(_rootPath).getUsableSpace < MIN_FREE_STORAGE_SIZE

  def refreshCache {}

  def contains(hash: String): Boolean = new File(getDataFileFromHash(hash)).exists()

  def shutdown {}

  def remove(hash: String): Boolean = {
    val file = new File(getDataFileFromHash(hash))
    if (file.exists) FileUtil.delete(file)
    true
  }

  def verify(hash: String): Boolean = {
    val file: File = new File(getDataFileFromHash(hash))
    if (file.exists) {
      val idx = hash.lastIndexOf(".")
      val testHash = if (idx >= 0) hash.substring(0, idx) else hash
      CryptoUtil.computeHashAsString(file) == testHash
    } else {
      false
    }
  }

  private def getPathFromHash(hash: String): String = _dataDir + File.separator + hash.substring(0, 2)

  private def getDataFileFromHash(hash: String): String = getPathFromHash(hash) + File.separator + hash

  private def getHashFromDataFile(hash: String): String = {
    val idx = hash.lastIndexOf(".")
    if (idx >= 0) hash.substring(0, idx) else hash
  }

  def store(is: InputStream, hash: String) {
    val writeHash = FileUtil.writeFileAndComputeHash(is, new File(getDataFileFromHash(hash)))
    if (!(writeHash == getHashFromDataFile(hash))) {
      FileUtil.delete(new File(getDataFileFromHash(hash)))
      throw new RuntimeException(String.format("failed to store data: expected %s got %s", hash, writeHash))
    }
  }

  def load(hash: String): InputStream = {
    val file = new File(getDataFileFromHash(hash))
    if (!file.exists) throw new DataNotFoundException(hash)
    new FileInputStream(file)
  }

  def loadChannel(hash: String): ChannelBuffer = {
    val file = new File(getDataFileFromHash(hash))
    if (!file.exists) throw new DataNotFoundException(hash)
    new FileChannelBuffer(file)
  }

  def describe: Set[String] = {
    val hashes = new mutable.HashSet[String] with mutable.SynchronizedSet[String]
    FileWalker.enumerateFolders(_dataDir, new FileWalker.FileHandler {
      def skipDir(file: File): Boolean = false
      def process(file: File) = hashes.add(file.getName)
    })
    hashes.toSet
  }
}