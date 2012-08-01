package cloudcmd.common.adapters

import cloudcmd.common._
import util.FileWalker
import org.jboss.netty.buffer.ChannelBuffer
import java.io._
import java.net.URI
import java.util.UUID
import collection.mutable

//     "file:///tmp/storage?tier=1&tags=image,movie,vacation"

class DirectFileAdapter extends Adapter with InlineStorable {

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
    if (IsOnLine()) bootstrap(_dataDir, _dbDir)
  }

  private def bootstrap(dataPath: String, dbPath: String) {
    val file: File = new File(_dataDir)
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
    if (file.exists) file.delete
    true
  }

  def verify(hash: String): Boolean = {
    val file: File = new File(getDataFileFromHash(hash))
    if (file.exists) {
      val idx: Int = hash.lastIndexOf(".")
      val testHash = if (idx >= 0) {
        hash.substring(0, idx)
      } else {
        hash
      }
      CryptoUtil.computeHashAsString(file) == testHash
    } else {
      false
    }
  }

  private def getPathFromHash(hash: String): String = _dataDir + File.separator + hash.substring(0, 2)

  private def getDataFileFromHash(hash: String): String = getPathFromHash(hash) + File.separator + hash

  private def getHashFromDataFile(hash: String): String = {
    val idx: Int = hash.lastIndexOf(".")
    if ((idx >= 0)) hash.substring(0, idx) else hash
  }

  def store(is: InputStream, hash: String) {
    val writeHash: String = FileUtil.writeFileAndComputeHash(is, new File(getDataFileFromHash(hash)))
    if (!(writeHash == getHashFromDataFile(hash))) {
      throw new RuntimeException(String.format("failed to store data: expected %s got %s", hash, writeHash))
    }
  }

  def store(is: InputStream): String = {
    if (is.available > LARGE_FILE_CUTOFF) {
      storeLargeFile(is)
    } else {
      storeSmallFile(is)
    }
  }

  private def storeSmallFile(is: InputStream): String = {
    var baos: ByteArrayOutputStream = null
    var bais: ByteArrayInputStream = null
    var hash: String = null
    try {
      baos = new ByteArrayOutputStream
      hash = CryptoUtil.digestToString(CryptoUtil.writeAndComputeHash(is, baos))
      bais = new ByteArrayInputStream(baos.toByteArray)
      FileUtil.writeFile(bais, getDataFileFromHash(hash))
    }
    finally {
      FileUtil.SafeClose(bais)
      FileUtil.SafeClose(baos)
    }
    hash
  }

  private def storeLargeFile(is: InputStream): String = {
    val tmpFile: File = new File(_dataDir + File.separator + UUID.randomUUID.toString + ".tmp")
    tmpFile.createNewFile
    val hash = FileUtil.writeFileAndComputeHash(is, tmpFile)
    val newFile = new File(getDataFileFromHash(hash))
    if (newFile.exists && newFile.length == tmpFile.length) {
      tmpFile.delete
    }
    else {
      val success = tmpFile.renameTo(newFile)
      if (!success) {
        tmpFile.delete
        throw new IOException("failed to move file: " + tmpFile.getAbsolutePath)
      }
    }
    hash
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