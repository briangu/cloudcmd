package cloudcmd.cld.commands

import cloudcmd.common.util.FileTypeUtil
import cloudcmd.common.{IndexedContentAddressableStorage, FileUtil}
import cloudcmd.common.util.FileWalker
import jpbetz.cli._
import java.io.File
import org.json.JSONObject
import cloudcmd.common.engine.{DefaultFileProcessor, FileProcessor}
import cloudcmd.cld.{CloudServices, AdapterUtil}

@SubCommand(name = "add", description = "add files")
class Add extends Command {

  @Arg(name = "path", optional = false) var _path: String = null
  @Arg(name = "tags", optional = true, isVararg = true) var _tags: java.util.List[String] = null
  @Opt(opt = "p", longOpt = "properties", description = "file meta properties JSON file", required = false) private var _inputFilePath: String = null
//  @Opt(opt = "t", longOpt = "threads", description = "number of add threads to use", required = false) private var _threadCount: Number = 1

  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false) var _minTier: Number = 0
  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false) var _maxTier: Number = Integer.MAX_VALUE
  @Opt(opt = "u", longOpt = "uri", description = "adapter URI", required = false) var _uri: String = null

  def exec(p1: CommandContext) {
    AdapterUtil.exec(p1, _uri, _minTier, _maxTier, doCommand)
  }

  def doCommand(adapter: IndexedContentAddressableStorage) {
    import scala.collection.JavaConversions._

    val path = if (_path == null) FileUtil.getCurrentWorkingDirectory else _path
    val properties = if (_inputFilePath != null) { FileUtil.readJson(_inputFilePath) } else { null }
    val tags = _tags.toSet

    addFiles(new DefaultFileProcessor(adapter), FileTypeUtil.instance, path, properties, tags)

    CloudServices.onMessage("Flushing metadata...")
    adapter.flushIndex()
  }

  def addFiles(fileProcessor: FileProcessor, fileTypeUtil: FileTypeUtil, path: String, properties: JSONObject, tags: Set[String]) {
    FileWalker.enumerateFolders(_path, new FileWalker.FileHandler {
      def skipDir(file: File): Boolean = {
        val skip = fileTypeUtil.skipDir(file.getName)
        if (skip) {
          System.err.println(String.format("Skipping dir: " + file.getAbsolutePath))
        }
        skip
      }

      def process(file: File) {
        if (!file.exists()) return // catch soft links
        val fileName: String = file.getName
        val extIndex: Int = fileName.lastIndexOf(".")
        val ext: String = if ((extIndex > 0)) fileName.substring(extIndex + 1) else null
        if (!fileTypeUtil.skipExt(ext)) {
          val startTime = System.currentTimeMillis
          try {
            System.err.print("adding: %s".format(file.getAbsolutePath))
            fileProcessor.add(file, file.getName, tags, properties)
          } catch {
            case e: Exception => {
              System.err.println("failed to index file: " + file.getAbsolutePath)
              System.err.println(e.printStackTrace())
            }
          } finally {
            System.err.println("\rtook %6d ms to add %s".format((System.currentTimeMillis - startTime), file.getName))
          }
        }
      }
    })
  }
}