package cloudcmd.cld.commands

import cloudcmd.common.util.FileTypeUtil
import cloudcmd.common.FileUtil
import cloudcmd.common.util.FileWalker
import jpbetz.cli._
import java.io.File
import collection.mutable
import cloudcmd.cld.CloudServices

@SubCommand(name = "add", description = "add files")
class Add extends Command {

  @Arg(name = "path", optional = false) var _path: String = null
  @Arg(name = "tags", optional = true, isVararg = true) var _tags: java.util.List[String] = null
  @Opt(opt = "p", longOpt = "properties", description = "file meta properties JSON file", required = false) private var _inputFilePath: String = null

  def exec(commandLine: CommandContext) {
    if (_path == null) _path = FileUtil.getCurrentWorkingDirectory

    val properties = if (_inputFilePath != null) { FileUtil.readJson(_inputFilePath) } else { null }

    val fileTypeUtil: FileTypeUtil = FileTypeUtil.instance

    import scala.collection.JavaConversions._
    val tagList = _tags.toList

    FileWalker.enumerateFolders(_path, new FileWalker.FileHandler {
      def skipDir(file: File): Boolean = {
        val skip: Boolean = fileTypeUtil.skipDir(file.getName)
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
          CloudServices.FileProcessor.add(file, file.getName, tagList, properties)
        }
      }
    })
  }
}