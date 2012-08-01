package cloudcmd.cld.commands

import cloudcmd.cld.IndexStorageService
import cloudcmd.common.util.{FileTypeUtil, FileWalker}
import cloudcmd.common.FileUtil
import cloudcmd.common.util.FileWalker
import jpbetz.cli.Arg
import jpbetz.cli.Command
import jpbetz.cli.CommandContext
import jpbetz.cli.SubCommand
import java.io.File
import collection.mutable

@SubCommand(name = "index", description = "Index files")
class Index extends Command {

  @Arg(name = "path", optional = false) var _path: String = null
  @Arg(name = "tags", optional = true, isVararg = true) var _tags: java.util.List[String] = null

  def exec(commandLine: CommandContext) {
    if (_path == null) {
      _path = FileUtil.getCurrentWorkingDirectory
    }
    val fileSet = new mutable.HashSet[File] with mutable.SynchronizedSet[File]
    val fileTypeUtil: FileTypeUtil = FileTypeUtil.instance
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
          fileSet.add(file)
        }
      }
    })
    import scala.collection.JavaConversions._
    IndexStorageService.instance.batchAdd(fileSet.toSet, _tags.toSet)
  }
}