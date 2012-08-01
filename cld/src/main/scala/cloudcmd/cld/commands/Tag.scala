package cloudcmd.cld.commands

import cloudcmd.cld.IndexStorageService
import cloudcmd.common.JsonUtil
import cloudcmd.common.MetaUtil
import jpbetz.cli._
import java.io.File
import java.io.FileInputStream

@SubCommand(name = "tag", description = "Add or remove tags to/from archived files.") class Tag extends Command {

  @Arg(name = "tags", optional = false, isVararg = true) var _tags: List[String] = null
  @Opt(opt = "r", longOpt = "remove", description = "remove tags", required = false) var _remove: Boolean = false
  @Opt(opt = "i", longOpt = "input", description = "input file", required = false) private var _inputFilePath: String = null

  def exec(commandLine: CommandContext) {
    import scala.collection.JavaConversions._

    val is = if ((_inputFilePath != null)) new FileInputStream(new File(_inputFilePath)) else System.in
    try {
      val selections = JsonUtil.loadJsonArray(is)

      var preparedTags = MetaUtil.prepareTags(_tags)
      if (_remove) {
        preparedTags = preparedTags.map("-" + _)
      }

      val newMeta = IndexStorageService.instance.addTags(selections, preparedTags.toSet)
      System.out.println(newMeta.toString)
    }
    finally {
      if (is ne System.in) is.close
    }
  }
}