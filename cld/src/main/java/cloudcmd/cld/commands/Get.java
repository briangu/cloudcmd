package cloudcmd.cld.commands;


import cloudcmd.common.FileUtil;
import cloudcmd.common.JsonUtil;
import cloudcmd.common.engine.CloudEngineService;
import jpbetz.cli.*;
import org.json.JSONArray;

@SubCommand(name = "get", description = "Fetch files from the cloud and store locally.")
public class Get implements Command
{
  @Opt(opt = "a", longOpt = "all", description = "get all files", required = false)
  boolean _pullAll = false;

  @Opt(opt = "f", longOpt = "filenames", description = "only use the filenames from archived files when storing locally", required = false)
  boolean _removePaths = false;

  @Opt(opt = "d", longOpt = "destdir", description = "destination directory where the files will be stored", required = false)
  String _outdir = null;

  @Opt(opt = "p", longOpt = "prefix", description = "path to prefix the archived file paths with when they are stored locally.", required = false)
  boolean _prefix = false;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    if (_outdir == null) _outdir = FileUtil.getCurrentWorkingDirectory();
    JSONArray selections = JsonUtil.loadJsonArray(System.in);
    CloudEngineService.instance().fetch(_outdir, selections);
  }
}
