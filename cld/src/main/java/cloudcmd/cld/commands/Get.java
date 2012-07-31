package cloudcmd.cld.commands;


import cloudcmd.cld.CloudEngineService;
import cloudcmd.cld.IndexStorageService;
import cloudcmd.common.FileUtil;
import cloudcmd.common.JsonUtil;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import jpbetz.cli.Opt;
import jpbetz.cli.SubCommand;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

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
  String _prefix = null;

  @Opt(opt = "y", longOpt = "dryrun", description = "perform a dryrun and just show what would be fetched.", required = false)
  boolean _dryrun = false;

  @Opt(opt = "i", longOpt = "input", description = "input file", required = false)
  String _inputFilePath = null;

  @Opt(opt = "n", longOpt = "minTier", description = "min tier to verify to", required = false)
  Number _minTier = 0;

  @Opt(opt = "m", longOpt = "maxTier", description = "max tier to verify to", required = false)
  Number _maxTier = Integer.MAX_VALUE;

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    JSONArray selections;

    if (_pullAll)
    {
      selections = IndexStorageService.instance().find(new JSONObject());
    }
    else
    {
      InputStream is = null;
      try
      {
        is = (_inputFilePath != null) ? new FileInputStream(new File(_inputFilePath)) : System.in;
        selections = JsonUtil.loadJsonArray(is);
      }
      finally
      {
        if (is != System.in) is.close();
      }
    }

    // Apply the path modification pipeline.  Intentionally not combining these for now to keep flexible.
    if (_removePaths) removePaths(selections);
    if (_prefix != null) prefixPaths(_prefix, selections);

    if (_outdir == null) _outdir = FileUtil.getCurrentWorkingDirectory();
    prefixPaths(_outdir, selections);

    if (!_dryrun)
    {
      CloudEngineService.instance().filterAdapters(_minTier.intValue(), _maxTier.intValue());
      IndexStorageService.instance().fetch(selections);
    }
    else
    {
      System.out.print(selections.toString());
    }
  }

  private void removePaths(JSONArray selections) throws JSONException {
    for (int i = 0; i < selections.length(); i++)
    {
      JSONObject data = selections.getJSONObject(i).getJSONObject("data");
      String path = data.getString("path");
      data.put("path", new File(path).getName());
    }
  }

  private void prefixPaths(String prefix, JSONArray selections) throws JSONException
  {
    for (int i = 0; i < selections.length(); i++)
    {
      JSONObject data = selections.getJSONObject(i).getJSONObject("data");
      String path = data.getString("path");
      path = prefix + (path.startsWith(File.separator) ? path : File.separator + path);
      data.put("path", path);
    }
  }
}
