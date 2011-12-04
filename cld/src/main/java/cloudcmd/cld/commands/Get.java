package cloudcmd.cld.commands;


import cloudcmd.common.FileUtil;
import cloudcmd.common.JsonUtil;
import cloudcmd.common.engine.CloudEngineService;
import jpbetz.cli.*;
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

  @Override
  public void exec(CommandContext commandLine) throws Exception
  {
    InputStream is = (_inputFilePath != null) ? new FileInputStream(new File(_inputFilePath)) : System.in;

    try
    {
      JSONArray selections = JsonUtil.loadJsonArray(is);

      // Apply the path modification pipeline.  Intentionally not combining these for now to keep flexible.
      if (_removePaths) removePaths(selections);
      if (_prefix != null) prefixPaths(_prefix, selections);

      if (_outdir == null) _outdir = FileUtil.getCurrentWorkingDirectory();
      prefixPaths(_outdir, selections);

      if (!_dryrun)
      {
        CloudEngineService.instance().fetch(selections);
      }
      else
      {
        System.out.print(selections.toString());
      }
    }
    finally
    {
      if (is != System.in) is.close();
    }
  }

  private void removePaths(JSONArray selections) throws JSONException {
    for (int i = 0; i < selections.length(); i++)
    {
      JSONObject selection = selections.getJSONObject(i);
      String path = selection.getString("path");
      selection.put("path", new File(path).getName());
    }
  }

  private void prefixPaths(String prefix, JSONArray selections) throws JSONException {
    for (int i = 0; i < selections.length(); i++)
    {
      JSONObject selection = selections.getJSONObject(i);
      String path = selection.getString("path");
      path = prefix + (path.startsWith(File.separator) ? path : File.separator + path);
      selection.put("path", path);
    }
  }
}
