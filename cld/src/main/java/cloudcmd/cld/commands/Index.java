package cloudcmd.cld.commands;


import cloudcmd.common.*;
import cloudcmd.cld.FileTypeUtil;
import cloudcmd.common.FileWalker;
import cloudcmd.common.FileHandler;
import cloudcmd.common.index.IndexStorageService;
import jpbetz.cli.*;
import jpbetz.cli.Command;
import jpbetz.cli.CommandContext;
import ops.*;
import org.json.JSONObject;

import java.io.File;
import java.util.Map;

@SubCommand(name="index", description="Index files")
public class Index implements Command {
	
	@Arg(name="path", optional=true)
	public String _path = ".";

	@Arg(name="tags", optional = true, isVararg = true)
	public String[] _tags = null;

    // TODO:
    //
    //  extract metadata from file and add back into ops
    //    calculate hash, get filename and extension, path, size
    //  add filetype specific processors in ops
    //  write two files:
    //    original file data with the name of the file as the hash
    //    meta data file
    //  write to log
    //
    // add reindex command cli/srv
    // add search in cli/srv
    // add fetch cmd in cli/srv
    //

	@Override
  public void exec(CommandContext commandLine) throws CommandError, Exception {

    Map<String, ops.Command> registry = OpsFactory.getDefaultRegistry();

    registry.put("process", new ops.Command() {
      @Override
      public void exec(ops.CommandContext context, Object[] args) throws Exception {
        File file = (File)args[0];
        String fileName = file.getName();
        int extIndex = fileName.lastIndexOf(".");
        String ext = extIndex > 0 ? fileName.substring(extIndex+1) : null;
        String type = ext != null ? FileTypeUtil.instance().getTypeFromExtension(ext) : "default";
        context.make(new MemoryElement("index", "name", fileName, "type", type, "ext", ext, "file", file));
      }
    });

    //           ["index", "name", "$name", "type", "image", "ext", "$ext", "file", "$file"]

    registry.put("index_image", new ops.Command() {
      @Override
      public void exec(ops.CommandContext context, Object[] args) throws Exception {
        File file = (File)args[0];
        String type = args[1].toString();
        JSONObject meta = MetaUtil.createMeta(file, type, _tags);
        IndexStorageService.instance().add(meta);
      }
    });

    registry.put("index_doc", new ops.Command() {
      @Override
      public void exec(ops.CommandContext context, Object[] args) throws Exception {
        File file = (File)args[0];
        String type = args[1].toString();
        JSONObject meta = MetaUtil.createMeta(file, type, _tags);
        IndexStorageService.instance().add(meta);
      }
    });

    registry.put("index_music", new ops.Command() {
      @Override
      public void exec(ops.CommandContext context, Object[] args) throws Exception {
        File file = (File)args[0];
        String type = args[1].toString();
        JSONObject meta = MetaUtil.createMeta(file, type, _tags);
        IndexStorageService.instance().add(meta);
      }
    });

    registry.put("index_java", new ops.Command() {
      @Override
      public void exec(ops.CommandContext context, Object[] args) throws Exception {
        File file = (File)args[0];
        String type = args[1].toString();
        JSONObject meta = MetaUtil.createMeta(file, type, _tags);
        IndexStorageService.instance().add(meta);
      }
    });

    registry.put("index_default", new ops.Command() {
      @Override
      public void exec(ops.CommandContext context, Object[] args) throws Exception {
        File file = (File)args[0];
        String type = args[1].toString();
        JSONObject meta = MetaUtil.createMeta(file, type, _tags);
        IndexStorageService.instance().add(meta);
      }
    });

    final OPS ops = OpsFactory.create(registry, ResourceUtil.loadOps("index.ops"));

    FileWalker.enumerateFolders(_path, new FileHandler()
    {
      @Override
      public void process(File file)
      {
        ops.make(new MemoryElement("rawFile", "name", file.getName(), "file", file));
      }
    });

    ops.run();
  }
}
