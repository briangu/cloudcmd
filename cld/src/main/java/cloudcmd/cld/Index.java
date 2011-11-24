package cloudcmd.cld;


@SubCommand(name="index", description="Index stuff")
public class Index implements Command {
	
	@Arg(name="Path to index", optional=true)
	public String path = ".";

//	@Opt(opt="n", longOpt="repeat", description="Number of times to yell the text")
//	public Number yells = 1;
  
	@Override
  public void exec(CommandContext commandLine) throws CommandError, Exception {
  }
}
