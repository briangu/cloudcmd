package cloudcmd.cld;

public class Main {

  @SuppressWarnings("unchecked")
  public static void main(String[] args) {
		CommandSet app = new CommandSet("cld");
		app.addSubCommands(Index.class);
		app.invoke(args);
	}
}
