package cloudcmd.common.adapters;

import org.json.JSONObject;

import java.io.InputStream;
import java.util.Set;

public class FileAdapter extends Adapter
{
  public FileAdapter(String type, Set<String> tags, JSONObject config)
  {
    super(type, tags, config);
  }

  @Override
  public void init()
  {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void shutdown()
  {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void store(InputStream data, JSONObject meta)
  {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public InputStream load(JSONObject meta)
  {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Set<JSONObject> describe()
  {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
