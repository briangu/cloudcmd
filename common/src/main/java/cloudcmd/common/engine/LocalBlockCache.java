package cloudcmd.common.engine;

import cloudcmd.common.adapters.Adapter;
import cloudcmd.common.adapters.FileAdapter;
import cloudcmd.common.config.ConfigStorageService;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.util.*;

public class LocalBlockCache implements BlockCache
{
  private Adapter _adapter = null;
  private Map<String, List<Adapter>> _hashProviders = null;

  public void init()
  {
    JSONObject obj = new JSONObject();
    try
    {
      obj.put("rootPath", ConfigStorageService.instance().getConfigRoot() + File.separator + "cache");
      _adapter = new FileAdapter();
      _adapter.init(0, FileAdapter.class.getName(), new HashSet<String>(), obj);
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  @Override
  public void shutdown()
  {
  }

  @Override
  public Adapter getBlockCache()
  {
    return _adapter;
  }

  @Override
  public void refreshCache(int maxTier) throws Exception
  {
    for (final Adapter adapter : ConfigStorageService.instance().getAdapters())
    {
      try
      {
        adapter.refreshCache();
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }

    _hashProviders = new HashMap<String, List<Adapter>>();

    for (final Adapter adapter : ConfigStorageService.instance().getAdapters())
    {
      if (adapter.Tier > maxTier) continue;

      Set<String> adapterDescription = adapter.describe();

      for (final String hash : adapterDescription)
      {
        if (!_hashProviders.containsKey(hash))
        {
          _hashProviders.put(hash, new ArrayList<Adapter>());
        }
        _hashProviders.get(hash).add(adapter);
      }
    }
  }

  @Override
  public Map<String, List<Adapter>> getHashProviders()
  {
    return Collections.unmodifiableMap(_hashProviders);
  }
}
