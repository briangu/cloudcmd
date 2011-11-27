package cloudcmd.common.index;

import cloudcmd.common.FileMetaData;
import cloudcmd.common.SqlUtil;
import cloudcmd.common.StringUtil;
import cloudcmd.common.config.ConfigStorageService;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class H2IndexStorage implements IndexStorage
{
  private static final int MAX_QUEUE_SIZE = 1000;

  private static String _configRoot;

  private String getDbFile()
  {
    return String.format("%s%sindex.db", _configRoot, File.separator);
  }

  private Connection getDbConnection() throws SQLException
  {
    String cs = String.format("jdbc:h2:%s", getDbFile());
    Connection conn = DriverManager.getConnection(cs, "sa", "");
    return conn;
  }

  private Connection getReadOnlyDbConnection() throws SQLException
  {
    Connection conn = getDbConnection();
    conn.setReadOnly(true);
    return conn;
  }

  @Override
  public void init() throws Exception
  {
    Class.forName("org.h2.Driver");

    _configRoot = ConfigStorageService.instance().getConfigRoot();

    File file = new File(getDbFile());
    if (!file.exists())
    {
      bootstrap();
    }
  }

  private void bootstrap()
  {
    Connection db = null;
    Statement st = null;
    try
    {
      db = getDbConnection();
      st = db.createStatement();

      st.execute("DROP TABLE if exists file_index;");
      st.execute("CREATE TABLE file_index ( id INTEGER PRIMARY KEY ASC, hash TEXT, path TEXT, filename TEXT, fileext  TEXT, filesize INTEGER, filedate INTEGER, type TEXT, blob TEXT );");
      st.execute("CREATE TABLE tags ( id INTEGER PRIMARY KEY ASC, fileId INTEGER, tag TEXT, UNIQUE(fieldId, tag) );");
      st.execute("CREATE INDEX idx_fi_path on file_index(path);");
      st.execute("CREATE INDEX idx_fi_hash on file_index(hash);");
      st.execute("CREATE INDEX idx_fi_filename on file_index(filename);");
      st.execute("CREATE INDEX idx_tags on tags(tag);");
    }
    catch (SQLException e)
    {
      e.printStackTrace();
    }
    finally
    {
      SqlUtil.SafeClose(st);
      SqlUtil.SafeClose(db);
    }
  }

  @Override
  public void purge()
  {
    Connection db = null;
    Statement st = null;
    try
    {
      db = getDbConnection();
      st = db.createStatement();

      st.execute("delete from file_index;");
      st.execute("delete from tags;");
    }
    catch (SQLException e)
    {
      e.printStackTrace();
    }
    finally
    {
      SqlUtil.SafeClose(st);
      SqlUtil.SafeClose(db);
    }
  }

  private ConcurrentLinkedQueue<FileMetaData> _queue = new ConcurrentLinkedQueue<FileMetaData>();

  @Override
  public void flush()
  {
    if (_queue.size() == 0) return;

    Connection db = null;
    try
    {
      db = getDbConnection();

      db.setAutoCommit(false);

      for (int i = 0; i < _queue.size(); i++)
      {
        FileMetaData obj = _queue.remove();
        addMeta(db, obj);
      }

      db.commit();
    }
    catch (SQLException e)
    {
      e.printStackTrace();
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }
    finally
    {
      SqlUtil.SafeClose(db);
    }
  }

  private void addMeta(Connection db, FileMetaData meta) throws JSONException, SQLException
  {
    String sql;

    List<Object> bind = new ArrayList<Object>();
    List<String> fields = new ArrayList<String>();

    Iterator<String> iter = meta.Meta.keys();

    while (iter.hasNext())
    {
      String key = iter.next();
      Object obj = meta.Meta.get(key);
      bind.add(obj);
    }

    fields.add("blob");
    bind.add(meta.Meta.toString());

    sql = String.format("insert into file_index (%s) values (%s);", StringUtil.join(fields, ","), repeat(bind.size(), "?"));

    PreparedStatement statement = db.prepareStatement(sql);

    try
    {
      for (int i = 0, paramIdx = 1; i < bind.size(); i++, paramIdx++)
      {
        Object obj = bind.get(i);
        if (obj instanceof String)
        {
          statement.setString(paramIdx, (String) obj);
        }
        else if (obj instanceof Long)
        {
          statement.setLong(paramIdx, (Long) obj);
        }
        else
        {
          throw new IllegalArgumentException("unknown obj type: " + obj.toString());
        }
      }

      statement.execute();

      if (meta.Tags != null)
      {
        long fieldId = db.getLastInsertId();
        insertTags(db, fieldId, meta.Tags);
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    finally
    {
      SqlUtil.SafeClose(statement);
    }
  }

  @Override
  public void shutdown()
  {
    flush();
  }

  @Override
  public void add(FileMetaData meta)
  {
    if (meta == null) return;

    _queue.add(meta);
    if (_queue.size() > MAX_QUEUE_SIZE)
    {
      flush();
    }
  }

  @Override
  public JSONArray find(JSONObject filter)
  {
    JSONArray results = new JSONArray();

    Connection db = null;
    PreparedStatement statement = null;

    try
    {
      String[] tags = filter.has("tags") ? (String[]) filter.get("tags") : null;

      db = getReadOnlyDbConnection();

      String sql;

      List<Object> bind = new ArrayList<Object>();

      if (tags != null)
      {
        String subSelect = String.format("select fileId from from tags where tag in (%s)", repeat(tags.length, "?"));
        sql = String.format("select blob from file_index where id in (%s);", subSelect);
        bind.addAll(Arrays.asList(tags));
      }
      else
      {
        List<String> list = new ArrayList<String>();

        Iterator<String> iter = filter.keys();

        while (iter.hasNext())
        {
          String key = iter.next();
          if (key.equals("tags")) continue;
          Object obj = filter.get(key);
          if (obj instanceof String[] || obj instanceof Long[])
          {
            Collection<Object> foo = Arrays.asList(obj);
            list.add(String.format("%s in (%s)", key, repeat(foo.size(), "?")));
            bind.addAll(foo);
          }
          else
          {
            list.add(String.format("%s in ?", key));
            bind.add(obj);
          }
        }

        sql = String.format("select blob from file_index where %s;", StringUtil.join(list, " and "));
      }

       statement = db.prepareStatement(sql);

      for (int i = 0, paramIdx = 1; i < bind.size(); i++, paramIdx++)
      {
        Object obj = bind.get(i);
        if (obj instanceof String)
        {
          statement.setString(paramIdx, (String)obj);
        }
        else if (obj instanceof Long)
        {
          statement.setLong(paramIdx, (Long)obj);
        }
        else
        {
          throw new IllegalArgumentException("unknown obj type: " + obj.toString());
        }
      }

      while (statement.step())
      {
        String rawJson = statement.columnString(0);
        JSONObject obj = new JSONObject(rawJson);
        results.put(obj);
      }
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }
    catch (SQLException e)
    {
      e.printStackTrace();
    }
    finally
    {
      SqlUtil.SafeClose(db);
    }

    return results;
  }

  @Override
  public Set<String> getTags(String hash)
  {
    Set<String> tags = new HashSet<String>();

    try
    {
      JSONObject filter = new JSONObject();
      filter.put("hash", hash);
      JSONArray results = find(filter);

      for (int i = 0; i < results.length(); i++)
      {
        tags.add(results.getString(i));
      }
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }

    return tags;
  }

  @Override
  public void addTag(JSONArray array, Set<String> tags)
  {
    Connection db = null;
    try
    {
      db = getDbConnection();

      db.setAutoCommit(false);

      for (int i = 0; i < array.length(); i++)
      {
        long fieldId = array.getJSONObject(i).getLong("id");
        insertTags(db, fieldId, tags);
      }

      db.commit();
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }
    catch (SQLException e)
    {
      e.printStackTrace();
    }
    finally
    {
      SqlUtil.SafeClose(db);
    }
  }

  private void insertTags(Connection db, long fieldId, Set<String> tags) throws SQLException
  {
    PreparedStatement statement = null;

    try
    {
      statement = db.prepareStatement("insert or replace into tags (fieldId, tag) values (?, ?)");

      for (String tag : tags)
      {
        statement.clearParameters();

        statement.setLong(1, fieldId);
        statement.setString(2, tag);

        statement.execute();
      }
    }
    finally
    {
      SqlUtil.SafeClose(statement);
    }
  }

  @Override
  public void removeTag(JSONArray array, Set<String> tags)
  {
    Connection db = null;

    try
    {
      db = getDbConnection();

      String sql = String.format("delete from tags where tag in (%s)", repeat(tags.size(), "?"));
      PreparedStatement statement = db.prepareStatement(sql);

      int i = 1;
      for (String tag : tags)
      {
        statement.setString(i++, tag);
      }

      statement.execute();
    }
    catch (SQLException e)
    {
      e.printStackTrace();
    }
    finally
    {
      SqlUtil.SafeClose(db);
    }
  }

  public String repeat(int n, String s)
  {
    return String.format(String.format("%%0%dd", n), 0).replace("0",s);
  }
}
