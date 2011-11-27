package cloudcmd.common.index;

import cloudcmd.common.FileMetaData;
import cloudcmd.common.MetaUtil;
import cloudcmd.common.SqlUtil;
import cloudcmd.common.StringUtil;
import cloudcmd.common.config.ConfigStorageService;
import org.h2.fulltext.FullText;
import org.h2.jdbcx.JdbcConnectionPool;
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

  JdbcConnectionPool _cp;

  private String createConnectionString()
  {
    return String.format("jdbc:h2:%s", getDbFile());
  }

  private Connection getDbConnection() throws SQLException
  {
    return _cp.getConnection();
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

    _cp = JdbcConnectionPool.create(createConnectionString(), "sa", "sa");

    File file = new File(getDbFile());
    if (!file.exists())
    {
      bootstrap();
    }
  }

  @Override
  public void shutdown()
  {
    flush();

    _cp.dispose();
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
      st.execute("CREATE TABLE file_index ( hash VARCHAR PRIMARY, path VARCHAR, filename VARCHAR, fileext VARCHAR, filesize INTEGER, filedate INTEGER, tags VARCHAR, rawMeta VARCHAR );");

      FullText.init(db);
      FullText.setWhitespaceChars(db, " ,:-._"+File.separator);
      FullText.createIndex(db, "PUBLIC", "file_index", "tags");
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
        addMeta(db, _queue.remove());
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

    fields.add("hash");
    fields.add("path");
    fields.add("filename");
    fields.add("fileext");
    fields.add("filesize");
    fields.add("filedate");
    fields.add("tags");
    fields.add("rawMeta");

    bind.add(meta.MetaHash);
    bind.add(meta.Meta.getString("path"));
    bind.add(meta.Meta.getString("filename"));
    bind.add(meta.Meta.getString("fileext"));
    bind.add(meta.Meta.getLong("filesize"));
    bind.add(meta.Meta.getLong("filedate"));
    bind.add(StringUtil.join(meta.Tags, " "));
    bind.add(meta.Meta.toString());

    sql = String.format("merge into file_index (%s) values (%s);", StringUtil.join(fields, ","), StringUtil.repeat(bind.size(), "?"));

    PreparedStatement statement = db.prepareStatement(sql);

    try
    {
      for (int i = 0, paramIdx = 1; i < bind.size(); i++, paramIdx++)
      {
        bind(statement, paramIdx, bind.get(i));
      }

      statement.execute();
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

  private void bind(PreparedStatement statement, int idx, Object obj) throws SQLException
  {
    if (obj instanceof String)
    {
      statement.setString(idx, (String) obj);
    }
    else if (obj instanceof Long)
    {
      statement.setLong(idx, (Long) obj);
    }
    else
    {
      throw new IllegalArgumentException("unknown obj type: " + obj.toString());
    }
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
      db = getReadOnlyDbConnection();

      String sql;

      List<Object> bind = new ArrayList<Object>();

      if (filter.has("tags"))
      {
        sql = "select hash,tags,rawMeta from file_index where hash in (select hash from FT_SEARCH(?, 0, 0))";
        bind.addAll(Arrays.asList(filter.getString("tags")));
      }
      else
      {
        List<String> list = new ArrayList<String>();

        Iterator<String> iter = filter.keys();

        while (iter.hasNext())
        {
          String key = iter.next();
          Object obj = filter.get(key);
          if (obj instanceof String[] || obj instanceof Long[])
          {
            Collection<Object> foo = Arrays.asList(obj);
            list.add(String.format("%s in (%s)", key, StringUtil.repeat(foo.size(), "?")));
            bind.addAll(foo);
          }
          else
          {
            list.add(String.format("%s in ?", key));
            bind.add(obj);
          }
        }

        if (list.size() > 0)
        {
          sql = String.format("select hash,tags,rawMeta from file_index where %s", StringUtil.join(list, " and "));
        }
        else
        {
          sql = String.format("select hash,tags,rawMeta from file_index");
        }
      }

      statement = db.prepareStatement(sql);

      for (int i = 0, paramIdx = 1; i < bind.size(); i++, paramIdx++)
      {
        bind(statement, paramIdx, bind.get(i));
      }

      ResultSet resultSet = statement.executeQuery();

      while (resultSet.next())
      {
        String rawJson = resultSet.getString("rawMeta");
        JSONObject obj = new JSONObject(rawJson);
        obj.put("hash", resultSet.getString("hash"));
        obj.put("tags", resultSet.getString("tags"));
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

  private ResultSet queryByHash(Connection db, List<String> hash) throws SQLException
  {
    PreparedStatement statement = null;

    try
    {
      String sql = String.format("select hash,tags,rawMeta from file_index where hash in (%s);", StringUtil.repeat(hash.size(), "?"));

      statement = db.prepareStatement(sql);

      for (int i = 0, paramIdx = 1; i < hash.size(); i++, paramIdx++)
      {
        bind(statement, paramIdx, hash.get(i));
      }

      return statement.executeQuery();
    }
    finally
    {
      SqlUtil.SafeClose(statement);
    }
  }

  @Override
  public void addTags(JSONArray array, Set<String> tags)
  {
    Connection db = null;
    try
    {
      List<String> hashes = new ArrayList<String>();

      for (int i = 0; i < array.length(); i++)
      {
        hashes.add(array.getJSONObject(i).getString("hash"));
      }

      db = getDbConnection();

      db.setAutoCommit(false);

      ResultSet rs = queryByHash(db, hashes);

      while(rs.next())
      {
        String rowTags = rs.getString("tags");
        Set<String> rowTagSet = MetaUtil.createRowTagSet(rowTags);
        rowTagSet.addAll(tags);
        rs.updateString("tags", StringUtil.join(rowTagSet, " "));
        rs.updateRow();
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

  @Override
  public void removeTags(JSONArray array, Set<String> tags)
  {
    Connection db = null;
    try
    {
      List<String> hashes = new ArrayList<String>();

      for (int i = 0; i < array.length(); i++)
      {
        hashes.add(array.getJSONObject(i).getString("hash"));
      }

      db = getDbConnection();

      db.setAutoCommit(false);

      ResultSet rs = queryByHash(db, hashes);

      while(rs.next())
      {
        String rowTags = rs.getString("tags");
        Set<String> rowTagSet = MetaUtil.createRowTagSet(rowTags);
        rowTagSet.removeAll(tags);
        rs.updateString("tags", StringUtil.join(rowTagSet, " "));
        rs.updateRow();
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
}
