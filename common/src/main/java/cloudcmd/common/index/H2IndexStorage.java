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
    return String.format("%s%sindex", _configRoot, File.separator);
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

    File file = new File(getDbFile() + ".h2.db");
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

      st.execute("DROP TABLE if exists FILE_INDEX;");
      st.execute("CREATE TABLE FILE_INDEX ( HASH VARCHAR PRIMARY KEY, PATH VARCHAR, FILENAME VARCHAR, FILEEXT VARCHAR, FILESIZE BIGINT, FILEDATE BIGINT, TAGS VARCHAR, RAWMETA VARCHAR );");

      db.commit();

      FullText.init(db);
      FullText.setWhitespaceChars(db, " ,:-._" + File.separator);
      FullText.createIndex(db, "PUBLIC", "FILE_INDEX", null);
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

      st.execute("delete from FILE_INDEX;");
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

    PreparedStatement statement = null;

    try
    {
      List<Object> bind = new ArrayList<Object>();
      List<String> fields = new ArrayList<String>();

      fields.add("HASH");
      fields.add("PATH");
      fields.add("FILENAME");
      if (meta.Meta.has("fileext")) fields.add("FILEEXT");
      fields.add("FILESIZE");
      fields.add("FILEDATE");
      fields.add("TAGS");
      fields.add("RAWMETA");

      bind.add(meta.MetaHash);
      bind.add(meta.Meta.getString("path"));
      bind.add(meta.Meta.getString("filename"));
      if (meta.Meta.has("fileext")) bind.add(meta.Meta.getString("fileext"));
      bind.add(meta.Meta.getLong("filesize"));
      bind.add(meta.Meta.getLong("filedate"));
      bind.add(StringUtil.join(meta.Tags, " "));
      bind.add(meta.Meta.toString());

      sql = String.format("MERGE INTO FILE_INDEX (%s) VALUES (%s);", StringUtil.join(fields, ","), StringUtil.joinRepeat(bind.size(), "?", ","));

      statement = db.prepareStatement(sql);

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

    Connection db = null;
    try
    {
      db = getDbConnection();
      addMeta(db, meta);
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
  public void addAll(List<FileMetaData> meta)
  {
    if (meta == null) return;

    Connection db = null;
    try
    {
      db = getDbConnection();
      db.setAutoCommit(false);

      for (FileMetaData fmd : meta)
      {
        addMeta(db, fmd);
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
        sql = "SELECT T.* FROM FT_SEARCH_DATA(?, 0, 0) FT, FILE_INDEX T WHERE FT.TABLE='FILE_INDEX' AND T.HASH = FT.KEYS[0]";
        bind.add(filter.getString("tags"));
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
            list.add(String.format("%s In (%s)", key.toUpperCase(), StringUtil.joinRepeat(foo.size(), "?", ",")));
            bind.addAll(foo);
          }
          else
          {
            list.add(String.format("%s IN (?)", key));
            bind.add(obj);
          }
        }

        if (list.size() > 0)
        {
          sql = String.format("SELECT HASH,TAGS,RAWMETA FROM FILE_INDEX WHERE %s", StringUtil.join(list, " AND "));
        }
        else
        {
          sql = String.format("SELECT HASH,TAGS,RAWMETA FROM FILE_INDEX");
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
        String rawJson = resultSet.getString("RAWMETA");
        JSONObject obj = new JSONObject(rawJson);
        obj.put("hash", resultSet.getString("HASH"));
        obj.put("tags", resultSet.getString("TAGS"));
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
  public void addTags(JSONArray array, Set<String> tags)
  {
    Connection db = null;
    PreparedStatement statement = null;

    try
    {
      List<String> hashes = new ArrayList<String>();

      for (int i = 0; i < array.length(); i++)
      {
        hashes.add(array.getJSONObject(i).getString("hash"));
      }

      db = getDbConnection();

      db.setAutoCommit(false);

      String sql = String.format("SELECT HASH,TAGS,RAWMETA FROM FILE_INDEX WHERE HASH IN (%s);", StringUtil.joinRepeat(hashes.size(), "?", ","));

      statement =
        db.prepareStatement(
          sql,
          ResultSet.TYPE_SCROLL_SENSITIVE,
          ResultSet.CONCUR_UPDATABLE,
          ResultSet.HOLD_CURSORS_OVER_COMMIT);

      for (int i = 0, paramIdx = 1; i < hashes.size(); i++, paramIdx++)
      {
        bind(statement, paramIdx, hashes.get(i));
      }

      ResultSet rs = statement.executeQuery();

      while (rs.next())
      {
        String rowTags = rs.getString("TAGS");
        Set<String> rowTagSet = MetaUtil.createRowTagSet(rowTags);
        rowTagSet.addAll(tags);
        rs.updateString("TAGS", StringUtil.join(rowTagSet, " "));
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
      SqlUtil.SafeClose(statement);
      SqlUtil.SafeClose(db);
    }
  }

  @Override
  public void removeTags(JSONArray array, Set<String> tags)
  {
    Connection db = null;
    PreparedStatement statement = null;
    try
    {
      List<String> hashes = new ArrayList<String>();

      for (int i = 0; i < array.length(); i++)
      {
        hashes.add(array.getJSONObject(i).getString("hash"));
      }

      db = getDbConnection();

      db.setAutoCommit(false);

      String sql = String.format("SELECT HASH,TAGS,RAWMETA FROM FILE_INDEX WHERE HASH IN (%s);", StringUtil.joinRepeat(hashes.size(), "?", ","));

      statement =
        db.prepareStatement(
          sql,
          ResultSet.TYPE_SCROLL_SENSITIVE,
          ResultSet.CONCUR_UPDATABLE,
          ResultSet.HOLD_CURSORS_OVER_COMMIT);

      for (int i = 0, paramIdx = 1; i < hashes.size(); i++, paramIdx++)
      {
        bind(statement, paramIdx, hashes.get(i));
      }

      ResultSet rs = statement.executeQuery();

      while (rs.next())
      {
        String rowTags = rs.getString("TAGS");
        Set<String> rowTagSet = MetaUtil.createRowTagSet(rowTags);
        rowTagSet.removeAll(tags);
        rs.updateString("TAGS", StringUtil.join(rowTagSet, " "));
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
      SqlUtil.SafeClose(statement);
      SqlUtil.SafeClose(db);
    }
  }
}
