package cloudcmd.common.index;


import cloudcmd.common.FileMetaData;
import cloudcmd.common.MetaUtil;
import cloudcmd.common.SqlUtil;
import cloudcmd.common.StringUtil;
import cloudcmd.common.config.ConfigStorageService;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.log4j.Logger;
import org.h2.fulltext.FullText;
import org.h2.jdbcx.JdbcConnectionPool;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class H2IndexStorage implements IndexStorage
{
  private static Logger log = Logger.getLogger(H2IndexStorage.class);

  private static final int MAX_QUEUE_SIZE = 1024 * 8;

  private static String _configRoot;

  JdbcConnectionPool _cp;

  // THIS IS NOT USED
  ConcurrentLinkedQueue<FileMetaData> _queue = new ConcurrentLinkedQueue<FileMetaData>();

  private String getDbFile()
  {
    return String.format("%s%sindex", _configRoot, File.separator);
  }

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
      bootstrapDb();
    }
  }

  @Override
  public void shutdown()
  {
    flush();

    if (_cp != null)
    {
      _cp.dispose();
    }
  }

  private void bootstrapDb()
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
      log.error(e);
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
      log.error(e);
    }
    finally
    {
      SqlUtil.SafeClose(st);
      SqlUtil.SafeClose(db);
    }
  }

  volatile boolean _flushing = false;

  @Override
  public synchronized void flush()
  {
    if (_queue.size() == 0) return;

    _flushing = true;

    Connection db = null;
    try
    {
      db = getDbConnection();

      db.setAutoCommit(false);

      while (!_queue.isEmpty())
      {
        addMeta(db, _queue.remove());
      }

      db.commit();
    }
    catch (SQLException e)
    {
      log.error(e);
    }
    catch (JSONException e)
    {
      log.error(e);
    }
    finally
    {
      SqlUtil.SafeClose(db);
      _flushing = false;
    }
  }

  private void removeMeta(Connection db, String hash) throws JSONException, SQLException
  {
    PreparedStatement statement = null;

    try
    {
      statement = db.prepareStatement("DELETE FROM FILE_INDEX WHERE HASH = ?;");
      bind(statement, 1, hash);
      statement.execute();
    }
    catch (Exception e)
    {
      log.error(e);
    }
    finally
    {
      SqlUtil.SafeClose(statement);
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
      if (meta.getFileExt() != null) fields.add("FILEEXT");
      fields.add("FILESIZE");
      fields.add("FILEDATE");
      fields.add("TAGS");
      fields.add("RAWMETA");

      bind.add(meta.getHash());
      bind.add(meta.getPath());
      bind.add(meta.getFilename());
      if (meta.getFileExt() != null) bind.add(meta.getFileExt());
      bind.add(meta.getFileSize());
      bind.add(meta.getFileDate());
      bind.add(StringUtil.join(meta.getTags(), " "));
      bind.add(meta.getDataAsString());

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
      log.error(e);
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

/*
    _queue.add(meta);
    if (_queue.size() > MAX_QUEUE_SIZE && !_flushing)
    {
      flush();
    }
*/
    Connection db = null;
    try
    {
      db = getDbConnection();
      addMeta(db, meta);
    }
    catch (JSONException e)
    {
      e.printStackTrace();
      log.error(e);
    }
    catch (SQLException e)
    {
      log.error(e);
    }
    finally
    {
      SqlUtil.SafeClose(db);
    }
  }

  @Override
  public void remove(FileMetaData meta)
  {
    Connection db = null;
    try
    {
      db = getDbConnection();
      db.setAutoCommit(false);

      removeMeta(db, meta.getHash());

      db.commit();
    }
    catch (JSONException e)
    {
      e.printStackTrace();
      log.error(e);
    }
    catch (SQLException e)
    {
      log.error(e);
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
      log.error(e);
    }
    catch (SQLException e)
    {
      log.error(e);
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
            if (obj.toString().contains("%"))
            {
              list.add(String.format("%s LIKE ?", key));
            }
            else
            {
              list.add(String.format("%s IN (?)", key));
            }
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

      ResultSet rs = statement.executeQuery();

      while (rs.next())
      {
        results.put(MetaUtil.loadMeta(rs.getString("HASH"), new JSONObject(rs.getString("RAWMETA"))).toJson());
      }
    }
    catch (JSONException e)
    {
      e.printStackTrace();
      log.error(e);
    }
    catch (SQLException e)
    {
      log.error(e);
    }
    finally
    {
      SqlUtil.SafeClose(statement);
      SqlUtil.SafeClose(db);
    }

    return results;
  }

  @Override
  public void pruneHistory(JSONArray selections)
  {
    Connection db = null;
    try
    {
      db = getDbConnection();
      db.setAutoCommit(false);

      for (int i = 0; i < selections.length(); i++)
      {
        JSONObject meta = selections.getJSONObject(i).getJSONObject("data");
        if (meta.has("parent"))
        {
          removeMeta(db, meta.getString("parent"));
        }
      }

      db.commit();
    }
    catch (JSONException e)
    {
      e.printStackTrace();
      log.error(e);
    }
    catch (SQLException e)
    {
      log.error(e);
    }
    finally
    {
      SqlUtil.SafeClose(db);
    }
  }
}
