package cloudcmd.common.index;


import cloudcmd.common.*;
import cloudcmd.common.config.ConfigStorageService;
import org.apache.log4j.Logger;
import org.h2.fulltext.FullText;
import org.h2.fulltext.FullTextLucene;
import org.h2.jdbcx.JdbcConnectionPool;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class H2IndexStorage implements IndexStorage
{
  private static Logger log = Logger.getLogger(H2IndexStorage.class);

  private static String _configRoot;

  JdbcConnectionPool _cp;

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
    Class.forName("org.h2.fulltext.FullTextLucene");

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

      FullTextLucene.init(db);
      FullTextLucene.setWhitespaceChars(db, " ,:-._" + File.separator);
      FullTextLucene.createIndex(db, "PUBLIC", "FILE_INDEX", "TAGS");
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
    File file = new File(getDbFile() + ".h2.db");
    if (file.exists())
    {
      try {
        FileUtil.delete(file);
        FileUtil.delete(new File(getDbFile()));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    bootstrapDb();
  }

  @Override
  public synchronized void flush()
  {
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

  private void addMeta(Connection db, List<FileMetaData> fmds) throws JSONException, SQLException
  {
    String sql;

    PreparedStatement statement = null;
    PreparedStatement statementB = null;

    try
    {
      List<Object> bind = new ArrayList<Object>();
      List<String> fields = new ArrayList<String>();

      fields.add("HASH");
      fields.add("PATH");
      fields.add("FILENAME");
      fields.add("FILEEXT");
      fields.add("FILESIZE");
      fields.add("FILEDATE");
      fields.add("TAGS");
      fields.add("RAWMETA");

      sql = String.format("MERGE INTO FILE_INDEX (%s) VALUES (%s);", StringUtil.join(fields, ","), StringUtil.joinRepeat(fields.size(), "?", ","));

      statement = db.prepareStatement(sql);

      int k = 0;
      for (FileMetaData meta : fmds) {
        bind.clear();

        bind.add(meta.getHash());
        bind.add(meta.getPath());
        bind.add(meta.getFilename());
        bind.add(meta.getFileExt());
        bind.add(meta.getFileSize());
        bind.add(meta.getFileDate());

        String tags = StringUtil.join(meta.getTags(), " ") + meta.getPath().toString();
        String filter = " ,:-._" + File.separator;
        for (int i = 0; i < filter.length(); i++) {
          tags = tags.replace(filter.charAt(i), ' ');
        }
        bind.add(tags);
        bind.add(meta.getDataAsString());

        for (int i = 0, paramIdx = 1; i < bind.size(); i++, paramIdx++)
        {
          bind(statement, paramIdx, bind.get(i));
        }

        statement.addBatch();

        if (++k > 8192) {
          System.out.print(".");
          statement.executeBatch();
          k = 0;
        }
      }

      statement.executeBatch();
    }
    catch (Exception e)
    {
      e.printStackTrace();
      log.error(e);
    }
    finally
    {
      SqlUtil.SafeClose(statement);
      SqlUtil.SafeClose(statementB);
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
    else if (obj == null)
    {
      statement.setString(idx, null);
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
      addMeta(db, Arrays.asList(meta));
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
      addMeta(db, meta);
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
      db = getDbConnection(); //getReadOnlyDbConnection();

      String sql;

      List<Object> bind = new ArrayList<Object>();

      if (filter.has("tags"))
      {
        sql = "SELECT T.HASH,T.RAWMETA FROM FTL_SEARCH_DATA(?, 0, 0) FTL, FILE_INDEX T WHERE FTL.TABLE='FILE_INDEX' AND T.HASH = FTL.KEYS[0]";
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
          sql = String.format("SELECT HASH,RAWMETA FROM FILE_INDEX WHERE %s", StringUtil.join(list, " AND "));
        }
        else
        {
          sql = String.format("SELECT HASH,RAWMETA FROM FILE_INDEX");
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
  public void pruneHistory(List<FileMetaData> selections)
  {
    Connection db = null;
    try
    {
      db = getDbConnection();
      db.setAutoCommit(false);

      for (FileMetaData meta : selections)
      {
        String parent = meta.getParent();
        if (parent != null)
        {
          removeMeta(db, parent);
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
