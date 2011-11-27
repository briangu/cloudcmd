package cloudcmd.common.index;

import cloudcmd.common.FileMetaData;
import cloudcmd.common.StringUtil;
import cloudcmd.common.config.ConfigStorageService;
import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SqliteIndexStorage implements IndexStorage
{
  private static final int MAX_QUEUE_SIZE = 1000;

  private static String _configRoot;

  private static File getDbFile()
  {
    return new File(_configRoot + File.separator + "cloudcmd.db");
  }

  @Override
  public void init()
  {
    _configRoot = ConfigStorageService.instance().getConfigRoot();

    File file = getDbFile();
    if (!file.exists())
    {
      bootstrap();
    }
  }

  private void bootstrap()
  {
    SQLiteConnection db = null;
    try
    {
      db = new SQLiteConnection(getDbFile());
      db.open(true);
      db.exec("DROP TABLE if exists file_index;");
      db.exec("CREATE TABLE file_index ( id INTEGER PRIMARY KEY ASC, hash TEXT, path TEXT, filename TEXT, fileext  TEXT, filesize INTEGER, filedate INTEGER, type TEXT, blob TEXT );");
      db.exec("CREATE TABLE tags ( id INTEGER PRIMARY KEY ASC, fileId INTEGER, tag TEXT, UNIQUE(fieldId, tag) );");
      db.exec("CREATE INDEX idx_fi_path on file_index(path);");
      db.exec("CREATE INDEX idx_fi_hash on file_index(hash);");
      db.exec("CREATE INDEX idx_fi_filename on file_index(filename);");
      db.exec("CREATE INDEX idx_tags on tags(tag);");
    }
    catch (SQLiteException e)
    {
      e.printStackTrace();
    }
    finally
    {
      if (db != null)
      {
        db.dispose();
      }
    }
  }

  @Override
  public void purge()
  {
    SQLiteConnection db = null;
    try
    {
      db = new SQLiteConnection(getDbFile());
      db.open(false);
      db.exec("delete from file_index;");
      db.exec("delete from tags;");
    }
    catch (SQLiteException e)
    {
      e.printStackTrace();
    }
    finally
    {
      if (db != null)
      {
        db.dispose();
      }
    }
  }

  private ConcurrentLinkedQueue<FileMetaData> _queue = new ConcurrentLinkedQueue<FileMetaData>();

  @Override
  public void flush()
  {
    if (_queue.size() == 0) return;

    SQLiteConnection db = null;

    try
    {
      db = new SQLiteConnection(getDbFile());
      db.open(false);

      db.exec("begin");

      for (int i = 0; i < _queue.size(); i++)
      {
        FileMetaData obj = _queue.remove();
        addMeta(db, obj);
      }

      db.exec("commit");
    }
    catch (SQLiteException e)
    {
      e.printStackTrace();
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }
    finally
    {
      if (db != null)
      {
        db.dispose();
      }
    }
  }

  private void addMeta(SQLiteConnection db, FileMetaData meta) throws JSONException, SQLiteException
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

    SQLiteStatement statement = db.prepare(sql);

    try
    {
      for (int i = 0; i < bind.size(); i++)
      {
        Object obj = bind.get(i);
        if (obj instanceof String)
        {
          statement.bind(i, (String)obj);
        }
        else if (obj instanceof Long)
        {
          statement.bind(i, (Long)obj);
        }
        else
        {
          throw new IllegalArgumentException("unknown obj type: " + obj.toString());
        }
      }

      statement.stepThrough();

      if (meta.Tags != null)
      {
        long fieldId = db.getLastInsertId();
        insertTags(db, fieldId, meta.Tags);
      }
    }
    catch (SQLiteException e)
    {
      e.printStackTrace();
    }
    finally
    {
      statement.dispose();
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

    SQLiteConnection db = null;

    try
    {
      String[] tags = filter.has("tags") ? (String[]) filter.get("tags") : null;

      db = new SQLiteConnection(getDbFile());
      db.openReadonly();

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

      SQLiteStatement statement = db.prepare(sql);

      try
      {
        for (int i = 0; i < bind.size(); i++)
        {
          Object obj = bind.get(i);
          if (obj instanceof String)
          {
            statement.bind(i, (String)obj);
          }
          else if (obj instanceof Long)
          {
            statement.bind(i, (Long)obj);
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
      catch (SQLiteException e)
      {
        e.printStackTrace();
      }
      catch (JSONException e)
      {
        e.printStackTrace();
      }
      finally
      {
        statement.dispose();
      }
    }
    catch (SQLiteException e)
    {
      e.printStackTrace();
    }
    catch (JSONException e)
    {
      e.printStackTrace();
    }
    finally
    {
      if (db != null)
      {
        db.dispose();
      }
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
    SQLiteConnection db = null;
    try
    {
      db = new SQLiteConnection(getDbFile());
      db.open(false);

      db.exec("begin");

      for (int i = 0; i < array.length(); i++)
      {
        long fieldId = array.getJSONObject(i).getLong("id");
        insertTags(db, fieldId, tags);
      }

      db.exec("commit");
    }
    catch (SQLiteException e)
    {
      e.printStackTrace();
    }
    catch (JSONException e1)
    {
      e1.printStackTrace();
    }
    finally
    {
      if (db != null)
      {
        db.dispose();
      }
    }
  }

  private void insertTags(SQLiteConnection db, long fieldId, Set<String> tags) throws SQLiteException
  {
    SQLiteStatement statement = db.prepare("insert or replace into tags (fieldId, tag) values (?, ?)");

    try
    {
      for (String tag : tags)
      {
        statement.reset();

        statement.bind(0, fieldId);
        statement.bind(1, tag);

        statement.stepThrough();
      }
    }
    catch (SQLiteException e)
    {
      e.printStackTrace();
    }
    finally
    {
      statement.dispose();
    }
  }

  @Override
  public void removeTag(JSONArray array, Set<String> tags)
  {
    SQLiteConnection db = null;
    try
    {
      db = new SQLiteConnection(getDbFile());
      db.open(false);

      String sql = String.format("delete from tags where tag in (%s)", repeat(tags.size(), "?"));
      SQLiteStatement statement = db.prepare(sql);

      int i = 0;
      for (String tag : tags)
      {
        statement.bind(i++, tag);
      }

      statement.stepThrough();
    }
    catch (SQLiteException e)
    {
      e.printStackTrace();
    }
    finally
    {
      if (db != null)
      {
        db.dispose();
      }
    }
  }

  public String repeat(int n, String s)
  {
    return String.format(String.format("%%0%dd", n), 0).replace("0",s);
  }
}
