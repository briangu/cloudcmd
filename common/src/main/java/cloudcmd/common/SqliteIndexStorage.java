package cloudcmd.common;

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

  private static File getDbFile()
  {
    // TODO: we need config
    return new File("./cloudcmd.db");
  }

  @Override
  public void init()
  {
    SQLiteConnection db = null;
    try
    {
      db = new SQLiteConnection(getDbFile());
      db.open(true);
      db.exec("DROP TABLE if exists file_index;");
      db.exec("CREATE TABLE file_index ( id INTEGER PRIMARY KEY ASC, hash TEXT, path TEXT, filename TEXT, fileext  TEXT, filesize INTEGER, filedate INTEGER, type TEXT );");
      db.exec("CREATE TABLE tags ( fileId INTEGER, tag TEXT, PRIMARY_KEY(fieldId, tag) );");
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

  private ConcurrentLinkedQueue<JSONObject> _queue = new ConcurrentLinkedQueue<JSONObject>();

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
        JSONObject obj = _queue.remove();
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

  private void addMeta(SQLiteConnection db, JSONObject meta) throws JSONException, SQLiteException
  {
    String sql;

    List<Object> bind = new ArrayList<Object>();

    Iterator iter = meta.keys();

    while (iter.hasNext())
    {
      String key = iter.next().toString();
      if (key.equals("tags")) continue;
      Object obj = meta.get(key);
      bind.add(obj);
    }

    sql = String.format("insert into file_index values (%s);", repeat(bind.size(), "?"));

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

      String[] tags = meta.has("tags") ? (String[]) meta.get("tags") : null;

      if (tags != null)
      {
        long fieldId = db.getLastInsertId();
        insertTags(db, fieldId, tags);
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

  @Override
  public void shutdown()
  {
    flush();
  }

  @Override
  public void add(JSONObject meta)
  {
    _queue.add(meta);
    if (_queue.size() > MAX_QUEUE_SIZE)
    {
      flush();
    }
  }

  public static String join(Collection<?> s, String delimiter) {
    StringBuilder builder = new StringBuilder();
    Iterator iter = s.iterator();
    while (iter.hasNext()) {
       builder.append(iter.next().toString());
       if (!iter.hasNext()) {
         break;
       }
       builder.append(delimiter);
    }
    return builder.toString();
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
        sql = String.format("select * from file_index where id in (%s);", subSelect);
        bind.addAll(Arrays.asList(tags));
      }
      else
      {
        List<String> list = new ArrayList<String>();

        Iterator iter = filter.keys();

        while (iter.hasNext())
        {
          String key = iter.next().toString();
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

        sql = String.format("select * from file_index where %s;", join(list, " and "));
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
          JSONObject obj = new JSONObject();

          obj.put("id", statement.columnString(0));
          obj.put("hash", statement.columnString(1));
          obj.put("path", statement.columnString(2));
          obj.put("filename", statement.columnString(3));
          obj.put("fileext", statement.columnString(4));
          obj.put("filesize", new Long(statement.columnLong(5)));
          obj.put("filedate", new Long(statement.columnLong(6)));
          obj.put("type", statement.columnString(7));
          obj.put("tags", tags);

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
  public void addTag(JSONArray array, String[] tags)
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

  private void insertTags(SQLiteConnection db, long fieldId, String[] tags) throws SQLiteException
  {
    SQLiteStatement statement = db.prepare("insert or replace into tags (fieldId, tag) values (?, ?)");

    try
    {
      for (int i = 0; i < tags.length; i++)
      {
        statement.reset();

        statement.bind(0, fieldId);
        statement.bind(1, tags[i]);

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
  public void removeTag(JSONArray array, String[] tags)
  {
    SQLiteConnection db = null;
    try
    {
      db = new SQLiteConnection(getDbFile());
      db.open(false);

      String sql = String.format("delete from tags where tag in (%s)", repeat(tags.length, "?"));
      SQLiteStatement statement = db.prepare(sql);
      for (int i = 0; i < tags.length; i++)
      {
        statement.bind(i, tags[i]);
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
