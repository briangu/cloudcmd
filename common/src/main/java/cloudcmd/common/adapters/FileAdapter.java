package cloudcmd.common.adapters;

import cloudcmd.common.*;
import org.h2.jdbcx.JdbcConnectionPool;
import org.json.JSONException;

import java.io.*;
import java.net.URI;
import java.sql.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

//     "file:///tmp/storage?tier=1&tags=image,movie,vacation"

public class FileAdapter extends Adapter
{
  private final static int MIN_FREE_STORAGE_SIZE = 1024 * 1024;
  private final static int LARGE_FILE_CUTOFF = 128 * 1024 * 1024;

  String _rootPath;
  JdbcConnectionPool _cp = null;
  volatile Set<String> _description = null;

  public FileAdapter()
  {
  }

  @Override
  public void init(String configDir, Integer tier, String type, Set<String> tags, URI config) throws Exception
  {
    super.init(configDir, tier, type, tags, config);

    _rootPath = URI.getPath();

    File rootPathDir = new File(_rootPath);
    rootPathDir.mkdirs();

    _isOnline = rootPathDir.exists();

    if (_isOnline) bootstrap(_rootPath);
  }

  private String getDbFile()
  {
    return String.format("%s%sindex", ConfigDir, File.separator);
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

  private void bootstrap(String rootPath)
    throws Exception
  {
    Class.forName("org.h2.Driver");
    _cp = JdbcConnectionPool.create(createConnectionString(), "sa", "sa");
    File file = new File(getDbFile() + ".h2.db");
    if (!file.exists())
    {
      bootstrapDb();
      initSubDirs(rootPath);
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

      st.execute("DROP TABLE if exists BLOCK_INDEX;");
      st.execute("CREATE TABLE BLOCK_INDEX ( HASH VARCHAR PRIMARY KEY );");

      db.commit();
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

  public void purge()
  {
    bootstrapDb();
  }

  @Override
  public boolean IsOnLine()
  {
    return _isOnline;
  }

  @Override
  public boolean IsFull()
  {
    return new File(_rootPath).getUsableSpace() < MIN_FREE_STORAGE_SIZE;
  }

  private static void initSubDirs(String rootPath)
  {
    for (int i = 0; i < 0x100; i++)
    {
      File tmpFile = new File(rootPath + File.separator + String.format("%02x", i));
      tmpFile.mkdirs();
    }
  }

  @Override
  public void refreshCache() throws Exception
  {
    purge();

    Connection db = null;
    PreparedStatement statement = null;
    try
    {
      db = getDbConnection();

      db.setAutoCommit(false);

      statement = db.prepareStatement("INSERT INTO BLOCK_INDEX VALUES (?)");

      for (String hash : rebuildHashIndexFromDisk())
      {
        statement.setString(1, hash);
        statement.execute();
        statement.clearParameters();
      }

      db.commit();
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
  public boolean contains(String hash) throws Exception
  {
    return getDescription().contains(hash);
  }

  @Override
  public void shutdown()
  {
    if (_cp != null)
    {
      _cp.dispose();
    }
  }

  @Override
  public boolean remove(String hash) throws Exception
  {
    File file = new File(getDataFileFromHash(hash));
    if (file.exists()) file.delete();
    return true;
  }

  @Override
  public boolean verify(String hash) throws Exception
  {
    File file = new File(getDataFileFromHash(hash));
    if (!file.exists()) return false;
    int idx = hash.lastIndexOf(".");
    if (idx >= 0)
    {
      hash = hash.substring(0, idx);
    }
    return (CryptoUtil.computeHashAsString(file).equals(hash));
  }

  private String getHashFromDataFile(String hash) {
    int idx = hash.lastIndexOf(".");
    return (idx >= 0) ? hash.substring(0, idx) : hash;
  }

  private String getPathFromHash(String hash) throws JSONException
  {
    return _rootPath + File.separator + hash.substring(0, 2);
  }

  private String getDataFileFromHash(String hash) throws JSONException
  {
    return getPathFromHash(hash) + File.separator + hash;
  }

  @Override
  public void store(InputStream is, String hash) throws Exception
  {
    String writeHash = FileUtil.writeFileAndComputeHash(is, new File(getDataFileFromHash(hash)));
    if (!writeHash.equals(getHashFromDataFile(hash))) {
      throw new RuntimeException(String.format("failed to store data: expected %s got %s", hash, writeHash));
    }
    insertHash(hash);
  }

  @Override
  public String store(InputStream is) throws Exception
  {
    if (is.available() > LARGE_FILE_CUTOFF) {
      return storeLargeFile(is);
    }

    ByteArrayOutputStream baos = null;
    ByteArrayInputStream bais = null;
    String hash = null;

    try {
      hash = CryptoUtil.digestToString(CryptoUtil.writeAndComputeHash(is, baos = new ByteArrayOutputStream()));
      FileUtil.writeFile(bais = new ByteArrayInputStream(baos.toByteArray()), getDataFileFromHash(hash));
      insertHash(hash);
    } finally {
      FileUtil.SafeClose(bais);
      FileUtil.SafeClose(baos);
    }

    return hash;
  }

  private String storeLargeFile(InputStream is) throws Exception
  {
    File tmpFile = new File(_rootPath + File.separator + UUID.randomUUID().toString() + ".tmp");
    tmpFile.createNewFile();
    String hash = FileUtil.writeFileAndComputeHash(is, tmpFile);
    File newFile = new File(getDataFileFromHash(hash));
    if (newFile.exists() && newFile.length() == tmpFile.length())
    {
      tmpFile.delete();
    }
    else
    {
      Boolean success = tmpFile.renameTo(newFile);
      if (!success)
      {
        tmpFile.delete();
        throw new IOException("failed to move file: " + tmpFile.getAbsolutePath());
      }
    }
    insertHash(hash);
    return hash;
  }

  @Override
  public InputStream load(String hash) throws Exception
  {
    File file = new File(getDataFileFromHash(hash));
    if (!file.exists()) throw new DataNotFoundException(hash);
    return new FileInputStream(file);
  }

  private void insertHash(String hash) throws Exception
  {
    if (getDescription().contains(hash)) return;

    Connection db = null;
    PreparedStatement statement = null;
    try
    {
      db = getDbConnection();

      db.setAutoCommit(false);

      statement = db.prepareStatement("MERGE INTO BLOCK_INDEX VALUES (?)");
      statement.setString(1, hash);
      statement.execute();

      db.commit();

      getDescription().add(hash);
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
  public Set<String> describe()
    throws Exception
  {
    return Collections.unmodifiableSet(getDescription());
  }

  private Set<String> getDescription()
    throws Exception
  {
    if (_description != null)
    {
      return _description;
    }

    synchronized (this) {
      if (_description == null) {
        Set<String> description = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

        Connection db = null;
        PreparedStatement statement = null;

        try
        {
          db = getDbConnection(); //getReadOnlyDbConnection();

          statement = db.prepareStatement("SELECT * FROM BLOCK_INDEX");

          ResultSet resultSet = statement.executeQuery();

          while (resultSet.next())
          {
            description.add(resultSet.getString("HASH"));
          }

          _description = description;
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

    return _description;
  }

  public Set<String> rebuildHashIndexFromDisk()
  {
    final Set<String> hashes = new HashSet<String>();

    FileWalker.enumerateFolders(_rootPath, new FileHandler()
    {
      @Override
      public boolean skipDir(File file)
      {
        return false;
      }

      @Override
      public void process(File file)
      {
        String fileName = file.getName();
        if (fileName.endsWith(".tmp") || fileName.endsWith(".db")) return;
        hashes.add(fileName);
      }
    });

    return hashes;
  }
}

