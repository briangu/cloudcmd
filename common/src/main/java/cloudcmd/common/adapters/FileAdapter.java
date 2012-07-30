package cloudcmd.common.adapters;

import cloudcmd.common.*;
import cloudcmd.common.engine.FileWalker;
import org.h2.jdbcx.JdbcConnectionPool;
import org.jboss.netty.buffer.ChannelBuffer;
import org.json.JSONException;

import java.io.*;
import java.net.URI;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

//     "file:///tmp/storage?tier=1&tags=image,movie,vacation"

public class FileAdapter extends Adapter implements InlineStorable {
  private final static int MIN_FREE_STORAGE_SIZE = 1024 * 1024;
  private final static int LARGE_FILE_CUTOFF = 128 * 1024 * 1024;

  String _rootPath;
  JdbcConnectionPool _cp = null;
  volatile Set<String> _description = null;
  String _dbDir = null;
  String _dataDir = null;

  public FileAdapter() {}

  @Override
  public void init(String configDir, Integer tier, String type, Set<String> tags, URI config) throws Exception {
    super.init(configDir, tier, type, tags, config);

    _rootPath = URI.getPath();
    _dbDir = _rootPath + File.separator + "db";
    ConfigDir = _dbDir;
    _dataDir = _rootPath + File.separator + "data";

    File rootPathDir = new File(_rootPath);
    rootPathDir.mkdirs();
    _isOnline = rootPathDir.exists();

    if (_isOnline) bootstrap(_dataDir, _dbDir);
  }

  private String getDbFileName(String dbPath) {
    return String.format("%s%sindex", dbPath, File.separator);
  }

  private String createConnectionString(String dbPath) {
    return String.format("jdbc:h2:%s", getDbFileName(dbPath));
  }

  private Connection getDbConnection() throws SQLException {
    return _cp.getConnection();
  }

  private Connection getReadOnlyDbConnection() throws SQLException {
    Connection conn = getDbConnection();
    conn.setReadOnly(true);
    return conn;
  }

  private void bootstrap(String dataPath, String dbPath)
    throws Exception {
    Class.forName("org.h2.Driver");
    _cp = JdbcConnectionPool.create(createConnectionString(dbPath), "sa", "sa");
    File file = new File(getDbFileName(dbPath) + ".h2.db");
    if (!file.exists()) {
      bootstrapDb();
      initSubDirs(dataPath);
    }
  }

  private void bootstrapDb() {
    Connection db = null;
    Statement st = null;
    try {
      db = getDbConnection();
      st = db.createStatement();

      st.execute("DROP TABLE if exists BLOCK_INDEX;");
      st.execute("CREATE TABLE BLOCK_INDEX ( HASH VARCHAR PRIMARY KEY );");

      db.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      SqlUtil.SafeClose(st);
      SqlUtil.SafeClose(db);
    }
  }

  @Override
  public boolean IsOnLine() {
    return _isOnline;
  }

  @Override
  public boolean IsFull() {
    return new File(_rootPath).getUsableSpace() < MIN_FREE_STORAGE_SIZE;
  }

  private static void initSubDirs(String rootPath) {
    for (int i = 0; i < 0x100; i++) {
      File tmpFile = new File(rootPath + File.separator + String.format("%02x", i));
      tmpFile.mkdirs();
    }
  }

  @Override
  public void refreshCache() throws Exception {
    // TODO: there must be a more efficient sequence of set operations
    Set<String> foundHashes = rebuildHashIndexFromDisk();
    Set<String> copyFoundHashes = new HashSet<String>(foundHashes);
    Set<String> description = getDescription();

    // find missing by removing intersection
    foundHashes.removeAll(description);
    addToDb(foundHashes);

    // delete description entries not in the found hashes
    description.removeAll(copyFoundHashes);
    deleteFromDb(description);
  }

  private void addToDb(Collection<String> hashes) {
    Connection db = null;
    PreparedStatement statement = null;
    try {
      db = getDbConnection();

      db.setAutoCommit(false);

      statement = db.prepareStatement("INSERT INTO BLOCK_INDEX VALUES (?)");

      int k = 0;

      for (String hash : hashes) {
        statement.setString(1, hash);
        statement.addBatch();

        if (++k > 1024) {
          statement.executeBatch();
          k = 0;
        }
      }

      statement.executeBatch();

      db.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      SqlUtil.SafeClose(statement);
      SqlUtil.SafeClose(db);
    }
  }

  @Override
  public boolean contains(String hash) throws Exception {
    return getDescription().contains(hash);
  }

  @Override
  public void shutdown() {
    if (_cp != null) {
      _cp.dispose();
    }
  }

  @Override
  public boolean remove(String hash) throws Exception {
    File file = new File(getDataFileFromHash(hash));
    if (file.exists()) file.delete();
    deleteFromDb(Arrays.asList(hash));
    return true;
  }

  private void deleteFromDb(Collection<String> hashes) {
    Connection db = null;
    PreparedStatement statement = null;
    try {
      db = getDbConnection();

      db.setAutoCommit(false);

      statement = db.prepareStatement("DELETE FROM BLOCK_INDEX WHERE HASH = ?");

      int k = 0;

      for (String hash : hashes) {
        statement.setString(1, hash);
        statement.addBatch();

        if (++k > 1024) {
          statement.executeBatch();
          k = 0;
        }
      }

      statement.executeBatch();

      db.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      SqlUtil.SafeClose(statement);
      SqlUtil.SafeClose(db);
    }
  }

  @Override
  public boolean verify(String hash) throws Exception {
    File file = new File(getDataFileFromHash(hash));
    if (!file.exists()) return false;
    int idx = hash.lastIndexOf(".");
    if (idx >= 0) {
      hash = hash.substring(0, idx);
    }
    return (CryptoUtil.computeHashAsString(file).equals(hash));
  }

  private String getHashFromDataFile(String hash) {
    int idx = hash.lastIndexOf(".");
    return (idx >= 0) ? hash.substring(0, idx) : hash;
  }

  private String getPathFromHash(String hash) throws JSONException {
    return _dataDir + File.separator + hash.substring(0, 2);
  }

  private String getDataFileFromHash(String hash) throws JSONException {
    return getPathFromHash(hash) + File.separator + hash;
  }

  @Override
  public void store(InputStream is, String hash) throws Exception {
    String writeHash = FileUtil.writeFileAndComputeHash(is, new File(getDataFileFromHash(hash)));
    if (!writeHash.equals(getHashFromDataFile(hash))) {
      throw new RuntimeException(String.format("failed to store data: expected %s got %s", hash, writeHash));
    }
    insertHash(hash);
  }

  @Override
  public String store(InputStream is) throws Exception {
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

  private String storeLargeFile(InputStream is) throws Exception {
    File tmpFile = new File(_dataDir + File.separator + UUID.randomUUID().toString() + ".tmp");
    tmpFile.createNewFile();
    String hash = FileUtil.writeFileAndComputeHash(is, tmpFile);
    File newFile = new File(getDataFileFromHash(hash));
    if (newFile.exists() && newFile.length() == tmpFile.length()) {
      tmpFile.delete();
    } else {
      Boolean success = tmpFile.renameTo(newFile);
      if (!success) {
        tmpFile.delete();
        throw new IOException("failed to move file: " + tmpFile.getAbsolutePath());
      }
    }
    insertHash(hash);
    return hash;
  }

  @Override
  public InputStream load(String hash) throws Exception {
    File file = new File(getDataFileFromHash(hash));
    if (!file.exists()) {
      System.err.println(String.format("could not find hash %s on %s.", hash, URI.toString()));
      throw new DataNotFoundException(hash);
    }
    return new FileInputStream(file);
  }

  @Override
  public ChannelBuffer loadChannel(String hash) throws Exception {
    File file = new File(getDataFileFromHash(hash));
    if (!file.exists()) {
      System.err.println(String.format("could not find hash %s on %s.", hash, URI.toString()));
      throw new DataNotFoundException(hash);
    }
    return new FileChannelBuffer(file);
  }

  private void insertHash(String hash) throws Exception {
    if (getDescription().contains(hash)) return;

    Connection db = null;
    PreparedStatement statement = null;
    try {
      db = getDbConnection();

      statement = db.prepareStatement("INSERT INTO BLOCK_INDEX VALUES (?)");
      statement.setString(1, hash);
      statement.execute();

      getDescription().add(hash);
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      SqlUtil.SafeClose(statement);
      SqlUtil.SafeClose(db);
    }
  }

  @Override
  public Set<String> describe()
    throws Exception {
    return Collections.unmodifiableSet(getDescription());
  }

  private Set<String> getDescription()
    throws Exception {
    if (_description != null) {
      return _description;
    }

    synchronized (this) {
      if (_description == null) {
        Set<String> description = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

        Connection db = null;
        PreparedStatement statement = null;

        try {
          db = getReadOnlyDbConnection();

          statement = db.prepareStatement("SELECT HASH FROM BLOCK_INDEX");

          ResultSet resultSet = statement.executeQuery();

          while (resultSet.next()) {
            description.add(resultSet.getString("HASH"));
          }

          _description = description;
        } catch (SQLException e) {
          e.printStackTrace();
        } finally {
          SqlUtil.SafeClose(statement);
          SqlUtil.SafeClose(db);
        }
      }
    }

    return _description;
  }

  public Set<String> rebuildHashIndexFromDisk() {
    final Set<String> hashes = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    FileWalker.enumerateFolders(_dataDir, new FileWalker.FileHandler() {
      @Override
      public boolean skipDir(File file) {
        return false;
      }

      @Override
      public void process(File file) {
        hashes.add(file.getName());
      }
    });

    return hashes;
  }
}

