package cloudcmd.common;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlUtil
{
  public static void SafeClose(Statement st)
  {
    if (st == null) return;

    try
    {
      st.close();
    }
    catch (SQLException e)
    {
      e.printStackTrace();
    }
  }

  public static void SafeClose(Connection connection)
  {
    if (connection == null) return;

    try
    {
      connection.setAutoCommit(true);
      connection.close();
    }
    catch (SQLException e)
    {
      e.printStackTrace();
    }
  }
}
