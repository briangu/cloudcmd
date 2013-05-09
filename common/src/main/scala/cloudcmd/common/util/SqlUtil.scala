package cloudcmd.common.util

import java.sql.{Connection, SQLException, Statement, PreparedStatement}

object SqlUtil {

  def bindVar(statement: PreparedStatement, idx: Int, obj: AnyRef) {
    if (obj.isInstanceOf[String]) {
      statement.setString(idx, obj.asInstanceOf[String])
    }
    else if (obj.isInstanceOf[Long]) {
      statement.setLong(idx, obj.asInstanceOf[Long])
    }
    else if (obj.isInstanceOf[java.lang.Integer]) {
      statement.setInt(idx, obj.asInstanceOf[java.lang.Integer])
    }
    else if (obj == null) {
      statement.setString(idx, null)
    }
    else {
      throw new IllegalArgumentException("unknown obj type: " + obj.toString)
    }
  }

  def SafeClose(st: Statement) {
    if (st != null) {
      try {
        st.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
  }

  def SafeClose(connection: Connection) {
    if (connection != null) {
      try {
        connection.setAutoCommit(true)
        connection.setReadOnly(false)
        connection.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
  }
}
