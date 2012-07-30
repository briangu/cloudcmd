package cloudcmd.common.adapters

import java.io.InputStream

trait MD5Storable {
  def store(data: InputStream, hash: String, md5: Array[Byte], length: Long)
}