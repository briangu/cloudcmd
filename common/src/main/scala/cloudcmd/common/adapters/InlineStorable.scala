package cloudcmd.common.adapters

import java.io.InputStream

trait InlineStorable {
  def store(data: InputStream): String
}