package cloudcmd.common.index

trait   IndexStorageListener {
  def onMessage(msg: String)
}