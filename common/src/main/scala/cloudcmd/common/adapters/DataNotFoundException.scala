package cloudcmd.common.adapters

class DataNotFoundException(hash: String) extends Exception {
  var Hash: String = hash
}