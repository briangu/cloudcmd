package cloudcmd.common.engine

object BlockCacheService {
  def instance: BlockCache = _instance
  private val _instance = new LocalBlockCache
}