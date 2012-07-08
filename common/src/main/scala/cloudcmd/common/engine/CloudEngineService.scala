package cloudcmd.common.engine

object CloudEngineService {
  def instance: CloudEngine = _instance
  private val _instance = new ParallelCloudEngine
}