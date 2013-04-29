package cloudcmd.common

import java.io.InputStream

trait ContentAddressableStorage {

  /***
   * Refresh the internal cache, which may be time consuming
   */
  def refreshCache()

  /***
   * Gets if the CAS contains the specified block.
   * @param hash
   * @return
   */
  def contains(hash: String): Boolean = containsAll(Set(hash)).get(hash).get

  /***
   * Gets if the CAS contains the specified blocks.
   * @param hashes
   * @return
   */
  def containsAll(hashes: Set[String]) : Map[String, Boolean]

  /***
   * Removes the specified block from the CAS.
   * Subsequently, for the same block, contains() will return false and load will throw DataNotFoundException.
   * @param hash
   * @return
   */
  def remove(hash: String): Boolean = removeAll(Set(hash)).get(hash).get

  /***
   * Removes the specified blocks.
   * @param hashes
   * @return
   */
  def removeAll(hashes: Set[String]) : Map[String, Boolean]

  /***
   * Ensure block level consistency with respect to the CAS implementation
   * @param hash
   * @param blockLevelCheck
   * @return
   */
  def ensure(hash: String, blockLevelCheck: Boolean = false): Boolean = ensureAll(Set(hash), blockLevelCheck).get(hash).get

  /***
   * Ensure block level consistency with respect to the CAS implementation
   * @param hashes
   * @param blockLevelCheck
   * @return
   */
  def ensureAll(hashes: Set[String], blockLevelCheck: Boolean = false) : Map[String, Boolean]

  /***
   * Store the specified block in accordance with the CAS implementation.
   * @param hash
   * @param is
   * @return
   */
  def store(hash: String, is: InputStream)

  /***
   * Load the specified block from the CAS.
   * @param hash
   * @return
   */
  def load(hash: String) : (InputStream, Int)

  /***
   * List all the block hashes stored in the CAS.
   * @return
   */
  def describe() : Set[String]

}
