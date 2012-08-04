package cloudcmd.common

import java.io.InputStream

trait ContentAddressableStorage {

  /***
   * Refresh the internal cache, which may be time consuming
   */
  def refreshCache()

  /***
   * Gets if the CAS contains the specified block.
   * @param ctx
   * @return
   */
  def contains(ctx: BlockContext): Boolean = containsAll(Set(ctx)).get(ctx).get

  /***
   * Gets if the CAS contains the specified blocks.
   * @param ctxs
   * @return
   */
  def containsAll(ctxs: Set[BlockContext]) : Map[BlockContext, Boolean]

  /***
   * Removes the specified block from the CAS.
   * Subsequently, for the same block, contains() will return false and load will throw DataNotFoundException.
   * @param ctx
   * @return
   */
  def remove(ctx: BlockContext): Boolean = removeAll(Set(ctx)).get(ctx).get

  /***
   * Removes the specified blocks.
   * @param ctxs
   * @return
   */
  def removeAll(ctxs: Set[BlockContext]) : Map[BlockContext, Boolean]

  /***
   * Ensure block level consistency with respect to the CAS implementation
   * @param ctx
   * @param blockLevelCheck
   * @return
   */
  def ensure(ctx: BlockContext, blockLevelCheck: Boolean = false): Boolean = ensureAll(Set(ctx), blockLevelCheck).get(ctx).get

  /***
   * Ensure block level consistency with respect to the CAS implementation
   * @param ctxs
   * @param blockLevelCheck
   * @return
   */
  def ensureAll(ctxs: Set[BlockContext], blockLevelCheck: Boolean = false) : Map[BlockContext, Boolean]

  /***
   * Store the specified block in accordance with the CAS implementation.
   * @param ctx
   * @param is
   * @return
   */
  def store(ctx: BlockContext, is: InputStream)

  /***
   * Load the specified block from the CAS.
   * @param ctx
   * @return
   */
  def load(ctx: BlockContext) : InputStream

  /***
   * List all the block hashes stored in the CAS.
   * @return
   */
  def describe() : Set[BlockContext]

  /***
   * List all hashes stored in the CAS without regard to block context.  There may be hashes stored in the CAS which are
   * not returned in describe(), so this method can help identify unreferenced blocks.
   * @return
   */
  def describeHashes() : Set[String]

}
