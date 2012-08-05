package cloudcmd.common.adapters

import cloudcmd.common.BlockContext
import java.io.InputStream

class DirectHttpAdapter extends Adapter {

  /***
   * Refresh the internal cache, which may be time consuming
   */
  def refreshCache() {}

  /***
   * Gets if the CAS contains the specified blocks.
   * @param ctxs
   * @return
   */
  def containsAll(ctxs: Set[BlockContext]) : Map[BlockContext, Boolean] = {

  }

  /***
   * Removes the specified blocks.
   * @param ctxs
   * @return
   */
  def removeAll(ctxs: Set[BlockContext]) : Map[BlockContext, Boolean] = {

  }

  /***
   * Ensure block level consistency with respect to the CAS implementation
   * @param ctxs
   * @param blockLevelCheck
   * @return
   */
  def ensureAll(ctxs: Set[BlockContext], blockLevelCheck: Boolean) : Map[BlockContext, Boolean] = {

  }

  /***
   * Store the specified block in accordance with the CAS implementation.
   * @param ctx
   * @param is
   * @return
   */
  def store(ctx: BlockContext, is: InputStream) {

  }

  /***
   * Load the specified block from the CAS.
   * @param ctx
   * @return
   */
  def load(ctx: BlockContext) : (InputStream, Int) = {

  }

  /***
   * List all the block hashes stored in the CAS.
   * @return
   */
  def describe() : Set[BlockContext] = {

  }

  /***
   * List all hashes stored in the CAS without regard to block context.  There may be hashes stored in the CAS which are
   * not returned in describe(), so this method can help identify unreferenced blocks.
   * @return
   */
  def describeHashes() : Set[BlockContext] = {

  }

  def shutdown() {}
}
