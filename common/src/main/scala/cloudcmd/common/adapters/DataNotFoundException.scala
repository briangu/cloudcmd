package cloudcmd.common.adapters

import cloudcmd.common.BlockContext

class DataNotFoundException(val ctx: BlockContext) extends Exception {}