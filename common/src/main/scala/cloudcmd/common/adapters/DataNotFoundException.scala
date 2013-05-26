package cloudcmd.common.adapters

import cloudcmd.common.{ContentAddressableStorage, BlockContext}

class BlockException(val ctx: BlockContext) extends Exception {}

class DataNotFoundException(ctx: BlockContext) extends BlockException(ctx) {}

class MultiWriteBlockException(ctx: BlockContext, adapters: List[IndexedAdapter]) extends BlockException(ctx) {}

class CASWriteBlockException(ctx: BlockContext, cas: ContentAddressableStorage) extends BlockException(ctx) {}

class AdapterFullException(ctx: BlockContext, val adapter: DirectAdapter) extends BlockException(ctx) {}