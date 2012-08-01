package cloudcmd.common.adapters

//     "file:///tmp/storage?tier=1&tags=image,movie,vacation"

class FileAdapter extends DescriptionCacheAdapter(new DirectFileAdapter) {
}