package cloudcmd.common.adapters

//     "s3://<aws id>@<bucket>?tier=2&tags=s3&secret=<aws secret>"

class S3Adapter extends DescriptionCacheAdapter(new DirectS3Adapter) {}