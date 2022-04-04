# go-memdbfs

This takes the excellent github.com/hashicorp/go-memdb and provides a serializer and deserializer. This might be used to periodically checkpoint the data to disk or to load up from a version that had been on disk previously.