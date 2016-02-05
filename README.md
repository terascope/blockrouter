BlockRouter

Socket listener that will distribute a Data stream to multiple destination ports.

Used for load distribution (round robin) and spliting a single incoming Data stream to multiple backends for additional processing. Data is cached on disk prior to distribution. The flow of data into the backends is more data block oriented than truly a stream but the effect is the same.

