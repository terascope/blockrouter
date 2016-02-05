BlockRouter

Socket listener that will distribute a Data stream broken up into block to multiple destination ports.

Used for load distribution (round robin) and spliting a single incoming Data stream into multiple streams for additional processing. Data is cached on disk prior to distribution.

