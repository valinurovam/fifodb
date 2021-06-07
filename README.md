## FifoDB - persistent fifo disk-based queue

Thread-safe FIFO-queue backend by a filesystem and acknowledgement opportunity.

### Motivation

- LSM based storage (like Badger, RocksDB, LevelDB, etc) are very nice and powerful, but much strong and has overhead for LSM mechanics
- LSM based storage need more resources for each instance (arena size, io/compaction loops). So we can't get new instance for every queue, only for every virtual host and need to implement bucket system on keys like 'vhost-name.queue-name.'
- need simple embedded native FIFO disk storage
- have fun and learn a lot :)


### Basic idea
Basic ideas of storage is
- stores data in 64MB segments for compaction performance (configurable)
- stores data strongly in FIFO order
- retrieves data strongly in FIFO order
- supports 4 main methods - Push, Pop, Ack

### Usage



### Internals
We need 2 additional and strange methods 'Ack' and 'Nack' to handle 'At least once' delivery guarantees after server crash or restart.
User can simply 'Push' data into queue tail and 'Pop' data from queue head.
'Pop' always returns NEXT message from queue after 'Open' and ignores Nack-ed messages. 'Ack' and 'Nack' are only for compaction
and rebuilding queue after server crash or restart.