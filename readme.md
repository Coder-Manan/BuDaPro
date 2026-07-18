# BuDaPro
**Bu**ffered **Da**ta **Pro**vider and Relay is a thread safe, generic component that can be used in any setup that constitutes of multiple listeners processing the same set of data in their own ways such that there is a single source of input data.

## Features
1. BuDaPro ingests data from the source and places it in an internal buffer array
2. A separate thread is spun up for each consumer from inside the relay, which watches the internal buffer and supplies data to the consumers
3. Allows multiple consumers running on different threads (and physical cores) to consume data simultaneously at different indices in the internal array, allowing consumers to run at their own pace
4. Use of incremental sequence numbers to inform users regarding any gaps

## Technical details
1. The relay sits in between the singular source of raw data and consumers
2. A thread of the relay (let's call it the `writer thread` for naming purposes) watches for new raw data, and puts it in the internal array along with a `sequence number` that can be used by consumers to spot any gaps in the data processed by them
3. Thread safety is guaranteed by locks on individual indices of the internal buffer array - there is a shared mutex for each index, with which the writer thread takes up a unique lock and the worker threads for each consumer take up a shared lock
4. Whenever a new subscriber wants to subscribe to data, it has to give two function objects - 
    1. A function that takes in the size of next data packet, and returns a pointer to void where data will be put
    2. A callback function which will be called after data is put into the buffer pointed to by the pointer returned by the other function.
> NOTE: The maximum size of a data packet is assummed to be known by consumers, relay and the raw data provider
5. For each subscriber, a new worker thread is spun up which watches for new data in the internal buffer, caches it internally when available (so that slow consumers do not block the internal buffer array), then passes it to the consumer.

## Implementation Details

### 1. RawDataConsumerAndRelay
This is the relay class. It has an internal struct type called `internal_buffer` which holds a pointer to a buffer, the size of the current data packet and the internal sequence number. All atomics, mutexes and condition variables used in the writer thread and the consumer worker threads are contained inside it.

- The <b><u>`start`</u></b> function is called when the operations have to be started. It is passed a function `get_network_data` which is blocked on, to get new data. `start` spins up a new thread which runs `raw_source_and_internal_queue_coordinator` which is passed `get_network_data` and a buffer pointer which is allocated within `start`
- <b><u>`get_network_data`</u></b> should take in a buffer (void *) and populate it with data, and return the number of bytes written
- The <b><u>`raw_source_and_internal_queue_coordinator`</u></b> function runs a loop inside which it checks for new data by calling `get_network_data`, then copies that data into the buffer it was given, along with the size and sequence number. It returns when the relay is stopped.
- <b><u>`stop`</u></b> just sets an atomic boolean flag which is checked by the writer thread and consumer worker threads to stop working.
- <b><u>`subscribe_to_md`</u></b> should be called by a consumer to subscribe itself to data. `subscribe_to_md` requires 
    - `get_next_data_loc_fn` -> called with a `size_t` argument to get a pointer to a buffer where the worker thread will place data. The worker thread blocks on it to get the buffer pointer
    - `data_copied_cb` -> a callback called by the worker thread to indicate that data has been produced.
- The design has been made in such a way that the worker threads act as proxies of consumers. Consumers can offload all processing to these threads via `data_copied_cb`.
- `subcribe_to_md` spins up a new thread with a lambda that watches the internal buffer array, copies data into a local buffer, checks for gaps, then gets location of the consumer's buffer and places data in that buffer.
    - The local buffer is used so that if `get_next_data_loc_fn` blocks the worker thread, we do not hold the read lock to the internal buffer array for long
- Since some consumers can be fast, there are condition variables, one for each index of the internal buffer array, on which the worker threads sleep if new data is not available. The predicate for waking up also includes the `m_stopped` flag so that workers don't keep sleeping when operations stop.

## Next Targets/Goals
These are the first next TODO steps in planning currently. They are not the end goals of this project, and single sections might be incomplete
### 1. Benchmarking
1. Write a sample application that uses this component to distribute real world market data to multiple consumers
2. Test out performance in cases where single data packets fit on a single cache line vs when they don't
3. Test out various sizes of internal buffer array of the relay

### 2. Testing different implementation choices
1. Using `mmap` to get page level memory allocation and dividing it internally
2. Allowing users to start relay with a mode to wait for all worker threads to consumer data before writing new data in internal buffer array