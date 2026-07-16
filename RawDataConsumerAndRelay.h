#include <condition_variable>
#include <cstdint>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include "CustomTypes.h"

namespace data_handling_framework
{
    using listener_count_t = uint16_t;
    using seq_num_t = uint64_t;

    template <size_t N>
    class RawDataConsumerAndRelay
    {

        // have separate packets on separate cache lines
        struct alignas(std::hardware_destructive_interference_size) internal_packet
        {
            // to enable consumers to check for any gaps
            seq_num_t seq_num;
            size_t curr_pkt_data_size;
            unique_ptr_void buf_ptr;

            explicit internal_packet(size_t size) : buf_ptr{malloc(size)}
            {
                if (buf_ptr == nullptr)
                {
                    throw std::runtime_error("Failed to init packet buffer");
                }
            }
        };

        // helper to construct array of internal buffers
        template <size_t... Is>
        static std::array<internal_packet, N>
        make_array(std::index_sequence<Is...>,
                   size_t size)
        {
            return {(void(Is), internal_packet{size})...};
        }

        std::array<internal_packet, N> m_internal_buffer_arr;
        std::array<std::shared_mutex, N> m_internal_buffer_mutex_arr;
        // so that fast consumers do not spin and waste cpu resources
        std::array<std::condition_variable_any, N> m_new_data_ready_cv_arr;

        // one thread for each consumer
        std::vector<std::jthread> m_subscriber_data_relay_threads;
        // thread consuming raw data from the source of truth
        std::jthread m_raw_data_consumer_thread;

        // max raw packet size
        size_t m_max_packet_size;

        std::atomic<bool> m_stopped;

        // fetches data from the source and places it in the internal buffer
        // get_data_fun - should take in a void*, populate the location of
        //                data into it and return the number of bytes in the packet
        void m_raw_source_and_internal_queue_coordinator(auto &&get_data_fn, unique_ptr_void buf_ptr)
        {
            size_t cur_pkt_size, next_write_idx{0};
            uint64_t next_seq_num{1};

            while (!m_stopped.load(std::memory_order_relaxed))
            {
                cur_pkt_size = get_data_fn(buf_ptr.get());
                std::unique_lock lock{m_internal_buffer_mutex_arr.at(next_write_idx)};
                m_internal_buffer.at(next_write_idx).curr_pkt_data_size = cur_pkt_size;
                memcpy(m_internal_buffer.at(next_write_idx).buf_ptr.get(), buf_ptr.get(), cur_pkt_size);
                m_internal_buffer.at(next_write_idx).seq_num.store(next_seq_num++, std::memory_order_relaxed);
            }

            m_new_data_ready_cv_arr.at(next_write_idx).notify_all();

            next_write_idx++;
            if (next_write_idx == N) [[unlikely]]
            {
                next_write_idx = 0;
            }
        }

        // function that runs on a separate thread, puts data from the source of truth to internal buffer
        // takes a buffer pointer to use between reading from source and writing to internal buffer
        void raw_source_and_internal_queue_coordinator(std::function<size_t(void *)> get_network_data, unique_ptr_void buf_ptr)
        {
            size_t cur_pkt_size;
            uint64_t next_write_idx{0}, next_seq_num{1};
            while (!m_stopped.load(std::memory_order_relaxed))
            {
                cur_pkt_size = get_data_fn(buf_ptr.get());
                {
                    std::unique_lock lock{m_internal_buffer_mutex_arr.at(next_write_idx)};
                    m_internal_buffer.at(next_write_idx).curr_pkt_data_size = cur_pkt_size;
                    memcpy(m_internal_buffer.at(next_write_idx).buf_ptr.get(), buf_ptr.get(), cur_pkt_size);
                    m_internal_buffer.at(next_write_idx).seq_num.store(next_seq_num++, std::memory_order_relaxed);
                }

                new_data_ready_cv_arr.at(next_write_idx).notify_all();
                next_write_idx++;
                if (next_write_idx == N) [[unlikely]]
                {
                    next_write_idx = 0;
                }
            }
        }

    public:
        explicit RawDataConsumerAndRelay(listener_count_t expected_number_of_subscribers, size_t max_packet_size) : m_max_packet_size{max_packet_size},
                                                                                                                    m_internal_buffer{
                                                                                                                        RawMarketDataHandler::make_array(std::make_index_sequence<N>{}, max_packet_size)},
                                                                                                                    m_num_active_subcribers{0}, m_stopped{false}
        {
            m_subscriber_data_relay_threads.reserve(expected_number_of_subscribers);
        };

        ~RawMarketDataHandler()
        {
            // stop if not already stopped
            m_stopped = true;
        }

        // called by consumers to register themselves
        //      get_next_data_loc_fn -> takes in the size of packet and gives a ptr to where we copy the data
        //      data_copied_cb -> callback called after the data packet has been successfully copied
        bool subscribe_to_md(std::function<void *(size_t)> get_next_data_loc_fn, std::function<void()> data_copied_cb)
        {
            m_subscriber_data_relay_threads.emplace_back(
                [&]()
                {
                    unique_ptr_void buffer{malloc(m_max_packet_size)};
                    if (internal_packet == nullptr)
                    {
                        // not throwing an exception, that can cause the whole process to die
                        return false;
                    }

                    uint64_t next_seq_num{1}, next_read_idx{0}, curr_pkt_seq_num;
                    size_t curr_pkt_size;

                    while (!m_stopped.load(std::memory_order_relaxed))
                    {
                        if (m_internal_buffer.at(next_read_idx).seq_num.load(std::memory_order_relaxed) < next_seq_num)
                        {
                            std::shared_lock lk{m_internal_buffer_mutex_arr.at(next_read_idx)};
                            // wake up when we see data with higher seq num or ops have stopped
                            m_new_data_ready_cv_arr.at(next_read_idx).wait(lk, [&]
                                                                           { return (m_internal_buffer.at(next_read_idx).seq_num.load(std::memory_order_relaxed) >= next_seq_num || m_stopped); });
                        }

                        if (m_stopped) [[unlikely]]
                        {
                            return;
                        }

                        {
                            std::shared_lock read_lock(m_internal_buffer_mutex_arr.at(next_read_idx));
                            next_seq_num = m_internal_buffer.at(next_read_idx).seq_num.load(std::memory_order_relaxed) + 1;
                            curr_pkt_size = m_internal_buffer.at(next_read_idx).curr_pkt_data_size;
                            memcpy(internal_buffer.get(), m_internal_buffer.at(next_read_idx).buf_ptr.get(), curr_pkt_size);
                        }

                        next_read_idx++;
                        if (next_read_idx == N) [[unlikely]]
                        {
                            next_read_idx = 0;
                        }
                        next_seq_num++;

                        void *dest = get_next_data_loc_fn(curr_pkt_size);
                        if (!dest) [[unlikely]]
                        {
                            // an issue with the consumer, stop current thread
                            return;
                        }

                        memcpy(dest, internal_buffer.get(), curr_pkt_size);
                        data_copied_cb();
                    }
                });
            return true;
        }

        // start listening to data from the source on a new thread
        // get_network_data should take a ptr to a buffer, put data
        //      and return the number of bytes copied
        bool start(std::function<size_t(void *)> get_network_data)
        {
            unique_ptr_void buf{malloc(m_max_packet_size)};
            if (buf.get() == nullptr)
            {
                return false;
            }

            m_raw_data_consumer_thread = std::jthread([this, &get_network_data, buf = std::move(buf)]() mutable
                                                          { raw_source_and_internal_queue_coordinator(get_network_data, std::move(buf)); });
        }

        void stop()
        {
            m_stopped = true;
            for (auto &cv : m_new_data_ready_cv_arr)
            {
                cv.notify_all();
            }
        }
    };
}