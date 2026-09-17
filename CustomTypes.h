#pragma once

#include <memory>
#include <stdlib.h>

struct FreeDeleter
{
    void operator()(void *p) const noexcept
    {
        std::free(p);
    }
};

// for use with buffers allocated malloc, calloc, etc.
using unique_ptr_void = std::unique_ptr<void, FreeDeleter>;

template<class T> requires (sizeof(T) <= 64)
struct alignas(std::bit_ceil(sizeof(T))) CacheAccessOptimizedStruct : public T {};