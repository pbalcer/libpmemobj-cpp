/*
 * Copyright 2020, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/container/segment_vector.hpp>
#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj_cpp_examples_common.hpp>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <algorithm>
#include <future>
#include <vector>
#include <numeric>

#define LAYOUT "vecs"

namespace
{

enum vecs_op {
	UNKNOWN_VECS_OP,
	VECS_INSERT,
	VECS_DROP,
	VECS_ITERATE,

	MAX_VECS_OP,
};

/* vecs operations strings */
const char *ops_str[MAX_VECS_OP] =
	{"", "insert", "drop", "iter"};

/*
 * parse_vecs_op -- parses the operation string and returns matching queue_op
 */
vecs_op
parse_vecs_op(const char *str)
{
	for (int i = 0; i < MAX_VECS_OP; ++i)
		if (strcmp(str, ops_str[i]) == 0)
			return (vecs_op)i;

	return UNKNOWN_VECS_OP;
}
}

namespace pobj = pmem::obj;
using object_key_type = pobj::p<uint64_t>;

namespace examples
{

static const size_t OBJECT_DATA_LEN = (1 << 14); /* 16kb */
static const size_t NTHREADS = 8;

struct object_value {
	pobj::array<uint8_t, OBJECT_DATA_LEN> data;
};

struct object_collection {
    pobj::segment_vector<object_value, pobj::fixed_size_vector_policy<1024>> vec;
};

using object_map_type = pobj::concurrent_hash_map<object_key_type, object_collection>;

class persistent_sink {
public:
    void init(pmem::obj::pool_base &pop) {
        if (object_map == nullptr) {
            pmem::obj::transaction::run(pop, [&] {
                object_map = pobj::make_persistent<object_map_type>();
            });
        }
        object_map->runtime_initialize();
    }

    void insert(object_key_type key, object_value value) {
        object_map_type::accessor accessor;
        object_map->insert(accessor, key);
        auto c = &accessor->second;

        c->vec.emplace_back(value);
    }

    bool foreach(object_key_type key, std::function<void(const object_value &value)> cb) {
        object_map_type::const_accessor accessor;
        if (!object_map->find(accessor, key))
            return false;

        auto c = &accessor->second;

        std::vector<std::future<void>> futures;
        auto itr = c->vec.cbegin();
        size_t partsize = c->vec.size() > NTHREADS ? c->vec.size() / NTHREADS : c->vec.size();

        do {
            size_t r = (size_t)std::distance(itr, c->vec.cend());
            auto partial_iter_end = itr +
                                    (ssize_t)std::min(r, partsize);
            futures.emplace_back(std::async(std::launch::async, [=] {
                for (auto n = itr; n != partial_iter_end; ++n)
                    cb((*n));
            }));
            itr = partial_iter_end;
        } while (itr != c->vec.cend());

        for (auto &iter: futures)
            iter.wait();

        return true;
    }

    int drop(object_key_type key) {
        object_map_type::accessor accessor;
        if (!object_map->find(accessor, key))
            return false;

        auto c = &accessor->second;

        c->vec.clear();

        return 0;
    }

private:
    pobj::persistent_ptr<object_map_type> object_map;
};

} /* namespace examples */

int
main(int argc, char *argv[])
{
	if (argc < 3) {
		std::cerr << "usage: " << argv[0]
			  << " file-name [populate|drop|iter|iter_mt|iter_omp]"
			  << std::endl;
		return 1;
	}

	const char *path = argv[1];

	vecs_op op = parse_vecs_op(argv[2]);

	pobj::pool<examples::persistent_sink> pop;

	if (file_exists(path) != 0) {
		pop = pobj::pool<examples::persistent_sink>::create(
			path, LAYOUT, PMEMOBJ_MIN_POOL * 10, CREATE_MODE_RW);
	} else {
		pop = pobj::pool<examples::persistent_sink>::open(path, LAYOUT);
	}

	object_key_type ktype = 1234;
	examples::object_value empty_object {};

	auto v = pop.root();
	v->init(pop);

	switch (op) {
		case VECS_INSERT:
		    v->insert(ktype, empty_object);
			break;
		case VECS_DROP:
		    v->drop(ktype);
			break;
		case VECS_ITERATE:
		    v->foreach(ktype, [] (const examples::object_value &) {
                std::cout << "object callback" << std::endl;
            });
			break;
		default:
			throw std::invalid_argument("invalid vecs operation");
	}

	pop.close();

	return 0;
}
