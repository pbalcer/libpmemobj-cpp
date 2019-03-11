/*
 * Copyright 2019, Intel Corporation
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

/*
 * simplekv.cpp -- implementation of a simple key-value store
 */

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/experimental/array.hpp>
#include <libpmemobj++/experimental/string.hpp>
#include <libpmemobj_cpp_examples_common.hpp>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <bitset>

#define LAYOUT "simplekv"

namespace
{

enum simplekv_op {
	UNKNOWN_SIMPLEKV_OP,
	SIMPLEKV_CREATE,
	SIMPLEKV_GET,
	SIMPLEKV_PUT,
	SIMPLEKV_REMOVE,

	MAX_SIMPLEKV_OP,
};

/* queue operations strings */
const char *ops_str[MAX_SIMPLEKV_OP] =
	{"", "create", "get", "put", "remove"};

/*
 * parse_simplekv_op -- parses the operation string and returns matching queue_op
 */
simplekv_op
parse_simplekv_op(const char *str)
{
	for (int i = 0; i < MAX_SIMPLEKV_OP; ++i)
		if (strcmp(str, ops_str[i]) == 0)
			return (simplekv_op)i;

	return UNKNOWN_SIMPLEKV_OP;
}
}

namespace ptl = pmem::obj::experimental;

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::pool;
using pmem::obj::pool_base;
using pmem::obj::transaction;

namespace examples
{

template<typename Key, typename Value, std::size_t N>
class kv {
public:
	const Value& at(const Key &k) {
		for (int n = 0; n < nhash; ++n) {
			const auto &p = data[n][key_hash(k, n)];
			if (p.key == k)
				return p.value;
		}
		throw std::out_of_range("no entry in simplekv");
	}

	bool insert(pool_base &pop, const Key &k, const Value &v) {
		return data[0][key_hash(k, 0)].set(pop, k, v);
	}
private:

	size_t key_hash(const Key &k, int n) const {
		return k.hash(0) & (N-1);
	}

	struct entry {
		enum flag {
			occupied,
		};
		entry(Key k, Value v) : key(k), value(v) {
		}
		p<std::bitset<1>> flags;
		Key key;
		Value value;
		bool set(pool_base &pop, const Key &k, const Value &v) {
			if (flags.get_ro()[entry::flag::occupied] == true)
				return false;

			transaction::run(pop, [&] {
				flags.get_rw()[entry::flag::occupied] = true;
				key = k;
				value = v;
			});

			return true;
		}

		bool clear(pool_base &pop) {
			if (flags[entry::flag::occupied] == false)
				return false;

			transaction::run(pop, [&] {
				flags[entry::flag::occupied] = true;
			});
		}
	};
	static const int nretries = 5;
	static const int nhash = 2;
	ptl::array<entry, N> data[nhash];
};

} /* namespace examples */

struct value {
public:
	value(uint32_t f, uint32_t b) : foo(f), bar(b) {
		
	}
	uint32_t foo;
	uint32_t bar;

	void print() {
		std::cout << foo << " " << bar << std::endl;
	}
};

struct key {
public:
	key(uint64_t k) : data(k) {
		
	}

	uint64_t data;

	std::size_t hash(int n) const {
		static std::size_t params[] = {
			0xff51afd7ed558ccd, 0xc4ceb9fe1a85ec53,
			0x5fcdfd7ed551af8c, 0xec53ba85e9fe1c4c,	
		};
		std::size_t key = data;
		key ^= data >> 33;
		key *= params[n * 2];
		key ^= key >> 33;
		key *= params[(n * 2) + 1];
		key ^= key >> 33;
		return key;
	}

	bool operator==(const key &other) const {
		return data == other.data;
	}
};

using my_kv = examples::kv<key, value, 1024>;

int
main(int argc, char *argv[])
{
	if (argc < 3) {
		std::cerr << "usage: " << argv[0]
			  << " file-name [push [value]|pop|show]" << std::endl;
		return 1;
	}

	const char *path = argv[1];

	simplekv_op op = parse_simplekv_op(argv[2]);

	pool<my_kv> pop;

	switch (op) {
		case SIMPLEKV_CREATE: {
			pop = pool<my_kv>::create(
				path, LAYOUT, PMEMOBJ_MIN_POOL, CREATE_MODE_RW);
		} break;
		case SIMPLEKV_GET: {
			pop = pool<my_kv>::open(path, LAYOUT);

			auto kv = pop.root();

			auto v = kv->at(123);
			v.print();
		} break;
		case SIMPLEKV_PUT: {
			pop = pool<my_kv>::open(path, LAYOUT);

			auto kv = pop.root();

			kv->insert(pop, 123, value(1, 2));
		} break;
		case SIMPLEKV_REMOVE: {
		} break;
		default:
			throw std::invalid_argument("invalid simplekv operation");
	}

	pop.close();

	return 0;
}
