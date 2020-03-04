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
	VECS_POPULATE,
	VECS_DROP,
	VECS_ITERATE,
	VECS_ITERATE_MT,
	VECS_ITERATE_OMP,

	MAX_VECS_OP,
};

/* vecs operations strings */
const char *ops_str[MAX_VECS_OP] =
	{"", "populate", "drop", "iter", "iter_mt", "iter_omp"};

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

namespace examples
{

static const size_t OBJECT_DATA_LEN = (1 << 14); /* 16kb */

struct foo {
	foo(uint64_t _pos) : pos(_pos) {
	}
	uint64_t pos;
	pobj::array<uint8_t, OBJECT_DATA_LEN> data;
};

class root {
	pobj::segment_vector<foo, pobj::fixed_size_vector_policy<1024>> foos;

public:
	void populate() {
		size_t n_sum = 0;
		for (size_t n = 0; ; ++n) {
			try {
				foos.emplace_back(n);
			} catch (pmem::transaction_error &err) {
				std::cout << err.what() << std::endl;
				break;
			}
			n_sum += n;
		}
		std::cout << n_sum << std::endl;
		
	}

	void drop(void) {
		foos.clear();
	}

	void iter(void) {
		size_t n_sum = 0;
		for (auto const iter: foos) {
			n_sum += iter.pos;
		}
		std::cout << n_sum << std::endl;
	}

	void iter_omp(void) {
		size_t n_sum = 0;

		#pragma omp parallel for
		for (size_t i = 0; i < foos.size(); ++i)
			#pragma omp atomic update
			n_sum += foos.const_at(i).pos;

		std::cout << n_sum << std::endl;
	}

	void iter_mt(size_t parts) {
		std::vector<std::future<size_t>> n_sums;
		auto itr = foos.cbegin();
		size_t partsize = foos.size() / parts;
		do {
			size_t r = (size_t)std::distance(itr, foos.cend());
			auto partial_iter_end = itr +
				(ssize_t)std::min(r, partsize);
			n_sums.emplace_back(std::async(std::launch::async, [=] {
				size_t n_sum = 0;
				for (auto n = itr; n != partial_iter_end; ++n)
					n_sum += n->pos;

				return n_sum;
			}));
			itr = partial_iter_end;
		} while (itr != foos.cend());

		size_t n_sum = 0;
		for (auto &iter: n_sums)
			n_sum += iter.get();

		std::cout << n_sum << std::endl;
	}
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

	pobj::pool<examples::root> pop;

	if (file_exists(path) != 0) {
		pop = pobj::pool<examples::root>::create(
			path, LAYOUT, PMEMOBJ_MIN_POOL * 1000, CREATE_MODE_RW);
	} else {
		pop = pobj::pool<examples::root>::open(path, LAYOUT);
	}

	auto v = pop.root();
	switch (op) {
		case VECS_POPULATE:
			v->populate();
			break;
		case VECS_DROP:
			v->drop();
			break;
		case VECS_ITERATE:
			v->iter();
			break;
		case VECS_ITERATE_MT:
			v->iter_mt(8);
			break;
		case VECS_ITERATE_OMP:
			v->iter_omp();
			break;
		default:
			throw std::invalid_argument("invalid vecs operation");
	}

	pop.close();

	return 0;
}
