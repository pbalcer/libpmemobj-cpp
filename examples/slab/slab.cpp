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

/*
 * slab.cpp -- implementation of a slab allocator based on a persistent
 * segment vector. It has the expected insert/remove interfaces as well as
 * parallel foreach implementation for fast traversal.
 * This example shows how to use such slab allocator to implement a hybrid
 * key-value store with DRAM index and PMEM values.
 */

#include <future>
#include <libpmemobj++/container/segment_vector.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/utils.hpp>
#include <libpmemobj_cpp_examples_common.hpp>

#define LAYOUT "slab"

namespace pobj = pmem::obj;

namespace examples
{
using slab_index = pobj::p<uint64_t>;

template <typename T>
class slab {
public:
	slab_index
	insert(T &value)
	{
		auto pop = pobj::pool_by_vptr(this);

		if (has_vacant()) {
			slab_index idx = vacant_index();
			slab_entry &entry = vec.at(idx);
			slab_index next_vacant = entry.vacant.next - 1;
			entry.occupied = value;
			pop.persist(entry.occupied);

			pmem::obj::transaction::run(pop, [&] {
				entry.type = ENTRY_OCCUPIED;
				set_vacant(next_vacant);
			});
			return idx;
		}

		vec.emplace_back(value);

		return vec.size() - 1;
	}

	void
	remove(slab_index idx)
	{
		auto pop = pobj::pool_by_vptr(this);

		pmem::obj::transaction::run(pop, [&] {
			slab_entry &entry = vec.at(idx);
			entry.type = ENTRY_VACANT;
			entry.vacant.next = vacant;
			set_vacant(idx);
		});
	}

	T &
	get(slab_index idx)
	{
		auto &entry = vec.at(idx);
		return entry.occupied.get_rw();
	}

	void foreach(std::function<void(slab_index idx, const T &value)> cb,
		int nthreads = 1)
	{
		std::vector<std::future<void>> futures;
		auto itr = vec.cbegin();
		size_t partsize = vec.size() > nthreads ? vec.size() / nthreads
							: vec.size();

		/*
		 * Splits the vector into equal parts and asynchronously
		 * traverses each part.
		 */
		do {
			auto r = (size_t)std::distance(itr, vec.cend());
			auto partial_iter_end =
				itr + (ssize_t)std::min(r, partsize);
			futures.emplace_back(
				std::async(std::launch::async, [=] {
					for (auto n = itr;
					     n != partial_iter_end; ++n) {
						if (n->type == ENTRY_VACANT)
							continue;
						slab_index index =
							std::distance(
								vec.cbegin(),
								n);
						cb(index, n->occupied.get_ro());
					}
				}));
			itr = partial_iter_end;
		} while (itr != vec.cend());

		for (auto &iter : futures)
			iter.wait();
	}

private:
	/* vacant entries are offset by one so that 0 can be used as a NULL */
	slab_index
	has_vacant()
	{
		return vacant != 0;
	}

	slab_index
	vacant_index()
	{
		return vacant - 1;
	}

	void
	set_vacant(slab_index entry)
	{
		vacant = entry + 1;
	}

	enum entry_type : uint32_t { ENTRY_VACANT, ENTRY_OCCUPIED };

	struct slab_entry_vacant {
		explicit slab_entry_vacant(slab_index _next) : next(_next)
		{
		}

		slab_index next;
	};

	struct slab_entry {
		explicit slab_entry(T value)
		{
			type = ENTRY_OCCUPIED;
			occupied = value;
		}

		pobj::p<enum entry_type> type;
		union {
			pobj::p<T> occupied;
			slab_entry_vacant vacant;
		};
	};

	pobj::segment_vector<slab_entry, pobj::fixed_size_vector_policy<1024>>
		vec;
	slab_index vacant;
};

/* simple key-value store with DRAM index and PMEM values */
template <typename T>
class kv {
public:
	explicit kv(examples::slab<T> &_slab) : slab(_slab)
	{
		/* if map were thread-safe, the lock would be unnecessary */
		slab.foreach ([&](slab_index idx, const T &value) {
			std::lock_guard<std::mutex> lk(lock);
			map[value.key] = idx;
		});
	}

	bool
	insert(T &f)
	{
		if (map.find(f.key) != map.end())
			return false;

		try {
			slab_index idx = slab.insert(f);
			map[f.key] = idx;
		} catch (pmem::transaction_error &err) {
			std::cout << err.what() << std::endl;
			return false;
		}
		return true;
	}

	void
	remove(uint64_t key)
	{
		auto f = map[key];
		slab.remove(f);
		map.erase(key);
	}

	T &
	get(uint64_t key)
	{
		auto f = map[key];
		return slab.get(f);
	}

private:
	std::mutex lock;
	std::unordered_map<uint64_t, slab_index> map;

	examples::slab<T> &slab;
};

} /* namespace examples */

struct foo {
	foo(uint64_t k, uint64_t v) : key(k), value(v)
	{
	}

	pobj::p<uint64_t> key;
	pobj::p<uint64_t> value;
};

class root {
public:
	examples::slab<foo> foos;
};

int
main(int argc, char *argv[])
{
	if (argc < 2) {
		std::cerr << "usage: " << argv[0] << " file-name" << std::endl;
		return 1;
	}

	const char *path = argv[1];

	pobj::pool<root> pop;

	if (file_exists(path) != 0) {
		pop = pobj::pool<root>::create(
			path, LAYOUT, PMEMOBJ_MIN_POOL * 10, CREATE_MODE_RW);
	} else {
		pop = pobj::pool<root>::open(path, LAYOUT);
	}

	{
		auto r = pop.root();
		std::cout << r.get() << std::endl;
		examples::kv<foo> kv(r->foos);
		foo a(5, 10);
		foo b(15, 20);

		kv.insert(a);
		kv.insert(b);

		auto &aref = kv.get(5);
		auto &bref = kv.get(15);
		std::cout << &aref << " " << aref.value << " " << &bref << " "
			  << bref.value << std::endl;
	}

	pop.close();

	pop = pobj::pool<root>::open(path, LAYOUT);
	{
		auto r = pop.root();
		examples::kv<foo> kv(r->foos);

		{
			auto &aref = kv.get(5);
			auto &bref = kv.get(15);
			std::cout << &aref << " " << aref.value << " " << &bref
				  << " " << bref.value << std::endl;

			kv.remove(5);
			kv.remove(15);

			foo a(5, 10);
			foo b(15, 20);

			/* the new entries will reuse the same vector positions
			 */
			kv.insert(a);
			kv.insert(b);
		}

		{
			auto &aref = kv.get(5);
			auto &bref = kv.get(15);
			std::cout << &aref << " " << aref.value << " " << &bref
				  << " " << bref.value << std::endl;

			foo c(20, 25);

			kv.insert(c);
			auto &cref = kv.get(20);
			std::cout << &cref << " " << cref.value << " "
				  << std::endl;
		}
	}

	return 0;
}
