#ifndef HASHSERVICE_H
#define HASHSERVICE_H
#pragma once
#include "core/ConnFacade.h"
#include "DolphinDBEverything.h"
#include <string>
#include <vector>

namespace rc {

class HashService {
public:
	explicit HashService(rc::ConnFacade& conn) : conn_(conn) {}

	void mget(const std::vector<std::string>& keys, std::vector<sw::redis::OptionalString>& out) const;
	void batchHSet(const std::vector<std::string>& keys, const ddb::TableSP& fieldData) const;
	void batchHSetThread(const std::vector<std::string>& keys, const ddb::TableSP& fieldData, int numThreads) const;
	void deleteKeys(const std::vector<std::string>& keys, bool useUnlink) const;
private:
	rc::ConnFacade& conn_;
};

}
#endif // HASHSERVICE_H