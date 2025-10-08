#ifndef HASHSERVICE_H
#define HASHSERVICE_H
#pragma once
#include <string>
#include <vector>
#include "core/ConnFacade.h"
#include "ddbplugin/Plugin.h"

namespace rc {

class BatchService {
public:
	explicit BatchService(rc::ConnFacade& conn) : conn_(conn) {}

	void batchGet(const std::vector<std::string>& keys, std::vector<sw::redis::OptionalString>& out) const;
	void batchSet(std::vector<std::string> keys, std::vector<std::string> values, int numThreads) const;
	void batchHSet(const std::vector<std::string>& keys, const ddb::TableSP& fieldData, int numThreads) const;
	void deleteKeys(const std::vector<std::string>& keys, bool useUnlink, int numThreads) const;
private:
	ConnFacade& conn_;
};

}
#endif // HASHSERVICE_H