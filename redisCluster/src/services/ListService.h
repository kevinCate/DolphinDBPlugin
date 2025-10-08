//
// Created by uplee on 10/8/25.
//

#ifndef LISTSERVICE_H
#define LISTSERVICE_H

#include "core/ConnFacade.h"
#include <string>
#include <vector>

namespace rc {

class ListService {
public:
    explicit ListService(ConnFacade& cf) : conn_(cf) {}

    // For each key, push all elements in vals[i] as one command (RPUSH/LPUSH).
    // Atomic per key; parallelism/pipeline are handled by dispatcher.
    void batchPush(const std::vector<std::string>& keys,
                   const std::vector<ddb::VectorSP>& vals,
                   bool rightPush,
                   int numThreads) const;
private:
    ConnFacade& conn_;
};

} // namespace rc
#endif // LISTSERVICE_H