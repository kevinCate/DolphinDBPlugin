//
// Created by uplee on 10/2/25.
//

#ifndef KVSERVICE_H
#define KVSERVICE_H
#pragma once
#include <string>
#include "core/ConnFacade.h"

namespace rc {

class KVService {

public:
    explicit KVService(rc::ConnFacade& conn): conn_(conn) {}

    [[nodiscard]] sw::redis::OptionalString get(const std::string& key) const;
    void set(const std::string& key, const std::string& val) const;
    void setex(const std::string& key, const std::string& val, int ttl_sec) const;

private:
    ConnFacade& conn_;
};

}

#endif //KVSERVICE_H
