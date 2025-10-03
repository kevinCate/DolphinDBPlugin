//
// Created by uplee on 10/2/25.
//

#ifndef KVSERVICE_H
#define KVSERVICE_H

// KVService.h
#pragma once
#include "core/ConnFacade.h"
#include "core/PipelinePolicy.h"
#include <string>

namespace rc {

class KVService {
public:
    explicit KVService(rc::ConnFacade& conn): conn_(conn) {}

    [[nodiscard]] sw::redis::OptionalString get(const std::string& key) const;
    void set(const std::string& key, const std::string& val) const;
    void setex(const std::string& key, const std::string& val, int ttl_sec) const;

private:
    rc::ConnFacade& conn_;
};

}

#endif //KVSERVICE_H
