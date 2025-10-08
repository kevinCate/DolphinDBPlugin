//
// Created by uplee on 10/2/25.
//

#include "services/KVService.h"

namespace rc {

sw::redis::OptionalString KVService::get(const std::string& key) const
{
    return conn_.rc().get(key);
}

void KVService::set(const std::string& key, const std::string& val) const
{
    conn_.rc().set(key, val);
}

void KVService::setex(const std::string& key, const std::string& val, int ttl_sec) const
{
    conn_.rc().setex(key, std::chrono::seconds(ttl_sec), val);
}

}