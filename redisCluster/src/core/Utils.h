//
// Created by uplee on 10/8/25.
//
#ifndef UTILS_H
#define UTILS_H
#pragma once

#include "ddbplugin/Plugin.h"

namespace rc::utils{

inline const char* ds_ptr(const ddb::DolphinString& ds) { return ds.c_str(); }

inline std::size_t ds_len(const ddb::DolphinString& ds) { return std::strlen(ds.c_str()); }

}

#endif //UTILS_H
