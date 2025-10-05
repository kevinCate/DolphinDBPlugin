#ifndef ROUTER_H
#define ROUTER_H
#pragma once
#include <string>
#include <string_view>
#include <vector>

namespace rc {

std::string_view extract_tag(std::string_view key) noexcept;
std::string extract_tag_owned(std::string_view key);
std::string_view hash_basis(std::string_view key) noexcept;
int key_slot(std::string_view key) noexcept;

// A group of rows that share a single route (same tag or same slot).
struct SingleSlotGroup {
    std::string routeKey;          // tag if available; otherwise a representative key in the slot
    std::vector<std::size_t> rows; // indices into caller¡¯s arrays/vectors
};

// Groups keys into single-slot batches. Tagged keys are grouped by tag first.
// Untagged keys are grouped by computed slot.
std::vector<SingleSlotGroup> group_by_single_slot(const std::vector<std::string>& keys);

} // namespace rc
#endif // ROUTER_H