#pragma once

#include <string>

/// @brief Utility function that checks whether two
/// hostnames resolve to the same underlying host.
// The hostnames can be domain names, IPv4 or IPv6 addresses.
bool isSameHost(const std::string& h1, const std::string& h2);