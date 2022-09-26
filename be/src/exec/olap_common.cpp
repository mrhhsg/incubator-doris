// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/olap_common.h"

#include <boost/lexical_cast.hpp>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "exec/olap_utils.h"
#include "runtime/large_int_value.h"

namespace doris {

template <>
void ColumnValueRange<PrimitiveType::TYPE_STRING>::convert_to_fixed_value() {
    return;
}

template <>
void ColumnValueRange<PrimitiveType::TYPE_CHAR>::convert_to_fixed_value() {
    return;
}

template <>
void ColumnValueRange<PrimitiveType::TYPE_VARCHAR>::convert_to_fixed_value() {
    return;
}

template <>
void ColumnValueRange<PrimitiveType::TYPE_HLL>::convert_to_fixed_value() {
    return;
}

template <>
void ColumnValueRange<PrimitiveType::TYPE_DECIMALV2>::convert_to_fixed_value() {
    return;
}

template <>
void ColumnValueRange<PrimitiveType::TYPE_LARGEINT>::convert_to_fixed_value() {
    return;
}

Status OlapScanKeys::get_key_range(std::vector<std::unique_ptr<OlapScanRange>>* key_range) {
    key_range->clear();

    for (int i = 0; i < _begin_scan_keys.size(); ++i) {
        std::unique_ptr<OlapScanRange> range(new OlapScanRange());
        range->begin_scan_range = _begin_scan_keys[i];
        range->end_scan_range = _end_scan_keys[i];
        range->begin_include = _begin_include;
        range->end_include = _end_include;
        key_range->emplace_back(std::move(range));
    }

    return Status::OK();
}

void OlapScanKeys::_merge(OlapScanKeys* scan_keys) {
    for (auto& range : _column_ranges) {
        std::visit(
                [&](auto&& range) {
                    using RangeType = std::decay_t<decltype(range)>;
                    using CppType = typename RangeType::CppType;
                    using ConstIterator = typename std::set<CppType>::const_iterator;
                    if (range.is_fixed_value_range() && range.is_range_value_convertible()) {
                        range.convert_to_range_value();
                    }

                    if (range.is_fixed_value_range()) {
                        const std::set<CppType>& fixed_value_set = range.get_fixed_value_set();
                        ConstIterator iter = fixed_value_set.begin();
                        if (scan_keys->_begin_scan_keys.empty()) {
                            for (; iter != fixed_value_set.end(); ++iter) {
                                scan_keys->_begin_scan_keys.emplace_back();
                                scan_keys->_begin_scan_keys.back().add_value(
                                        cast_to_string<RangeType::Type, CppType>(*iter,
                                                                                 range.scale()));
                                scan_keys->_end_scan_keys.emplace_back();
                                scan_keys->_end_scan_keys.back().add_value(
                                        cast_to_string<RangeType::Type, CppType>(*iter,
                                                                                 range.scale()));
                            }

                            if (range.contain_null()) {
                                scan_keys->_begin_scan_keys.emplace_back();
                                scan_keys->_begin_scan_keys.back().add_null();
                                scan_keys->_end_scan_keys.emplace_back();
                                scan_keys->_end_scan_keys.back().add_null();
                            }
                        } else {
                            int original_key_range_size = _begin_scan_keys.size();
                            for (int i = 0; i < original_key_range_size; ++i) {
                                OlapTuple start_base_key_range = scan_keys->_begin_scan_keys[i];
                                OlapTuple end_base_key_range = scan_keys->_end_scan_keys[i];

                                ConstIterator iter = fixed_value_set.begin();

                                for (; iter != fixed_value_set.end(); ++iter) {
                                    // alter the first ScanKey in original place
                                    if (iter == fixed_value_set.begin()) {
                                        scan_keys->_begin_scan_keys[i].add_value(
                                                cast_to_string<RangeType::Type, CppType>(
                                                        *iter, range.scale()));
                                        scan_keys->_end_scan_keys[i].add_value(
                                                cast_to_string<RangeType::Type, CppType>(
                                                        *iter, range.scale()));
                                    } // append follow ScanKey
                                    else {
                                        scan_keys->_begin_scan_keys.push_back(start_base_key_range);
                                        scan_keys->_begin_scan_keys.back().add_value(
                                                cast_to_string<RangeType::Type, CppType>(
                                                        *iter, range.scale()));
                                        scan_keys->_end_scan_keys.push_back(end_base_key_range);
                                        scan_keys->_end_scan_keys.back().add_value(
                                                cast_to_string<RangeType::Type, CppType>(
                                                        *iter, range.scale()));
                                    }
                                }
                                if (range.contain_null()) {
                                    scan_keys->_begin_scan_keys.push_back(start_base_key_range);
                                    scan_keys->_begin_scan_keys.back().add_null();
                                    scan_keys->_end_scan_keys.push_back(end_base_key_range);
                                    scan_keys->_end_scan_keys.back().add_null();
                                }
                            }
                        }
                        scan_keys->_begin_include = true;
                        scan_keys->_end_include = true;
                    } else {
                        scan_keys->_has_range_value = true;

                        if (scan_keys->_begin_scan_keys.empty()) {
                            scan_keys->_begin_scan_keys.emplace_back();
                            scan_keys->_begin_scan_keys.back().add_value(
                                    cast_to_string<RangeType::Type, CppType>(
                                            range.get_range_min_value(), range.scale()),
                                    range.contain_null());
                            scan_keys->_end_scan_keys.emplace_back();
                            scan_keys->_end_scan_keys.back().add_value(
                                    cast_to_string<RangeType::Type, CppType>(
                                            range.get_range_max_value(), range.scale()));
                        } else {
                            for (int i = 0; i < scan_keys->_begin_scan_keys.size(); ++i) {
                                scan_keys->_begin_scan_keys[i].add_value(
                                        cast_to_string<RangeType::Type, CppType>(
                                                range.get_range_min_value(), range.scale()),
                                        range.contain_null());
                            }

                            for (int i = 0; i < scan_keys->_end_scan_keys.size(); ++i) {
                                scan_keys->_end_scan_keys[i].add_value(
                                        cast_to_string<RangeType::Type, CppType>(
                                                range.get_range_max_value(), range.scale()));
                            }
                        }

                        scan_keys->_begin_include = range.is_begin_include();
                        scan_keys->_end_include = range.is_end_include();
                    }
                },
                range);
    }
}

} // namespace doris

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
