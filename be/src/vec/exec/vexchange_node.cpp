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

#include "vec/exec/vexchange_node.h"

#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/runtime/vdata_stream_recvr.h"
#include "exec/rowid_fetcher.h"
#include "common/consts.h"
#include "util/defer_op.h"

namespace doris::vectorized {
VExchangeNode::VExchangeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _num_senders(0),
          _is_merging(tnode.exchange_node.__isset.sort_info),
          _stream_recvr(nullptr),
          _input_row_desc(descs, tnode.exchange_node.input_row_tuples,
                          std::vector<bool>(tnode.nullable_tuples.begin(),
                                            tnode.nullable_tuples.begin() +
                                                    tnode.exchange_node.input_row_tuples.size())),
          _offset(tnode.exchange_node.__isset.offset ? tnode.exchange_node.offset : 0) {}

Status VExchangeNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    if (!_is_merging) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.exchange_node.sort_info, _pool));
    RETURN_IF_ERROR(_vsort_exec_exprs_backup.init(tnode.exchange_node.sort_info, _pool));
    _is_asc_order = tnode.exchange_node.sort_info.is_asc_order;
    _nulls_first = tnode.exchange_node.sort_info.nulls_first;

    if (tnode.exchange_node.__isset.nodes_info) {
        _nodes_info = _pool->add(new DorisNodesInfo(tnode.exchange_node.nodes_info));
    }
    _scan_node_tuple_desc = state->desc_tbl().get_tuple_descriptor(tnode.olap_scan_node.tuple_id);
    return Status::OK();
}

Status VExchangeNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    DCHECK_GT(_num_senders, 0);
    _sub_plan_query_statistics_recvr.reset(new QueryStatisticsRecvr());
    _stream_recvr = state->exec_env()->vstream_mgr()->create_recvr(
            state, _input_row_desc, state->fragment_instance_id(), _id, _num_senders,
            config::exchg_node_buffer_size_bytes, _runtime_profile.get(), _is_merging,
            _sub_plan_query_statistics_recvr);

    if (_is_merging) {
        RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, _row_descriptor, _row_descriptor));
    }
    return Status::OK();
}
Status VExchangeNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VExchangeNode::open");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    if (_is_merging) {
        RETURN_IF_ERROR(_vsort_exec_exprs.open(state));
        RETURN_IF_ERROR(_stream_recvr->create_merger(_vsort_exec_exprs.lhs_ordering_expr_ctxs(),
                                                     _is_asc_order, _nulls_first,
                                                     state->batch_size(), _limit, _offset));
    }

    return Status::OK();
}
Status VExchangeNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented VExchange Node::get_next scalar");
}

Status VExchangeNode::second_phase_fetch_data(RuntimeState* state, Block* final_block) {
    auto row_id_col =  final_block->try_get_by_name(BeConsts::ROWID_COL);
    if (row_id_col != nullptr && final_block->rows() > 0) {
        MonotonicStopWatch watch;
        watch.start();
        RowIDFetcher id_fetcher(_scan_node_tuple_desc);
        RETURN_IF_ERROR(id_fetcher.init(_nodes_info));
        vectorized::Block b(_scan_node_tuple_desc->slots(), final_block->rows());
        auto tmp_block = MutableBlock::build_mutable_block(&b);
        // fetch will sort block by sequence of ROWID_COL
        RETURN_IF_ERROR(id_fetcher.fetch(row_id_col->column, &tmp_block));
        b.swap(tmp_block.to_block());

        // materialize
        if (_vsort_exec_exprs_backup.need_materialize_tuple()) {
            // TOO trick here??
            Defer _derfer([&](){ _vsort_exec_exprs_backup.close(state); });
            RETURN_IF_ERROR(_vsort_exec_exprs_backup.prepare(
                state, {state->desc_tbl(), {0}, {0}}, _row_descriptor));
            RETURN_IF_ERROR(_vsort_exec_exprs_backup.open(state));
            Block new_block;
            auto output_tuple_expr_ctxs = _vsort_exec_exprs_backup.sort_tuple_slot_expr_ctxs();
            std::vector<int> valid_column_ids(output_tuple_expr_ctxs.size());
            for (int i = 0; i < output_tuple_expr_ctxs.size(); ++i) {
                RETURN_IF_ERROR(output_tuple_expr_ctxs[i]->execute(&b, &valid_column_ids[i]));
            }
            for (auto column_id : valid_column_ids) {
                new_block.insert(b.get_by_position(column_id));
            }
            // last rowid column
            new_block.insert(b.get_by_position(b.columns() - 1));
            final_block->swap(new_block);
        } else {
            final_block->swap(b);
        } 
        LOG(INFO) << "fetch_id finished, cost(ms):" << watch.elapsed_time() / 1000 / 1000; 
    }
    return Status::OK();
}


Status VExchangeNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span, "VExchangeNode::get_next");
    SCOPED_TIMER(runtime_profile()->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    auto status = _stream_recvr->get_next(block, eos);
    if (block != nullptr) {
        if (_num_rows_returned + block->rows() < _limit) {
            _num_rows_returned += block->rows();
        } else {
            *eos = true;
            auto limit = _limit - _num_rows_returned;
            block->set_num_rows(limit);
        }
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }
    RETURN_IF_ERROR(second_phase_fetch_data(state, block)); 
    return status;
}

Status VExchangeNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VExchangeNode::close");

    if (_stream_recvr != nullptr) {
        _stream_recvr->close();
    }
    if (_is_merging) _vsort_exec_exprs.close(state);

    return ExecNode::close(state);
}

} // namespace doris::vectorized
