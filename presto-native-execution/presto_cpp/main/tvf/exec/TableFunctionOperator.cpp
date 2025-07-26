/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "presto_cpp/main/tvf/exec/TableFunctionOperator.h"

#include "presto_cpp/main/tvf/exec/TableFunctionPartition.h"

#include "velox/common/memory/MemoryArbitrator.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::presto::tvf {

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

const RowTypePtr requiredColumnType(
    const std::string& name,
    const TableFunctionProcessorNodePtr& tableFunctionNode) {
  VELOX_CHECK_GT(tableFunctionNode->requiredColumns().count(name), 0);
  auto columns = tableFunctionNode->requiredColumns().at(name);

  // TODO: This assumes single source.
  auto inputType = tableFunctionNode->sources()[0]->outputType();
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (const auto& idx : columns) {
    names.push_back(inputType->nameOf(idx));
    types.push_back(inputType->childAt(idx));
  }
  return ROW(std::move(names), std::move(types));
}
} // namespace

TableFunctionOperator::TableFunctionOperator(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const TableFunctionProcessorNodePtr& tableFunctionNode)
    : Operator(
          driverCtx,
          tableFunctionNode->outputType(),
          operatorId,
          tableFunctionNode->id(),
          "TableFunctionOperator",
          tableFunctionNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt),
      pool_(pool()),
      stringAllocator_(pool_),
      tableFunctionNode_(tableFunctionNode),
      inputType_(tableFunctionNode->sources()[0]->outputType()),
      requiredColummType_(requiredColumnType("t1", tableFunctionNode)),
      data_(std::make_unique<RowContainer>(
          requiredColummType_->children(),
          pool_)),
      decodedInputVectors_(requiredColummType_->children().size()),
      rows_(0),
      tableFunctionPartition_(
          std::make_unique<TableFunctionPartition>(data_.get())),
      needsInput_(true),
      result_(nullptr),
      input_(nullptr) {
  tablePartitionBuild_ = std::make_unique<TablePartitionBuild>(
      requiredColummType_,
      tableFunctionNode->partitionKeys(),
      tableFunctionNode->sortingKeys(),
      tableFunctionNode->sortingOrders(),
      pool(),
      common::PrefixSortConfig{
          driverCtx->queryConfig().prefixSortNormalizedKeyMaxBytes(),
          driverCtx->queryConfig().prefixSortMinRows(),
          driverCtx->queryConfig().prefixSortMaxStringPrefixLength()});
}

void TableFunctionOperator::initialize() {
  Operator::initialize();
  VELOX_CHECK_NOT_NULL(tableFunctionNode_);
  createTableFunction(tableFunctionNode_);
  // TODO: Why was this needed
  // tableFunctionNode_.reset();
}

void TableFunctionOperator::createTableFunction(
    const std::shared_ptr<const TableFunctionProcessorNode>& node) {
  function_ = TableFunction::create(
      node->functionName(),
      node->handle(),
      pool_,
      &stringAllocator_,
      operatorCtx_->driverCtx()->queryConfig());
  VELOX_CHECK(function_);
}

// Writing the code to add the input rows -> call TableFunction::process and
// return the rows from it. This is done per input vectors basis. If we have
// partition by an order by this would need a change but just testing with a
// simple model for now.
void TableFunctionOperator::addInput(RowVectorPtr input) {
  VELOX_CHECK(needsInput_);

  tablePartitionBuild_->addInput(input);
}

void TableFunctionOperator::noMoreInput() {
  Operator::noMoreInput();

  needsInput_ = false;
}

void TableFunctionOperator::assembleInput() {
  auto input =
      BaseVector::create<RowVector>(requiredColummType_, rows_.size(), pool_);
  for (int i = 0; i < decodedInputVectors_.size(); i++) {
    input->childAt(i)->resize(rows_.size());
    tableFunctionPartition_->extractColumn(
        i, 0, rows_.size(), 0, input->childAt(i));
  }
  input_ = std::move(input);
}

void TableFunctionOperator::clear() {
  result_ = nullptr;
  input_ = nullptr;

  tableFunctionPartition_->clear();
  needsInput_ = true;

  if (noMoreInput_) {
    data_->clear();
    data_->pool()->release();
    needsInput_ = false;
  }
}

RowVectorPtr TableFunctionOperator::getOutput() {
  if (needsInput_) {
    return nullptr;
  }

  // This is the first call to TableFunction::apply or a previous
  // apply for this input has completed.
  if (result_ == nullptr) {
    VELOX_CHECK(!needsInput_);
    assembleInput();
  }

  VELOX_CHECK(function_);
  auto result = function_->apply({input_});
  if (result->state() == TableFunctionResult::TableFunctionState::kFinished) {
    noMoreInput_ = true;
    clear();
    return nullptr;
  }

  VELOX_CHECK(
      result->state() == TableFunctionResult::TableFunctionState::kProcessed);
  auto resultRows = result->result();
  VELOX_CHECK(resultRows);
  if (!result->usedInput()) {
    // Since the input rows are not completely consumed, the result_
    // should be maintained.
    result_ = result;
  } else {
    clear();
  }

  return std::move(resultRows);
}

void TableFunctionOperator::reclaim(
    uint64_t /*targetBytes*/,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_NYI("TableFunctionOperator::reclaim not implemented");
}

} // namespace facebook::presto::tvf
