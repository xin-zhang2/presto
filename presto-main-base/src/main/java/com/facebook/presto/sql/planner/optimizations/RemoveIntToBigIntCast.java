/*
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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class RemoveIntToBigIntCast
        implements PlanOptimizer
{
    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        Rewriter rewriter = new Rewriter();
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan, ImmutableSet.of());
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Set<VariableReferenceExpression>>
            implements RowExpressionVisitor<RowExpression, Set<VariableReferenceExpression>>
    {
        private boolean planChanged;

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        private static boolean isEmptyContext(RewriteContext<Set<VariableReferenceExpression>> context)
        {
            return context == null || context.get() == null || context.get().isEmpty();
        }

        private static Function<VariableReferenceExpression, VariableReferenceExpression> replaceIfIn(Set<VariableReferenceExpression> variables)
        {
            return variable -> replaceIfIn(variable, variables);
        }

        private static VariableReferenceExpression replaceIfIn(VariableReferenceExpression variable, Set<VariableReferenceExpression> variables)
        {
            return variables.contains(variable) ? new VariableReferenceExpression(variable.getSourceLocation(), variable.getName(), BigintType.BIGINT) : variable;
        }

        private static Optional<VariableReferenceExpression> getIntToBigIntCastArgument(RowExpression rowExpression)
        {
            if (rowExpression instanceof CallExpression) {
                CallExpression callExpression = (CallExpression) rowExpression;
                if ("CAST".equals(callExpression.getDisplayName())) {
                    if (callExpression.getType() instanceof BigintType) {
                        RowExpression argument = callExpression.getArguments().get(0);
                        if (argument.getType() instanceof IntegerType && argument instanceof VariableReferenceExpression) {
                            VariableReferenceExpression variable = (VariableReferenceExpression) argument;
                            return Optional.of(variable);
                        }
                    }
                }
            }
            return Optional.empty();
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            Assignments.Builder newAssignments = Assignments.builder();
            Set<VariableReferenceExpression> newContext = new HashSet<>(context.get());
            node.getAssignments().forEach((variable, expression) -> {
                getIntToBigIntCastArgument(expression)
                        .filter(castArgument -> !newContext.contains(castArgument))
                        .ifPresent(newContext::add);
            });

            node.getAssignments().forEach((variable, expression) -> {
                RowExpression value = expression;

                Optional<VariableReferenceExpression> castArgument = getIntToBigIntCastArgument(expression);
                if (castArgument.isPresent()) {
                    if (node.getAssignments().getVariables().contains(castArgument.get())) {
                        newContext.remove(castArgument.get());
                    }
                    else {
                        planChanged = true;
                        value = new VariableReferenceExpression(castArgument.get().getSourceLocation(), castArgument.get().getName(), BigintType.BIGINT);
                    }
                }
                newAssignments.put(variable, value);
            });
            PlanNode newSource = context.rewrite(node.getSource(), ImmutableSet.copyOf(newContext));
            return new ProjectNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), newSource, newAssignments.build(), node.getLocality());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            if (isEmptyContext(context)) {
                return node;
            }

            List<VariableReferenceExpression> newOutputs = node.getOutputVariables().stream()
                    .map(variable -> {
                        if (context.get().contains(variable)) {
                            planChanged = true;
                            return new VariableReferenceExpression(variable.getSourceLocation(), variable.getName(), BigintType.BIGINT);
                        }
                        else {
                            return variable;
                        }
                    })
                    .collect(toImmutableList());

            Map<VariableReferenceExpression, ColumnHandle> newAssignments = node.getAssignments().keySet().stream()
                    .collect(toImmutableMap(key -> {
                        if (context.get().contains(key)) {
                            planChanged = true;
                            return new VariableReferenceExpression(key.getSourceLocation(), key.getName(), BigintType.BIGINT);
                        }
                        return key;
                    }, key -> node.getAssignments().get(key)));

            return new TableScanNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    node.getTable(),
                    newOutputs,
                    newAssignments,
                    node.getTableConstraints(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint(),
                    node.getCteMaterializationInfo());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            List<EquiJoinClause> equiJoinClauseList = node.getCriteria();
            List<EquiJoinClause> newEquiJoinClauseList = new ArrayList<>();
            ImmutableSet.Builder<VariableReferenceExpression> newContextBuilder = ImmutableSet.builder();
            newContextBuilder.addAll(context.get());
            for (EquiJoinClause equiJoinClause : equiJoinClauseList) {
                VariableReferenceExpression leftVariable = equiJoinClause.getLeft();
                VariableReferenceExpression rightVariable = equiJoinClause.getRight();
                if (context.get().contains(leftVariable) || context.get().contains(rightVariable)) {
                    if (!context.get().contains(leftVariable)) {
                        newContextBuilder.add(leftVariable);
                    }
                    if (!context.get().contains(rightVariable)) {
                        newContextBuilder.add(rightVariable);
                    }
                    planChanged = true;
                    leftVariable = new VariableReferenceExpression(leftVariable.getSourceLocation(), leftVariable.getName(), BigintType.BIGINT);
                    rightVariable = new VariableReferenceExpression(rightVariable.getSourceLocation(), rightVariable.getName(), BigintType.BIGINT);
                }
                newEquiJoinClauseList.add(new EquiJoinClause(leftVariable, rightVariable));
            }
            Set<VariableReferenceExpression> newContext = newContextBuilder.build();
            PlanNode left = context.rewrite(node.getLeft(), newContext);
            PlanNode right = context.rewrite(node.getRight(), newContext);
            List<VariableReferenceExpression> outputVariables = node.getOutputVariables().stream().map(variable -> {
                if (context.get().contains(variable)) {
                    planChanged = true;
                    return new VariableReferenceExpression(variable.getSourceLocation(), variable.getName(), BigintType.BIGINT);
                }
                return variable;
            }).collect(toImmutableList());

            return new JoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    node.getType(),
                    left,
                    right,
                    newEquiJoinClauseList,
                    outputVariables,
                    node.getFilter(),
                    node.getLeftHashVariable(),
                    node.getRightHashVariable(),
                    node.getDistributionType(),
                    node.getDynamicFilters());
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            List<PlanNode> newSources = node.getSources().stream()
                    .map(source -> context.rewrite(source, context.get()))
                    .collect(toImmutableList());

            PartitioningScheme partitioningScheme = node.getPartitioningScheme();
            Partitioning partitioning = partitioningScheme.getPartitioning();
            Partitioning newPartitioning = new Partitioning(
                    partitioning.getHandle(),
                    partitioning.getArguments().stream().map(expression -> {
                        if (expression instanceof VariableReferenceExpression) {
                            VariableReferenceExpression variable = (VariableReferenceExpression) expression;
                            if (context.get().contains(variable)) {
                                planChanged = true;
                                return new VariableReferenceExpression(variable.getSourceLocation(), variable.getName(), BigintType.BIGINT);
                            }
                        }
                        return expression;
                    }).collect(toImmutableList()));

            PartitioningScheme newPartitioningScheme = new PartitioningScheme(
                    newPartitioning,
                    partitioningScheme.getOutputLayout().stream()
                            .map(variable -> {
                                if (context.get().contains(variable)) {
                                    planChanged = true;
                                    return new VariableReferenceExpression(variable.getSourceLocation(), variable.getName(), BigintType.BIGINT);
                                }
                                return variable;
                            }).collect(toImmutableList()),
                    partitioningScheme.getHashColumn()
                            .map(variable -> {
                                if (context.get().contains(variable)) {
                                    planChanged = true;
                                    return new VariableReferenceExpression(variable.getSourceLocation(), variable.getName(), BigintType.BIGINT);
                                }
                                return variable;
                            }),
                    partitioningScheme.isReplicateNullsAndAny(),
                    partitioningScheme.isScaleWriters(),
                    partitioningScheme.getEncoding(),
                    partitioningScheme.getBucketToPartition());

            List<List<VariableReferenceExpression>> inputs = node.getInputs().stream()
                    .map(list -> list.stream()
                            .map(replaceIfIn(context.get()))
                            .collect(toImmutableList()))
                    .collect(toImmutableList());
            Optional<OrderingScheme> newOrderingScheme = node.getOrderingScheme().map(scheme -> {
                List<Ordering> newOrdering = scheme.getOrderBy().stream()
                        .map(ordering -> {
                            VariableReferenceExpression variable = ordering.getVariable();
                            if (context.get().contains(variable)) {
                                planChanged = true;
                                return new Ordering(new VariableReferenceExpression(variable.getSourceLocation(), variable.getName(), BigintType.BIGINT), ordering.getSortOrder());
                            }
                            return ordering;
                        })
                        .collect(toImmutableList());
                return new OrderingScheme(newOrdering);
            });

            return new ExchangeNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getType(),
                    node.getScope(),
                    newPartitioningScheme,
                    newSources,
                    inputs,
                    node.isEnsureSourceOrdering(),
                    newOrderingScheme);
        }

//        @Override
//        public PlanNode visitFilter(FilterNode node, RewriteContext<Set<VariableReferenceExpression>> context)
//        {
//            RowExpression newPredicate = node.getPredicate().accept(this, context.get());
//            PlanNode newSource = context.rewrite(node.getSource(), context.get());
//            return new FilterNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), newSource, newPredicate);
//        }
    }
}
