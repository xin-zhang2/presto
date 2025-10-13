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
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;

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
            return variable -> variables.contains(variable) ? new VariableReferenceExpression(variable.getSourceLocation(), variable.getName(), BigintType.BIGINT) : variable;
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
            // When context is Empty
            Assignments.Builder newAssignments = Assignments.builder();
            Set<VariableReferenceExpression> newContext = new HashSet<>();
            ImmutableSet.Builder<VariableReferenceExpression> newContextBuilder = ImmutableSet.builder();
            node.getAssignments().forEach((variable, expression) -> {
                if (getIntToBigIntCastArgument(expression).isPresent()) {
//                    newContextBuilder.add(getIntToBigIntCastArgument(expression).get());
                    newContext.add(getIntToBigIntCastArgument(expression).get());
                }
            });

            node.getAssignments().forEach((variable, expression) -> {
                VariableReferenceExpression key = variable;
                RowExpression value = expression;
//                if (newContext.contains(variable)) {
//                    key = new VariableReferenceExpression(variable.getSourceLocation(), variable.getName(), BigintType.BIGINT);
//                }
                if (getIntToBigIntCastArgument(expression).isPresent()) {
                    VariableReferenceExpression v = getIntToBigIntCastArgument(expression).get();
                    if (!node.getAssignments().getMap().containsKey(v)) {
                        value = new VariableReferenceExpression(v.getSourceLocation(), v.getName(), BigintType.BIGINT);
                    }
                    else {
                        newContext.remove(v);
                    }
                }
                newAssignments.put(key, value);
            });
//            Set<VariableReferenceExpression> newContext = newContextBuilder.build();
            PlanNode source = context.rewrite(node.getSource(), newContext);
            return new ProjectNode(node.getSourceLocation(), node.getId(), node.getStatsEquivalentPlanNode(), source, newAssignments.build(), node.getLocality());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            if (isEmptyContext(context)) {
                return node;
            }

            List<VariableReferenceExpression> newOutputs = node.getOutputVariables().stream()
                    .map(replaceIfIn(context.get()))
                    .collect(toImmutableList());

            Map<VariableReferenceExpression, ColumnHandle> newAssignments = node.getAssignments().entrySet().stream()
                    .collect(toImmutableMap(e -> replaceIfIn(context.get()).apply(e.getKey()), Map.Entry::getValue));

            planChanged = true;
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
            if (isEmptyContext(context)) {
                return node;
            }
            planChanged = true;
            PlanNode left = context.rewrite(node.getLeft(), context.get());
            PlanNode right = context.rewrite(node.getRight(), context.get());
            List<VariableReferenceExpression> outputVariables = node.getOutputVariables().stream().map(replaceIfIn(context.get())).collect(toImmutableList());
            return new JoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    node.getType(),
                    left,
                    right,
                    node.getCriteria(),
                    outputVariables,
                    node.getFilter(),
                    node.getLeftHashVariable(),
                    node.getRightHashVariable(),
                    node.getDistributionType(),
                    node.getDynamicFilters());
        }

        @Override
        public PlanNode visitRemoteSource(RemoteSourceNode node, RewriteContext<Set<VariableReferenceExpression>> context)
        {
            return super.visitRemoteSource(node, context);
        }
    }
}
