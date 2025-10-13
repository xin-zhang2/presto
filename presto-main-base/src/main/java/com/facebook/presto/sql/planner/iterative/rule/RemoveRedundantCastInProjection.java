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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.BuiltInFunctionHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.planner.plan.Patterns.project;

public class RemoveRedundantCastInProjection
        implements Rule<ProjectNode>
{
    private static final Pattern<ProjectNode> PATTERN = project();
    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        Assignments assignments = node.getAssignments();
        Map<VariableReferenceExpression, RowExpression> newAssignments = new HashMap<>();
        boolean changed = false;

        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : assignments.entrySet()) {
            RowExpression expression = entry.getValue();
            if (isIntToBigintCast(expression)) {
                CallExpression callExpression = (CallExpression) expression;
                RowExpression argument = callExpression.getArguments().get(0);
                if (argument instanceof VariableReferenceExpression) {
                    VariableReferenceExpression variable = (VariableReferenceExpression) argument;
                    RowExpression newRowExpression = new VariableReferenceExpression(
                            variable.getSourceLocation(),
                            variable.getName(),
                            BigintType.BIGINT);
                    changed = true;
                    newAssignments.put(entry.getKey(), newRowExpression);
                }
                else {
                    newAssignments.put(entry.getKey(), expression);
                }
            }
            else {
                newAssignments.put(entry.getKey(), expression);
            }
        }
        if (changed) {
            PlanNode source = context.getLookup().resolve(node.getSource());
            if (!(source instanceof TableScanNode)) {
                return Result.ofPlanNode(new ProjectNode(
                        node.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        node.getSource(),
                        new Assignments(newAssignments),
                        node.getLocality()));
            }

            Map<String, VariableReferenceExpression> newAssignmentMap = new HashMap<>();
            for (RowExpression expression : newAssignments.values()) {
                if (expression instanceof VariableReferenceExpression) {
                    VariableReferenceExpression variable = (VariableReferenceExpression) expression;
                    newAssignmentMap.put(variable.getName(), variable);
                }
            }

            TableScanNode tableScanNode = (TableScanNode) source;
            List<VariableReferenceExpression> outputVariables = tableScanNode.getOutputVariables();
            List<VariableReferenceExpression> newOutputVariables = new ArrayList<>();

            Map<VariableReferenceExpression, ColumnHandle> assignments2 = tableScanNode.getAssignments();
            Map<VariableReferenceExpression, ColumnHandle> newAssignments2 = new HashMap<>();

            for (VariableReferenceExpression variable : outputVariables) {
                if (variable.getType() instanceof IntegerType && newAssignmentMap.get(variable.getName()) != null && newAssignmentMap.get(variable.getName()).getType() instanceof BigintType) {
                    newOutputVariables.add(new VariableReferenceExpression(variable.getSourceLocation(), variable.getName(), BigintType.BIGINT));
                }
                else {
                    newOutputVariables.add(variable);
                }
            }
            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : assignments2.entrySet()) {
                VariableReferenceExpression variable = entry.getKey();
                if (variable.getType() instanceof IntegerType && newAssignmentMap.get(variable.getName()) != null && newAssignmentMap.get(variable.getName()).getType() instanceof BigintType) {
                    newAssignments2.put(new VariableReferenceExpression(variable.getSourceLocation(), variable.getName(), BigintType.BIGINT), entry.getValue());
                }
                else {
                    newAssignments2.put(variable, entry.getValue());
                }
            }
            TableScanNode newTableScanNode = new TableScanNode(
                    tableScanNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    tableScanNode.getStatsEquivalentPlanNode(),
                    tableScanNode.getTable(),
                    newOutputVariables,
                    newAssignments2,
                    tableScanNode.getTableConstraints(),
                    tableScanNode.getCurrentConstraint(),
                    tableScanNode.getEnforcedConstraint(),
                    tableScanNode.getCteMaterializationInfo());
            return Result.ofPlanNode(new ProjectNode(
                    node.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    newTableScanNode,
                    new Assignments(newAssignments),
                    node.getLocality()));
        }
        else {
            return Result.empty();
        }
    }

    private boolean isIntToBigintCast(RowExpression expression)
    {
        if (!(expression instanceof CallExpression)) {
            return false;
        }
        CallExpression callExpression = (CallExpression) expression;
        if (!(callExpression.getType() instanceof BigintType)) {
            return false;
        }
        FunctionHandle functionHandle = callExpression.getFunctionHandle();
        if (!(functionHandle instanceof BuiltInFunctionHandle)) {
            return false;
        }
        BuiltInFunctionHandle builtInFunctionHandle = (BuiltInFunctionHandle) functionHandle;
        TypeSignature signature = builtInFunctionHandle.getSignature().getArgumentTypes().get(0);
        return "integer".equals(signature.getTypeSignatureBase().getStandardTypeBase());
    }
}
