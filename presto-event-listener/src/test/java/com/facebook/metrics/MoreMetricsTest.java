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
package com.facebook.metrics;

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.analyzer.UpdateInfo;
import com.facebook.presto.spi.eventlistener.CTEInformation;
import com.facebook.presto.spi.eventlistener.Column;
import com.facebook.presto.spi.eventlistener.OperatorStatistics;
import com.facebook.presto.spi.eventlistener.OutputColumnMetadata;
import com.facebook.presto.spi.eventlistener.PlanOptimizerInformation;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryInputMetadata;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryOutputMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.facebook.presto.spi.eventlistener.StageStatistics;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.prestospark.PrestoSparkExecutionContext;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.spi.statistics.PlanStatisticsWithSourceInfo;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.metrics.MoreMetrics.INNER_JOIN_PUSHDOWN_ENABLED;
import static com.facebook.metrics.MoreMetrics.traceMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MoreMetricsTest
{
    public static QueryCompletedEvent createDummyQueryCompletedEvent()
    {
        QueryMetadata metadata = createDummyQueryMetadata();
        QueryStatistics statistics = createDummyQueryStatistics();
        QueryContext context = createDummyQueryContext();
        QueryIOMetadata ioMetadata = createDummyQueryIoMetadata();
        Optional<QueryFailureInfo> failureInfo = Optional.empty();
        List<PrestoWarning> warnings = new ArrayList<>();
        Optional<QueryType> queryType = Optional.empty();
        List<String> failedTasks = new ArrayList<>();
        Instant createTime = Instant.now();
        Instant executionStartTime = Instant.now().minusSeconds(10);
        Instant endTime = Instant.now().plusSeconds(10);
        List<StageStatistics> stageStatistics = new ArrayList<>();
        List<OperatorStatistics> operatorStatistics = new ArrayList<>();
        List<PlanStatisticsWithSourceInfo> planStatisticsRead = new ArrayList<>();
        List<PlanStatisticsWithSourceInfo> planStatisticsWritten = new ArrayList<>();
        Map<PlanNodeId, Map<PlanCanonicalizationStrategy, String>> planNodeHash = new HashMap<>();
        Map<PlanCanonicalizationStrategy, String> canonicalPlan = new HashMap<>();
        Optional<String> statsEquivalentPlan = Optional.empty();
        Optional<String> expandedQuery = Optional.empty();
        // Create a positive rule for successful join pushdown
        List<PlanOptimizerInformation> optimizerInformation = ImmutableList.of(new PlanOptimizerInformation(
                "OnlyJoinRule",
                true,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));
        List<CTEInformation> cteInformationList = new ArrayList<>();
        Set<String> scalarFunctions = new HashSet<>();
        Set<String> aggregateFunctions = new HashSet<>();
        Set<String> windowFunctions = new HashSet<>();
        Optional<PrestoSparkExecutionContext> prestoSparkExecutionContext = Optional.empty();
        Map<PlanCanonicalizationStrategy, String> hboPlanHash = new HashMap<>();
        Optional<Map<PlanNodeId, PlanNode>> planIdNodeMap = Optional.ofNullable(new HashMap<>());
        UpdateInfo updateInfo = new UpdateInfo("CREATE TABLE", "ctlog.schema.tbl");
        return new QueryCompletedEvent(
                metadata,
                statistics,
                context,
                ioMetadata,
                failureInfo,
                warnings,
                queryType,
                failedTasks,
                createTime,
                executionStartTime,
                endTime,
                stageStatistics,
                operatorStatistics,
                planStatisticsRead,
                planStatisticsWritten,
                planNodeHash,
                canonicalPlan,
                statsEquivalentPlan,
                expandedQuery,
                optimizerInformation,
                cteInformationList,
                scalarFunctions,
                aggregateFunctions,
                windowFunctions,
                prestoSparkExecutionContext,
                hboPlanHash,
                planIdNodeMap,
                Optional.of(updateInfo.getUpdateObject()));
    }

    private static QueryMetadata createDummyQueryMetadata()
    {
        String queryId = "20250216_173945_00000_9r4vt";
        Optional<String> transactionId = Optional.of("dummy-transaction-id");
        String query = "SELECT * FROM dummy_table";
        String queryHash = "dummy-query-hash";
        Optional<String> preparedQuery = Optional.of("PREPARE SELECT * FROM dummy_table");
        String queryState = "COMPLETED";
        URI uri = URI.create("http://localhost/query/dummy-query-id");
        Optional<String> plan = Optional.of("dummy-plan");
        Optional<String> jsonPlan = Optional.of("{\"plan\": \"dummy-plan\"}");
        Optional<String> graphvizPlan = Optional.of("digraph {node1 -> node2}");
        Optional<String> payload = Optional.of("dummy-payload");
        List<String> runtimeOptimizedStages = new ArrayList<>(Arrays.asList("stage1", "stage2"));
        Optional<String> tracingId = Optional.of("dummy-tracing-id");
        Optional<String> updateType = Optional.of("CREATE TABLE");

        return new QueryMetadata(
                queryId,
                transactionId,
                query,
                queryHash,
                preparedQuery,
                queryState,
                uri,
                plan,
                jsonPlan,
                graphvizPlan,
                payload,
                runtimeOptimizedStages,
                tracingId,
                updateType);
    }

    private static QueryContext createDummyQueryContext()
    {
        String user = "dummyUser";
        String serverAddress = "127.0.0.1";
        String serverVersion = "testversion";
        String environment = "testing";
        String workerType = "worker-1";

        Optional<String> principal = Optional.of("dummyPrincipal");
        Optional<String> remoteClientAddress = Optional.of("192.168.1.100");
        Optional<String> userAgent = Optional.of("Mozilla/5.0");
        Optional<String> clientInfo = Optional.of("Dummy Client Info");
        Optional<String> source = Optional.empty();
        Optional<String> catalog = Optional.of("dummyCatalog");
        Optional<String> schema = Optional.of("dummySchema");
        Optional<ResourceGroupId> resourceGroupId = Optional.of(new ResourceGroupId("dummyGroupId"));

        Set<String> clientTags = new HashSet<>(Arrays.asList("tag1", "tag2", "tag3"));

        Map<String, String> sessionProperties = new HashMap<>();
        sessionProperties.put("property1", "value1");
        // This will increment the joinPushdownEnabledQueryCounter by 1
        sessionProperties.put(INNER_JOIN_PUSHDOWN_ENABLED, "true");

        ResourceEstimates resourceEstimates = new ResourceEstimates(
                Optional.of(new com.facebook.airlift.units.Duration(1200, TimeUnit.SECONDS)),
                Optional.of(new com.facebook.airlift.units.Duration(1200, TimeUnit.SECONDS)),
                Optional.of(new com.facebook.airlift.units.DataSize(2, DataSize.Unit.GIGABYTE)),
                Optional.of(new com.facebook.airlift.units.DataSize(2, DataSize.Unit.GIGABYTE)));
        return new QueryContext(
                user,
                principal,
                remoteClientAddress,
                userAgent,
                clientInfo,
                clientTags,
                source,
                catalog,
                schema,
                resourceGroupId,
                sessionProperties,
                resourceEstimates,
                serverAddress,
                serverVersion,
                environment,
                workerType);
    }

    private static QueryIOMetadata createDummyQueryIoMetadata()
    {
        List<QueryInputMetadata> inputs = new ArrayList<>();
        QueryInputMetadata queryInputMetadata = getQueryInputMetadata();
        inputs.add(queryInputMetadata);
        OutputColumnMetadata column1 = new OutputColumnMetadata("column1", "int", new HashSet<>());
        OutputColumnMetadata column2 = new OutputColumnMetadata("column2", "varchar", new HashSet<>());
        OutputColumnMetadata column3 = new OutputColumnMetadata("column3", "varchar", new HashSet<>());
        List<OutputColumnMetadata> columns = new ArrayList<>();
        columns.add(column1);
        columns.add(column2);
        columns.add(column3);
        QueryOutputMetadata outputMetadata = new QueryOutputMetadata(
                "dummyCatalog",
                "dummySchema",
                "dummyTable",
                Optional.of("dummyConnectorMetadata"),
                Optional.of(true),
                Optional.of(columns),
                Optional.of((Object) "commitOutputDummy"));
        return new QueryIOMetadata(inputs, Optional.of(outputMetadata));
    }

    private static QueryInputMetadata getQueryInputMetadata()
    {
        String catalogName = "dummyCatalog";
        String schema = "dummySchema";
        String table = "dummyTable";
        String serializedCommitOutput = "commitOutputDummy";
        Column column1 = new Column("column1", "int");
        Column column2 = new Column("column2", "varchar");
        Column column3 = new Column("column3", "varchar");
        List<Column> columns = Arrays.asList(column1, column2, column3);
        Optional<Object> connectorInfo = Optional.of(new Object());
        return new QueryInputMetadata(
                catalogName,
                schema,
                table,
                columns,
                connectorInfo,
                Optional.empty(),
                Optional.of((Object) serializedCommitOutput));
    }

    public static QueryStatistics createDummyQueryStatistics()
    {
        Duration cpuTime = Duration.ofMillis(1000);
        Duration retriedCpuTime = Duration.ofMillis(500);
        Duration wallTime = Duration.ofMillis(2000);
        Duration totalScheduledTime = Duration.ofMillis(2500);
        Duration waitingForPrerequisitesTime = Duration.ofMillis(300);
        Duration queuedTime = Duration.ofMillis(1500);
        Duration waitingForResourcesTime = Duration.ofMillis(600);
        Duration semanticAnalyzingTime = Duration.ofMillis(700);
        Duration columnAccessPermissionCheckingTime = Duration.ofMillis(200);
        Duration dispatchingTime = Duration.ofMillis(1200);
        Duration planningTime = Duration.ofMillis(2500);
        Optional<Duration> analysisTime = Optional.of(Duration.ofMillis(1800));
        Duration executionTime = Duration.ofMillis(3500);
        Duration finishingTime = Duration.ofMillis(500);

        int peakRunningTasks = 5;
        long peakUserMemoryBytes = 500000000L;
        long peakTotalNonRevocableMemoryBytes = 800000000L;
        long peakTaskUserMemory = 100000000L;
        long peakTaskTotalMemory = 200000000L;
        long peakNodeTotalMemory = 120000000L;
        long shuffledBytes = 10000000L;
        long shuffledRows = 200000L;
        long totalBytes = 30000000L;
        long totalRows = 400000L;
        long outputBytes = 5000000L;
        long outputRows = 60000L;
        long writtenOutputBytes = 7000000L;
        long writtenOutputRows = 80000L;
        long writtenIntermediateBytes = 9000000L;
        long spilledBytes = 1000000L;
        double cumulativeMemory = 150.5;
        double cumulativeTotalMemory = 200.5;
        int completedSplits = 100;
        boolean complete = true;
        RuntimeStats runtimeStats = new RuntimeStats();
        return new QueryStatistics(
                cpuTime,
                retriedCpuTime,
                wallTime,
                totalScheduledTime,
                waitingForPrerequisitesTime,
                queuedTime,
                waitingForResourcesTime,
                semanticAnalyzingTime,
                columnAccessPermissionCheckingTime,
                dispatchingTime,
                planningTime,
                analysisTime,
                executionTime,
                finishingTime,
                peakRunningTasks,
                peakUserMemoryBytes,
                peakTotalNonRevocableMemoryBytes,
                peakTaskUserMemory,
                peakTaskTotalMemory,
                peakNodeTotalMemory,
                shuffledBytes,
                shuffledRows,
                totalBytes,
                totalRows,
                outputBytes,
                outputRows,
                writtenOutputBytes,
                writtenOutputRows,
                writtenIntermediateBytes,
                spilledBytes,
                cumulativeMemory,
                cumulativeTotalMemory,
                completedSplits,
                complete,
                runtimeStats);
    }

    @Test
    public void smokeTest()
    {
        traceMetrics(createDummyQueryCompletedEvent());
        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            assertEquals(1L, mbeanServer.getAttribute(ObjectName.getInstance
                            ("com.facebook.metrics:type=MoreMetrics,name=EventListenerMetrics"),
                    "JoinPushdownEnabledQueryCounter.TotalCount"));
            assertEquals(1L, mbeanServer.getAttribute(ObjectName.getInstance
                            ("com.facebook.metrics:type=MoreMetrics,name=EventListenerMetrics"),
                    "JoinPushdownSuccessfulCounter.TotalCount"));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
