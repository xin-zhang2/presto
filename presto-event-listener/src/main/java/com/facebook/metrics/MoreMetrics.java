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

import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.google.common.annotations.Beta;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;
import org.weakref.jmx.ObjectNames;

import java.lang.management.ManagementFactory;

/**
 * A class to create JMX metrics _outside_ of the Presto engine. This is an ideal place for metrics that are
 * 1. WxD specific metrics that do not make a lot of sense in OSS
 * 2. Derived metrics that are still being experimented with
 */
@Beta
public class MoreMetrics
{
    public static final String INNER_JOIN_PUSHDOWN_ENABLED = "optimizer_inner_join_pushdown_enabled";
    private static final MoreMetrics INSTANCE = new MoreMetrics();
    private final CounterStat joinPushdownEnabledQueryCounter = new CounterStat();
    private final CounterStat joinPushdownSuccessfulCounter = new CounterStat();

    private MoreMetrics()
    {
    }

    public static void traceMetrics(QueryCompletedEvent queryCompletedEvent)
    {
        INSTANCE.traceJoinPushdownMetrics(queryCompletedEvent);
    }

    private void traceJoinPushdownMetrics(QueryCompletedEvent queryCompletedEvent)
    {
        boolean joinPushdownSessionPropertySet = Boolean.parseBoolean(queryCompletedEvent.getContext().getSessionProperties().get(INNER_JOIN_PUSHDOWN_ENABLED));

        if (joinPushdownSessionPropertySet) {
            // TODO : We can introspect the query more before logging this metric, e.g if the query does not involve any JDBC sources
            // there is no point in updating this counter. For now, we assume that users are only going to set this session flag when they
            // are running queries against at least one JDBC source
            joinPushdownEnabledQueryCounter.update(1L);

            boolean didJoinPushdownTrigger = queryCompletedEvent.getOptimizerInformation().stream()
                    .anyMatch(p -> (p.getOptimizerName().equals("OnlyJoinRule") || p.getOptimizerName().equals("FilterOnJoinRule"))
                            && p.getOptimizerTriggered()
                            && !(p.getOptimizerFailure().isPresent() && !p.getOptimizerFailure().get())); // No failure during the rule processing;

            if (didJoinPushdownTrigger) {
                joinPushdownSuccessfulCounter.update(1L);
            }
        }
    }

    @Managed
    @Nested
    public CounterStat getJoinPushdownEnabledQueryCounter()
    {
        return joinPushdownEnabledQueryCounter;
    }

    @Managed
    @Nested
    public CounterStat getJoinPushdownSuccessfulCounter()
    {
        return joinPushdownSuccessfulCounter;
    }

    static {
        new MBeanExporter(ManagementFactory.getPlatformMBeanServer()).export(ObjectNames.generatedNameOf(MoreMetrics.class, "EventListenerMetrics"), INSTANCE);
    }
}
