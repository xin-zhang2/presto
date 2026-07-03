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
package io.ahana.eventplugin.opentelemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.extension.trace.propagation.B3Propagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.ServiceAttributes;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.ahana.eventplugin.QueryEventListenerFactory.EXPORTER_ENDPOINT;
import static io.ahana.eventplugin.QueryEventListenerFactory.EXPORTER_TIMEOUT;
import static io.ahana.eventplugin.QueryEventListenerFactory.MAX_EXPORTER_BATCH_SIZE;
import static io.ahana.eventplugin.QueryEventListenerFactory.MAX_QUEUE_SIZE;
import static io.ahana.eventplugin.QueryEventListenerFactory.SCHEDULE_DELAY;
import static io.ahana.eventplugin.QueryEventListenerFactory.TRACE_SAMPLING_RATIO;

public final class OpenTelemetryBuilder
{
    private OpenTelemetryBuilder()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated.");
    }

    /**
     * Get instance of propagator.
     * Currently, only B3_SINGLE_HEADER can be passed in.
     */
    private static TextMapPropagator getPropagatorInstance(String contextPropagator)
    {
        TextMapPropagator propagator;
        if (contextPropagator.equals(OpenTelemetryContextPropagator.W3C)) {
            propagator = W3CTraceContextPropagator.getInstance();
        }
        else if (contextPropagator.equals(OpenTelemetryContextPropagator.B3_SINGLE_HEADER)) {
            propagator = B3Propagator.injectingSingleHeader();
        }
        else {
            propagator = B3Propagator.injectingMultiHeaders();
        }
        return propagator;
    }

    public static OpenTelemetry build(String contextPropagator, Map<String, String> config)
    {
        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(ServiceAttributes.SERVICE_NAME, "presto")));

        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                .setSampler(Sampler.traceIdRatioBased(Double.parseDouble(config.get(TRACE_SAMPLING_RATIO))))
                .addSpanProcessor(BatchSpanProcessor.builder(OtlpGrpcSpanExporter.builder().setEndpoint(config.get(EXPORTER_ENDPOINT)).build())
                        .setMaxExportBatchSize(Integer.parseInt(config.get(MAX_EXPORTER_BATCH_SIZE)))
                        .setMaxQueueSize(Integer.parseInt(config.get(MAX_QUEUE_SIZE)))
                        .setScheduleDelay(Integer.parseInt(config.get(SCHEDULE_DELAY)), TimeUnit.MILLISECONDS)
                        .setExporterTimeout(Integer.parseInt(config.get(EXPORTER_TIMEOUT)), TimeUnit.MILLISECONDS)
                        .build())
                .setResource(resource)
                .build();

        return OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .setPropagators(ContextPropagators.create(OpenTelemetryBuilder.getPropagatorInstance(contextPropagator)))
                .buildAndRegisterGlobal();
    }
}
