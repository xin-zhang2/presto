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
package io.ahana.eventplugin;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.OperatorStatistics;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.ResourceDistribution;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.facebook.presto.spi.eventlistener.StageGcStatistics;
import com.facebook.presto.spi.eventlistener.StageStatistics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.ahana.eventplugin.kafka.CustomOAuthBearerTokenCallbackHandler;
import io.ahana.eventplugin.opentelemetry.OpenTelemetryBuilder;
import io.ahana.eventplugin.opentelemetry.OpenTelemetryTracer;
import io.ahana.eventplugin.opentelemetry.OpenTelemetryTracerHandle;
import io.ahana.eventplugin.opentelemetry.OpenTelemetryTracerProvider;
import io.opentelemetry.api.OpenTelemetry;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.skife.jdbi.v2.DBI;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.metrics.MoreMetrics.traceMetrics;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.ahana.eventplugin.CesKafkaEventType.FINOPS;
import static io.ahana.eventplugin.CesKafkaEventType.GUARDIUM;
import static io.ahana.eventplugin.QueryEventListenerFactory.CES_TRUST_STORE_PASSWORD;
import static io.ahana.eventplugin.QueryEventListenerFactory.CES_TRUST_STORE_PATH;
import static io.ahana.eventplugin.QueryEventListenerFactory.CES_TRUST_STORE_TYPE;
import static io.ahana.eventplugin.QueryEventListenerFactory.EXPORTER_ENDPOINT;
import static io.ahana.eventplugin.QueryEventListenerFactory.FINOPS_EVENT_KAFKA_TOPIC;
import static io.ahana.eventplugin.QueryEventListenerFactory.GUARDIUM_EVENT_KAFKA_TOPIC;
import static io.ahana.eventplugin.QueryEventListenerFactory.KAFKA_BROKERS;
import static io.ahana.eventplugin.opentelemetry.OpenTelemetryTracer.currentContextPropagator;
import static io.ahana.eventplugin.util.EnvUtil.getEnvValue;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.common.config.SslConfigs.DEFAULT_SSL_TRUSTSTORE_TYPE;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;

public final class QueryEventListener
        implements EventListener
{
    private static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final JsonCodec<ResourceDistribution> RESOURCE_DISTRIBUTION_JSON_CODEC = new JsonCodecFactory().jsonCodec(ResourceDistribution.class);
    private static final JsonCodec<StageGcStatistics> STAGE_GC_STATISTICS_JSON_CODEC = new JsonCodecFactory().jsonCodec(StageGcStatistics.class);
    private static final JsonCodec<RuntimeStats> RUNTIME_STATS_JSON_CODEC = new JsonCodecFactory().jsonCodec(RuntimeStats.class);
    public static final String CERT_PATH = "CERT_PATH";
    private static final String QUERY_EVENT_BLOCK = "Query :: ";
    private static final String QUERY_ID = "query_id";
    private static final String QUERY_STATE = "query_state";
    private static final String QUERY = "query";
    private static final String ERROR = "ERROR";
    private static final String STAGE_ID = "stage_id";
    private static final String OPERATOR_ID = "operator_id";
    private static final String OPERATOR_TYPE = "operator_type";
    private static final String TRACE_ID = "trace_id";
    private static final String COORDINATOR = "coordinator";
    private static final String INSTANCE_ID = "instance_id";
    private static final String CRN = "CRN";
    private static final String LH_INSTANCE_ID = "LH_INSTANCE_ID";
    private static final String SSL_IDENTIFICATION_ALGORITHM = "ssl.endpoint.identification.algorithm";
    private static final String SSL_IDENTIFICATION_ALGORITHM_DEFAULT = "";
    private static final String OAUTHBEARER_SASL_MECHANISM = "OAUTHBEARER";
    private static final String SECURITY_PROTOCOL = "security.protocol";
    private static final String SASL_SECURITY_PROTOCOL = "SASL_SSL";
    private static final String CREATED = "CREATED";
    private static final String COMPLETED = "COMPLETED";
    private static final String PRESTO_ENGINE = "presto";
    private static final String PRESTO_SERVER_TYPE = "presto java";

    private final Logger eventLogger;
    private final com.facebook.airlift.log.Logger logger = com.facebook.airlift.log.Logger.get(QueryEventListener.class);
    private final boolean trackEventCreated;
    private final boolean trackEventCompleted;
    private final boolean trackEventCompletedSplit;
    private final String instanceId;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String clusterName;
    private final boolean sendToWebSocketServer;
    private boolean useMysqlServiceCollector;
    private boolean useAuditCollector;
    private String auditUrl;
    private FilterConfig filterConfig;
    private WebSocketCollectorChannel webSocketCollectorChannel;
    private final Map<String, String> config = new HashMap<>();
    private MySQLWriter mySQLWriter;
    private static final Pattern FILTER_PATTERN = Pattern.compile(
            "SELECT.*FROM.*WHERE.*('qhmm'='qhmm'|'%''qhmm''=''qhmm''%')",
            Pattern.CASE_INSENSITIVE);
    private final Map<String, String> queryMap = new HashMap<>();
    private OpenTelemetryTracerProvider tracerProvider;
    private static OpenTelemetry openTelemetry;
    private boolean tracingEnabled;
    private String lhInstanceId;
    private String crn;
    private boolean guardiumEnabled;
    private boolean finOpsEnabled;
    private KafkaProducer<String, String> producer;
    private String guardiumCesKafkatopic;
    private String cesKafkaBootstrapServers;
    private String trustStorePath;
    private String trustStorePassword;
    private String trustStoreType;
    private String finOpsCesKafkaTopic;

    public QueryEventListener(
            final String clusterName,
            final LoggerContext loggerContext,
            final boolean sendToWebSocketServer,
            final String webSockerCollectUrl,
            final boolean trackEventCreated,
            final boolean trackEventCompleted,
            final boolean trackEventCompletedSplit)
    {
        this.instanceId = UUID.randomUUID().toString();
        this.clusterName = clusterName;
        this.trackEventCreated = trackEventCreated;
        this.trackEventCompleted = trackEventCompleted;
        this.trackEventCompletedSplit = trackEventCompletedSplit;
        this.eventLogger = loggerContext.getLogger(QueryEventListener.class.getName());
        this.sendToWebSocketServer = sendToWebSocketServer;

        if (sendToWebSocketServer) {
            this.webSocketCollectorChannel = new WebSocketCollectorChannel(webSockerCollectUrl);
            this.webSocketCollectorChannel.connect();
        }
        this.mapper.registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule());
    }

    public QueryEventListener(final String clusterName,
            final LoggerContext loggerContext,
            final boolean sendToWebSocketServer,
            final String webSockerCollectUrl,
            final boolean trackEventCreated,
            final boolean trackEventCompleted,
            final boolean trackEventCompletedSplit,
            final boolean useMysqlServiceCollector,
            final boolean useAuditCollector,
            final String auditUrl,
            final FilterConfig filterConfig,
            final Map<String, String> config,
            final boolean tracingEnabled,
            final boolean guardiumEnabled,
            final boolean finOpsEnabled)
    {
        this(clusterName, loggerContext, sendToWebSocketServer, webSockerCollectUrl, trackEventCreated, trackEventCompleted, trackEventCompletedSplit);
        this.useMysqlServiceCollector = useMysqlServiceCollector;
        this.useAuditCollector = useAuditCollector;
        this.auditUrl = auditUrl;
        this.filterConfig = filterConfig;
        this.config.putAll(config);

        if (this.useMysqlServiceCollector) {
            mySQLWriter = new MySQLWriter(config);
        }

        this.tracingEnabled = tracingEnabled;
        logger.debug("Query tracing [%s]", tracingEnabled);
        if (this.tracingEnabled) {
            fetchEnvParameters();
            this.tracerProvider = new OpenTelemetryTracerProvider();
            openTelemetry = OpenTelemetryBuilder.build(currentContextPropagator, config);
            logger.debug("Otel tracing end point: [%s]", config.get(EXPORTER_ENDPOINT));
        }

        this.guardiumEnabled = guardiumEnabled;
        this.finOpsEnabled = finOpsEnabled;
        if (guardiumEnabled || finOpsEnabled) {
            // Guardium is used in CPD deployments, while FinOps is only there for SaaS deployments
            configureKafkaProducer();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            eventLogger.debug("Shutdown hook triggered, closing kafka producer");
            try {
                if (producer != null) {
                    producer.flush();
                    producer.close();
                }
                eventLogger.debug("kafka producer closed successfully");
            }
            catch (Exception e) {
                eventLogger.error("Error during kafka producer shutdown " + e);
            }
        }));
    }

    private void configureKafkaProducer()
    {
        try {
            this.guardiumCesKafkatopic = config.get(GUARDIUM_EVENT_KAFKA_TOPIC);
            this.finOpsCesKafkaTopic = config.get(FINOPS_EVENT_KAFKA_TOPIC);
            this.cesKafkaBootstrapServers = config.get(KAFKA_BROKERS);
            this.trustStorePath = config.get(CES_TRUST_STORE_PATH);
            this.trustStorePassword = config.get(CES_TRUST_STORE_PASSWORD);
            this.trustStoreType = config.getOrDefault(CES_TRUST_STORE_TYPE, DEFAULT_SSL_TRUSTSTORE_TYPE);
            logger.debug(String.format("Configured CES with trustStorePath %s, trustStoreType %s", trustStorePath, trustStoreType));

            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cesKafkaBootstrapServers);
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(SECURITY_PROTOCOL, SASL_SECURITY_PROTOCOL);
            properties.setProperty(SASL_MECHANISM, OAUTHBEARER_SASL_MECHANISM);
            properties.setProperty(SASL_JAAS_CONFIG, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
            properties.setProperty(SASL_LOGIN_CALLBACK_HANDLER_CLASS, CustomOAuthBearerTokenCallbackHandler.class.getName());

            if (guardiumEnabled) {
                properties.setProperty(SSL_TRUSTSTORE_LOCATION_CONFIG, trustStorePath); // only for CPD
                properties.setProperty(SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword); // only for CPD
                properties.setProperty(SSL_TRUSTSTORE_TYPE_CONFIG, trustStoreType); // only for CPD
            }

            properties.put(SSL_IDENTIFICATION_ALGORITHM, SSL_IDENTIFICATION_ALGORITHM_DEFAULT);
            Properties producerProperties = new Properties(properties);
            if (guardiumEnabled) {
                producerProperties.remove(SSL_TRUSTSTORE_PASSWORD_CONFIG);
            }
            logger.debug("Configuring CES kafka producer with " + producerProperties);
            producer = new KafkaProducer<>(properties);
            logger.debug("Kafka configured successfully!");
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unable to configure CES kafka producer", e);
        }
    }

    private void sendQueryEventToKafka(String message, String queryId, String kafkaTopic, CesKafkaEventType eventType)
    {
        ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, queryId, message);
        if (producer != null) {
            producer.send(record);
            logger.debug("Sent query event of type [%s] for query id [%s] to Kafka topic [%s] successfully!", eventType, queryId, kafkaTopic);
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        if (!trackEventCreated || !isCoordinator()) {
            return;
        }

        String query = queryCreatedEvent.getMetadata().getQuery();
        String queryId = queryCreatedEvent.getMetadata().getQueryId();
        QueryContext context = queryCreatedEvent.getContext();

        if (filterConfig.isFilteringEnabled()) {
            String eventType = "queryCreated";
            if (filterConfig.isQueryActionTypeEnabled() && queryCreatedEvent.getMetadata().getUpdateQueryType().isPresent() && queryCreatedEvent.getMetadata().getUpdateQueryType().get().equalsIgnoreCase(filterConfig.getQueryActionType())) {
                logger.debug("%s :: QueryId [%s] - query filtered from event listener logging for MataData Type :  [%s]", eventType, queryId, filterConfig.getQueryActionType());
                return;
            }
            if (filterQueryEventUsingQueryContext(context, eventType, queryId)) {
                return;
            }
            logger.debug("queryCreated :: QueryId [%s] - query check initiated to match filter condition", queryId);
            if (identifySelectQueryType(query) && hasFilterCondition(query)) {
                logger.debug("queryCreated :: QueryId [%s] - query filtered from event listener logging ", queryId);
                return;
            }
        }
        QueryMetadata queryMetadata = new QueryMetadata(
                queryId,
                queryCreatedEvent.getMetadata().getTransactionId(),
                flatten(query),
                queryCreatedEvent.getMetadata().getQueryHash(),
                queryCreatedEvent.getMetadata().getPreparedQuery(),
                queryCreatedEvent.getMetadata().getQueryState(),
                queryCreatedEvent.getMetadata().getUri(),
                Optional.of(""),
                queryCreatedEvent.getMetadata().getJsonPlan(),
                queryCreatedEvent.getMetadata().getGraphvizPlan(),
                Optional.of(""),
                queryCreatedEvent.getMetadata().getRuntimeOptimizedStages(),
                queryCreatedEvent.getMetadata().getTracingId(),
                queryCreatedEvent.getMetadata().getUpdateQueryType());

        QueryCreatedEvent queryCreatedEvent1 = new QueryCreatedEvent(
                queryCreatedEvent.getCreateTime(),
                queryCreatedEvent.getContext(),
                queryMetadata);

        String eventPayload = "";
        try {
            eventPayload = this.mapper.writeValueAsString(new QueryEvent(this.instanceId, this.clusterName, queryCreatedEvent1, null,
                    null, flatten(queryCreatedEvent.getMetadata().getPlan().orElse("null")), 0, 0, 0, 0, 0));
            eventLogger.info(eventPayload);
            logger.debug("queryCreated :: QueryId [%s] - Created event triggered", queryId);

            if (sendToWebSocketServer) {
                this.webSocketCollectorChannel.sendMessage(eventPayload);
                logger.debug("queryCreated :: QueryId [%s] - Created event send to websocket server", queryId);
            }
        }
        catch (JsonProcessingException e) {
            logger.error("queryCreated :: QueryId [%s] - Failed to serialize query log event - [%s]", queryId, e.getMessage());
        }
        catch (Exception e) {
            logger.error("queryCreated :: QueryId [%s] - Failed to process query created event", queryId, e.getMessage());
        }

        // Post to mysql service
        if (useMysqlServiceCollector) {
            mySQLWriter.post(queryCreatedEvent);
            logger.debug("queryCreated :: QueryId [%s] - Created event submitted to mysql service", queryId);
        }

        if (this.tracingEnabled) {
            queryMap.put(queryId, eventPayload);
        }
        try {
            sendEventToCesKafka(queryCreatedEvent.getContext(),
                    queryCreatedEvent.getMetadata(),
                    eventPayload,
                    query,
                    queryId,
                    queryCreatedEvent.getCreateTime(),
                    CREATED,
                    Optional.empty(),
                    Optional.empty());
        }
        catch (Exception e) {
            logger.error("queryCreated :: QueryId [%s] - Failed to process query created event", queryId, e.getMessage());
        }
        logger.debug("queryCreated :: QueryId [%s] - Finished Query created event", queryId);
    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        traceMetrics(queryCompletedEvent);
        if (!trackEventCompleted || !isCoordinator()) {
            return;
        }
        String zenAuditEnable = Optional.ofNullable(System.getenv("ENABLE_AUDIT")).orElse("");
        String queryId = queryCompletedEvent.getMetadata().getQueryId();
        if ("true".equalsIgnoreCase(zenAuditEnable)) {
            logger.debug("queryCompleted :: QueryId [%s] - Asynchronously calling API with log data...", queryId);
            auditEvents(queryCompletedEvent);
        }
        String query = queryCompletedEvent.getMetadata().getQuery();
        logger.debug("queryCompleted :: QueryId [%s] - Started Query completed event", queryId);
        QueryContext context = queryCompletedEvent.getContext();

        if (filterConfig.isFilteringEnabled()) {
            String eventType = "queryCompleted";
            if (filterConfig.isQueryActionTypeEnabled() && queryCompletedEvent.getQueryType().isPresent() && queryCompletedEvent.getQueryType().get().toString().equalsIgnoreCase(filterConfig.getQueryActionType())) {
                logger.debug("%s :: QueryId [%s] - query filtered from event listener logging for MataData Type :  [%s]", eventType, queryId, filterConfig.getQueryActionType());
                return;
            }
            if (filterQueryEventUsingQueryContext(context, eventType, queryId)) {
                return;
            }

            logger.debug("queryCompleted :: QueryId [%s] - query check initiated to match filter condition", queryId);
            Optional<QueryType> queryType = queryCompletedEvent.getQueryType();
            if (queryType.isPresent() && queryType.get().equals(QueryType.SELECT) && hasFilterCondition(query)) {
                logger.debug("queryCompleted :: QueryId [%s] - query filtered from event listener logging", queryId);
                return;
            }
        }
        QueryMetadata queryMetadata = new QueryMetadata(
                queryId,
                queryCompletedEvent.getMetadata().getTransactionId(),
                flatten(query),
                queryCompletedEvent.getMetadata().getQueryHash(),
                queryCompletedEvent.getMetadata().getPreparedQuery(),
                queryCompletedEvent.getMetadata().getQueryState(),
                queryCompletedEvent.getMetadata().getUri(),
                Optional.of(""),
                queryCompletedEvent.getMetadata().getJsonPlan(),
                queryCompletedEvent.getMetadata().getGraphvizPlan(),
                Optional.of(""),
                queryCompletedEvent.getMetadata().getRuntimeOptimizedStages(),
                queryCompletedEvent.getMetadata().getTracingId(),
                queryCompletedEvent.getMetadata().getUpdateQueryType());

        QueryCompletedEvent queryCompletedEvent1 = new QueryCompletedEvent(
                queryMetadata,
                queryCompletedEvent.getStatistics(),
                queryCompletedEvent.getContext(),
                queryCompletedEvent.getIoMetadata(),
                queryCompletedEvent.getFailureInfo(),
                queryCompletedEvent.getWarnings(),
                queryCompletedEvent.getQueryType(),
                queryCompletedEvent.getFailedTasks(),
                queryCompletedEvent.getCreateTime(),
                queryCompletedEvent.getExecutionStartTime(),
                queryCompletedEvent.getEndTime(),
                queryCompletedEvent.getStageStatistics(),
                queryCompletedEvent.getOperatorStatistics(),
                queryCompletedEvent.getPlanStatisticsRead(),
                queryCompletedEvent.getPlanStatisticsWritten(),
                queryCompletedEvent.getPlanNodeHash(),
                queryCompletedEvent.getCanonicalPlan(),
                queryCompletedEvent.getStatsEquivalentPlan(),
                queryCompletedEvent.getExpandedQuery(),
                queryCompletedEvent.getOptimizerInformation(),
                queryCompletedEvent.getCteInformationList(),
                queryCompletedEvent.getScalarFunctions(),
                queryCompletedEvent.getAggregateFunctions(),
                queryCompletedEvent.getWindowFunctions(),
                queryCompletedEvent.getPrestoSparkExecutionContext(),
                queryCompletedEvent.getHboPlanHash(),
                Optional.empty(),
                queryCompletedEvent.getQualifiedName());

        try {
            // Logging for payload
            String eventPayload = "";
            try {
                eventPayload = this.mapper.writeValueAsString(new QueryEvent(this.instanceId, this.clusterName, null, queryCompletedEvent1,
                        null, flatten(queryCompletedEvent.getMetadata().getPlan().orElse("null")), getTimeValue(queryCompletedEvent.getStatistics().getCpuTime()),
                        getTimeValue(queryCompletedEvent.getStatistics().getRetriedCpuTime()), getTimeValue(queryCompletedEvent.getStatistics().getWallTime()), getTimeValue(queryCompletedEvent.getStatistics().getQueuedTime()),
                        getTimeValue(queryCompletedEvent.getStatistics().getAnalysisTime().orElse(null))));
            }
            catch (JsonProcessingException e) {
                logger.error("queryCompleted :: QueryId [%s] - Failed to serialize query log event - [%s]", queryId, e.getMessage());
            }
            eventLogger.info(eventPayload);
            logger.debug("queryCompleted :: QueryId [%s] - Completed event triggered", queryId);
            captureTraces(queryCompletedEvent, query, queryId, eventPayload);

            if (sendToWebSocketServer) {
                this.webSocketCollectorChannel.sendMessage(eventPayload);
                logger.debug("queryCompleted :: QueryId [%s] -  Completed event send to websocket server", queryId);
            }

            sendEventToCesKafka(queryCompletedEvent.getContext(),
                    queryCompletedEvent.getMetadata(),
                    eventPayload,
                    query,
                    queryId,
                    queryCompletedEvent.getCreateTime(),
                    COMPLETED,
                    queryCompletedEvent.getFailureInfo(),
                    queryCompletedEvent.getQueryType());
        }
        catch (Exception e) {
            logger.error("queryCompleted :: QueryId [%s] - Failed to process query completed event", queryId, e.getMessage());
        }
        finally {
            queryMap.remove(queryId);
        }

        // Post to mysql service
        if (useMysqlServiceCollector) {
            mySQLWriter.post(queryCompletedEvent);
            logger.debug("Query  %s - Completed event submitted to mysql service", queryId);
        }
    }

    private void sendEventToCesKafka(QueryContext context, QueryMetadata metadata, String eventPayload, String query, String queryId, Instant createTimeInstant, String queryEventType, Optional<QueryFailureInfo> failureInfo, Optional<QueryType> queryType)
            throws JsonProcessingException
    {
        try {
            if (guardiumEnabled) {
                sendEventToGuardiumTopic(context,
                        metadata,
                        query,
                        queryId,
                        createTimeInstant,
                        queryEventType,
                        failureInfo,
                        queryType);
            }
            if (finOpsEnabled) {
                sendEventToFinOpsTopic(eventPayload, queryId, queryEventType);
            }
        }
        catch (Exception e) {
            logger.error("QueryId [%s] - Failed to send query event to CES", queryId, e.getMessage());
            throw e;
        }
    }

    private void sendEventToGuardiumTopic(QueryContext context,
                                          QueryMetadata metadata,
                                          String query,
                                          String queryId,
                                          Instant createTimeInstant,
                                          String queryEventType,
                                          Optional<QueryFailureInfo> failureInfoOptional,
                                          Optional<QueryType> queryTypeOptional)
            throws JsonProcessingException
    {
        String user = context.getUser();
        String serverIp = context.getServerAddress();
        QueryEventInfo.Data data = new QueryEventInfo.Data(query, metadata.getQueryId(), metadata.getQueryState());
        QueryEventInfo.Accessor accessor = new QueryEventInfo.Accessor.Builder()
                .setDbUser(user)
                .setServerOs(context.getEnvironment())
                .setServerHostName(serverIp)
                .setServiceName(PRESTO_ENGINE)
                .setServerType(PRESTO_SERVER_TYPE)
                .setDbProtocol(metadata.getUri().getScheme())
                .setType(queryTypeOptional.map(Enum::toString).orElse(""))
                .build();
        QueryEventInfo.SessionLocator sessionLocator = new QueryEventInfo.SessionLocator.Builder()
                .setClientIp(context.getRemoteClientAddress().orElse("null"))
                .setServerIp(serverIp)
                .setServerPort(metadata.getUri().getPort())
                .build();

        String createTime = createTimeInstant.atZone(UTC).format(DATETIME_FORMAT);
        QueryEventInfo.Time time = new QueryEventInfo.Time(createTime);
        QueryEventInfo.QueryException exception = null;

        if (queryEventType.equals(COMPLETED) && metadata.getQueryState().equalsIgnoreCase("failed")
                && failureInfoOptional.isPresent()) {
            QueryFailureInfo failureInfo = failureInfoOptional.get();
            exception = new QueryEventInfo.QueryException(failureInfo.getErrorCode().getName(), failureInfo.getFailureMessage().orElse(""));
        }

        String sessionId = metadata.getTransactionId().orElse(null);
        String dbName = context.getSchema().orElse("null");
        QueryEventInfo queryEventInfo = new QueryEventInfo(sessionId, dbName, user, data, sessionLocator, accessor, exception, time);

        String eventMessage = this.mapper.writeValueAsString(queryEventInfo);
        sendQueryEventToKafka(eventMessage, queryId, guardiumCesKafkatopic, GUARDIUM);
    }

    private boolean filterQueryEventUsingQueryContext(QueryContext context, String eventType, String queryId)
    {
        if (filterConfig.isQueryContextEnvEnabled() && context.getEnvironment() != null && context.getEnvironment().equalsIgnoreCase(filterConfig.getQueryContextEnv())) {
            logger.debug("%s :: QueryId [%s] - query filtered from event listener logging for Query Context Environment :  [%s]", eventType, queryId, filterConfig.getQueryContextEnv());
            return true;
        }
        if (filterConfig.isQueryContextSourceEnabled() && context.getSource().isPresent() && context.getSource().get().equalsIgnoreCase(filterConfig.getQueryContextSource())) {
            logger.debug("%s :: QueryId [%s] - query filtered from event listener logging for Query Context Source :  [%s]", eventType, queryId, filterConfig.getQueryContextSource());
            return true;
        }

        return false;
    }

    private void captureTraces(QueryCompletedEvent queryCompletedEvent, String query, String queryId, String eventPayload)
    {
        if (this.tracingEnabled) {
            logger.debug("Starting OpenTelemetry Tracer for QueryId [%s] ", queryId);
            Map<String, String> requestHeaders = new HashMap<>();
            OpenTelemetryTracerHandle tracerHandle = this.tracerProvider.getHandleGenerator().apply(requestHeaders);
            OpenTelemetryTracer tracer = Optional.of(requireNonNull(this.tracerProvider.getNewTracer(openTelemetry, tracerHandle, config), "tracer is null")).get();
            String queryBlockName = QUERY_EVENT_BLOCK + query;
            logger.debug("Starting span [%s]", queryBlockName);
            tracer.startBlock(queryBlockName, queryMap.get(queryId));

            String queryState = queryCompletedEvent.getMetadata().getQueryState();
            String traceId = queryCompletedEvent.getMetadata().getTracingId().get();

            tracer.addAttributeToBlock(queryBlockName, QUERY_ID, queryId);
            tracer.addAttributeToBlock(queryBlockName, QUERY_STATE, queryState);
            tracer.addAttributeToBlock(queryBlockName, QUERY, query);
            tracer.addAttributeToBlock(queryBlockName, TRACE_ID, traceId);

            if (!lhInstanceId.isEmpty()) {
                tracer.addAttributeToBlock(queryBlockName, INSTANCE_ID, lhInstanceId);
            }
            if (!crn.isEmpty()) {
                tracer.addAttributeToBlock(queryBlockName, CRN.toLowerCase(Locale.ENGLISH), crn);
            }

            if (queryCompletedEvent.getFailureInfo().isPresent()) {
                tracer.setBlockStatus(queryBlockName, ERROR);
                String failureJson = queryCompletedEvent.getFailureInfo().get().getFailuresJson();
                tracer.addPointToBlock(queryBlockName, failureJson);
            }

            Map<String, List<OperatorStatistics>> stageOperatorsMap = new HashMap<>();
            List<OperatorStatistics> operatorStatisticsList = queryCompletedEvent.getOperatorStatistics();
            for (OperatorStatistics operatorStatistics : operatorStatisticsList) {
                int stageId = operatorStatistics.getStageId();
                String stageSpanName = "stage-" + stageId;
                stageOperatorsMap.putIfAbsent(stageSpanName, new ArrayList<>());
                List<OperatorStatistics> operatorList = stageOperatorsMap.get(stageSpanName);
                operatorList.add(operatorStatistics);
                stageOperatorsMap.put(stageSpanName, operatorList);
            }

            List<StageStatistics> stageStatisticsList = queryCompletedEvent.getStageStatistics();
            for (StageStatistics stageStatistics : stageStatisticsList) {
                int stageId = stageStatistics.getStageId();
                String stageSpanName = "stage-" + stageId;
                String stageInfosJson = "";
                try {
                    stageInfosJson = this.mapper.writeValueAsString(stageStatistics);
                }
                catch (JsonProcessingException e) {
                    logger.error("queryCompleted :: QueryId [%s] - Failed to serialize stage statistics - [%s] -  [%s]", queryId, stageSpanName, e.getMessage());
                }
                logger.debug("Starting stage span [%s]", stageSpanName);
                tracer.startBlock(stageSpanName, queryBlockName, stageInfosJson);
                tracer.addAttributeToBlock(stageSpanName, STAGE_ID, String.valueOf(stageId));

                List<OperatorStatistics> operatorList = stageOperatorsMap.get(stageSpanName);
                for (OperatorStatistics operatorStatistics : operatorList) {
                    int operatorId = operatorStatistics.getOperatorId();
                    String spanName = "stage-" + stageId + "_" + operatorStatistics.getOperatorType() + "-" + operatorId;
                    String parentSpanName = "stage-" + operatorStatistics.getStageId();
                    String operatorInfosJson = "";
                    try {
                        operatorInfosJson = this.mapper.writeValueAsString(operatorStatistics);
                    }
                    catch (JsonProcessingException e) {
                        logger.error("queryCompleted :: QueryId [%s] - Failed to serialize operator statistics - [%s] -[%s]", queryId, parentSpanName, e.getMessage());
                    }
                    logger.debug("Starting operator span [%s]", spanName);
                    tracer.startBlock(spanName, parentSpanName, operatorInfosJson);
                    tracer.addAttributeToBlock(spanName, STAGE_ID, String.valueOf(stageId));
                    tracer.addAttributeToBlock(spanName, OPERATOR_ID, String.valueOf(operatorId));
                    tracer.addAttributeToBlock(spanName, OPERATOR_TYPE, operatorStatistics.getOperatorType());
                    tracer.endBlock(spanName, "");
                    logger.debug("Ending operator span [%s]", spanName);
                }
                logger.debug("Ending stage span [%s]", stageSpanName);
                tracer.endBlock(stageSpanName, "");
            }

            logger.debug("Ending span [%s]", queryBlockName);
            tracer.endBlock(queryBlockName, eventPayload);
            tracer.endTrace("Qeury Completed with state " + queryState);
            logger.debug("Ending OpenTelemetry Tracer for QueryId [%s] ", queryId);
        }
    }

    private void sendEventToFinOpsTopic(String eventPayload, String queryId, String queryEventType)
            throws JsonProcessingException
    {
        fetchEnvParameters();
        logger.debug("Found crn value as " + crn + " for queryId: " + queryId);
        QueryEventFinOpsInfo queryEventFinOpsInfo = new QueryEventFinOpsInfo(eventPayload, queryEventType, crn);
        logger.debug("Found engine_id as " + queryEventFinOpsInfo.getEngineId() + " for queryId " + queryId);
        String eventFinOpsMessage = this.mapper.writeValueAsString(queryEventFinOpsInfo);
        sendQueryEventToKafka(eventFinOpsMessage, queryId, finOpsCesKafkaTopic, FINOPS);
    }

    private void fetchEnvParameters()
    {
        // CRN is set for wxd Saas deployments
        if (this.crn == null) {
            String crn = getEnvValue(CRN);
            this.crn = crn != null ? crn : "";
        }
        // LH_INSTANCE_ID is set for wxd CPD deployments
        if (this.lhInstanceId == null) {
            String instanceId = getEnvValue(LH_INSTANCE_ID);
            this.lhInstanceId = instanceId != null ? instanceId : "";
        }
    }

    /**
     * @param queryCompletedEvent
     */
    private void auditEvents(QueryCompletedEvent queryCompletedEvent)
    {
        String queryId = queryCompletedEvent.getMetadata().getQueryId();
        logger.debug("auditEvents :: QueryId [%s] - Posting query events audit data", queryId);
        LogEntry log = getLogEntryDataObject(queryCompletedEvent);
        try {
            logger.debug("auditEvents :: QueryId [%s] - Asynchronously call an API with the log data", queryId);
            sendAuditEventToZen(log, queryId);
        }
        catch (Exception e) {
            logger.debug("auditEvents :: QueryId [%s] - error in auditing events %s", queryId, e.getMessage());
        }
        if (useAuditCollector) {
            logger.debug("auditEvents :: QueryId [%s] - Writing audit log to at log path ");
            log.log();
        }
    }

    private void sendAuditEventToZen(LogEntry log, String queryId)
    {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            try {
                String inputJsonString = log.toString();
                String path = "records";
                String outputPath = "/var/presto/data/var/log/auditApiOut.txt";
                String certPath = "/mnt/infra/tls";
                String caFile;
                String keyFile;
                String certFile;
                if (System.getenv("SSL_CUSTOM_KEY") == null || System.getenv("SSL_CUSTOM_KEY").isEmpty()) {
                    keyFile = certPath + "/tls.key";
                }
                else {
                    keyFile = System.getenv("SSL_CUSTOM_KEY");
                }
                if (System.getenv(CERT_PATH) == null || System.getenv(CERT_PATH).isEmpty()) {
                    caFile = certPath + "/ca.crt";
                    certFile = certPath + "/tls.crt";
                }
                else {
                    caFile = System.getenv(CERT_PATH) + "/ca.crt";
                    certFile = System.getenv(CERT_PATH) + "/tls.crt";
                }
                List<String> builder = new ArrayList<>(Arrays.asList("curl", "-s",
                        "-X", "POST",
                        "-d", inputJsonString,
                        "-o", outputPath,
                        "--cert", certFile,
                        "--key", keyFile,
                        "--cacert", caFile,
                        "--header", "Accept: application/json"));
                builder.add(String.format("%s/%s", auditUrl, path));
                ProcessBuilder processBuilder = new ProcessBuilder(builder);
                String curlCommand = String.join(" ", builder);
                logger.debug("sendAuditEventToZen :: QueryId [%s] - Audit message sent to url: %s%s, Using curl command %s", queryId, auditUrl, path, curlCommand);
                Process process = processBuilder.start();
                int exitCode = process.waitFor();
                logger.debug("sendAuditEventToZen :: QueryId [%s] - Process finished execution with Code: %s", queryId, exitCode);
            }
            catch (Exception exception) {
                logger.error("sendAuditEventToZen :: QueryId [%s] - Exception in zen audit logs %s", queryId, exception.getMessage());
            }
        });
        // Adding a callback to be executed when the task is completed
        future.thenRunAsync(() -> {
            logger.debug("sendAuditEventToZen :: QueryId [%s] - Task completed!", queryId);
        });
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        if (!trackEventCompletedSplit) {
            return;
        }
        String queryId = splitCompletedEvent.getQueryId();
        logger.debug("splitCompleted :: QueryId [%s] - Started Query completed event", queryId);

        try {
            String eventPayload = this.mapper.writeValueAsString(new QueryEvent(this.instanceId, this.clusterName, null, null,
                    splitCompletedEvent, null, getTimeValue(splitCompletedEvent.getStatistics().getCpuTime()), 0,
                    getTimeValue(splitCompletedEvent.getStatistics().getWallTime()), getTimeValue(splitCompletedEvent.getStatistics().getQueuedTime()), 0));
            eventLogger.info(eventPayload);
            logger.debug("splitCompleted :: QueryId [%s] - Split completed event triggered", queryId);
            if (sendToWebSocketServer) {
                this.webSocketCollectorChannel.sendMessage(eventPayload);
                logger.debug("splitCompleted :: QueryId [%s] - Created event send to websocket server", queryId);
            }
        }
        catch (JsonProcessingException e) {
            logger.error("splitCompleted :: QueryId [%s] - Failed to serialize query log event", queryId, e);
        }
        logger.debug("splitCompleted :: QueryId [%s] - Finished Query split completed event ", queryId);
    }

    private String flatten(String query)
    {
        return (Optional.ofNullable(query).isPresent())
                ? query.replaceAll("\n", "<<>>") : "";
    }

    public static boolean identifySelectQueryType(String sqlQuery)
    {
        if (sqlQuery != null) {
            String normalizedQuery = sqlQuery.trim();
            return normalizedQuery.regionMatches(true, 0, "SELECT", 0, Math.min(normalizedQuery.length(), 6));
        }
        return false;
    }

    private long getTimeValue(Duration duration)
    {
        return Optional.ofNullable(duration).isPresent()
                ? duration.toMillis() : 0L;
    }

    public static boolean hasFilterCondition(String input)
    {
        Matcher matcher = FILTER_PATTERN.matcher(input);
        return matcher.find();
    }

    public static boolean isCoordinator()
    {
        String isCoordinator = System.getenv(COORDINATOR);
        return StringUtils.isBlank(isCoordinator) || Boolean.parseBoolean(isCoordinator);
    }

    public LogEntry getLogEntryDataObject(QueryCompletedEvent queryCompletedEvent)
    {
        String user = queryCompletedEvent.getContext().getUser();
        String query = queryCompletedEvent.getMetadata().getQuery();
        String data;
        if (query.length() >= 20) {
            data = query.substring(0, 20).toLowerCase(Locale.ENGLISH) + ".....";
        }
        else {
            data = query.toLowerCase(Locale.ENGLISH);
        }
        URI typeURI = queryCompletedEvent.getMetadata().getUri();
        Optional<String> remoteAddr = queryCompletedEvent.getContext().getRemoteClientAddress();
        Map<String, String> host = new HashMap<>();
        host.put("name", user);
        host.put("typeURI", typeURI.toString());
        String[] action = data.split("\\s+");
        String val;
        if (action.length > 0) {
            val = "lakehouse." + "event." + action[0].toString();
        }
        else {
            val = "No data passed";
        }
        Map<String, String> observer = new HashMap<>();
        observer.put("name", "CommonAuditService");
        observer.put("id", "target");
        Map<String, String> target = new HashMap<>();
        target.put("id", System.getenv("CRN"));
        target.put("name", user);
        target.put("typeURI", String.valueOf(typeURI));
        target.put("host", String.valueOf(remoteAddr));

        boolean queryFailed = queryCompletedEvent.getFailureInfo().isPresent();
        Map<String, String> reason = new HashMap<>();
        if (queryFailed) {
            reason.put("reasonForFailure", queryCompletedEvent.getFailureInfo().get().getFailureMessage().orElse(null));
            reason.put("reasonType", queryCompletedEvent.getFailureInfo().get().getFailureType().orElse(null));
            reason.put("reasonCode", String.valueOf(queryCompletedEvent.getFailureInfo().get().getErrorCode().getCode()));
        }
        else {
            reason.put("reasonForFailure", " ");
            reason.put("reasonType", " ");
            reason.put("reasonCode", "200");
        }
        LogEntry auditData;
        auditData = new LogEntry(
                ".log-level",
                "sample",
                reason.get("reasonCode"),
                queryCompletedEvent.getMetadata().getQueryState(),
                queryCompletedEvent.getCreateTime().toString(),
                host,
                val,
                observer,
                "normal",
                "{ type : user}",
                target,
                reason,
                data);

        return auditData;
    }

    private class MySQLWriter
    {
        private static final int MAX_THREADS_FOR_SQL_UPDATE = 10;
        private final Executor executor = new BoundedExecutor(newCachedThreadPool(daemonThreadsNamed("mysql-writer-%s")), MAX_THREADS_FOR_SQL_UPDATE);
        private final Map<String, String> config;
        private final DBI dbi;
        private final PrestoQueryStatsDao dao;

        MySQLWriter(Map<String, String> config)
        {
            this.config = config;
            dbi = new DBI(
                    config.get(QueryEventListenerFactory.QUERYEVENT_JDBC_URI),
                    config.get(QueryEventListenerFactory.QUERYEVENT_JDBC_USER),
                    config.get(QueryEventListenerFactory.QUERY_EVENT_JDBC_PWD));
            dao = dbi.onDemand(PrestoQueryStatsDao.class);
        }

        private synchronized void executeSqlUpdate(Runnable runnable)
                throws PrestoException
        {
            executor.execute(() -> {
                try {
                    runnable.run();
                }
                catch (RuntimeException ex) {
                    logger.warn(ex.getMessage(), "Failed to write query stats for query to MySQL.");
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to write query stats for query to MySQL. \n", ex.getCause());
                }
            });
        }

        private void post(QueryCompletedEvent queryCompletedEvent)
        {
            /**
             * EXPORT TO TABLE: presto_query_plans
             */
            postQueryPlans(queryCompletedEvent);
            /**
             *  EXPORT TO TABLE: presto_query_stage_stats
             */
            postQueryStageStats(queryCompletedEvent.getMetadata().getQueryId(), queryCompletedEvent.getStageStatistics(), queryCompletedEvent.getCreateTime());
            /**
             * EXPORT TO TABLE: presto_query_operator_stats
             */
            postQueryOperatorStats(
                    queryCompletedEvent.getMetadata().getQueryId(),
                    queryCompletedEvent.getOperatorStatistics(),
                    queryCompletedEvent.getCreateTime());
            /**
             * EXPORT TO TABLE: presto_query_statistics
             */
            postQueryStatistics(queryCompletedEvent);
        }

        private void post(QueryCreatedEvent queryCreatedEvent)
        {
            /**
             * EXPORT TO TABLE: presto_query_creation_info
             */
            postQueryCreationInfo(queryCreatedEvent);
        }

        private void postQueryCreationInfo(QueryCreatedEvent queryCreatedEvent)
        {
            executeSqlUpdate(() -> dao.insertQueryCreationInfo(
                    queryCreatedEvent.getMetadata().getQueryId(),
                    queryCreatedEvent.getMetadata().getQuery(),
                    queryCreatedEvent.getCreateTime().atZone(UTC).format(DATETIME_FORMAT),
                    queryCreatedEvent.getContext().getSchema().orElse("null"),
                    queryCreatedEvent.getContext().getCatalog().orElse("null"),
                    queryCreatedEvent.getContext().getEnvironment(),
                    queryCreatedEvent.getContext().getUser(),
                    queryCreatedEvent.getContext().getRemoteClientAddress().orElse("null"),
                    queryCreatedEvent.getContext().getSource().orElse("null"),
                    queryCreatedEvent.getContext().getUserAgent().orElse("null"),
                    queryCreatedEvent.getMetadata().getUri().toASCIIString(),
                    queryCreatedEvent.getContext().getSessionProperties().toString(),
                    queryCreatedEvent.getContext().getServerVersion(),
                    queryCreatedEvent.getContext().getClientInfo().orElse("null"),
                    queryCreatedEvent.getContext().getResourceGroupId().toString(),
                    queryCreatedEvent.getContext().getPrincipal().orElse("null"),
                    queryCreatedEvent.getMetadata().getTransactionId().toString(),
                    queryCreatedEvent.getContext().getClientTags().toString(),
                    queryCreatedEvent.getContext().getResourceEstimates().toString(),
                    queryCreatedEvent.getCreateTime().atZone(UTC).format(DATETIME_FORMAT)));
        }

        private void postQueryPlans(QueryCompletedEvent queryCompletedEvent)
        {
            executeSqlUpdate(() -> dao.insertQueryPlans(
                    queryCompletedEvent.getMetadata().getQueryId(),
                    queryCompletedEvent.getMetadata().getQuery(),
                    queryCompletedEvent.getMetadata().getPlan().orElse("null"),
                    queryCompletedEvent.getMetadata().getJsonPlan().orElse("null"),
                    queryCompletedEvent.getContext().getEnvironment(),
                    queryCompletedEvent.getCreateTime().atZone(UTC).format(DATETIME_FORMAT)));
        }

        private void postQueryStatistics(QueryCompletedEvent queryCompletedEvent)
        {
            boolean queryFailed = queryCompletedEvent.getFailureInfo().isPresent();
            if (queryFailed) {
                executeSqlUpdate(() -> dao.insertQueryStatistics(
                        queryCompletedEvent.getMetadata().getQueryId(),
                        queryCompletedEvent.getMetadata().getQuery(),
                        queryCompletedEvent.getQueryType().isPresent() ? queryCompletedEvent.getQueryType().get().toString() : null,
                        queryCompletedEvent.getContext().getSchema().orElse(null),
                        queryCompletedEvent.getContext().getCatalog().orElse(null),
                        queryCompletedEvent.getContext().getEnvironment(),
                        queryCompletedEvent.getContext().getUser(),
                        queryCompletedEvent.getContext().getRemoteClientAddress().orElse(null),
                        queryCompletedEvent.getContext().getSource().orElse(null),
                        queryCompletedEvent.getContext().getUserAgent().orElse(null),
                        queryCompletedEvent.getMetadata().getUri().toASCIIString(),
                        queryCompletedEvent.getContext().getSessionProperties().toString(),
                        queryCompletedEvent.getContext().getServerVersion(),
                        queryCompletedEvent.getContext().getClientInfo().orElse(null),
                        queryCompletedEvent.getContext().getResourceGroupId().isPresent() ? queryCompletedEvent.getContext().getResourceGroupId().get().toString() : null,
                        queryCompletedEvent.getContext().getPrincipal().orElse(null),
                        queryCompletedEvent.getMetadata().getTransactionId().orElse(null),
                        queryCompletedEvent.getContext().getClientTags().toString(),
                        queryCompletedEvent.getContext().getResourceEstimates().toString(),
                        queryCompletedEvent.getCreateTime().atZone(UTC).format(DATETIME_FORMAT),
                        queryCompletedEvent.getEndTime().atZone(UTC).format(DATETIME_FORMAT),
                        queryCompletedEvent.getExecutionStartTime().atZone(UTC).format(DATETIME_FORMAT),
                        queryCompletedEvent.getMetadata().getQueryState(),
                        queryCompletedEvent.getFailureInfo().get().getFailureMessage().orElse(null),
                        queryCompletedEvent.getFailureInfo().get().getFailureType().orElse(null),
                        queryCompletedEvent.getFailureInfo().get().getFailuresJson(),
                        queryCompletedEvent.getFailureInfo().get().getFailureTask().orElse(null),
                        queryCompletedEvent.getFailureInfo().get().getFailureHost().orElse(null),
                        queryCompletedEvent.getFailureInfo().get().getErrorCode().getCode(),
                        queryCompletedEvent.getFailureInfo().get().getErrorCode().getName(),
                        queryCompletedEvent.getFailureInfo().get().getErrorCode().getType().toString(),
                        Arrays.toString(queryCompletedEvent.getWarnings().toArray()),
                        queryCompletedEvent.getStatistics().getCompletedSplits(),
                        queryCompletedEvent.getStatistics().getAnalysisTime().isPresent() ? queryCompletedEvent.getStatistics().getAnalysisTime().get().toMillis() : 0,
                        queryCompletedEvent.getStatistics().getQueuedTime().toMillis(),
                        queryCompletedEvent.getStatistics().getWallTime().toMillis(),
                        queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().isPresent() ? queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().get().toMillis() : 0,
                        queryCompletedEvent.getStatistics().getCpuTime().toMillis() == 0 ? 0 : queryCompletedEvent.getStatistics().getTotalBytes() / queryCompletedEvent.getStatistics().getCpuTime().toMillis(),
                        queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().isPresent() ? queryCompletedEvent.getStatistics().getTotalBytes() / queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().get().toMillis() : 0,
                        queryCompletedEvent.getStatistics().getTotalRows() == 0 ? 0 : queryCompletedEvent.getStatistics().getTotalRows() / queryCompletedEvent.getStatistics().getCpuTime().toMillis(),
                        queryCompletedEvent.getStatistics().getTotalBytes(),
                        queryCompletedEvent.getStatistics().getTotalRows(),
                        queryCompletedEvent.getStatistics().getOutputRows(),
                        queryCompletedEvent.getStatistics().getOutputBytes(),
                        queryCompletedEvent.getStatistics().getWrittenOutputRows(),
                        queryCompletedEvent.getStatistics().getWrittenOutputBytes(),
                        queryCompletedEvent.getStatistics().getCumulativeMemory(),
                        queryCompletedEvent.getStatistics().getPeakUserMemoryBytes(),
                        queryCompletedEvent.getStatistics().getPeakTotalNonRevocableMemoryBytes(),
                        queryCompletedEvent.getStatistics().getPeakTaskTotalMemory(),
                        queryCompletedEvent.getStatistics().getPeakTaskUserMemory(),
                        queryCompletedEvent.getStatistics().getWrittenOutputBytes(),
                        queryCompletedEvent.getStatistics().getPeakNodeTotalMemory(),
                        queryCompletedEvent.getStatistics().getCpuTime().toMillis(),
                        queryCompletedEvent.getStageStatistics().size(),
                        queryCompletedEvent.getStatistics().getCumulativeTotalMemory(),
                        queryCompletedEvent.getCreateTime().atZone(UTC).format(DATETIME_FORMAT)));
            }
            else {
                executeSqlUpdate(() -> dao.insertQueryStatistics(
                        queryCompletedEvent.getMetadata().getQueryId(),
                        queryCompletedEvent.getMetadata().getQuery(),
                        queryCompletedEvent.getQueryType().isPresent() ? queryCompletedEvent.getQueryType().get().toString() : null,
                        queryCompletedEvent.getContext().getSchema().orElse(null),
                        queryCompletedEvent.getContext().getCatalog().orElse(null),
                        queryCompletedEvent.getContext().getEnvironment(),
                        queryCompletedEvent.getContext().getUser(),
                        queryCompletedEvent.getContext().getRemoteClientAddress().orElse(null),
                        queryCompletedEvent.getContext().getSource().orElse(null),
                        queryCompletedEvent.getContext().getUserAgent().orElse(null),
                        queryCompletedEvent.getMetadata().getUri().toASCIIString(),
                        queryCompletedEvent.getContext().getSessionProperties().toString(),
                        queryCompletedEvent.getContext().getServerVersion(),
                        queryCompletedEvent.getContext().getClientInfo().orElse(null),
                        queryCompletedEvent.getContext().getResourceGroupId().isPresent() ? queryCompletedEvent.getContext().getResourceGroupId().get().toString() : null,
                        queryCompletedEvent.getContext().getPrincipal().orElse(null),
                        queryCompletedEvent.getMetadata().getTransactionId().orElse(null),
                        queryCompletedEvent.getContext().getClientTags().toString(),
                        queryCompletedEvent.getContext().getResourceEstimates().toString(),
                        queryCompletedEvent.getCreateTime().atZone(UTC).format(DATETIME_FORMAT),
                        queryCompletedEvent.getEndTime().atZone(UTC).format(DATETIME_FORMAT),
                        queryCompletedEvent.getExecutionStartTime().atZone(UTC).format(DATETIME_FORMAT),
                        queryCompletedEvent.getMetadata().getQueryState(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        0,
                        null,
                        null,
                        Arrays.toString(queryCompletedEvent.getWarnings().toArray()),
                        queryCompletedEvent.getStatistics().getCompletedSplits(),
                        queryCompletedEvent.getStatistics().getAnalysisTime().isPresent() ? queryCompletedEvent.getStatistics().getAnalysisTime().get().toMillis() : 0,
                        queryCompletedEvent.getStatistics().getQueuedTime().toMillis(),
                        queryCompletedEvent.getStatistics().getWallTime().toMillis(),
                        queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().isPresent() ? queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().get().toMillis() : 0,
                        queryCompletedEvent.getStatistics().getCpuTime().toMillis() == 0 ? 0 : queryCompletedEvent.getStatistics().getTotalBytes() / queryCompletedEvent.getStatistics().getCpuTime().toMillis(),
                        queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().isPresent() ? queryCompletedEvent.getStatistics().getTotalBytes() / queryCompletedEvent.getContext().getResourceEstimates().getExecutionTime().get().toMillis() : 0,
                        queryCompletedEvent.getStatistics().getTotalRows() == 0 ? 0 : queryCompletedEvent.getStatistics().getTotalRows() / queryCompletedEvent.getStatistics().getCpuTime().toMillis(),
                        queryCompletedEvent.getStatistics().getTotalBytes(),
                        queryCompletedEvent.getStatistics().getTotalRows(),
                        queryCompletedEvent.getStatistics().getOutputRows(),
                        queryCompletedEvent.getStatistics().getOutputBytes(),
                        queryCompletedEvent.getStatistics().getWrittenOutputRows(),
                        queryCompletedEvent.getStatistics().getWrittenOutputBytes(),
                        queryCompletedEvent.getStatistics().getCumulativeMemory(),
                        queryCompletedEvent.getStatistics().getPeakUserMemoryBytes(),
                        queryCompletedEvent.getStatistics().getPeakTotalNonRevocableMemoryBytes(),
                        queryCompletedEvent.getStatistics().getPeakTaskTotalMemory(),
                        queryCompletedEvent.getStatistics().getPeakUserMemoryBytes(),
                        queryCompletedEvent.getStatistics().getWrittenOutputBytes(),
                        queryCompletedEvent.getStatistics().getPeakNodeTotalMemory(),
                        queryCompletedEvent.getStatistics().getCpuTime().toMillis(),
                        queryCompletedEvent.getStageStatistics().size(),
                        queryCompletedEvent.getStatistics().getCumulativeMemory(),
                        queryCompletedEvent.getCreateTime().atZone(UTC).format(DATETIME_FORMAT)));
            }
        }

        private void postQueryStageStats(String queryId, List<StageStatistics> stageStatistics, Instant createTime)
        {
            for (StageStatistics stageStatistic : stageStatistics) {
                executeSqlUpdate(() -> dao.insertQueryStageStats(
                        queryId,
                        stageStatistic.getStageId(),
                        stageStatistic.getStageExecutionId(),
                        stageStatistic.getTasks(),
                        stageStatistic.getTotalScheduledTime().toMillis(),
                        stageStatistic.getTotalCpuTime().toMillis(),
                        stageStatistic.getRetriedCpuTime().toMillis(),
                        stageStatistic.getTotalBlockedTime().toMillis(),
                        stageStatistic.getRawInputDataSize().toBytes(),
                        stageStatistic.getProcessedInputDataSize().toBytes(),
                        stageStatistic.getPhysicalWrittenDataSize().toBytes(),
                        STAGE_GC_STATISTICS_JSON_CODEC.toJson(stageStatistic.getGcStatistics()),
                        RESOURCE_DISTRIBUTION_JSON_CODEC.toJson(stageStatistic.getCpuDistribution()),
                        RESOURCE_DISTRIBUTION_JSON_CODEC.toJson(stageStatistic.getMemoryDistribution()),
                        createTime.atZone(UTC).format(DATETIME_FORMAT)));
            }
        }

        private void postQueryOperatorStats(String queryId, List<OperatorStatistics> operatorStatistics, Instant createTime)
        {
            for (OperatorStatistics operatorStatistic : operatorStatistics) {
                executeSqlUpdate(() -> dao.insertQueryOperatorStats(
                        queryId,
                        operatorStatistic.getStageId(),
                        operatorStatistic.getStageExecutionId(),
                        operatorStatistic.getPipelineId(),
                        operatorStatistic.getOperatorId(),
                        operatorStatistic.getPlanNodeId().toString(),
                        operatorStatistic.getOperatorType(),
                        operatorStatistic.getTotalDrivers(),
                        operatorStatistic.getAddInputCalls(),
                        operatorStatistic.getAddInputWall().toMillis(),
                        operatorStatistic.getAddInputCpu().toMillis(),
                        operatorStatistic.getAddInputAllocation().toBytes(),
                        operatorStatistic.getRawInputDataSize().toBytes(),
                        operatorStatistic.getRawInputPositions(),
                        operatorStatistic.getInputDataSize().toBytes(),
                        operatorStatistic.getInputPositions(),
                        operatorStatistic.getSumSquaredInputPositions(),
                        operatorStatistic.getGetOutputCalls(),
                        operatorStatistic.getGetOutputWall().toMillis(),
                        operatorStatistic.getGetOutputCpu().toMillis(),
                        operatorStatistic.getGetOutputAllocation().toBytes(),
                        operatorStatistic.getOutputDataSize().toBytes(),
                        operatorStatistic.getOutputPositions(),
                        operatorStatistic.getPhysicalWrittenDataSize().toBytes(),
                        operatorStatistic.getBlockedWall().toMillis(),
                        operatorStatistic.getFinishCalls(),
                        operatorStatistic.getFinishWall().toMillis(),
                        operatorStatistic.getFinishCpu().toMillis(),
                        operatorStatistic.getFinishAllocation().toBytes(),
                        operatorStatistic.getUserMemoryReservation().toBytes(),
                        operatorStatistic.getRevocableMemoryReservation().toBytes(),
                        operatorStatistic.getSystemMemoryReservation().toBytes(),
                        operatorStatistic.getPeakUserMemoryReservation().toBytes(),
                        operatorStatistic.getPeakSystemMemoryReservation().toBytes(),
                        operatorStatistic.getPeakTotalMemoryReservation().toBytes(),
                        operatorStatistic.getSpilledDataSize().toBytes(),
                        operatorStatistic.getInfo().orElse("null"),
                        RUNTIME_STATS_JSON_CODEC.toJson(operatorStatistic.getRuntimeStats()),
                        createTime.atZone(UTC).format(DATETIME_FORMAT)));
            }
        }
    }

    static class LogEntry
    {
        private final String messageID;
        private final String message;
        private final boolean saveServiceCopy;
        private final String logSourceCRN;
        private final String outcome;
        private static final String typeURI = "http://schemas.dmtf.org/cloud/audit/1.0/event";
        private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        private static final String logDirectory = Objects.nonNull(System.getenv("custom_log_path")) ? System.getenv("custom_log_path") : "/var/log/at/";
        private static final String eventType = "activity";
        private static final long MAX_LOG_FILES = 10; // Maximum number of log files before zipping

        private static final long MAX_FILE_SIZE = 100 * 1024 * 1024; // 100 MB in bytes
        private static final ExecutorService executor = Executors.newSingleThreadExecutor();

        private String eventTime;
        private final Map<String, String> initiator;
        private final Map<String, String> observer;
        private final Map<String, String> reason;
        private final Map<String, String> target;
        private final String requestData;
        private String action;
        private String severity;
        private String credential;

        public String getLogSourceCRN()
        {
            return logSourceCRN;
        }

        public String getMessageID()
        {
            return messageID;
        }

        public String getEventTime()
        {
            return eventTime;
        }

        public void setEventTime(String eventTime)
        {
            this.eventTime = eventTime;
        }

        public String getAction()
        {
            return action;
        }

        public void setAction(String action)
        {
            this.action = action;
        }

        public String getSeverity()
        {
            return severity;
        }

        public void setSeverity(String severity)
        {
            this.severity = severity;
        }

        public String getCredential()
        {
            return credential;
        }

        public void setCredential(String credential)
        {
            this.credential = credential;
        }

        public LogEntry(String logLevel, String message, String errorCode, String outcome, String eventTime, Map<String, String> initiator, String action, Map<String, String> observer, String severity, String credential, Map<String, String> target, Map<String, String> reason, String requestData)
        {
            this.message = message;
            this.messageID = "lakehouse." + errorCode + logLevel;
            this.logSourceCRN = System.getenv("CRN");
            this.saveServiceCopy = true;
            this.outcome = String.valueOf(outcome);
            this.eventTime = eventTime;
            this.initiator = initiator;
            this.action = action;
            this.observer = observer;
            this.severity = severity;
            this.credential = credential;
            this.target = target;
            this.reason = reason;
            this.requestData = requestData;
        }

        public void log()
        {
            String logFileName = "audit-log_" + LocalDate.now().format(dateTimeFormatter) + ".log";
            File logFile = Paths.get(logDirectory, logFileName).toFile();
            // Check if the log file size exceeds the maximum
            if (logFile.length() > MAX_FILE_SIZE) {
                rotateLogFiles(logFileName);
            }
            // Submit log writing task to the executor
            Future<?> future = executor.submit(() -> {
                try (BufferedWriter bufferedWriter = Files.newBufferedWriter(logFile.toPath(), UTF_8, APPEND)) {
                    bufferedWriter.write(this.toString());
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                }
                catch (IOException e) {
                    System.out.println("Error while writing log: " + e.getMessage());
                }
            });
        }

        public String toString()
        {
            return String.format(
                    "{\"outcome\":\"%s\", \"eventType\":\"%s\",  \"typeURI\":\"%s\", \"eventTime\":\"%s\", \"initiator\":\"%s\", \"action\":\"%s\", \"observer\":\"%s\", \"severity\":\"%s\", \"credential\":\"%s\", \"target\":\"%s\", \"reason\":\"%s\", \"requestData\":\"%s\"}",
                    outcome, eventType, typeURI, eventTime, initiator, action, observer, severity, credential, target, reason, requestData);
        }

        private static void rotateLogFiles(String currentLogFileName)
        {
            // Check if the maximum number of log files is reached, if yes, zip and remove older logs
            File logDir = Paths.get(logDirectory).toFile();
            File[] logFiles = logDir.listFiles((dir, name) -> name.startsWith("audit-log_") && name.endsWith(".log"));
            if (logFiles != null && logFiles.length >= MAX_LOG_FILES) {
                for (File logFile : logFiles) {
                    if (!logFile.getName().equals(currentLogFileName)) {
                        try (FileInputStream fis = (FileInputStream) Files.newInputStream(logFile.toPath());
                                GZIPOutputStream gzos = new GZIPOutputStream(
                                        Files.newOutputStream(Paths.get(logFile.getName() + ".gz")))) {
                            byte[] buffer = new byte[1024];
                            int length;
                            while ((length = fis.read(buffer)) > 0) {
                                gzos.write(buffer, 0, length);
                            }
                        }
                        catch (IOException e) {
                            System.out.println("Error while gzipping log files: " + e.getMessage());
                        }
                        logFile.delete();
                    }
                }
            }
        }
    }
}
