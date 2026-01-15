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
package com.facebook.presto.tvf;

import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.TableFunctionHandleResolver;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.facebook.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.presto.tvf.HttpClientHolder.getHttpClient;
import static com.facebook.presto.tvf.NativeTVFProvider.getWorkerLocation;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.util.Objects.requireNonNull;

public class NativeTableFunctionHandle
        implements ConnectorTableFunctionHandle
{
    private static final String TVF_SPLITS_ENDPOINT = "/v1/tvf/splits";

    private final QualifiedObjectName functionName;
    private final String serializedTableFunctionHandle;

    @JsonCreator
    public NativeTableFunctionHandle(
            @JsonProperty("serializedTableFunctionHandle") String serializedTableFunctionHandle,
            @JsonProperty("functionName") QualifiedObjectName functionName)
    {
        this.serializedTableFunctionHandle = requireNonNull(serializedTableFunctionHandle, "serializedTableFunctionHandle is null");
        this.functionName = requireNonNull(functionName, "functionName is null");
    }

    @JsonProperty
    public String getSerializedTableFunctionHandle()
    {
        return serializedTableFunctionHandle;
    }

    @JsonProperty("functionName")
    public QualifiedObjectName getFunctionName()
    {
        return functionName;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, NodeManager nodeManager)
    {
        return new FixedSplitSource(
                getHttpClient().execute(
                        prepareSplitsPostRequest(nodeManager, this),
                        createJsonResponseHandler(JsonCodec.listJsonCodec(NativeTableFunctionSplit.class))));
    }

    private static Request prepareSplitsPostRequest(NodeManager nodeManager, NativeTableFunctionHandle nativeTableFunctionHandle)
    {
        return preparePost()
                .setUri(getWorkerLocation(nodeManager, TVF_SPLITS_ENDPOINT))
                .setBodyGenerator(jsonBodyGenerator(
                        JsonCodec.jsonCodec(ManualNativeTableFunctionHandleJsonHandler.class),
                        new ManualNativeTableFunctionHandleJsonHandler(
                                // The handle resolver for TVF providers is always the factory name suffixed by a colon.
                                NativeTVFProviderFactory.NAME + ":" + NativeTableFunctionHandle.class.getName(),
                                nativeTableFunctionHandle.serializedTableFunctionHandle,
                                nativeTableFunctionHandle.functionName)))
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(ACCEPT, JSON_UTF_8.toString())
                .build();
    }

    public static class Resolver
            implements TableFunctionHandleResolver
    {
        @Override
        public Set<Class<? extends ConnectorTableFunctionHandle>> getTableFunctionHandleClasses()
        {
            return ImmutableSet.of(NativeTableFunctionHandle.class);
        }
    }

    // todo: Hacky way to manually convert a NativeTableFunctionHandle JSON
    protected static class ManualNativeTableFunctionHandleJsonHandler
    {
        private final String type;
        private final String serializedTableFunctionHandle;
        private final QualifiedObjectName functionName;

        @JsonCreator
        public ManualNativeTableFunctionHandleJsonHandler(
                @JsonProperty("@type") String type,
                @JsonProperty("serializedTableFunctionHandle") String serializedTableFunctionHandle,
                @JsonProperty("functionName") QualifiedObjectName functionName)
        {
            this.type = requireNonNull(type, "type is null");
            this.serializedTableFunctionHandle = requireNonNull(serializedTableFunctionHandle, "serializedTableFunctionHandle is null");
            this.functionName = requireNonNull(functionName, "functionName is null");
        }

        @JsonProperty("@type")
        public String getType()
        {
            return type;
        }

        @JsonProperty
        public String getSerializedTableFunctionHandle()
        {
            return serializedTableFunctionHandle;
        }

        @JsonProperty("functionName")
        public QualifiedObjectName getFunctionName()
        {
            return functionName;
        }
    }
}
