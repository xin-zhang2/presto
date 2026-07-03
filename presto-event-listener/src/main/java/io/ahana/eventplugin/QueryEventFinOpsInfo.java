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

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.ahana.eventplugin.util.EnvUtil.getEnvValue;

public class QueryEventFinOpsInfo
{
    @JsonProperty("event_payload")
    private final String eventPayload;

    @JsonProperty("event_type")
    private final String eventType;

    @JsonProperty("instance_id")
    private final String instanceId;

    @JsonProperty("engine_id")
    private final String engineId;

    // FinOps initiative is only for SaaS deployments. Hence, crn is introduced as a field.
    @JsonProperty("crn")
    private final String crn;

    @JsonProperty("engine_type")
    private final String engineType = "presto";

    @JsonProperty("timestamp")
    private final String timestamp = String.valueOf(Instant.now());

    public QueryEventFinOpsInfo(String eventPayload, String eventType, String crn)
    {
        this.eventPayload = eventPayload;
        this.eventType = eventType;
        this.instanceId = extractInstanceIdFromCrn(crn).orElseThrow(() -> new PrestoException(NOT_FOUND, "CRN value was not found"));
        this.crn = crn;
        this.engineId = getEnvValue("GROUP");
        if (isNullOrEmpty(engineId)) {
            throw new PrestoException(NOT_FOUND, "GROUP variable was not found");
        }
    }

    public String getEventPayload()
    {
        return eventPayload;
    }

    public String getEventType()
    {
        return eventType;
    }

    public String getInstanceId()
    {
        return instanceId;
    }

    public String getCrn()
    {
        return crn;
    }

    public String getEngineType()
    {
        return engineType;
    }

    public String getEngineId()
    {
        return engineId;
    }

    public String getTimestamp()
    {
        return timestamp;
    }

    private Optional<String> extractInstanceIdFromCrn(String crn)
    {
        // example: crn:v1:bluemix:public:watsonxdata:us-south:a/48024265e13b4f25a1fe439457506951:fa93b6d6-cd02-4bf2-a3f9-6682060f0794::
        if (crn.isEmpty()) {
            return Optional.empty();
        }
        String[] splits = crn.split(":");
        return Optional.of(splits[splits.length - 1]);
    }
}
