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
package io.ahana.eventplugin.kafka;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

import java.util.Set;

public class CustomOAuthBearerToken
        implements OAuthBearerToken
{
    private final String tokenValue;
    private final String principalName;
    private final long startTime;
    private final long expirationTime;
    private final Set<String> scopes;

    public CustomOAuthBearerToken(String tokenValue, String principalName, long expTime, long startTime, Set<String> scopes)
    {
        this.tokenValue = tokenValue;
        this.principalName = principalName;
        this.expirationTime = expTime;
        this.startTime = startTime;
        this.scopes = scopes;
    }

    @Override
    public String value()
    {
        return tokenValue;
    }

    @Override
    public Set<String> scope()
    {
        return scopes;
    }

    @Override
    public long lifetimeMs()
    {
        return expirationTime;
    }

    @Override
    public String principalName()
    {
        return principalName;
    }

    @Override
    public Long startTimeMs()
    {
        return startTime;
    }
}
