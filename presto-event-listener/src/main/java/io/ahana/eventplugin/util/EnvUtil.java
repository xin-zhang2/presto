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
package io.ahana.eventplugin.util;

import com.facebook.airlift.log.Logger;

import java.util.Map;

public class EnvUtil
{
    private EnvUtil()
    {}

    private static final Logger logger = Logger.get(EnvUtil.class);
    private static final String SAAS = "saas";
    private static final String LH_CONTEXT_PROPERTY = "LH_CONTEXT";

    public static String getEnvValue(String key)
    {
        if (key == null || key.trim().length() == 0) {
            return null;
        }
        Map<String, String> env = System.getenv();
        String value = System.getProperty(key);
        logger.debug("key:value = " + key + ":" + value);
        logger.debug("key:env_value = " + key + ":" + env.get(key));
        return value != null ? value : env.get(key);
    }

    public static boolean isOnCloud()
    {
        String platform = getEnvValue(LH_CONTEXT_PROPERTY);
        logger.debug("found platform value as: " + platform);
        // platform value should be sw_ent for CPD and LH_CONTEXT is set in CPD, but not in SaaS.
        if (platform == null) {
            platform = SAAS;
        }
        boolean isCloud = SAAS.equalsIgnoreCase(platform);
        logger.debug("isOnCloud: " + isCloud);
        return isCloud;
    }
}
