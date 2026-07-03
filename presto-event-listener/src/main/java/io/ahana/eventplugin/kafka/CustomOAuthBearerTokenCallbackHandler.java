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

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.auth0.jwt.algorithms.Algorithm.HMAC256;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.ahana.eventplugin.util.EnvUtil.getEnvValue;
import static io.ahana.eventplugin.util.EnvUtil.isOnCloud;
import static java.lang.String.format;

public class CustomOAuthBearerTokenCallbackHandler
        implements AuthenticateCallbackHandler
{
    private static final Logger logger = Logger.get(CustomOAuthBearerTokenCallbackHandler.class);
    private static final String ROLES = "roles";
    private static final String LH_INSTANCE_SECRET = "LH_INSTANCE_SECRET";
    private static final String ACCOUNT_ID_PROPERTY = "ACCOUNT";
    private static final String MDS = "mds";
    private static final String CRN = "CRN";
    private static final String ADMIN = "admin";
    private static final String INTERNAL = "internal";
    private static final String INTERNAL_ROLE = "internal_role";
    private static final String TENANT_ID = "tenantId";
    private static final String EXPIRY_MINUTES = "15";
    private static final String TOKEN_EXPIRY_PROPERTY = "TOKEN_EXPIRY";
    private static final String EMPTY_STRING = "";
    private static final String CRN_ACCOUNT_ID_REGEX = ":(a|sub)/([^:]+)";
    private static final String VALIDATE_CRN_REGEX = "^crn:v1:[^:]+:[^:]+:[^:]+:[^:]+:[^:]+/[^:]+:[^:]+::$";

    /**
     * Handles the {@link Callback}s passed by the Kafka client.
     *
     * <p>If the callback is an instance of {@link OAuthBearerTokenCallback}, a new JWT token is created
     * with predefined values and set in the callback. Otherwise, an {@link UnsupportedCallbackException}
     * is thrown.</p>
     *
     * @param callbacks the array of callbacks to handle
     * @throws UnsupportedCallbackException if an unsupported callback is encountered
     */
    @Override
    public void handle(Callback[] callbacks)
            throws UnsupportedCallbackException
    {
        logger.debug("Starting authentication process with updated CallbackHandler");
        String accountId = getAccountId();

        for (Callback callback : callbacks) {
            logger.debug(format("received callback %s with values: accountId: %s", callback, accountId));
            logger.debug(format("callback is instance of OAuthBearerTokenCallback: %s, SaslExtensionsCallback: %s", (callback instanceof OAuthBearerTokenCallback), (callback instanceof SaslExtensionsCallback)));
            if (callback instanceof OAuthBearerTokenCallback) {
                long nowMillis = System.currentTimeMillis();
                String token = createJWT(MDS, INTERNAL, Set.of(INTERNAL_ROLE));
                ((OAuthBearerTokenCallback) callback).token(new CustomOAuthBearerToken(token, ADMIN, fetchExpirationTime(nowMillis).getTime(), nowMillis, Set.of(INTERNAL_ROLE)));
            }
            else if (callback instanceof SaslExtensionsCallback && isOnCloud()) {
                SaslExtensionsCallback extCallback = (SaslExtensionsCallback) callback;
                if (isNullOrEmpty(accountId)) {
                    throw new PrestoException(NOT_FOUND, "ACCOUNT_ID env variable not found");
                }
                Map<String, String> extensions = Map.of(
                        TENANT_ID, accountId);
                extCallback.extensions(new SaslExtensions(extensions));
            }
            else {
                logger.info("Unsupported callback handler {}", callback);
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    /**
     * Configures the handler when the Kafka client is initialized.
     *
     * @param configs Kafka client configuration map
     * @param saslMechanism the SASL mechanism (should be OAUTHBEARER)
     * @param jaasConfigEntries the JAAS configuration entries
     */
    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries)
    {
        logger.info("CallbackHandler is configured with saslMechanism as " + saslMechanism);
    }

    @Override
    public void close()
    {
        logger.info("Closing CustomOAuthBearerTokenCallback");
    }

    public static String createJWT(String issuer, String subject, Set<String> roles)
    {
        long nowMillis = System.currentTimeMillis();
        Date now = new Date(nowMillis);

        return JWT.create()
                .withIssuedAt(now)
                .withSubject(subject)
                .withIssuer(issuer)
                .withExpiresAt(fetchExpirationTime(nowMillis))
                .withClaim(ROLES, new ArrayList<>(roles))
                .sign(getValidSigningKey(System.getenv().getOrDefault(LH_INSTANCE_SECRET, EMPTY_STRING)));
    }

    public static Date fetchExpirationTime(long nowMillis)
    {
        long expiryMinutes = Long.parseLong(System.getProperty(TOKEN_EXPIRY_PROPERTY, EXPIRY_MINUTES));
        return new Date(nowMillis + expiryMinutes * 60 * 1000L);
    }

    public static Algorithm getValidSigningKey(String rawSecret)
    {
        if (rawSecret.isEmpty()) {
            throw new PrestoException(NOT_FOUND, "LH_INSTANCE_SECRET env variable not found");
        }
        final int minSizeKey = 32;

        byte[] secretBytes = rawSecret.getBytes(StandardCharsets.UTF_8);

        // Pad the secret if it's too short
        if (secretBytes.length < minSizeKey) {
            byte[] extended = new byte[minSizeKey];
            for (int i = 0; i < minSizeKey; i++) {
                extended[i] = secretBytes[i % secretBytes.length];
            }
            secretBytes = extended;
        }

        return HMAC256(secretBytes);
    }

    private String getAccountId()
    {
        String account = getEnvValue(ACCOUNT_ID_PROPERTY);
        logger.debug("Found ACCOUNT variable value: " + account);
        if (isNullOrEmpty(account)) {
            String crn = getEnvValue(CRN);
            String accountId = generateAccountId(crn);
            logger.debug("ACCOUNT ID from CRN: " + accountId);
            return accountId;
        }
        return account;
    }

    private String generateAccountId(String crn)
    {
        logger.debug("CRN: " + crn);
        if (isValidCRN(crn)) {
            Matcher matcher = Pattern.compile(CRN_ACCOUNT_ID_REGEX).matcher(crn);
            if (matcher.find()) {
                return matcher.group(2);
            }
        }
        throw new PrestoException(INVALID_ARGUMENTS, "Invalid CRN format. Please provide a valid CRN.");
    }

    private boolean isValidCRN(String crn)
    {
        return crn != null && Pattern.compile(VALIDATE_CRN_REGEX).matcher(crn).matches();
    }
}
