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

import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryEventInfo
{
    @JsonProperty("session_id")
    private String sessionId;
    @JsonProperty("db_name")
    private String dbName;
    @JsonProperty("app_user_name")
    private String appUserName;
    @JsonProperty("data")
    private final Data data;
    @JsonProperty("session_locator")
    private SessionLocator sessionLocator;
    @JsonProperty("accessor")
    private final Accessor accessor;
    @JsonProperty("time")
    private final Time time;
    @JsonProperty("exception")
    private final QueryException exception;

    public QueryEventInfo(String sessionId,
                          String dbName,
                          String appUserName,
                          Data data,
                          SessionLocator sessionLocator,
                          Accessor accessor,
                          QueryException exception,
                          Time time)
    {
        this.sessionId = sessionId;
        this.dbName = dbName;
        this.appUserName = appUserName;
        this.data = data;
        this.sessionLocator = sessionLocator;
        this.accessor = accessor;
        this.exception = exception;
        this.time = time;
    }

    static class Data
    {
        @JsonProperty("original_sql_command")
        private String originalSqlCommand;

        @JsonProperty("query_id")
        private String queryId;

        @JsonProperty("state")
        private String state;

        public Data(String sqlCommand, String queryId, String queryState)
        {
            this.originalSqlCommand = sqlCommand;
            this.queryId = queryId;
            this.state = queryState;
        }
    }

    static class Time
    {
        @JsonProperty("createTime")
        private final String createTime;
        @JsonProperty("minOffsetFromGMT")
        private int minOffsetFromGMT;
        @JsonProperty("minDst")
        private int minDst;

        public Time(String createTime)
        {
            this.createTime = createTime;
        }
    }

    static class SessionLocator
    {
        @JsonProperty("client_ip")
        private String clientIp;
        @JsonProperty("client_port")
        private int clientPort;
        @JsonProperty("server_ip")
        private String serverIp;
        @JsonProperty("server_port")
        private int serverPort;
        @JsonProperty("is_ipv6")
        private boolean isIpV6;
        @JsonProperty("client_ipv6")
        private String clientIpV6;
        @JsonProperty("server_ipv6")
        private String serverIpV6;

        public SessionLocator(Builder builder)
        {
            this.clientIp = builder.clientIp;
            this.clientPort = builder.clientPort;
            this.serverIp = builder.serverIp;
            this.serverPort = builder.serverPort;
            this.isIpV6 = builder.isIpV6;
            this.clientIpV6 = builder.clientIpV6;
            this.serverIpV6 = builder.serverIpV6;
        }

        static class Builder
        {
            private String clientIp;
            private int clientPort;
            private String serverIp;
            private int serverPort;
            private boolean isIpV6;
            private String clientIpV6;
            private String serverIpV6;

            public Builder setClientIp(String clientIp)
            {
                this.clientIp = clientIp;
                return this;
            }

            public Builder setServerIp(String serverIp)
            {
                this.serverIp = serverIp;
                return this;
            }

            public Builder setServerPort(int serverPort)
            {
                this.serverPort = serverPort;
                return this;
            }

            public SessionLocator build()
            {
                return new SessionLocator(this);
            }
        }
    }

    static class Accessor
    {
        @JsonProperty("db_user")
        private String dbUser;
        @JsonProperty("server_type")
        private String serverType;
        @JsonProperty("server_os")
        private String serverOs;
        @JsonProperty("client_os")
        private String clientOs;
        @JsonProperty("client_hostname")
        private String clientHostName;
        @JsonProperty("server_hostname")
        private String serverHostName;
        @JsonProperty("comm_protocol")
        private String commProtocol;
        @JsonProperty("db_protocol")
        private String dbProtocol;
        @JsonProperty("db_protocol_version")
        private String dbProtocolVersion;
        @JsonProperty("os_user")
        private String osUser;
        @JsonProperty("source_program")
        private String sourceProgram;
        @JsonProperty("client_mac")
        private String clientMac;
        @JsonProperty("server_description")
        private String serverDescription;
        @JsonProperty("service_name")
        private String serviceName;
        @JsonProperty("type")
        private String type;

        public Accessor(Builder builder)
        {
            this.dbUser = builder.dbUser;
            this.serverType = builder.serverType;
            this.serverOs = builder.serverOs;
            this.clientOs = builder.clientOs;
            this.clientHostName = builder.clientHostName;
            this.serverHostName = builder.serverHostname;
            this.commProtocol = builder.commProtocol;
            this.dbProtocol = builder.dbProtocol;
            this.dbProtocolVersion = builder.dbProtocolVersion;
            this.osUser = builder.osUser;
            this.sourceProgram = builder.sourceProgram;
            this.clientMac = builder.clientMac;
            this.serverDescription = builder.serverDescription;
            this.serviceName = builder.serviceName;
            this.type = builder.type;
        }

        static class Builder
        {
            private String dbUser;
            private String serverOs;
            private String serverHostname;
            private String serverType;
            private String clientOs;
            private String clientHostName;
            private String commProtocol;
            private String dbProtocol;
            private String dbProtocolVersion;
            private String osUser;
            private String sourceProgram;
            private String clientMac;
            private String serverDescription;
            private String serviceName;
            private String type;

            public Builder setDbUser(String dbUser)
            {
                this.dbUser = dbUser;
                return this;
            }

            public Builder setServerOs(String serverOs)
            {
                this.serverOs = serverOs;
                return this;
            }

            public Builder setServerHostName(String serverHostName)
            {
                this.serverHostname = serverHostname;
                return this;
            }

            public Builder setServiceName(String serviceName)
            {
                this.serviceName = serviceName;
                return this;
            }

            public Builder setServerType(String serverType)
            {
                this.serverType = serverType;
                return this;
            }

            public Builder setDbProtocol(String dbProtocol)
            {
                this.dbProtocol = dbProtocol;
                return this;
            }

            public Builder setType(String type)
            {
                this.type = type;
                return this;
            }

            public Accessor build()
            {
                return new Accessor(this);
            }
        }
    }

    static class QueryException
    {
        @JsonProperty("exception_type_id")
        private String exceptionTypeId;
        @JsonProperty("description")
        private String description;

        public QueryException(String exceptionTypeId, String description)
        {
            this.exceptionTypeId = exceptionTypeId;
            this.description = description;
        }
    }
}
