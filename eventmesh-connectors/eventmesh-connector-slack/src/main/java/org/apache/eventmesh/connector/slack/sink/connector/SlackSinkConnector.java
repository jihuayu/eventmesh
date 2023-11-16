/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eventmesh.connector.slack.sink.connector;

import com.slack.api.Slack;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import org.apache.eventmesh.connector.slack.sink.config.SlackSinkConfig;
import org.apache.eventmesh.openconnect.api.config.Config;
import org.apache.eventmesh.openconnect.api.connector.ConnectorContext;
import org.apache.eventmesh.openconnect.api.connector.SinkConnectorContext;
import org.apache.eventmesh.openconnect.api.sink.Sink;
import org.apache.eventmesh.openconnect.offsetmgmt.api.data.ConnectRecord;

import java.io.IOException;
import java.util.List;
import java.util.Objects;


import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.apache.eventmesh.connector.slack.common.constants.ConnectRecordExtensionKeys.*;

@Slf4j
public class SlackSinkConnector implements Sink {


    private SlackSinkConfig sinkConfig;

    private MethodsClient methods;
    private volatile boolean isRunning = false;

    @Override
    public Class<? extends Config> configClass() {
        return SlackSinkConfig.class;
    }

    @Override
    public void init(Config config) throws Exception {
        // init config for dingding sink connector
        this.sinkConfig = (SlackSinkConfig) config;
        initSlackApp();
    }

    @Override
    public void init(ConnectorContext connectorContext) throws Exception {
        // init config for dingding source connector
        SinkConnectorContext sinkConnectorContext = (SinkConnectorContext) connectorContext;
        this.sinkConfig = (SlackSinkConfig) sinkConnectorContext.getSinkConfig();
        initSlackApp();
    }

    private void initSlackApp() {
        Slack slack = Slack.getInstance();
        methods = slack.methods(sinkConfig.getSinkConnectorConfig().getApiToken());
    }

    @Override
    public void start() {
        isRunning = true;
    }

    @Override
    public void commit(ConnectRecord record) {

    }

    @Override
    public String name() {
        return this.sinkConfig.getSinkConnectorConfig().getConnectorName();
    }

    @Override
    public void stop() {
        isRunning = false;
    }

    public boolean isRunning() {
        return isRunning;
    }

    @SneakyThrows
    @Override
    public void put(List<ConnectRecord> sinkRecords) {

        for (ConnectRecord record : sinkRecords) {
            if (Objects.isNull(record.getData())) {
                log.warn("ConnectRecord data is null, ignore.");
                continue;
            }
            String channelName = record.getExtension(SLACK_CHANNEL_NAME);
            ChatPostMessageRequest request = ChatPostMessageRequest.builder()
                    .channel(channelName) // Use a channel ID `C1234567` is preferable
                    .text(String.valueOf(record.getData()))
                    .build();
            try {
                ChatPostMessageResponse response = methods.chatPostMessage(request);
                if (response.isOk()) {
                    System.out.println("消息发送成功！");
                } else {
                    System.out.println("消息发送失败。错误信息：" + response.getError());
                }
            } catch (IOException e) {
                System.out.println("发送消息时出现异常：" + e.getMessage());
            }
        }
    }


}
