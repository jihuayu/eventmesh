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

package org.apache.eventmesh.connector.dingding.server;

import org.apache.eventmesh.common.Constants;
import org.apache.eventmesh.connector.dingding.config.DingDingConnectServerConfig;
import org.apache.eventmesh.connector.dingding.sink.connector.DingDingSinkConnector;
import org.apache.eventmesh.openconnect.Application;
import org.apache.eventmesh.openconnect.util.ConfigUtil;

public class DingDingConnectServer {

    public static void main(String[] args) throws Exception {

        DingDingConnectServerConfig dingDingConnectServerConfig = ConfigUtil.parse(DingDingConnectServerConfig.class,
            Constants.CONNECT_SERVER_CONFIG_FILE_NAME);

        if (dingDingConnectServerConfig.isSinkEnable()) {
            Application application = new Application();
            application.run(DingDingSinkConnector.class);
        }
    }
}
