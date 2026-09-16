/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.infrastructure.core.condition;

import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.infrastructure.core.config.FineractProperties;

@Slf4j
public class FineractRemoteJobMessageHandlerCondition extends PropertiesCondition {

    @Override
    protected boolean matches(FineractProperties properties) {
        if (!hasValidRemoteJobMessageHandlerConfig(properties)) {
            return false;
        }

        boolean isSpringEventsEnabled = isSpringEventsEnabled(properties);

        boolean conditionFails = false;
        if (isAnyMessageHandlerConfigured(properties) && isBatchInstance(properties)) {
            if (!isOnlyOneMessageHandlerEnabled(properties)) {
                conditionFails = true;
                log.error("For remote partitioning jobs exactly one Message Handler must be enabled.");
            } else if (isSpringEventsEnabled && !isBatchManagerAndWorkerTogether(properties)) {
                conditionFails = true;
                log.error("If Spring Event Message Handler is enabled, the instance must be Batch Manager and Batch Worker too.");
            }
        }
        return conditionFails;
    }

    private boolean hasValidRemoteJobMessageHandlerConfig(FineractProperties properties) {
        return properties != null && properties.getRemoteJobMessageHandler() != null;
    }

    private boolean isSpringEventsEnabled(FineractProperties properties) {
        return hasValidRemoteJobMessageHandlerConfig(properties)
                && properties.getRemoteJobMessageHandler().getSpringEvents() != null
                && properties.getRemoteJobMessageHandler().getSpringEvents().isEnabled();
    }

    private boolean isJmsEnabled(FineractProperties properties) {
        return hasValidRemoteJobMessageHandlerConfig(properties)
                && properties.getRemoteJobMessageHandler().getJms() != null
                && properties.getRemoteJobMessageHandler().getJms().isEnabled();
    }

    private boolean isOnlyOneMessageHandlerEnabled(FineractProperties properties) {
        return isSpringEventsEnabled(properties) ^ isJmsEnabled(properties);
    }

    private boolean isAnyMessageHandlerConfigured(FineractProperties properties) {
        return isSpringEventsEnabled(properties) || isJmsEnabled(properties);
    }

    private boolean isBatchInstance(FineractProperties properties) {
        if (properties == null || properties.getMode() == null) {
            return false;
        }
        boolean isBatchManagerModeEnabled = properties.getMode().isBatchManagerEnabled();
        boolean isBatchWorkerModeEnabled = properties.getMode().isBatchWorkerEnabled();
        return isBatchManagerModeEnabled || isBatchWorkerModeEnabled;
    }

    private boolean isBatchManagerAndWorkerTogether(FineractProperties properties) {
        if (properties == null || properties.getMode() == null) {
            return false;
        }
        boolean isBatchManagerModeEnabled = properties.getMode().isBatchManagerEnabled();
        boolean isBatchWorkerModeEnabled = properties.getMode().isBatchWorkerEnabled();
        return isBatchManagerModeEnabled && isBatchWorkerModeEnabled;
    }
}