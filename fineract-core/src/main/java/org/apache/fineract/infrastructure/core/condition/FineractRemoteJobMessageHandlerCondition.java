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