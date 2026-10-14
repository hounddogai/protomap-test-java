package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the notifications contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class NotificationsClients {
    @Bean
    public NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking(ChannelRegistry channels) {
        return NotificationQueryServiceGrpc.newBlockingStub(channels.lookup("acme.notifications.v1.NotificationQueryService"));
    }

    @Bean
    public NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking(ChannelRegistry channels) {
        return NotificationServiceGrpc.newBlockingStub(channels.lookup("acme.notifications.v1.NotificationService"));
    }

    @Bean
    public NotificationServiceGrpc.NotificationServiceFutureStub notificationsNotificationServiceFuture(ChannelRegistry channels) {
        return NotificationServiceGrpc.newFutureStub(channels.lookup("acme.notifications.v1.NotificationService"));
    }

    @Bean
    public NotificationValidationServiceGrpc.NotificationValidationServiceStub notificationsNotificationValidationServiceAsync(ChannelRegistry channels) {
        return NotificationValidationServiceGrpc.newStub(channels.lookup("acme.notifications.v1.NotificationValidationService"));
    }

    @Bean
    public NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking(ChannelRegistry channels) {
        return NotificationValidationServiceGrpc.newBlockingStub(channels.lookup("acme.notifications.v1.NotificationValidationService"));
    }

    @Bean
    public NotificationValidationServiceGrpc.NotificationValidationServiceFutureStub notificationsNotificationValidationServiceFuture(ChannelRegistry channels) {
        return NotificationValidationServiceGrpc.newFutureStub(channels.lookup("acme.notifications.v1.NotificationValidationService"));
    }
}
