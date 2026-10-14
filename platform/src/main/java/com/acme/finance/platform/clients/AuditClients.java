package com.acme.finance.platform.clients;

import com.acme.audit.v1.AuditEventAdminServiceGrpc;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailAdminServiceGrpc;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.RetentionPolicyAdminServiceGrpc;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the audit contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class AuditClients {
    @Bean
    public AuditEventAdminServiceGrpc.AuditEventAdminServiceBlockingStub auditAuditEventAdminServiceBlocking(ChannelRegistry channels) {
        return AuditEventAdminServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.AuditEventAdminService"));
    }

    @Bean
    public AuditEventExportServiceGrpc.AuditEventExportServiceStub auditAuditEventExportServiceAsync(ChannelRegistry channels) {
        return AuditEventExportServiceGrpc.newStub(channels.lookup("acme.audit.v1.AuditEventExportService"));
    }

    @Bean
    public AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking(ChannelRegistry channels) {
        return AuditEventExportServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.AuditEventExportService"));
    }

    @Bean
    public AuditEventExportServiceGrpc.AuditEventExportServiceFutureStub auditAuditEventExportServiceFuture(ChannelRegistry channels) {
        return AuditEventExportServiceGrpc.newFutureStub(channels.lookup("acme.audit.v1.AuditEventExportService"));
    }

    @Bean
    public AuditEventHistoryServiceGrpc.AuditEventHistoryServiceStub auditAuditEventHistoryServiceAsync(ChannelRegistry channels) {
        return AuditEventHistoryServiceGrpc.newStub(channels.lookup("acme.audit.v1.AuditEventHistoryService"));
    }

    @Bean
    public AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking(ChannelRegistry channels) {
        return AuditEventHistoryServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.AuditEventHistoryService"));
    }

    @Bean
    public AuditEventNotificationServiceGrpc.AuditEventNotificationServiceStub auditAuditEventNotificationServiceAsync(ChannelRegistry channels) {
        return AuditEventNotificationServiceGrpc.newStub(channels.lookup("acme.audit.v1.AuditEventNotificationService"));
    }

    @Bean
    public AuditEventNotificationServiceGrpc.AuditEventNotificationServiceBlockingStub auditAuditEventNotificationServiceBlocking(ChannelRegistry channels) {
        return AuditEventNotificationServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.AuditEventNotificationService"));
    }

    @Bean
    public AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking(ChannelRegistry channels) {
        return AuditEventQueryServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.AuditEventQueryService"));
    }

    @Bean
    public AuditEventQueryServiceGrpc.AuditEventQueryServiceFutureStub auditAuditEventQueryServiceFuture(ChannelRegistry channels) {
        return AuditEventQueryServiceGrpc.newFutureStub(channels.lookup("acme.audit.v1.AuditEventQueryService"));
    }

    @Bean
    public AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync(ChannelRegistry channels) {
        return AuditEventServiceGrpc.newStub(channels.lookup("acme.audit.v1.AuditEventService"));
    }

    @Bean
    public AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking(ChannelRegistry channels) {
        return AuditEventServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.AuditEventService"));
    }

    @Bean
    public AuditEventValidationServiceGrpc.AuditEventValidationServiceStub auditAuditEventValidationServiceAsync(ChannelRegistry channels) {
        return AuditEventValidationServiceGrpc.newStub(channels.lookup("acme.audit.v1.AuditEventValidationService"));
    }

    @Bean
    public AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking(ChannelRegistry channels) {
        return AuditEventValidationServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.AuditEventValidationService"));
    }

    @Bean
    public AuditTrailAdminServiceGrpc.AuditTrailAdminServiceStub auditAuditTrailAdminServiceAsync(ChannelRegistry channels) {
        return AuditTrailAdminServiceGrpc.newStub(channels.lookup("acme.audit.v1.AuditTrailAdminService"));
    }

    @Bean
    public AuditTrailAdminServiceGrpc.AuditTrailAdminServiceBlockingStub auditAuditTrailAdminServiceBlocking(ChannelRegistry channels) {
        return AuditTrailAdminServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.AuditTrailAdminService"));
    }

    @Bean
    public AuditTrailExportServiceGrpc.AuditTrailExportServiceStub auditAuditTrailExportServiceAsync(ChannelRegistry channels) {
        return AuditTrailExportServiceGrpc.newStub(channels.lookup("acme.audit.v1.AuditTrailExportService"));
    }

    @Bean
    public AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking(ChannelRegistry channels) {
        return AuditTrailExportServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.AuditTrailExportService"));
    }

    @Bean
    public AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceStub auditAuditTrailHistoryServiceAsync(ChannelRegistry channels) {
        return AuditTrailHistoryServiceGrpc.newStub(channels.lookup("acme.audit.v1.AuditTrailHistoryService"));
    }

    @Bean
    public AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceBlockingStub auditAuditTrailHistoryServiceBlocking(ChannelRegistry channels) {
        return AuditTrailHistoryServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.AuditTrailHistoryService"));
    }

    @Bean
    public AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking(ChannelRegistry channels) {
        return AuditTrailNotificationServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.AuditTrailNotificationService"));
    }

    @Bean
    public AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceFutureStub auditAuditTrailNotificationServiceFuture(ChannelRegistry channels) {
        return AuditTrailNotificationServiceGrpc.newFutureStub(channels.lookup("acme.audit.v1.AuditTrailNotificationService"));
    }

    @Bean
    public AuditTrailQueryServiceGrpc.AuditTrailQueryServiceStub auditAuditTrailQueryServiceAsync(ChannelRegistry channels) {
        return AuditTrailQueryServiceGrpc.newStub(channels.lookup("acme.audit.v1.AuditTrailQueryService"));
    }

    @Bean
    public AuditTrailQueryServiceGrpc.AuditTrailQueryServiceBlockingStub auditAuditTrailQueryServiceBlocking(ChannelRegistry channels) {
        return AuditTrailQueryServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.AuditTrailQueryService"));
    }

    @Bean
    public AuditTrailQueryServiceGrpc.AuditTrailQueryServiceFutureStub auditAuditTrailQueryServiceFuture(ChannelRegistry channels) {
        return AuditTrailQueryServiceGrpc.newFutureStub(channels.lookup("acme.audit.v1.AuditTrailQueryService"));
    }

    @Bean
    public AuditTrailSyncServiceGrpc.AuditTrailSyncServiceStub auditAuditTrailSyncServiceAsync(ChannelRegistry channels) {
        return AuditTrailSyncServiceGrpc.newStub(channels.lookup("acme.audit.v1.AuditTrailSyncService"));
    }

    @Bean
    public AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking(ChannelRegistry channels) {
        return AuditTrailSyncServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.AuditTrailSyncService"));
    }

    @Bean
    public AuditTrailValidationServiceGrpc.AuditTrailValidationServiceStub auditAuditTrailValidationServiceAsync(ChannelRegistry channels) {
        return AuditTrailValidationServiceGrpc.newStub(channels.lookup("acme.audit.v1.AuditTrailValidationService"));
    }

    @Bean
    public AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking(ChannelRegistry channels) {
        return AuditTrailValidationServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.AuditTrailValidationService"));
    }

    @Bean
    public AuditTrailValidationServiceGrpc.AuditTrailValidationServiceFutureStub auditAuditTrailValidationServiceFuture(ChannelRegistry channels) {
        return AuditTrailValidationServiceGrpc.newFutureStub(channels.lookup("acme.audit.v1.AuditTrailValidationService"));
    }

    @Bean
    public RetentionPolicyAdminServiceGrpc.RetentionPolicyAdminServiceStub auditRetentionPolicyAdminServiceAsync(ChannelRegistry channels) {
        return RetentionPolicyAdminServiceGrpc.newStub(channels.lookup("acme.audit.v1.RetentionPolicyAdminService"));
    }

    @Bean
    public RetentionPolicyAdminServiceGrpc.RetentionPolicyAdminServiceBlockingStub auditRetentionPolicyAdminServiceBlocking(ChannelRegistry channels) {
        return RetentionPolicyAdminServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.RetentionPolicyAdminService"));
    }

    @Bean
    public RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking(ChannelRegistry channels) {
        return RetentionPolicyExportServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.RetentionPolicyExportService"));
    }

    @Bean
    public RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceFutureStub auditRetentionPolicyExportServiceFuture(ChannelRegistry channels) {
        return RetentionPolicyExportServiceGrpc.newFutureStub(channels.lookup("acme.audit.v1.RetentionPolicyExportService"));
    }

    @Bean
    public RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking(ChannelRegistry channels) {
        return RetentionPolicyHistoryServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.RetentionPolicyHistoryService"));
    }

    @Bean
    public RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceStub auditRetentionPolicyNotificationServiceAsync(ChannelRegistry channels) {
        return RetentionPolicyNotificationServiceGrpc.newStub(channels.lookup("acme.audit.v1.RetentionPolicyNotificationService"));
    }

    @Bean
    public RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking(ChannelRegistry channels) {
        return RetentionPolicyNotificationServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.RetentionPolicyNotificationService"));
    }

    @Bean
    public RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceFutureStub auditRetentionPolicyNotificationServiceFuture(ChannelRegistry channels) {
        return RetentionPolicyNotificationServiceGrpc.newFutureStub(channels.lookup("acme.audit.v1.RetentionPolicyNotificationService"));
    }

    @Bean
    public RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync(ChannelRegistry channels) {
        return RetentionPolicySyncServiceGrpc.newStub(channels.lookup("acme.audit.v1.RetentionPolicySyncService"));
    }

    @Bean
    public RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking(ChannelRegistry channels) {
        return RetentionPolicySyncServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.RetentionPolicySyncService"));
    }

    @Bean
    public RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceStub auditRetentionPolicyValidationServiceAsync(ChannelRegistry channels) {
        return RetentionPolicyValidationServiceGrpc.newStub(channels.lookup("acme.audit.v1.RetentionPolicyValidationService"));
    }

    @Bean
    public RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking(ChannelRegistry channels) {
        return RetentionPolicyValidationServiceGrpc.newBlockingStub(channels.lookup("acme.audit.v1.RetentionPolicyValidationService"));
    }

    @Bean
    public RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceFutureStub auditRetentionPolicyValidationServiceFuture(ChannelRegistry channels) {
        return RetentionPolicyValidationServiceGrpc.newFutureStub(channels.lookup("acme.audit.v1.RetentionPolicyValidationService"));
    }
}
