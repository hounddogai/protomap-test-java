package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.financeportal.v1.ApprovalInboxIntegrationServiceGrpc;
import com.acme.financeportal.v1.ApprovalInboxNotificationServiceGrpc;
import com.acme.financeportal.v1.ApprovalInboxQueryServiceGrpc;
import com.acme.financeportal.v1.ApprovalInboxWorkflowServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistIntegrationServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistQueryServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistWorkflowServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspaceIntegrationServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspaceNotificationServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspaceQueryServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspaceServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the financeportal contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class FinanceportalClients {
    @Bean
    public ApprovalInboxIntegrationServiceGrpc.ApprovalInboxIntegrationServiceStub financeportalApprovalInboxIntegrationServiceAsync(ChannelRegistry channels) {
        return ApprovalInboxIntegrationServiceGrpc.newStub(channels.lookup("acme.financeportal.v1.ApprovalInboxIntegrationService"));
    }

    @Bean
    public ApprovalInboxIntegrationServiceGrpc.ApprovalInboxIntegrationServiceBlockingStub financeportalApprovalInboxIntegrationServiceBlocking(ChannelRegistry channels) {
        return ApprovalInboxIntegrationServiceGrpc.newBlockingStub(channels.lookup("acme.financeportal.v1.ApprovalInboxIntegrationService"));
    }

    @Bean
    public ApprovalInboxNotificationServiceGrpc.ApprovalInboxNotificationServiceBlockingStub financeportalApprovalInboxNotificationServiceBlocking(ChannelRegistry channels) {
        return ApprovalInboxNotificationServiceGrpc.newBlockingStub(channels.lookup("acme.financeportal.v1.ApprovalInboxNotificationService"));
    }

    @Bean
    public ApprovalInboxQueryServiceGrpc.ApprovalInboxQueryServiceBlockingStub financeportalApprovalInboxQueryServiceBlocking(ChannelRegistry channels) {
        return ApprovalInboxQueryServiceGrpc.newBlockingStub(channels.lookup("acme.financeportal.v1.ApprovalInboxQueryService"));
    }

    @Bean
    public ApprovalInboxWorkflowServiceGrpc.ApprovalInboxWorkflowServiceBlockingStub financeportalApprovalInboxWorkflowServiceBlocking(ChannelRegistry channels) {
        return ApprovalInboxWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.financeportal.v1.ApprovalInboxWorkflowService"));
    }

    @Bean
    public CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceStub financeportalCloseChecklistIntegrationServiceAsync(ChannelRegistry channels) {
        return CloseChecklistIntegrationServiceGrpc.newStub(channels.lookup("acme.financeportal.v1.CloseChecklistIntegrationService"));
    }

    @Bean
    public CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceBlockingStub financeportalCloseChecklistIntegrationServiceBlocking(ChannelRegistry channels) {
        return CloseChecklistIntegrationServiceGrpc.newBlockingStub(channels.lookup("acme.financeportal.v1.CloseChecklistIntegrationService"));
    }

    @Bean
    public CloseChecklistIntegrationServiceGrpc.CloseChecklistIntegrationServiceFutureStub financeportalCloseChecklistIntegrationServiceFuture(ChannelRegistry channels) {
        return CloseChecklistIntegrationServiceGrpc.newFutureStub(channels.lookup("acme.financeportal.v1.CloseChecklistIntegrationService"));
    }

    @Bean
    public CloseChecklistQueryServiceGrpc.CloseChecklistQueryServiceBlockingStub financeportalCloseChecklistQueryServiceBlocking(ChannelRegistry channels) {
        return CloseChecklistQueryServiceGrpc.newBlockingStub(channels.lookup("acme.financeportal.v1.CloseChecklistQueryService"));
    }

    @Bean
    public CloseChecklistWorkflowServiceGrpc.CloseChecklistWorkflowServiceBlockingStub financeportalCloseChecklistWorkflowServiceBlocking(ChannelRegistry channels) {
        return CloseChecklistWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.financeportal.v1.CloseChecklistWorkflowService"));
    }

    @Bean
    public FinanceWorkspaceIntegrationServiceGrpc.FinanceWorkspaceIntegrationServiceBlockingStub financeportalFinanceWorkspaceIntegrationServiceBlocking(ChannelRegistry channels) {
        return FinanceWorkspaceIntegrationServiceGrpc.newBlockingStub(channels.lookup("acme.financeportal.v1.FinanceWorkspaceIntegrationService"));
    }

    @Bean
    public FinanceWorkspaceNotificationServiceGrpc.FinanceWorkspaceNotificationServiceBlockingStub financeportalFinanceWorkspaceNotificationServiceBlocking(ChannelRegistry channels) {
        return FinanceWorkspaceNotificationServiceGrpc.newBlockingStub(channels.lookup("acme.financeportal.v1.FinanceWorkspaceNotificationService"));
    }

    @Bean
    public FinanceWorkspaceQueryServiceGrpc.FinanceWorkspaceQueryServiceBlockingStub financeportalFinanceWorkspaceQueryServiceBlocking(ChannelRegistry channels) {
        return FinanceWorkspaceQueryServiceGrpc.newBlockingStub(channels.lookup("acme.financeportal.v1.FinanceWorkspaceQueryService"));
    }

    @Bean
    public FinanceWorkspaceServiceGrpc.FinanceWorkspaceServiceBlockingStub financeportalFinanceWorkspaceServiceBlocking(ChannelRegistry channels) {
        return FinanceWorkspaceServiceGrpc.newBlockingStub(channels.lookup("acme.financeportal.v1.FinanceWorkspaceService"));
    }
}
