package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimLifecycleServiceGrpc;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageLifecycleServiceGrpc;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyLifecycleServiceGrpc;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.PolicyServiceGrpc;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.insurance.v1.UnderwritingLifecycleServiceGrpc;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the insurance contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class InsuranceClients {
    @Bean
    public ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking(ChannelRegistry channels) {
        return ClaimApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.ClaimApprovalService"));
    }

    @Bean
    public ClaimCalculationServiceGrpc.ClaimCalculationServiceStub insuranceClaimCalculationServiceAsync(ChannelRegistry channels) {
        return ClaimCalculationServiceGrpc.newStub(channels.lookup("acme.insurance.v1.ClaimCalculationService"));
    }

    @Bean
    public ClaimCalculationServiceGrpc.ClaimCalculationServiceBlockingStub insuranceClaimCalculationServiceBlocking(ChannelRegistry channels) {
        return ClaimCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.ClaimCalculationService"));
    }

    @Bean
    public ClaimCalculationServiceGrpc.ClaimCalculationServiceFutureStub insuranceClaimCalculationServiceFuture(ChannelRegistry channels) {
        return ClaimCalculationServiceGrpc.newFutureStub(channels.lookup("acme.insurance.v1.ClaimCalculationService"));
    }

    @Bean
    public ClaimLifecycleServiceGrpc.ClaimLifecycleServiceBlockingStub insuranceClaimLifecycleServiceBlocking(ChannelRegistry channels) {
        return ClaimLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.ClaimLifecycleService"));
    }

    @Bean
    public ClaimQueryServiceGrpc.ClaimQueryServiceBlockingStub insuranceClaimQueryServiceBlocking(ChannelRegistry channels) {
        return ClaimQueryServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.ClaimQueryService"));
    }

    @Bean
    public ClaimValidationServiceGrpc.ClaimValidationServiceStub insuranceClaimValidationServiceAsync(ChannelRegistry channels) {
        return ClaimValidationServiceGrpc.newStub(channels.lookup("acme.insurance.v1.ClaimValidationService"));
    }

    @Bean
    public ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking(ChannelRegistry channels) {
        return ClaimValidationServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.ClaimValidationService"));
    }

    @Bean
    public ClaimValidationServiceGrpc.ClaimValidationServiceFutureStub insuranceClaimValidationServiceFuture(ChannelRegistry channels) {
        return ClaimValidationServiceGrpc.newFutureStub(channels.lookup("acme.insurance.v1.ClaimValidationService"));
    }

    @Bean
    public ClaimWorkflowServiceGrpc.ClaimWorkflowServiceBlockingStub insuranceClaimWorkflowServiceBlocking(ChannelRegistry channels) {
        return ClaimWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.ClaimWorkflowService"));
    }

    @Bean
    public CoverageApprovalServiceGrpc.CoverageApprovalServiceBlockingStub insuranceCoverageApprovalServiceBlocking(ChannelRegistry channels) {
        return CoverageApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.CoverageApprovalService"));
    }

    @Bean
    public CoverageApprovalServiceGrpc.CoverageApprovalServiceFutureStub insuranceCoverageApprovalServiceFuture(ChannelRegistry channels) {
        return CoverageApprovalServiceGrpc.newFutureStub(channels.lookup("acme.insurance.v1.CoverageApprovalService"));
    }

    @Bean
    public CoverageCalculationServiceGrpc.CoverageCalculationServiceStub insuranceCoverageCalculationServiceAsync(ChannelRegistry channels) {
        return CoverageCalculationServiceGrpc.newStub(channels.lookup("acme.insurance.v1.CoverageCalculationService"));
    }

    @Bean
    public CoverageCalculationServiceGrpc.CoverageCalculationServiceBlockingStub insuranceCoverageCalculationServiceBlocking(ChannelRegistry channels) {
        return CoverageCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.CoverageCalculationService"));
    }

    @Bean
    public CoverageCalculationServiceGrpc.CoverageCalculationServiceFutureStub insuranceCoverageCalculationServiceFuture(ChannelRegistry channels) {
        return CoverageCalculationServiceGrpc.newFutureStub(channels.lookup("acme.insurance.v1.CoverageCalculationService"));
    }

    @Bean
    public CoverageLifecycleServiceGrpc.CoverageLifecycleServiceBlockingStub insuranceCoverageLifecycleServiceBlocking(ChannelRegistry channels) {
        return CoverageLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.CoverageLifecycleService"));
    }

    @Bean
    public CoverageReportingServiceGrpc.CoverageReportingServiceStub insuranceCoverageReportingServiceAsync(ChannelRegistry channels) {
        return CoverageReportingServiceGrpc.newStub(channels.lookup("acme.insurance.v1.CoverageReportingService"));
    }

    @Bean
    public CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking(ChannelRegistry channels) {
        return CoverageReportingServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.CoverageReportingService"));
    }

    @Bean
    public CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking(ChannelRegistry channels) {
        return CoverageValidationServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.CoverageValidationService"));
    }

    @Bean
    public CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking(ChannelRegistry channels) {
        return CoverageWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.CoverageWorkflowService"));
    }

    @Bean
    public PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking(ChannelRegistry channels) {
        return PolicyApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.PolicyApprovalService"));
    }

    @Bean
    public PolicyApprovalServiceGrpc.PolicyApprovalServiceFutureStub insurancePolicyApprovalServiceFuture(ChannelRegistry channels) {
        return PolicyApprovalServiceGrpc.newFutureStub(channels.lookup("acme.insurance.v1.PolicyApprovalService"));
    }

    @Bean
    public PolicyCalculationServiceGrpc.PolicyCalculationServiceStub insurancePolicyCalculationServiceAsync(ChannelRegistry channels) {
        return PolicyCalculationServiceGrpc.newStub(channels.lookup("acme.insurance.v1.PolicyCalculationService"));
    }

    @Bean
    public PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking(ChannelRegistry channels) {
        return PolicyCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.PolicyCalculationService"));
    }

    @Bean
    public PolicyCalculationServiceGrpc.PolicyCalculationServiceFutureStub insurancePolicyCalculationServiceFuture(ChannelRegistry channels) {
        return PolicyCalculationServiceGrpc.newFutureStub(channels.lookup("acme.insurance.v1.PolicyCalculationService"));
    }

    @Bean
    public PolicyLifecycleServiceGrpc.PolicyLifecycleServiceBlockingStub insurancePolicyLifecycleServiceBlocking(ChannelRegistry channels) {
        return PolicyLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.PolicyLifecycleService"));
    }

    @Bean
    public PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking(ChannelRegistry channels) {
        return PolicyQueryServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.PolicyQueryService"));
    }

    @Bean
    public PolicyQueryServiceGrpc.PolicyQueryServiceFutureStub insurancePolicyQueryServiceFuture(ChannelRegistry channels) {
        return PolicyQueryServiceGrpc.newFutureStub(channels.lookup("acme.insurance.v1.PolicyQueryService"));
    }

    @Bean
    public PolicyServiceGrpc.PolicyServiceBlockingStub insurancePolicyServiceBlocking(ChannelRegistry channels) {
        return PolicyServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.PolicyService"));
    }

    @Bean
    public PolicyValidationServiceGrpc.PolicyValidationServiceStub insurancePolicyValidationServiceAsync(ChannelRegistry channels) {
        return PolicyValidationServiceGrpc.newStub(channels.lookup("acme.insurance.v1.PolicyValidationService"));
    }

    @Bean
    public PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking(ChannelRegistry channels) {
        return PolicyValidationServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.PolicyValidationService"));
    }

    @Bean
    public PolicyValidationServiceGrpc.PolicyValidationServiceFutureStub insurancePolicyValidationServiceFuture(ChannelRegistry channels) {
        return PolicyValidationServiceGrpc.newFutureStub(channels.lookup("acme.insurance.v1.PolicyValidationService"));
    }

    @Bean
    public PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking(ChannelRegistry channels) {
        return PolicyWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.PolicyWorkflowService"));
    }

    @Bean
    public PolicyWorkflowServiceGrpc.PolicyWorkflowServiceFutureStub insurancePolicyWorkflowServiceFuture(ChannelRegistry channels) {
        return PolicyWorkflowServiceGrpc.newFutureStub(channels.lookup("acme.insurance.v1.PolicyWorkflowService"));
    }

    @Bean
    public PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking(ChannelRegistry channels) {
        return PremiumApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.PremiumApprovalService"));
    }

    @Bean
    public PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking(ChannelRegistry channels) {
        return PremiumCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.PremiumCalculationService"));
    }

    @Bean
    public PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking(ChannelRegistry channels) {
        return PremiumReportingServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.PremiumReportingService"));
    }

    @Bean
    public PremiumReportingServiceGrpc.PremiumReportingServiceFutureStub insurancePremiumReportingServiceFuture(ChannelRegistry channels) {
        return PremiumReportingServiceGrpc.newFutureStub(channels.lookup("acme.insurance.v1.PremiumReportingService"));
    }

    @Bean
    public PremiumSyncServiceGrpc.PremiumSyncServiceStub insurancePremiumSyncServiceAsync(ChannelRegistry channels) {
        return PremiumSyncServiceGrpc.newStub(channels.lookup("acme.insurance.v1.PremiumSyncService"));
    }

    @Bean
    public PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking(ChannelRegistry channels) {
        return PremiumSyncServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.PremiumSyncService"));
    }

    @Bean
    public PremiumValidationServiceGrpc.PremiumValidationServiceStub insurancePremiumValidationServiceAsync(ChannelRegistry channels) {
        return PremiumValidationServiceGrpc.newStub(channels.lookup("acme.insurance.v1.PremiumValidationService"));
    }

    @Bean
    public PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking(ChannelRegistry channels) {
        return PremiumValidationServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.PremiumValidationService"));
    }

    @Bean
    public UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking(ChannelRegistry channels) {
        return UnderwritingCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.UnderwritingCalculationService"));
    }

    @Bean
    public UnderwritingImportServiceGrpc.UnderwritingImportServiceStub insuranceUnderwritingImportServiceAsync(ChannelRegistry channels) {
        return UnderwritingImportServiceGrpc.newStub(channels.lookup("acme.insurance.v1.UnderwritingImportService"));
    }

    @Bean
    public UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking(ChannelRegistry channels) {
        return UnderwritingImportServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.UnderwritingImportService"));
    }

    @Bean
    public UnderwritingLifecycleServiceGrpc.UnderwritingLifecycleServiceBlockingStub insuranceUnderwritingLifecycleServiceBlocking(ChannelRegistry channels) {
        return UnderwritingLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.UnderwritingLifecycleService"));
    }

    @Bean
    public UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking(ChannelRegistry channels) {
        return UnderwritingReportingServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.UnderwritingReportingService"));
    }

    @Bean
    public UnderwritingReportingServiceGrpc.UnderwritingReportingServiceFutureStub insuranceUnderwritingReportingServiceFuture(ChannelRegistry channels) {
        return UnderwritingReportingServiceGrpc.newFutureStub(channels.lookup("acme.insurance.v1.UnderwritingReportingService"));
    }

    @Bean
    public UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync(ChannelRegistry channels) {
        return UnderwritingSyncServiceGrpc.newStub(channels.lookup("acme.insurance.v1.UnderwritingSyncService"));
    }

    @Bean
    public UnderwritingSyncServiceGrpc.UnderwritingSyncServiceBlockingStub insuranceUnderwritingSyncServiceBlocking(ChannelRegistry channels) {
        return UnderwritingSyncServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.UnderwritingSyncService"));
    }

    @Bean
    public UnderwritingValidationServiceGrpc.UnderwritingValidationServiceBlockingStub insuranceUnderwritingValidationServiceBlocking(ChannelRegistry channels) {
        return UnderwritingValidationServiceGrpc.newBlockingStub(channels.lookup("acme.insurance.v1.UnderwritingValidationService"));
    }

    @Bean
    public UnderwritingValidationServiceGrpc.UnderwritingValidationServiceFutureStub insuranceUnderwritingValidationServiceFuture(ChannelRegistry channels) {
        return UnderwritingValidationServiceGrpc.newFutureStub(channels.lookup("acme.insurance.v1.UnderwritingValidationService"));
    }
}
