package com.acme.finance.platform.clients;

import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.AttestationLifecycleServiceGrpc;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.AttestationValidationServiceGrpc;
import com.acme.compliance.v1.AttestationWorkflowServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseApprovalServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseQueryServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseWorkflowServiceGrpc;
import com.acme.compliance.v1.ControlApprovalServiceGrpc;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlLifecycleServiceGrpc;
import com.acme.compliance.v1.ControlQueryServiceGrpc;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.ControlWorkflowServiceGrpc;
import com.acme.compliance.v1.SanctionApprovalServiceGrpc;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the compliance contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class ComplianceClients {
    @Bean
    public AttestationApprovalServiceGrpc.AttestationApprovalServiceBlockingStub complianceAttestationApprovalServiceBlocking(ChannelRegistry channels) {
        return AttestationApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.AttestationApprovalService"));
    }

    @Bean
    public AttestationCalculationServiceGrpc.AttestationCalculationServiceBlockingStub complianceAttestationCalculationServiceBlocking(ChannelRegistry channels) {
        return AttestationCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.AttestationCalculationService"));
    }

    @Bean
    public AttestationLifecycleServiceGrpc.AttestationLifecycleServiceBlockingStub complianceAttestationLifecycleServiceBlocking(ChannelRegistry channels) {
        return AttestationLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.AttestationLifecycleService"));
    }

    @Bean
    public AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking(ChannelRegistry channels) {
        return AttestationReportingServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.AttestationReportingService"));
    }

    @Bean
    public AttestationValidationServiceGrpc.AttestationValidationServiceStub complianceAttestationValidationServiceAsync(ChannelRegistry channels) {
        return AttestationValidationServiceGrpc.newStub(channels.lookup("acme.compliance.v1.AttestationValidationService"));
    }

    @Bean
    public AttestationValidationServiceGrpc.AttestationValidationServiceBlockingStub complianceAttestationValidationServiceBlocking(ChannelRegistry channels) {
        return AttestationValidationServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.AttestationValidationService"));
    }

    @Bean
    public AttestationValidationServiceGrpc.AttestationValidationServiceFutureStub complianceAttestationValidationServiceFuture(ChannelRegistry channels) {
        return AttestationValidationServiceGrpc.newFutureStub(channels.lookup("acme.compliance.v1.AttestationValidationService"));
    }

    @Bean
    public AttestationWorkflowServiceGrpc.AttestationWorkflowServiceBlockingStub complianceAttestationWorkflowServiceBlocking(ChannelRegistry channels) {
        return AttestationWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.AttestationWorkflowService"));
    }

    @Bean
    public ComplianceCaseApprovalServiceGrpc.ComplianceCaseApprovalServiceBlockingStub complianceComplianceCaseApprovalServiceBlocking(ChannelRegistry channels) {
        return ComplianceCaseApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.ComplianceCaseApprovalService"));
    }

    @Bean
    public ComplianceCaseApprovalServiceGrpc.ComplianceCaseApprovalServiceFutureStub complianceComplianceCaseApprovalServiceFuture(ChannelRegistry channels) {
        return ComplianceCaseApprovalServiceGrpc.newFutureStub(channels.lookup("acme.compliance.v1.ComplianceCaseApprovalService"));
    }

    @Bean
    public ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceStub complianceComplianceCaseCalculationServiceAsync(ChannelRegistry channels) {
        return ComplianceCaseCalculationServiceGrpc.newStub(channels.lookup("acme.compliance.v1.ComplianceCaseCalculationService"));
    }

    @Bean
    public ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking(ChannelRegistry channels) {
        return ComplianceCaseCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.ComplianceCaseCalculationService"));
    }

    @Bean
    public ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceFutureStub complianceComplianceCaseCalculationServiceFuture(ChannelRegistry channels) {
        return ComplianceCaseCalculationServiceGrpc.newFutureStub(channels.lookup("acme.compliance.v1.ComplianceCaseCalculationService"));
    }

    @Bean
    public ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceStub complianceComplianceCaseQueryServiceAsync(ChannelRegistry channels) {
        return ComplianceCaseQueryServiceGrpc.newStub(channels.lookup("acme.compliance.v1.ComplianceCaseQueryService"));
    }

    @Bean
    public ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceBlockingStub complianceComplianceCaseQueryServiceBlocking(ChannelRegistry channels) {
        return ComplianceCaseQueryServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.ComplianceCaseQueryService"));
    }

    @Bean
    public ComplianceCaseServiceGrpc.ComplianceCaseServiceStub complianceComplianceCaseServiceAsync(ChannelRegistry channels) {
        return ComplianceCaseServiceGrpc.newStub(channels.lookup("acme.compliance.v1.ComplianceCaseService"));
    }

    @Bean
    public ComplianceCaseServiceGrpc.ComplianceCaseServiceBlockingStub complianceComplianceCaseServiceBlocking(ChannelRegistry channels) {
        return ComplianceCaseServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.ComplianceCaseService"));
    }

    @Bean
    public ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceStub complianceComplianceCaseValidationServiceAsync(ChannelRegistry channels) {
        return ComplianceCaseValidationServiceGrpc.newStub(channels.lookup("acme.compliance.v1.ComplianceCaseValidationService"));
    }

    @Bean
    public ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking(ChannelRegistry channels) {
        return ComplianceCaseValidationServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.ComplianceCaseValidationService"));
    }

    @Bean
    public ComplianceCaseWorkflowServiceGrpc.ComplianceCaseWorkflowServiceBlockingStub complianceComplianceCaseWorkflowServiceBlocking(ChannelRegistry channels) {
        return ComplianceCaseWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.ComplianceCaseWorkflowService"));
    }

    @Bean
    public ControlApprovalServiceGrpc.ControlApprovalServiceBlockingStub complianceControlApprovalServiceBlocking(ChannelRegistry channels) {
        return ControlApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.ControlApprovalService"));
    }

    @Bean
    public ControlCalculationServiceGrpc.ControlCalculationServiceStub complianceControlCalculationServiceAsync(ChannelRegistry channels) {
        return ControlCalculationServiceGrpc.newStub(channels.lookup("acme.compliance.v1.ControlCalculationService"));
    }

    @Bean
    public ControlCalculationServiceGrpc.ControlCalculationServiceBlockingStub complianceControlCalculationServiceBlocking(ChannelRegistry channels) {
        return ControlCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.ControlCalculationService"));
    }

    @Bean
    public ControlLifecycleServiceGrpc.ControlLifecycleServiceBlockingStub complianceControlLifecycleServiceBlocking(ChannelRegistry channels) {
        return ControlLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.ControlLifecycleService"));
    }

    @Bean
    public ControlQueryServiceGrpc.ControlQueryServiceStub complianceControlQueryServiceAsync(ChannelRegistry channels) {
        return ControlQueryServiceGrpc.newStub(channels.lookup("acme.compliance.v1.ControlQueryService"));
    }

    @Bean
    public ControlQueryServiceGrpc.ControlQueryServiceBlockingStub complianceControlQueryServiceBlocking(ChannelRegistry channels) {
        return ControlQueryServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.ControlQueryService"));
    }

    @Bean
    public ControlValidationServiceGrpc.ControlValidationServiceBlockingStub complianceControlValidationServiceBlocking(ChannelRegistry channels) {
        return ControlValidationServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.ControlValidationService"));
    }

    @Bean
    public ControlValidationServiceGrpc.ControlValidationServiceFutureStub complianceControlValidationServiceFuture(ChannelRegistry channels) {
        return ControlValidationServiceGrpc.newFutureStub(channels.lookup("acme.compliance.v1.ControlValidationService"));
    }

    @Bean
    public ControlWorkflowServiceGrpc.ControlWorkflowServiceStub complianceControlWorkflowServiceAsync(ChannelRegistry channels) {
        return ControlWorkflowServiceGrpc.newStub(channels.lookup("acme.compliance.v1.ControlWorkflowService"));
    }

    @Bean
    public ControlWorkflowServiceGrpc.ControlWorkflowServiceBlockingStub complianceControlWorkflowServiceBlocking(ChannelRegistry channels) {
        return ControlWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.ControlWorkflowService"));
    }

    @Bean
    public SanctionApprovalServiceGrpc.SanctionApprovalServiceStub complianceSanctionApprovalServiceAsync(ChannelRegistry channels) {
        return SanctionApprovalServiceGrpc.newStub(channels.lookup("acme.compliance.v1.SanctionApprovalService"));
    }

    @Bean
    public SanctionApprovalServiceGrpc.SanctionApprovalServiceBlockingStub complianceSanctionApprovalServiceBlocking(ChannelRegistry channels) {
        return SanctionApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.SanctionApprovalService"));
    }

    @Bean
    public SanctionCalculationServiceGrpc.SanctionCalculationServiceBlockingStub complianceSanctionCalculationServiceBlocking(ChannelRegistry channels) {
        return SanctionCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.SanctionCalculationService"));
    }

    @Bean
    public SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking(ChannelRegistry channels) {
        return SanctionReportingServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.SanctionReportingService"));
    }

    @Bean
    public SanctionValidationServiceGrpc.SanctionValidationServiceBlockingStub complianceSanctionValidationServiceBlocking(ChannelRegistry channels) {
        return SanctionValidationServiceGrpc.newBlockingStub(channels.lookup("acme.compliance.v1.SanctionValidationService"));
    }
}
