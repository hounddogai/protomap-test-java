package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.risk.v1.ExposureApprovalServiceGrpc;
import com.acme.risk.v1.ExposureCalculationServiceGrpc;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.acme.risk.v1.StressTestApprovalServiceGrpc;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import com.acme.risk.v1.StressTestLifecycleServiceGrpc;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the risk contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class RiskClients {
    @Bean
    public ExposureApprovalServiceGrpc.ExposureApprovalServiceBlockingStub riskExposureApprovalServiceBlocking(ChannelRegistry channels) {
        return ExposureApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.ExposureApprovalService"));
    }

    @Bean
    public ExposureCalculationServiceGrpc.ExposureCalculationServiceBlockingStub riskExposureCalculationServiceBlocking(ChannelRegistry channels) {
        return ExposureCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.ExposureCalculationService"));
    }

    @Bean
    public ExposureCalculationServiceGrpc.ExposureCalculationServiceFutureStub riskExposureCalculationServiceFuture(ChannelRegistry channels) {
        return ExposureCalculationServiceGrpc.newFutureStub(channels.lookup("acme.risk.v1.ExposureCalculationService"));
    }

    @Bean
    public ExposureValidationServiceGrpc.ExposureValidationServiceBlockingStub riskExposureValidationServiceBlocking(ChannelRegistry channels) {
        return ExposureValidationServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.ExposureValidationService"));
    }

    @Bean
    public ExposureWorkflowServiceGrpc.ExposureWorkflowServiceStub riskExposureWorkflowServiceAsync(ChannelRegistry channels) {
        return ExposureWorkflowServiceGrpc.newStub(channels.lookup("acme.risk.v1.ExposureWorkflowService"));
    }

    @Bean
    public ExposureWorkflowServiceGrpc.ExposureWorkflowServiceBlockingStub riskExposureWorkflowServiceBlocking(ChannelRegistry channels) {
        return ExposureWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.ExposureWorkflowService"));
    }

    @Bean
    public RiskRegisterApprovalServiceGrpc.RiskRegisterApprovalServiceBlockingStub riskRiskRegisterApprovalServiceBlocking(ChannelRegistry channels) {
        return RiskRegisterApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.RiskRegisterApprovalService"));
    }

    @Bean
    public RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceBlockingStub riskRiskRegisterQueryServiceBlocking(ChannelRegistry channels) {
        return RiskRegisterQueryServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.RiskRegisterQueryService"));
    }

    @Bean
    public RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceFutureStub riskRiskRegisterQueryServiceFuture(ChannelRegistry channels) {
        return RiskRegisterQueryServiceGrpc.newFutureStub(channels.lookup("acme.risk.v1.RiskRegisterQueryService"));
    }

    @Bean
    public RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync(ChannelRegistry channels) {
        return RiskRegisterServiceGrpc.newStub(channels.lookup("acme.risk.v1.RiskRegisterService"));
    }

    @Bean
    public RiskRegisterServiceGrpc.RiskRegisterServiceBlockingStub riskRiskRegisterServiceBlocking(ChannelRegistry channels) {
        return RiskRegisterServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.RiskRegisterService"));
    }

    @Bean
    public RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceStub riskRiskRegisterValidationServiceAsync(ChannelRegistry channels) {
        return RiskRegisterValidationServiceGrpc.newStub(channels.lookup("acme.risk.v1.RiskRegisterValidationService"));
    }

    @Bean
    public RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceBlockingStub riskRiskRegisterValidationServiceBlocking(ChannelRegistry channels) {
        return RiskRegisterValidationServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.RiskRegisterValidationService"));
    }

    @Bean
    public RiskRegisterValidationServiceGrpc.RiskRegisterValidationServiceFutureStub riskRiskRegisterValidationServiceFuture(ChannelRegistry channels) {
        return RiskRegisterValidationServiceGrpc.newFutureStub(channels.lookup("acme.risk.v1.RiskRegisterValidationService"));
    }

    @Bean
    public RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceStub riskRiskRegisterWorkflowServiceAsync(ChannelRegistry channels) {
        return RiskRegisterWorkflowServiceGrpc.newStub(channels.lookup("acme.risk.v1.RiskRegisterWorkflowService"));
    }

    @Bean
    public RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking(ChannelRegistry channels) {
        return RiskRegisterWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.RiskRegisterWorkflowService"));
    }

    @Bean
    public RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking(ChannelRegistry channels) {
        return RiskScenarioApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.RiskScenarioApprovalService"));
    }

    @Bean
    public RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceFutureStub riskRiskScenarioApprovalServiceFuture(ChannelRegistry channels) {
        return RiskScenarioApprovalServiceGrpc.newFutureStub(channels.lookup("acme.risk.v1.RiskScenarioApprovalService"));
    }

    @Bean
    public RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceStub riskRiskScenarioQueryServiceAsync(ChannelRegistry channels) {
        return RiskScenarioQueryServiceGrpc.newStub(channels.lookup("acme.risk.v1.RiskScenarioQueryService"));
    }

    @Bean
    public RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking(ChannelRegistry channels) {
        return RiskScenarioQueryServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.RiskScenarioQueryService"));
    }

    @Bean
    public RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceStub riskRiskScenarioValidationServiceAsync(ChannelRegistry channels) {
        return RiskScenarioValidationServiceGrpc.newStub(channels.lookup("acme.risk.v1.RiskScenarioValidationService"));
    }

    @Bean
    public RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking(ChannelRegistry channels) {
        return RiskScenarioValidationServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.RiskScenarioValidationService"));
    }

    @Bean
    public RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceFutureStub riskRiskScenarioValidationServiceFuture(ChannelRegistry channels) {
        return RiskScenarioValidationServiceGrpc.newFutureStub(channels.lookup("acme.risk.v1.RiskScenarioValidationService"));
    }

    @Bean
    public RiskScenarioWorkflowServiceGrpc.RiskScenarioWorkflowServiceBlockingStub riskRiskScenarioWorkflowServiceBlocking(ChannelRegistry channels) {
        return RiskScenarioWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.RiskScenarioWorkflowService"));
    }

    @Bean
    public StressTestApprovalServiceGrpc.StressTestApprovalServiceBlockingStub riskStressTestApprovalServiceBlocking(ChannelRegistry channels) {
        return StressTestApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.StressTestApprovalService"));
    }

    @Bean
    public StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking(ChannelRegistry channels) {
        return StressTestCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.StressTestCalculationService"));
    }

    @Bean
    public StressTestCalculationServiceGrpc.StressTestCalculationServiceFutureStub riskStressTestCalculationServiceFuture(ChannelRegistry channels) {
        return StressTestCalculationServiceGrpc.newFutureStub(channels.lookup("acme.risk.v1.StressTestCalculationService"));
    }

    @Bean
    public StressTestLifecycleServiceGrpc.StressTestLifecycleServiceBlockingStub riskStressTestLifecycleServiceBlocking(ChannelRegistry channels) {
        return StressTestLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.StressTestLifecycleService"));
    }

    @Bean
    public StressTestValidationServiceGrpc.StressTestValidationServiceStub riskStressTestValidationServiceAsync(ChannelRegistry channels) {
        return StressTestValidationServiceGrpc.newStub(channels.lookup("acme.risk.v1.StressTestValidationService"));
    }

    @Bean
    public StressTestValidationServiceGrpc.StressTestValidationServiceBlockingStub riskStressTestValidationServiceBlocking(ChannelRegistry channels) {
        return StressTestValidationServiceGrpc.newBlockingStub(channels.lookup("acme.risk.v1.StressTestValidationService"));
    }
}
