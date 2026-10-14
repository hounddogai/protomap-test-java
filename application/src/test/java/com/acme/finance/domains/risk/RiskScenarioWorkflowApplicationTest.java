package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.FindAuditEventsRequest;
import com.acme.audit.v1.FindAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetryRetentionPolicyExportRequest;
import com.acme.audit.v1.RetryRetentionPolicyExportResponse;
import com.acme.certificates.v1.Certificate;
import com.acme.certificates.v1.CertificateBankSettlement;
import com.acme.certificates.v1.CertificateCalculationServiceGrpc;
import com.acme.certificates.v1.CompareCertificateOptionsRequest;
import com.acme.certificates.v1.CompareCertificateOptionsResponse;
import com.acme.certificates.v1.EstimateCertificateRequest;
import com.acme.certificates.v1.EstimateCertificateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.ScoreConsentRecordRequest;
import com.acme.customers.v1.ScoreConsentRecordResponse;
import com.acme.customers.v1.WatchCustomersRequest;
import com.acme.customers.v1.WatchCustomersResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.ResumeClaimWorkflowRequest;
import com.acme.insurance.v1.ResumeClaimWorkflowResponse;
import com.acme.insurance.v1.SignalCoverageWorkflowRequest;
import com.acme.insurance.v1.SignalCoverageWorkflowResponse;
import com.acme.insurance.v1.StartClaimWorkflowRequest;
import com.acme.insurance.v1.StartClaimWorkflowResponse;
import com.acme.lending.v1.GenerateRepaymentReportRequest;
import com.acme.lending.v1.GenerateRepaymentReportResponse;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.SearchMetricSeriesesRequest;
import com.acme.metrics.v1.SearchMetricSeriesesResponse;
import com.acme.risk.v1.AdvanceRiskScenarioWorkflowRequest;
import com.acme.risk.v1.AdvanceRiskScenarioWorkflowResponse;
import com.acme.risk.v1.CancelRiskScenarioWorkflowRequest;
import com.acme.risk.v1.CancelRiskScenarioWorkflowResponse;
import com.acme.risk.v1.CommentRiskScenarioApprovalRequest;
import com.acme.risk.v1.CommentRiskScenarioApprovalResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.InspectRiskScenarioWorkflowRequest;
import com.acme.risk.v1.InspectRiskScenarioWorkflowResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.ListRiskScenarioWorkflowsRequest;
import com.acme.risk.v1.ListRiskScenarioWorkflowsResponse;
import com.acme.risk.v1.NormalizeStressTestRequest;
import com.acme.risk.v1.NormalizeStressTestResponse;
import com.acme.risk.v1.PeekRiskScenarioRequest;
import com.acme.risk.v1.PeekRiskScenarioResponse;
import com.acme.risk.v1.ResumeRiskRegisterWorkflowRequest;
import com.acme.risk.v1.ResumeRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RetryRiskScenarioWorkflowRequest;
import com.acme.risk.v1.RetryRiskScenarioWorkflowResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.RiskScenarioComment;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import com.acme.risk.v1.ScheduleStressTestExpiryRequest;
import com.acme.risk.v1.ScheduleStressTestExpiryResponse;
import com.acme.risk.v1.SignalRiskScenarioWorkflowRequest;
import com.acme.risk.v1.SignalRiskScenarioWorkflowResponse;
import com.acme.risk.v1.StartRiskRegisterWorkflowRequest;
import com.acme.risk.v1.StartRiskRegisterWorkflowResponse;
import com.acme.risk.v1.StartRiskScenarioWorkflowRequest;
import com.acme.risk.v1.StartRiskScenarioWorkflowResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestLifecycleServiceGrpc;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class RiskScenarioWorkflowApplicationTest {
    @Test
    void signalRiskScenarioWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var listener = new RiskScenarioWorkflowApplicationEvents(RiskRegisterWorkflowServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof RiskScenarioWorkflowApplication.SignalRiskScenarioWorkflowStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new RiskScenarioWorkflowApplication(
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyExportServiceGrpc.newBlockingStub(channel),
                CertificateCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ConsentRecordValidationServiceGrpc.newFutureStub(channel),
                CustomerServiceGrpc.newBlockingStub(channel),
                events,
                ClaimWorkflowServiceGrpc.newBlockingStub(channel),
                CoverageWorkflowServiceGrpc.newBlockingStub(channel),
                RepaymentReportingServiceGrpc.newBlockingStub(channel),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterValidationServiceGrpc.newBlockingStub(channel),
                RiskRegisterWorkflowServiceGrpc.newBlockingStub(channel),
                RiskScenarioApprovalServiceGrpc.newBlockingStub(channel),
                RiskScenarioQueryServiceGrpc.newBlockingStub(channel),
                StressTestLifecycleServiceGrpc.newBlockingStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(SignalRiskScenarioWorkflowRequest.getDefaultInstance());
        var response = application.signalRiskScenarioWorkflow(request);
        var sent0 = channel.request("acme.risk.v1.RiskRegisterWorkflowService/StartRiskRegisterWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "assignee", "display_name"),
                ProbeSupport.value(sent0, "risk_register", "children", "children", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "audit", "updated_at"),
                ProbeSupport.value(sent0, "risk_register", "children", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "audit", "created_at"),
                ProbeSupport.value(sent0, "risk_register", "children", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "customer", "display_name"),
                ProbeSupport.value(sent0, "risk_register", "children", "manager", "display_name"));
        var sent1 = channel.request("acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterWorkflowService/StartRiskRegisterWorkflow"), "risk_register", "location", "region"),
                ProbeSupport.value(sent1, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterWorkflowService/StartRiskRegisterWorkflow"), "risk_register", "location", "locality"),
                ProbeSupport.value(sent1, "details", "destination", "locality"));
        var sent2 = channel.request("acme.audit.v1.AuditEventQueryService/FindAuditEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterWorkflowService/StartRiskRegisterWorkflow"), "risk_register", "location", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterWorkflowService/StartRiskRegisterWorkflow"), "risk_register", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "sla", "policy_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        var sent3 = channel.request("acme.risk.v1.RiskScenarioQueryService/PeekRiskScenario");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "risk_scenario_id"),
                ProbeSupport.value(sent3, "risk_scenario_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterWorkflowService/StartRiskRegisterWorkflow"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventQueryService/FindAuditEvents"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventQueryService/FindAuditEvents"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioQueryService/PeekRiskScenario"), "risk_scenario", "subject"),
                ProbeSupport.value(response, "risk_scenario", "subject"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioQueryService/PeekRiskScenario"), "risk_scenario", "requester", "email"),
                ProbeSupport.value(response, "risk_scenario", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario_id"),
                ProbeSupport.value(response, "risk_scenario", "risk_scenario_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "custom_fields"),
                ProbeSupport.value(response, "risk_scenario", "custom_fields"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new RiskScenarioWorkflowApplicationEvents(RiskRegisterWorkflowServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<StartRiskRegisterWorkflowResponse>();
        listener.on(new RiskScenarioWorkflowApplication.SignalRiskScenarioWorkflowStage0Requested(ProbeSupport.sample(StartRiskRegisterWorkflowRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
