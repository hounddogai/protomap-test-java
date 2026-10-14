package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.CheckAuditTrailEligibilityRequest;
import com.acme.audit.v1.CheckAuditTrailEligibilityResponse;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyNoticesRequest;
import com.acme.audit.v1.StreamRetentionPolicyNoticesResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.RejectCollectionCaseRequest;
import com.acme.collections.v1.RejectCollectionCaseResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityRequest;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityResponse;
import com.acme.customers.v1.CheckHouseholdEligibilityRequest;
import com.acme.customers.v1.CheckHouseholdEligibilityResponse;
import com.acme.customers.v1.ComparePreferenceOptionsRequest;
import com.acme.customers.v1.ComparePreferenceOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CreateCustomerRequest;
import com.acme.customers.v1.CreateCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.ExplainConsentRecordDecisionRequest;
import com.acme.customers.v1.ExplainConsentRecordDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.CommentLoanApprovalRequest;
import com.acme.lending.v1.CommentLoanApprovalResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.risk.v1.AdvanceRiskRegisterWorkflowRequest;
import com.acme.risk.v1.AdvanceRiskRegisterWorkflowResponse;
import com.acme.risk.v1.BatchGetRiskRegistersRequest;
import com.acme.risk.v1.BatchGetRiskRegistersResponse;
import com.acme.risk.v1.CancelRiskRegisterWorkflowRequest;
import com.acme.risk.v1.CancelRiskRegisterWorkflowResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.GetRiskRegisterRulesRequest;
import com.acme.risk.v1.GetRiskRegisterRulesResponse;
import com.acme.risk.v1.InspectRiskRegisterWorkflowRequest;
import com.acme.risk.v1.InspectRiskRegisterWorkflowResponse;
import com.acme.risk.v1.PauseRiskRegisterWorkflowRequest;
import com.acme.risk.v1.PauseRiskRegisterWorkflowResponse;
import com.acme.risk.v1.ResumeRiskRegisterWorkflowRequest;
import com.acme.risk.v1.ResumeRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RetryRiskRegisterWorkflowRequest;
import com.acme.risk.v1.RetryRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.ScoreExposureRequest;
import com.acme.risk.v1.ScoreExposureResponse;
import com.acme.risk.v1.ScoreRiskRegisterRequest;
import com.acme.risk.v1.ScoreRiskRegisterResponse;
import com.acme.risk.v1.SignalRiskRegisterWorkflowRequest;
import com.acme.risk.v1.SignalRiskRegisterWorkflowResponse;
import com.acme.risk.v1.StartRiskRegisterWorkflowRequest;
import com.acme.risk.v1.StartRiskRegisterWorkflowResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.UpdateRiskRegisterRequest;
import com.acme.risk.v1.UpdateRiskRegisterResponse;
import com.acme.search.v1.CountSearchIndexesRequest;
import com.acme.search.v1.CountSearchIndexesResponse;
import com.acme.search.v1.ExportSearchIndexesRequest;
import com.acme.search.v1.ExportSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class RiskRegisterWorkflowApplicationTest {
    @Test
    void startRiskRegisterWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var listener = new RiskRegisterWorkflowApplicationEvents(CollectionCaseApprovalServiceGrpc.newStub(channel), PreferenceCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof RiskRegisterWorkflowApplication.CancelRiskRegisterWorkflowStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof RiskRegisterWorkflowApplication.PauseRiskRegisterWorkflowStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new RiskRegisterWorkflowApplication(
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                CustomerServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                events,
                LoanApprovalServiceGrpc.newBlockingStub(channel),
                new RiskRegisterWorkflowRequestMapperImpl(),
                repository,
                ExposureValidationServiceGrpc.newBlockingStub(channel),
                RiskRegisterServiceGrpc.newBlockingStub(channel),
                RiskRegisterValidationServiceGrpc.newBlockingStub(channel),
                SearchIndexQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StartRiskRegisterWorkflowRequest.getDefaultInstance());
        var response = application.startRiskRegisterWorkflow(request);
        var sent0 = channel.request("acme.risk.v1.ExposureValidationService/ScoreExposure");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent0, "exposure", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register", "audit", "updated_by"),
                ProbeSupport.value(sent0, "exposure", "audit", "updated_by"));
        var sent1 = channel.request("acme.audit.v1.RetentionPolicyNotificationService/StreamRetentionPolicyNotices");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureValidationService/ScoreExposure"), "exposure", "contact", "phone"),
                ProbeSupport.value(sent1, "subscriber", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register", "manager", "email"),
                ProbeSupport.value(sent1, "subscriber", "email"));
        var sent2 = channel.request("acme.risk.v1.RiskRegisterValidationService/ScoreRiskRegister");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register", "location", "region"),
                ProbeSupport.value(sent2, "risk_register", "children", "children", "location", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register", "audit", "created_by"),
                ProbeSupport.value(sent2, "risk_register", "children", "children", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureValidationService/ScoreExposure"), "exposure", "holder", "display_name"),
                ProbeSupport.value(sent2, "risk_register", "children", "children", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register", "location", "postal_code"),
                ProbeSupport.value(sent2, "risk_register", "children", "children", "location", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureValidationService/ScoreExposure"), "exposure", "contact", "email"),
                ProbeSupport.value(response, "risk_register", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterValidationService/ScoreRiskRegister"), "risk_register", "depth"),
                ProbeSupport.value(response, "risk_register", "depth"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterValidationService/ScoreRiskRegister"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register_id"),
                ProbeSupport.value(response, "risk_register", "risk_register_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register", "location"),
                ProbeSupport.value(response, "risk_register", "location"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new RiskRegisterWorkflowApplicationEvents(CollectionCaseApprovalServiceGrpc.newStub(channel), PreferenceCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<RejectCollectionCaseResponse>();
        listener.on(new RiskRegisterWorkflowApplication.CancelRiskRegisterWorkflowStage0Requested(ProbeSupport.sample(RejectCollectionCaseRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<ComparePreferenceOptionsResponse>();
        listener.on(new RiskRegisterWorkflowApplication.PauseRiskRegisterWorkflowStage2Requested(ProbeSupport.sample(ComparePreferenceOptionsRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
