package com.acme.finance.domains.risk;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityRequest;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentDiscountAction;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.EstimateCustomerRequest;
import com.acme.customers.v1.EstimateCustomerResponse;
import com.acme.customers.v1.ExplainConsentRecordDecisionRequest;
import com.acme.customers.v1.ExplainConsentRecordDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.ScoreHouseholdRequest;
import com.acme.customers.v1.ScoreHouseholdResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.GenerateCoverageReportRequest;
import com.acme.insurance.v1.GenerateCoverageReportResponse;
import com.acme.insurance.v1.PullUnderwritingsRequest;
import com.acme.insurance.v1.PullUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanBankFunding;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.RetryLoanWorkflowRequest;
import com.acme.lending.v1.RetryLoanWorkflowResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.SearchMetricSeriesesRequest;
import com.acme.metrics.v1.SearchMetricSeriesesResponse;
import com.acme.risk.v1.ApproveExposureRequest;
import com.acme.risk.v1.ApproveExposureResponse;
import com.acme.risk.v1.BrowseRiskRegistersRequest;
import com.acme.risk.v1.BrowseRiskRegistersResponse;
import com.acme.risk.v1.CommentExposureApprovalRequest;
import com.acme.risk.v1.CommentExposureApprovalResponse;
import com.acme.risk.v1.DelegateExposureApprovalRequest;
import com.acme.risk.v1.DelegateExposureApprovalResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureLimit;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.GetExposureApprovalStatusRequest;
import com.acme.risk.v1.GetExposureApprovalStatusResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.LintRiskScenariosRequest;
import com.acme.risk.v1.LintRiskScenariosResponse;
import com.acme.risk.v1.ListPendingExposuresRequest;
import com.acme.risk.v1.ListPendingExposuresResponse;
import com.acme.risk.v1.ListPendingRiskScenariosRequest;
import com.acme.risk.v1.ListPendingRiskScenariosResponse;
import com.acme.risk.v1.NormalizeExposureRequest;
import com.acme.risk.v1.NormalizeExposureResponse;
import com.acme.risk.v1.RecallExposureRequest;
import com.acme.risk.v1.RecallExposureResponse;
import com.acme.risk.v1.RecallRiskRegisterRequest;
import com.acme.risk.v1.RecallRiskRegisterResponse;
import com.acme.risk.v1.RejectExposureRequest;
import com.acme.risk.v1.RejectExposureResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.RiskScenarioComment;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.SubmitExposureRequest;
import com.acme.risk.v1.SubmitExposureResponse;
import com.acme.risk.v1.SubmitRiskRegisterRequest;
import com.acme.risk.v1.SubmitRiskRegisterResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ExposureApprovalApplicationTest {
    @Test
    void submitExposurePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var listener = new ExposureApprovalApplicationEvents(RiskRegisterValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ExposureApprovalApplication.DelegateExposureApprovalStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ExposureApprovalApplication(
                ChannelRegistry.fixed(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                events,
                CoverageReportingServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newBlockingStub(channel),
                LoanWorkflowServiceGrpc.newBlockingStub(channel),
                new ExposureApprovalRequestMapperImpl(),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                repository,
                ExposureValidationServiceGrpc.newBlockingStub(channel),
                RiskRegisterApprovalServiceGrpc.newBlockingStub(channel),
                RiskScenarioApprovalServiceGrpc.newBlockingStub(channel),
                RiskScenarioValidationServiceGrpc.newBlockingStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(SubmitExposureRequest.getDefaultInstance());
        var response = application.submitExposure(request);
        var sent0 = channel.request("acme.lending.v1.LoanWorkflowService/RetryLoanWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "bank", "iban"),
                ProbeSupport.value(sent0, "loan", "bank", "iban"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "bank", "bic"),
                ProbeSupport.value(sent0, "loan", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "contact", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "notify", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "audit", "created_by"),
                ProbeSupport.value(sent0, "loan", "audit", "created_by"));
        var sent1 = channel.request("acme.insurance.v1.UnderwritingSyncService/PullUnderwritings");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanWorkflowService/RetryLoanWorkflow"), "loan", "audit", "created_at"),
                ProbeSupport.value(sent1, "underwriting", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent1, "underwriting", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "contact", "preferred_channel"),
                ProbeSupport.value(sent1, "underwriting", "requester", "preferred_channel"));
        var sent2 = channel.request("acme.risk.v1.RiskRegisterApprovalService/RecallRiskRegister");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "limits", "name"),
                ProbeSupport.value(sent2, "risk_register", "children", "children", "children", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanWorkflowService/RetryLoanWorkflow"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/PullUnderwritings"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure_id"),
                ProbeSupport.value(response, "exposure", "exposure_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "balance"),
                ProbeSupport.value(response, "exposure", "balance"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ExposureApprovalApplicationEvents(RiskRegisterValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<LintRiskRegistersResponse>();
        listener.on(new ExposureApprovalApplication.DelegateExposureApprovalStage2Requested(ProbeSupport.sample(LintRiskRegistersRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
