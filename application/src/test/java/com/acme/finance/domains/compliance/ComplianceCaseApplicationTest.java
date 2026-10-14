package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AnnotateRetentionPolicyVersionRequest;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionResponse;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersRequest;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesResponse;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.certificates.v1.Certificate;
import com.acme.certificates.v1.CertificateCalculationServiceGrpc;
import com.acme.certificates.v1.CsrRef;
import com.acme.certificates.v1.SimulateCertificateRequest;
import com.acme.certificates.v1.SimulateCertificateResponse;
import com.acme.collections.v1.PreviewWriteOffRequest;
import com.acme.collections.v1.PreviewWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffCardSettlement;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationRef;
import com.acme.compliance.v1.AttestationValidationServiceGrpc;
import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.BatchGetComplianceCasesRequest;
import com.acme.compliance.v1.BatchGetComplianceCasesResponse;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityRequest;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityResponse;
import com.acme.compliance.v1.CloneComplianceCaseRequest;
import com.acme.compliance.v1.CloneComplianceCaseResponse;
import com.acme.compliance.v1.CompareComplianceCaseOptionsRequest;
import com.acme.compliance.v1.CompareComplianceCaseOptionsResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseChannelOrigin;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.ComplianceCaseQueryServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.ComplianceCaseSla;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.CreateComplianceCaseRequest;
import com.acme.compliance.v1.CreateComplianceCaseResponse;
import com.acme.compliance.v1.DeleteComplianceCaseRequest;
import com.acme.compliance.v1.DeleteComplianceCaseResponse;
import com.acme.compliance.v1.ExplainAttestationDecisionRequest;
import com.acme.compliance.v1.ExplainAttestationDecisionResponse;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.compliance.v1.GetComplianceCaseRequest;
import com.acme.compliance.v1.GetComplianceCaseResponse;
import com.acme.compliance.v1.LintComplianceCasesRequest;
import com.acme.compliance.v1.LintComplianceCasesResponse;
import com.acme.compliance.v1.ListComplianceCasesRequest;
import com.acme.compliance.v1.ListComplianceCasesResponse;
import com.acme.compliance.v1.PreviewAttestationRequest;
import com.acme.compliance.v1.PreviewAttestationResponse;
import com.acme.compliance.v1.QuoteControlRequest;
import com.acme.compliance.v1.QuoteControlResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SearchComplianceCasesRequest;
import com.acme.compliance.v1.SearchComplianceCasesResponse;
import com.acme.compliance.v1.SummarizeComplianceCasesRequest;
import com.acme.compliance.v1.SummarizeComplianceCasesResponse;
import com.acme.compliance.v1.UpdateComplianceCaseRequest;
import com.acme.compliance.v1.UpdateComplianceCaseResponse;
import com.acme.compliance.v1.WatchComplianceCasesRequest;
import com.acme.compliance.v1.WatchComplianceCasesResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.forecasting.v1.DemandForecastQueryServiceGrpc;
import com.acme.forecasting.v1.ResolveDemandForecastsRequest;
import com.acme.forecasting.v1.ResolveDemandForecastsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageBlockAction;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.EstimateCoverageRequest;
import com.acme.insurance.v1.EstimateCoverageResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.RecalculatePoliciesRequest;
import com.acme.insurance.v1.RecalculatePoliciesResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SendPostingBatchTestNoticeRequest;
import com.acme.ledger.v1.SendPostingBatchTestNoticeResponse;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.scheduling.v1.BrowseShiftsRequest;
import com.acme.scheduling.v1.BrowseShiftsResponse;
import com.acme.scheduling.v1.ShiftQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
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
import org.springframework.stereotype.Service;

class ComplianceCaseApplicationTest {
    @Test
    void watchComplianceCasesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new ComplianceCaseApplication(
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BankAccountServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                WriteOffValidationServiceGrpc.newBlockingStub(channel),
                AttestationValidationServiceGrpc.newFutureStub(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseQueryServiceGrpc.newBlockingStub(channel),
                ComplianceCaseValidationServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                DemandForecastQueryServiceGrpc.newFutureStub(channel),
                CoverageCalculationServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                ShiftQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(WatchComplianceCasesRequest.getDefaultInstance());
        var response = application.watchComplianceCases(request);
        var sent0 = channel.request("acme.insurance.v1.PolicyCalculationService/RecalculatePolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent0, "policy", "contact", "phone"));
        var sent1 = channel.request("acme.collections.v1.WriteOffValidationService/PreviewWriteOff");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/RecalculatePolicies"), "policy", "card", "network"),
                ProbeSupport.value(sent1, "write_off", "card", "network"));
        var sent2 = channel.request("acme.ledger.v1.PostingBatchNotificationService/SendPostingBatchTestNotice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(sent2, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent2, "details", "recipient", "phone"));
        var sent3 = channel.request("acme.audit.v1.RetentionPolicyHistoryService/AnnotateRetentionPolicyVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/RecalculatePolicies"), "policy", "audit", "updated_at"),
                ProbeSupport.value(sent3, "retention_policy", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/PreviewWriteOff"), "write_off", "audit", "created_at"),
                ProbeSupport.value(sent3, "retention_policy", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/PreviewWriteOff"), "write_off", "counterparty", "display_name"),
                ProbeSupport.value(sent3, "retention_policy", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchNotificationService/SendPostingBatchTestNotice"), "posting_batch", "audit", "created_by"),
                ProbeSupport.value(sent3, "retention_policy", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyCalculationService/RecalculatePolicies"), "policy", "holder", "display_name"),
                ProbeSupport.value(response, "compliance_case", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/PreviewWriteOff"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchNotificationService/SendPostingBatchTestNotice"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchNotificationService/SendPostingBatchTestNotice"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyHistoryService/AnnotateRetentionPolicyVersion"), "retention_policy", "audit", "updated_at"),
                ProbeSupport.value(response, "compliance_case", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(response, "compliance_case", "requester", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(response, "compliance_case", "requester", "name"));
        var endpoint = new ComplianceCaseEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<WatchComplianceCasesResponse> observer = new StreamObserver<>() {
            public void onNext(WatchComplianceCasesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.watchComplianceCases(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void batchCreateComplianceCasesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new ComplianceCaseApplication(
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BankAccountServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                WriteOffValidationServiceGrpc.newBlockingStub(channel),
                AttestationValidationServiceGrpc.newFutureStub(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseQueryServiceGrpc.newBlockingStub(channel),
                ComplianceCaseValidationServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                DemandForecastQueryServiceGrpc.newFutureStub(channel),
                CoverageCalculationServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                ShiftQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(BatchCreateComplianceCasesRequest.getDefaultInstance());
        var response = application.batchCreateComplianceCases(request);
        var sent0 = channel.request("acme.forecasting.v1.DemandForecastQueryService/ResolveDemandForecasts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.forecasting.v1.DemandForecastQueryService/ResolveDemandForecasts"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.forecasting.v1.DemandForecastQueryService/ResolveDemandForecasts"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        var endpoint = new ComplianceCaseEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<BatchCreateComplianceCasesResponse> observer = new StreamObserver<>() {
            public void onNext(BatchCreateComplianceCasesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.batchCreateComplianceCases(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
