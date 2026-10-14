package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.ExportAuditTrailsRequest;
import com.acme.audit.v1.ExportAuditTrailsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ApproveAttestationRequest;
import com.acme.compliance.v1.ApproveAttestationResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationLifecycleServiceGrpc;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.CommentAttestationApprovalRequest;
import com.acme.compliance.v1.CommentAttestationApprovalResponse;
import com.acme.compliance.v1.CompareSanctionOptionsRequest;
import com.acme.compliance.v1.CompareSanctionOptionsResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseQueryServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.CountAttestationTotalsRequest;
import com.acme.compliance.v1.CountAttestationTotalsResponse;
import com.acme.compliance.v1.DelegateAttestationApprovalRequest;
import com.acme.compliance.v1.DelegateAttestationApprovalResponse;
import com.acme.compliance.v1.EscalateAttestationRequest;
import com.acme.compliance.v1.EscalateAttestationResponse;
import com.acme.compliance.v1.GetAttestationApprovalStatusRequest;
import com.acme.compliance.v1.GetAttestationApprovalStatusResponse;
import com.acme.compliance.v1.RejectAttestationRequest;
import com.acme.compliance.v1.RejectAttestationResponse;
import com.acme.compliance.v1.RenewAttestationRequest;
import com.acme.compliance.v1.RenewAttestationResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.compliance.v1.ScheduleControlExpiryRequest;
import com.acme.compliance.v1.ScheduleControlExpiryResponse;
import com.acme.compliance.v1.ScheduleSanctionReportRequest;
import com.acme.compliance.v1.ScheduleSanctionReportResponse;
import com.acme.compliance.v1.SubmitAttestationRequest;
import com.acme.compliance.v1.SubmitAttestationResponse;
import com.acme.compliance.v1.SummarizeComplianceCasesRequest;
import com.acme.compliance.v1.SummarizeComplianceCasesResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.ResumePolicyWorkflowRequest;
import com.acme.insurance.v1.ResumePolicyWorkflowResponse;
import com.acme.insurance.v1.StartCoverageWorkflowRequest;
import com.acme.insurance.v1.StartCoverageWorkflowResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsResponse;
import com.acme.lending.v1.EscalateRepaymentRequest;
import com.acme.lending.v1.EscalateRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentApprovalServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.SubmitRepaymentRequest;
import com.acme.lending.v1.SubmitRepaymentResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureApprovalServiceGrpc;
import com.acme.risk.v1.SubmitExposureRequest;
import com.acme.risk.v1.SubmitExposureResponse;
import com.acme.search.v1.CreateSearchIndexRequest;
import com.acme.search.v1.CreateSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class AttestationApprovalApplicationTest {
    @Test
    void rejectAttestationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var listener = new AttestationApprovalApplicationEvents(ComplianceCaseQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof AttestationApprovalApplication.CommentAttestationApprovalStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new AttestationApprovalApplication(
                AuditTrailQueryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseServiceGrpc.newStub(channel),
                AttestationLifecycleServiceGrpc.newBlockingStub(channel),
                AttestationReportingServiceGrpc.newBlockingStub(channel),
                SanctionReportingServiceGrpc.newBlockingStub(channel),
                events,
                CoverageWorkflowServiceGrpc.newBlockingStub(channel),
                PolicyWorkflowServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newFutureStub(channel),
                RepaymentApprovalServiceGrpc.newBlockingStub(channel),
                new AttestationApprovalRequestMapperImpl(),
                repository,
                ExposureApprovalServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SearchIndexServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RejectAttestationRequest.getDefaultInstance());
        var response = application.rejectAttestation(request);
        var sent0 = channel.request("acme.lending.v1.RepaymentApprovalService/SubmitRepayment");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent0, "repayment", "participants", "contact", "email"));
        var sent1 = channel.request("acme.compliance.v1.SanctionReportingService/ScheduleSanctionReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent1, "reason"));
        var sent2 = channel.request("acme.ledger.v1.FiscalPeriodNotificationService/ListFiscalPeriodSubscriptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentApprovalService/SubmitRepayment"), "repayment", "location", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentApprovalService/SubmitRepayment"), "repayment", "audit", "updated_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        var sent3 = channel.request("acme.insurance.v1.PolicyWorkflowService/ResumePolicyWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentApprovalService/SubmitRepayment"), "repayment", "audit", "updated_at"),
                ProbeSupport.value(sent3, "policy", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentApprovalService/SubmitRepayment"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentApprovalService/SubmitRepayment"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.SanctionReportingService/ScheduleSanctionReport"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodNotificationService/ListFiscalPeriodSubscriptions"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyWorkflowService/ResumePolicyWorkflow"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation_id"),
                ProbeSupport.value(response, "attestation", "attestation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation", "billing_address"),
                ProbeSupport.value(response, "attestation", "billing_address"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new AttestationApprovalApplicationEvents(ComplianceCaseQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<SummarizeComplianceCasesResponse>();
        listener.on(new AttestationApprovalApplication.CommentAttestationApprovalStage0Requested(ProbeSupport.sample(SummarizeComplianceCasesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
