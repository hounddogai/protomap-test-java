package com.acme.finance.domains.compliance;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.ListCollectionCaseWorkflowsRequest;
import com.acme.collections.v1.ListCollectionCaseWorkflowsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.ApproveComplianceCaseRequest;
import com.acme.compliance.v1.ApproveComplianceCaseResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.CommentComplianceCaseApprovalRequest;
import com.acme.compliance.v1.CommentComplianceCaseApprovalResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlLifecycleServiceGrpc;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.CountAttestationTotalsRequest;
import com.acme.compliance.v1.CountAttestationTotalsResponse;
import com.acme.compliance.v1.DelegateComplianceCaseApprovalRequest;
import com.acme.compliance.v1.DelegateComplianceCaseApprovalResponse;
import com.acme.compliance.v1.EscalateComplianceCaseRequest;
import com.acme.compliance.v1.EscalateComplianceCaseResponse;
import com.acme.compliance.v1.EstimateControlRequest;
import com.acme.compliance.v1.EstimateControlResponse;
import com.acme.compliance.v1.ExplainControlCalculationRequest;
import com.acme.compliance.v1.ExplainControlCalculationResponse;
import com.acme.compliance.v1.GetComplianceCaseApprovalStatusRequest;
import com.acme.compliance.v1.GetComplianceCaseApprovalStatusResponse;
import com.acme.compliance.v1.LintControlsRequest;
import com.acme.compliance.v1.LintControlsResponse;
import com.acme.compliance.v1.ListPendingComplianceCasesRequest;
import com.acme.compliance.v1.ListPendingComplianceCasesResponse;
import com.acme.compliance.v1.RecallComplianceCaseRequest;
import com.acme.compliance.v1.RecallComplianceCaseResponse;
import com.acme.compliance.v1.RejectComplianceCaseRequest;
import com.acme.compliance.v1.RejectComplianceCaseResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.compliance.v1.ScheduleControlExpiryRequest;
import com.acme.compliance.v1.ScheduleControlExpiryResponse;
import com.acme.compliance.v1.ScoreSanctionRequest;
import com.acme.compliance.v1.ScoreSanctionResponse;
import com.acme.compliance.v1.SubmitComplianceCaseRequest;
import com.acme.compliance.v1.SubmitComplianceCaseResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.PullUnderwritingsRequest;
import com.acme.insurance.v1.PullUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.itsm.v1.CloneIncidentRequest;
import com.acme.itsm.v1.CloneIncidentResponse;
import com.acme.itsm.v1.Incident;
import com.acme.itsm.v1.IncidentComment;
import com.acme.itsm.v1.IncidentServiceGrpc;
import com.acme.ledger.v1.BrowseJournalEntriesRequest;
import com.acme.ledger.v1.BrowseJournalEntriesResponse;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.lending.v1.CancelLoanWorkflowRequest;
import com.acme.lending.v1.CancelLoanWorkflowResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.notifications.v1.ExplainNotificationDecisionRequest;
import com.acme.notifications.v1.ExplainNotificationDecisionResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationStatusChange;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.PreviewNotificationRequest;
import com.acme.notifications.v1.PreviewNotificationResponse;
import com.acme.payroll.v1.BatchCreatePayRunsRequest;
import com.acme.payroll.v1.BatchCreatePayRunsResponse;
import com.acme.payroll.v1.PayRunServiceGrpc;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureApprovalServiceGrpc;
import com.acme.risk.v1.SubmitExposureRequest;
import com.acme.risk.v1.SubmitExposureResponse;
import com.acme.secrets.v1.FindSecretsRequest;
import com.acme.secrets.v1.FindSecretsResponse;
import com.acme.secrets.v1.SecretQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
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

class ComplianceCaseApprovalApplicationTest {
    @Test
    void approveComplianceCasePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var listener = new ComplianceCaseApprovalApplicationEvents(ComplianceCaseCalculationServiceGrpc.newStub(channel), ControlCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ComplianceCaseApprovalApplication.RejectComplianceCaseStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof ComplianceCaseApprovalApplication.GetComplianceCaseApprovalStatusStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ComplianceCaseApprovalApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseWorkflowServiceGrpc.newBlockingStub(channel),
                AttestationReportingServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                ControlValidationServiceGrpc.newBlockingStub(channel),
                SanctionValidationServiceGrpc.newBlockingStub(channel),
                events,
                UnderwritingSyncServiceGrpc.newBlockingStub(channel),
                IncidentServiceGrpc.newBlockingStub(channel),
                JournalEntryQueryServiceGrpc.newBlockingStub(channel),
                LoanWorkflowServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newFutureStub(channel),
                PayRunServiceGrpc.newStub(channel),
                repository,
                ExposureApprovalServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SecretQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ApproveComplianceCaseRequest.getDefaultInstance());
        var response = application.approveComplianceCase(request);
        var sent0 = channel.request("acme.ledger.v1.JournalEntryQueryService/BrowseJournalEntries");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "custom_fields"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.compliance.v1.ControlValidationService/LintControls");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "assignee", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        var sent2 = channel.request("acme.risk.v1.ExposureApprovalService/SubmitExposure");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlValidationService/LintControls"), "control", "owner", "email"),
                ProbeSupport.value(sent2, "exposure", "holder", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryQueryService/BrowseJournalEntries"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlValidationService/LintControls"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlValidationService/LintControls"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureApprovalService/SubmitExposure"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case_id"),
                ProbeSupport.value(response, "compliance_case", "compliance_case_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "requester"),
                ProbeSupport.value(response, "compliance_case", "requester"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ComplianceCaseApprovalApplicationEvents(ComplianceCaseCalculationServiceGrpc.newStub(channel), ControlCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<EstimateControlResponse>();
        listener.on(new ComplianceCaseApprovalApplication.RejectComplianceCaseStage0Requested(ProbeSupport.sample(EstimateControlRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<CalculateComplianceCaseResponse>();
        listener.on(new ComplianceCaseApprovalApplication.GetComplianceCaseApprovalStatusStage0Requested(ProbeSupport.sample(CalculateComplianceCaseRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
