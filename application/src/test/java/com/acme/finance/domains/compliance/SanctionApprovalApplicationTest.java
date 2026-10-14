package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.PreviewAuditEventRequest;
import com.acme.audit.v1.PreviewAuditEventResponse;
import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyDiscountAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AdvanceControlWorkflowRequest;
import com.acme.compliance.v1.AdvanceControlWorkflowResponse;
import com.acme.compliance.v1.ApproveSanctionRequest;
import com.acme.compliance.v1.ApproveSanctionResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.CommentAttestationApprovalRequest;
import com.acme.compliance.v1.CommentAttestationApprovalResponse;
import com.acme.compliance.v1.CommentComplianceCaseApprovalRequest;
import com.acme.compliance.v1.CommentComplianceCaseApprovalResponse;
import com.acme.compliance.v1.CommentSanctionApprovalRequest;
import com.acme.compliance.v1.CommentSanctionApprovalResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseApprovalServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlApprovalServiceGrpc;
import com.acme.compliance.v1.ControlLifecycleServiceGrpc;
import com.acme.compliance.v1.DelegateSanctionApprovalRequest;
import com.acme.compliance.v1.DelegateSanctionApprovalResponse;
import com.acme.compliance.v1.EscalateSanctionRequest;
import com.acme.compliance.v1.EscalateSanctionResponse;
import com.acme.compliance.v1.FetchSanctionReportRequest;
import com.acme.compliance.v1.FetchSanctionReportResponse;
import com.acme.compliance.v1.GetSanctionApprovalStatusRequest;
import com.acme.compliance.v1.GetSanctionApprovalStatusResponse;
import com.acme.compliance.v1.ListPendingSanctionsRequest;
import com.acme.compliance.v1.ListPendingSanctionsResponse;
import com.acme.compliance.v1.RecallSanctionRequest;
import com.acme.compliance.v1.RecallSanctionResponse;
import com.acme.compliance.v1.RejectAttestationRequest;
import com.acme.compliance.v1.RejectAttestationResponse;
import com.acme.compliance.v1.RejectSanctionRequest;
import com.acme.compliance.v1.RejectSanctionResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.compliance.v1.ScheduleControlExpiryRequest;
import com.acme.compliance.v1.ScheduleControlExpiryResponse;
import com.acme.compliance.v1.SubmitControlRequest;
import com.acme.compliance.v1.SubmitControlResponse;
import com.acme.compliance.v1.SubmitSanctionRequest;
import com.acme.compliance.v1.SubmitSanctionResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.forms.v1.BatchGetFormsRequest;
import com.acme.forms.v1.BatchGetFormsResponse;
import com.acme.forms.v1.FormServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ListPremiumConflictsRequest;
import com.acme.insurance.v1.ListPremiumConflictsResponse;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionRequest;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountLimit;
import com.acme.lending.v1.CommentRepaymentApprovalRequest;
import com.acme.lending.v1.CommentRepaymentApprovalResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentApprovalServiceGrpc;
import com.acme.shipping.v1.ShipmentQueryServiceGrpc;
import com.acme.shipping.v1.SuggestShipmentsRequest;
import com.acme.shipping.v1.SuggestShipmentsResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class SanctionApprovalApplicationTest {
    @Test
    void submitSanctionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new SanctionApprovalApplication(
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BankAccountServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationApprovalServiceGrpc.newBlockingStub(channel),
                ComplianceCaseApprovalServiceGrpc.newBlockingStub(channel),
                ControlApprovalServiceGrpc.newBlockingStub(channel),
                ControlLifecycleServiceGrpc.newBlockingStub(channel),
                SanctionReportingServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                RepaymentApprovalServiceGrpc.newBlockingStub(channel),
                new SanctionApprovalRequestMapperImpl(),
                repository,
                ShipmentQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SubmitSanctionRequest.getDefaultInstance());
        var response = application.submitSanction(request);
        var sent0 = channel.request("acme.compliance.v1.ControlWorkflowService/AdvanceControlWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent0, "control", "owner", "user_id"));
        var sent1 = channel.request("acme.lending.v1.RepaymentApprovalService/CommentRepaymentApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlWorkflowService/AdvanceControlWorkflow"), "control", "owner", "display_name"),
                ProbeSupport.value(sent1, "details", "approver", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction", "manager", "user_id"),
                ProbeSupport.value(sent1, "repayment", "organizer", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction", "manager", "email"),
                ProbeSupport.value(sent1, "details", "approver", "email"));
        var sent2 = channel.request("acme.compliance.v1.ComplianceCaseApprovalService/CommentComplianceCaseApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent2, "details", "approver", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlWorkflowService/AdvanceControlWorkflow"), "control", "owner", "user_id"),
                ProbeSupport.value(sent2, "compliance_case", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentApprovalService/CommentRepaymentApproval"), "repayment", "audit", "updated_at"),
                ProbeSupport.value(sent2, "compliance_case", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlWorkflowService/AdvanceControlWorkflow"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlWorkflowService/AdvanceControlWorkflow"), "control", "name"),
                ProbeSupport.value(response, "sanction", "children", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentApprovalService/CommentRepaymentApproval"), "repayment", "location", "lines"),
                ProbeSupport.value(response, "sanction", "location", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentApprovalService/CommentRepaymentApproval"), "repayment", "location", "recipient"),
                ProbeSupport.value(response, "sanction", "location", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseApprovalService/CommentComplianceCaseApproval"), "compliance_case", "assignee", "display_name"),
                ProbeSupport.value(response, "sanction", "children", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseApprovalService/CommentComplianceCaseApproval"), "compliance_case", "audit", "etag"),
                ProbeSupport.value(response, "sanction", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction_id"),
                ProbeSupport.value(response, "sanction", "sanction_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction", "manager"),
                ProbeSupport.value(response, "sanction", "manager"));
    }
}
