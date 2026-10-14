package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.BrowseAuditEventsRequest;
import com.acme.audit.v1.BrowseAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ApproveControlRequest;
import com.acme.compliance.v1.ApproveControlResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.AttestationLine;
import com.acme.compliance.v1.AttestationRef;
import com.acme.compliance.v1.BrowseControlsRequest;
import com.acme.compliance.v1.BrowseControlsResponse;
import com.acme.compliance.v1.CommentControlApprovalRequest;
import com.acme.compliance.v1.CommentControlApprovalResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseApprovalServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlCondition;
import com.acme.compliance.v1.ControlQueryServiceGrpc;
import com.acme.compliance.v1.EscalateComplianceCaseRequest;
import com.acme.compliance.v1.EscalateComplianceCaseResponse;
import com.acme.compliance.v1.EscalateControlRequest;
import com.acme.compliance.v1.EscalateControlResponse;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.compliance.v1.GetControlApprovalStatusRequest;
import com.acme.compliance.v1.GetControlApprovalStatusResponse;
import com.acme.compliance.v1.ListPendingComplianceCasesRequest;
import com.acme.compliance.v1.ListPendingComplianceCasesResponse;
import com.acme.compliance.v1.ListPendingControlsRequest;
import com.acme.compliance.v1.ListPendingControlsResponse;
import com.acme.compliance.v1.PeekControlRequest;
import com.acme.compliance.v1.PeekControlResponse;
import com.acme.compliance.v1.RecalculateAttestationsRequest;
import com.acme.compliance.v1.RecalculateAttestationsResponse;
import com.acme.compliance.v1.RecallControlRequest;
import com.acme.compliance.v1.RecallControlResponse;
import com.acme.compliance.v1.RejectComplianceCaseRequest;
import com.acme.compliance.v1.RejectComplianceCaseResponse;
import com.acme.compliance.v1.RejectControlRequest;
import com.acme.compliance.v1.RejectControlResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.compliance.v1.SearchComplianceCasesRequest;
import com.acme.compliance.v1.SearchComplianceCasesResponse;
import com.acme.compliance.v1.SimulateControlRequest;
import com.acme.compliance.v1.SimulateControlResponse;
import com.acme.compliance.v1.StreamSanctionMetricsRequest;
import com.acme.compliance.v1.StreamSanctionMetricsResponse;
import com.acme.compliance.v1.SubmitControlRequest;
import com.acme.compliance.v1.SubmitControlResponse;
import com.acme.datasets.v1.Dataset;
import com.acme.datasets.v1.DatasetServiceGrpc;
import com.acme.datasets.v1.UpdateDatasetRequest;
import com.acme.datasets.v1.UpdateDatasetResponse;
import com.acme.expenses.v1.BatchCreateExpenseReportsRequest;
import com.acme.expenses.v1.BatchCreateExpenseReportsResponse;
import com.acme.expenses.v1.ExpenseReportServiceGrpc;
import com.acme.featureflags.v1.CreateFeatureFlagRequest;
import com.acme.featureflags.v1.CreateFeatureFlagResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagBlockAction;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.forms.v1.BrowseFormsRequest;
import com.acme.forms.v1.BrowseFormsResponse;
import com.acme.forms.v1.FormQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CloneLedgerAccountRequest;
import com.acme.ledger.v1.CloneLedgerAccountResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.risk.v1.DelegateRiskRegisterApprovalRequest;
import com.acme.risk.v1.DelegateRiskRegisterApprovalResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class ControlApprovalApplicationTest {
    @Test
    void recallControlPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new ControlApprovalApplication(
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationCalculationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseApprovalServiceGrpc.newBlockingStub(channel),
                ComplianceCaseApprovalServiceGrpc.newFutureStub(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                ControlQueryServiceGrpc.newBlockingStub(channel),
                SanctionReportingServiceGrpc.newBlockingStub(channel),
                DatasetServiceGrpc.newBlockingStub(channel),
                ExpenseReportServiceGrpc.newStub(channel),
                FeatureFlagServiceGrpc.newBlockingStub(channel),
                FormQueryServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(RecallControlRequest.getDefaultInstance());
        var response = application.recallControl(request);
        var sent0 = channel.request("acme.ledger.v1.LedgerAccountService/CloneLedgerAccount");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "audit", "etag"),
                ProbeSupport.value(sent0, "ledger_account", "audit", "etag"));
        var sent1 = channel.request("acme.risk.v1.RiskRegisterApprovalService/DelegateRiskRegisterApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountService/CloneLedgerAccount"), "ledger_account", "audit", "created_at"),
                ProbeSupport.value(sent1, "risk_register", "children", "children", "audit", "created_at"));
        var sent2 = channel.request("acme.compliance.v1.ComplianceCaseApprovalService/RejectComplianceCase");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterApprovalService/DelegateRiskRegisterApproval"), "risk_register", "audit", "etag"),
                ProbeSupport.value(sent2, "compliance_case", "audit", "etag"));
        var sent3 = channel.request("acme.compliance.v1.ComplianceCaseCalculationService/ExplainComplianceCaseCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterApprovalService/DelegateRiskRegisterApproval"), "risk_register", "manager", "user_id"),
                ProbeSupport.value(sent3, "compliance_case", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountService/CloneLedgerAccount"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterApprovalService/DelegateRiskRegisterApproval"), "risk_register", "audit", "updated_by"),
                ProbeSupport.value(response, "control", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterApprovalService/DelegateRiskRegisterApproval"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseApprovalService/RejectComplianceCase"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/ExplainComplianceCaseCalculation"), "compliance_case", "assignee", "email"),
                ProbeSupport.value(response, "control", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/ExplainComplianceCaseCalculation"), "compliance_case", "priority"),
                ProbeSupport.value(response, "control", "priority"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control_id"),
                ProbeSupport.value(response, "control", "control_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "status"),
                ProbeSupport.value(response, "control", "status"));
    }
}
