package com.acme.finance.domains.compliance;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountWorkflowServiceGrpc;
import com.acme.banking.v1.CancelBankAccountWorkflowRequest;
import com.acme.banking.v1.CancelBankAccountWorkflowResponse;
import com.acme.collections.v1.SignalWriteOffWorkflowRequest;
import com.acme.collections.v1.SignalWriteOffWorkflowResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffCardSettlement;
import com.acme.collections.v1.WriteOffWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ActivateAttestationRequest;
import com.acme.compliance.v1.ActivateAttestationResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.AttestationCardSettlement;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.AttestationValidationServiceGrpc;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.EstimateAttestationRequest;
import com.acme.compliance.v1.EstimateAttestationResponse;
import com.acme.compliance.v1.ExpireAttestationRequest;
import com.acme.compliance.v1.ExpireAttestationResponse;
import com.acme.compliance.v1.FetchAttestationReportRequest;
import com.acme.compliance.v1.FetchAttestationReportResponse;
import com.acme.compliance.v1.GetAttestationLifecycleRequest;
import com.acme.compliance.v1.GetAttestationLifecycleResponse;
import com.acme.compliance.v1.LintAttestationsRequest;
import com.acme.compliance.v1.LintAttestationsResponse;
import com.acme.compliance.v1.NormalizeComplianceCaseRequest;
import com.acme.compliance.v1.NormalizeComplianceCaseResponse;
import com.acme.compliance.v1.RecalculateControlsRequest;
import com.acme.compliance.v1.RecalculateControlsResponse;
import com.acme.compliance.v1.RenewAttestationRequest;
import com.acme.compliance.v1.RenewAttestationResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SuspendAttestationRequest;
import com.acme.compliance.v1.SuspendAttestationResponse;
import com.acme.compliance.v1.TerminateAttestationRequest;
import com.acme.compliance.v1.TerminateAttestationResponse;
import com.acme.compliance.v1.ValidateAttestationRequest;
import com.acme.compliance.v1.ValidateAttestationResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApproveClaimRequest;
import com.acme.insurance.v1.ApproveClaimResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.lending.v1.ApproveLoanRequest;
import com.acme.lending.v1.ApproveLoanResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.performers.v1.BatchGetPerformersRequest;
import com.acme.performers.v1.BatchGetPerformersResponse;
import com.acme.performers.v1.PerformerServiceGrpc;
import com.acme.risk.v1.ExposureApprovalServiceGrpc;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.risk.v1.StartRiskRegisterWorkflowRequest;
import com.acme.risk.v1.StartRiskRegisterWorkflowResponse;
import com.acme.risk.v1.SubmitExposureRequest;
import com.acme.risk.v1.SubmitExposureResponse;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class AttestationLifecycleApplicationTest {
    @Test
    void terminateAttestationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new AttestationLifecycleApplication(
                FixedAssetServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BankAccountWorkflowServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                WriteOffWorkflowServiceGrpc.newBlockingStub(channel),
                AttestationCalculationServiceGrpc.newBlockingStub(channel),
                AttestationReportingServiceGrpc.newBlockingStub(channel),
                AttestationValidationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseValidationServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                LoanApprovalServiceGrpc.newBlockingStub(channel),
                new AttestationLifecycleRequestMapperImpl(),
                PerformerServiceGrpc.newBlockingStub(channel),
                repository,
                ExposureApprovalServiceGrpc.newBlockingStub(channel),
                RiskRegisterWorkflowServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(TerminateAttestationRequest.getDefaultInstance());
        var response = application.terminateAttestation(request);
        var sent0 = channel.request("acme.lending.v1.LoanApprovalService/ApproveLoan");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "email"),
                ProbeSupport.value(sent0, "loan", "contact", "email"));
        var sent1 = channel.request("acme.performers.v1.PerformerService/BatchGetPerformers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        var sent2 = channel.request("acme.compliance.v1.AttestationCalculationService/EstimateAttestation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApprovalService/ApproveLoan"), "loan", "card", "network"),
                ProbeSupport.value(sent2, "attestation", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApprovalService/ApproveLoan"), "loan", "card", "last4"),
                ProbeSupport.value(sent2, "attestation", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApprovalService/ApproveLoan"), "loan", "audit", "created_at"),
                ProbeSupport.value(sent2, "attestation", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApprovalService/ApproveLoan"), "loan", "holder", "number"),
                ProbeSupport.value(sent2, "attestation", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApprovalService/ApproveLoan"), "loan", "audit", "updated_by"),
                ProbeSupport.value(response, "attestation", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApprovalService/ApproveLoan"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.performers.v1.PerformerService/BatchGetPerformers"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationCalculationService/EstimateAttestation"), "attestation", "billing_address", "postal_code"),
                ProbeSupport.value(response, "attestation", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation_id"),
                ProbeSupport.value(response, "attestation", "attestation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
