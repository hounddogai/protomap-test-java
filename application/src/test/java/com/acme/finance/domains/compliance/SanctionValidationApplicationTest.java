package com.acme.finance.domains.compliance;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.UpdateAccessPolicyRequest;
import com.acme.authorization.v1.UpdateAccessPolicyResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.ResolveBankAccountsRequest;
import com.acme.banking.v1.ResolveBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.CheckSanctionEligibilityRequest;
import com.acme.compliance.v1.CheckSanctionEligibilityResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlRouteAction;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.EstimateControlRequest;
import com.acme.compliance.v1.EstimateControlResponse;
import com.acme.compliance.v1.ExplainSanctionDecisionRequest;
import com.acme.compliance.v1.ExplainSanctionDecisionResponse;
import com.acme.compliance.v1.GetSanctionRulesRequest;
import com.acme.compliance.v1.GetSanctionRulesResponse;
import com.acme.compliance.v1.LintControlsRequest;
import com.acme.compliance.v1.LintControlsResponse;
import com.acme.compliance.v1.NormalizeSanctionRequest;
import com.acme.compliance.v1.NormalizeSanctionResponse;
import com.acme.compliance.v1.PreviewSanctionRequest;
import com.acme.compliance.v1.PreviewSanctionResponse;
import com.acme.compliance.v1.QuoteComplianceCaseRequest;
import com.acme.compliance.v1.QuoteComplianceCaseResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.ScoreSanctionRequest;
import com.acme.compliance.v1.ScoreSanctionResponse;
import com.acme.compliance.v1.ValidateControlRequest;
import com.acme.compliance.v1.ValidateControlResponse;
import com.acme.compliance.v1.ValidateSanctionRequest;
import com.acme.compliance.v1.ValidateSanctionResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.SearchPoliciesRequest;
import com.acme.insurance.v1.SearchPoliciesResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.StreamFiscalPeriodExportRequest;
import com.acme.ledger.v1.StreamFiscalPeriodExportResponse;
import com.acme.lending.v1.CheckLoanApplicationEligibilityRequest;
import com.acme.lending.v1.CheckLoanApplicationEligibilityResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class SanctionValidationApplicationTest {
    @Test
    void previewSanctionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new SanctionValidationApplication(
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                BankAccountQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                ControlValidationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                LoanApplicationValidationServiceGrpc.newBlockingStub(channel),
                new SanctionValidationRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(PreviewSanctionRequest.getDefaultInstance());
        var response = application.previewSanction(request);
        var sent0 = channel.request("acme.compliance.v1.ComplianceCaseCalculationService/QuoteComplianceCase");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction", "audit", "updated_by"),
                ProbeSupport.value(sent0, "compliance_case", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction", "manager", "user_id"),
                ProbeSupport.value(sent0, "compliance_case", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction", "audit", "created_at"),
                ProbeSupport.value(sent0, "compliance_case", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction", "location", "postal_code"),
                ProbeSupport.value(sent0, "details", "destination", "postal_code"));
        var sent1 = channel.request("acme.authorization.v1.AccessPolicyService/UpdateAccessPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction", "audit", "created_at"),
                ProbeSupport.value(sent1, "access_policy", "audit", "created_at"));
        var sent2 = channel.request("acme.banking.v1.BankAccountQueryService/ResolveBankAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/QuoteComplianceCase"), "compliance_case", "customer", "display_name"),
                ProbeSupport.value(response, "sanction", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyService/UpdateAccessPolicy"), "access_policy", "owner", "email"),
                ProbeSupport.value(response, "sanction", "children", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyService/UpdateAccessPolicy"), "access_policy", "audit", "updated_at"),
                ProbeSupport.value(response, "sanction", "children", "children", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.BankAccountQueryService/ResolveBankAccounts"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction_id"),
                ProbeSupport.value(response, "sanction", "sanction_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction", "status"),
                ProbeSupport.value(response, "sanction", "status"));
    }
}
