package com.acme.finance.domains.payroll;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.ValidateAuditEventRequest;
import com.acme.audit.v1.ValidateAuditEventResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ListUnderwritingImportsRequest;
import com.acme.insurance.v1.ListUnderwritingImportsResponse;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.keymanagement.v1.CryptoKeyServiceGrpc;
import com.acme.keymanagement.v1.WatchCryptoKeysRequest;
import com.acme.keymanagement.v1.WatchCryptoKeysResponse;
import com.acme.lending.v1.CommentRepaymentApprovalRequest;
import com.acme.lending.v1.CommentRepaymentApprovalResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentApprovalServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.payroll.v1.ActivateDeductionRequest;
import com.acme.payroll.v1.ActivateDeductionResponse;
import com.acme.payroll.v1.CheckDeductionEligibilityRequest;
import com.acme.payroll.v1.CheckDeductionEligibilityResponse;
import com.acme.payroll.v1.ComparePayStubOptionsRequest;
import com.acme.payroll.v1.ComparePayStubOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import com.acme.payroll.v1.FindPayRunsRequest;
import com.acme.payroll.v1.FindPayRunsResponse;
import com.acme.payroll.v1.GetDeductionLifecycleRequest;
import com.acme.payroll.v1.GetDeductionLifecycleResponse;
import com.acme.payroll.v1.GetDeductionRatesRequest;
import com.acme.payroll.v1.GetDeductionRatesResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunQueryServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.ReinstateDeductionRequest;
import com.acme.payroll.v1.ReinstateDeductionResponse;
import com.acme.payroll.v1.RenewDeductionRequest;
import com.acme.payroll.v1.RenewDeductionResponse;
import com.acme.payroll.v1.ScheduleDeductionExpiryRequest;
import com.acme.payroll.v1.ScheduleDeductionExpiryResponse;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportRequest;
import com.acme.payroll.v1.ScheduleTaxWithholdingReportResponse;
import com.acme.payroll.v1.ShareTaxWithholdingReportRequest;
import com.acme.payroll.v1.ShareTaxWithholdingReportResponse;
import com.acme.payroll.v1.SuspendDeductionRequest;
import com.acme.payroll.v1.SuspendDeductionResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import com.acme.payroll.v1.TerminateDeductionRequest;
import com.acme.payroll.v1.TerminateDeductionResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class DeductionLifecycleApplicationTest {
    @Test
    void reinstateDeductionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var application = new DeductionLifecycleApplication(
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                UnderwritingImportServiceGrpc.newBlockingStub(channel),
                CryptoKeyServiceGrpc.newBlockingStub(channel),
                RepaymentApprovalServiceGrpc.newBlockingStub(channel),
                DeductionCalculationServiceGrpc.newBlockingStub(channel),
                DeductionValidationServiceGrpc.newBlockingStub(channel),
                PayRunQueryServiceGrpc.newBlockingStub(channel),
                TaxWithholdingReportingServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ReinstateDeductionRequest.getDefaultInstance());
        var response = application.reinstateDeduction(request);
        var sent0 = channel.request("acme.lending.v1.RepaymentApprovalService/CommentRepaymentApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent0, "repayment", "participants", "contact", "email"));
        var sent1 = channel.request("acme.payroll.v1.PayStubCalculationService/ComparePayStubOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentApprovalService/CommentRepaymentApproval"), "repayment", "audit", "updated_at"),
                ProbeSupport.value(sent1, "pay_stub", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentApprovalService/CommentRepaymentApproval"), "repayment", "location", "country_code"),
                ProbeSupport.value(sent1, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentApprovalService/CommentRepaymentApproval"), "repayment", "location", "lines"),
                ProbeSupport.value(sent1, "details", "destination", "lines"));
        var sent2 = channel.request("acme.payroll.v1.DeductionValidationService/CheckDeductionEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/ComparePayStubOptions"), "pay_stub", "audit", "created_by"),
                ProbeSupport.value(sent2, "deduction", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/ComparePayStubOptions"), "pay_stub", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "deduction", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentApprovalService/CommentRepaymentApproval"), "repayment", "audit", "etag"),
                ProbeSupport.value(response, "deduction", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/ComparePayStubOptions"), "pay_stub", "counterparty", "display_name"),
                ProbeSupport.value(response, "deduction", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/ComparePayStubOptions"), "pay_stub", "counterparty", "version"),
                ProbeSupport.value(response, "deduction", "version"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.DeductionValidationService/CheckDeductionEligibility"), "outcome", "reference_id"),
                ProbeSupport.value(response, "outcome", "reference_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction_id"),
                ProbeSupport.value(response, "deduction", "deduction_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
