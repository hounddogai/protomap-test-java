package com.acme.finance.domains.payroll;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailAdminServiceGrpc;
import com.acme.audit.v1.AuditTrailBankSettlement;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.PushRetentionPolicyRequest;
import com.acme.audit.v1.PushRetentionPolicyResponse;
import com.acme.audit.v1.RepairAuditTrailRequest;
import com.acme.audit.v1.RepairAuditTrailResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyDiscountAction;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamAuditEventNoticesRequest;
import com.acme.audit.v1.StreamAuditEventNoticesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.insurance.v1.ValidatePolicyRequest;
import com.acme.insurance.v1.ValidatePolicyResponse;
import com.acme.insurance.v1.ValidateUnderwritingRequest;
import com.acme.insurance.v1.ValidateUnderwritingResponse;
import com.acme.media.v1.CountMediaAssetsRequest;
import com.acme.media.v1.CountMediaAssetsResponse;
import com.acme.media.v1.MediaAssetQueryServiceGrpc;
import com.acme.payroll.v1.CalculatePayStubRequest;
import com.acme.payroll.v1.CalculatePayStubResponse;
import com.acme.payroll.v1.CalculateTaxWithholdingRequest;
import com.acme.payroll.v1.CalculateTaxWithholdingResponse;
import com.acme.payroll.v1.ComparePayStubOptionsRequest;
import com.acme.payroll.v1.ComparePayStubOptionsResponse;
import com.acme.payroll.v1.CompareTaxWithholdingOptionsRequest;
import com.acme.payroll.v1.CompareTaxWithholdingOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionCondition;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.EstimateTaxWithholdingRequest;
import com.acme.payroll.v1.EstimateTaxWithholdingResponse;
import com.acme.payroll.v1.ExplainDeductionCalculationRequest;
import com.acme.payroll.v1.ExplainDeductionCalculationResponse;
import com.acme.payroll.v1.ExplainPayRunCalculationRequest;
import com.acme.payroll.v1.ExplainPayRunCalculationResponse;
import com.acme.payroll.v1.ExplainTaxWithholdingCalculationRequest;
import com.acme.payroll.v1.ExplainTaxWithholdingCalculationResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubLineTax;
import com.acme.payroll.v1.PreviewPayRunRequest;
import com.acme.payroll.v1.PreviewPayRunResponse;
import com.acme.payroll.v1.QuotePayStubRequest;
import com.acme.payroll.v1.QuotePayStubResponse;
import com.acme.payroll.v1.QuoteTaxWithholdingRequest;
import com.acme.payroll.v1.QuoteTaxWithholdingResponse;
import com.acme.payroll.v1.RecalculateTaxWithholdingsRequest;
import com.acme.payroll.v1.RecalculateTaxWithholdingsResponse;
import com.acme.payroll.v1.SimulatePayStubRequest;
import com.acme.payroll.v1.SimulatePayStubResponse;
import com.acme.payroll.v1.SimulateTaxWithholdingRequest;
import com.acme.payroll.v1.SimulateTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholding;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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

class TaxWithholdingCalculationApplicationTest {
    @Test
    void recalculateTaxWithholdingsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var application = new TaxWithholdingCalculationApplication(
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                AuditTrailAdminServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                UnderwritingValidationServiceGrpc.newBlockingStub(channel),
                MediaAssetQueryServiceGrpc.newFutureStub(channel),
                DeductionCalculationServiceGrpc.newBlockingStub(channel),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                PayRunValidationServiceGrpc.newBlockingStub(channel),
                PayStubCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(RecalculateTaxWithholdingsRequest.getDefaultInstance());
        var response = application.recalculateTaxWithholdings(request);
        var sent0 = channel.request("acme.payroll.v1.PayRunCalculationService/ExplainPayRunCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "discount", "amount"),
                ProbeSupport.value(sent0, "pay_run", "lines", "amount"));
        var sent1 = channel.request("acme.audit.v1.AuditEventNotificationService/StreamAuditEventNotices");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "owner", "email"),
                ProbeSupport.value(sent1, "subscriber", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunCalculationService/ExplainPayRunCalculation"), "pay_run", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.payroll.v1.PayStubCalculationService/ComparePayStubOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "discount", "amount"),
                ProbeSupport.value(sent2, "pay_stub", "lines", "tax", "amount"));
        var sent3 = channel.request("acme.insurance.v1.UnderwritingValidationService/ValidateUnderwriting");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunCalculationService/ExplainPayRunCalculation"), "pay_run", "audit", "created_at"),
                ProbeSupport.value(sent3, "underwriting", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/ComparePayStubOptions"), "pay_stub", "audit", "created_at"),
                ProbeSupport.value(sent3, "underwriting", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "owner", "display_name"),
                ProbeSupport.value(sent3, "underwriting", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunCalculationService/ExplainPayRunCalculation"), "pay_run", "audit", "created_at"),
                ProbeSupport.value(response, "tax_withholding", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunCalculationService/ExplainPayRunCalculation"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/ComparePayStubOptions"), "pay_stub", "total"),
                ProbeSupport.value(response, "breakdown", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingValidationService/ValidateUnderwriting"), "underwriting", "assignee", "email"),
                ProbeSupport.value(response, "tax_withholding", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding_id"),
                ProbeSupport.value(response, "tax_withholding", "tax_withholding_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "version"),
                ProbeSupport.value(response, "tax_withholding", "version"));
    }
}
