package com.acme.finance.domains.payroll;

import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.LintFixedAssetsRequest;
import com.acme.assets.v1.LintFixedAssetsResponse;
import com.acme.assets.v1.SummarizeDepreciationsRequest;
import com.acme.assets.v1.SummarizeDepreciationsResponse;
import com.acme.assets.v1.ValidateFixedAssetRequest;
import com.acme.assets.v1.ValidateFixedAssetResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.CloneLoanApplicationRequest;
import com.acme.lending.v1.CloneLoanApplicationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationLineTax;
import com.acme.lending.v1.LoanApplicationServiceGrpc;
import com.acme.payroll.v1.CalculateDeductionRequest;
import com.acme.payroll.v1.CalculateDeductionResponse;
import com.acme.payroll.v1.CompareDeductionOptionsRequest;
import com.acme.payroll.v1.CompareDeductionOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.EstimateDeductionRequest;
import com.acme.payroll.v1.EstimateDeductionResponse;
import com.acme.payroll.v1.ExplainDeductionCalculationRequest;
import com.acme.payroll.v1.ExplainDeductionCalculationResponse;
import com.acme.payroll.v1.ExplainPayRunCalculationRequest;
import com.acme.payroll.v1.ExplainPayRunCalculationResponse;
import com.acme.payroll.v1.GetDeductionRatesRequest;
import com.acme.payroll.v1.GetDeductionRatesResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunCardSettlement;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.QuoteDeductionRequest;
import com.acme.payroll.v1.QuoteDeductionResponse;
import com.acme.payroll.v1.QuotePayRunRequest;
import com.acme.payroll.v1.QuotePayRunResponse;
import com.acme.payroll.v1.RecalculateDeductionsRequest;
import com.acme.payroll.v1.RecalculateDeductionsResponse;
import com.acme.payroll.v1.SimulateDeductionRequest;
import com.acme.payroll.v1.SimulateDeductionResponse;
import com.acme.payroll.v1.SimulatePayRunRequest;
import com.acme.payroll.v1.SimulatePayRunResponse;
import com.acme.payroll.v1.SimulatePayStubRequest;
import com.acme.payroll.v1.SimulatePayStubResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.ValidatePayRunRequest;
import com.acme.payroll.v1.ValidatePayRunResponse;
import com.acme.procurement.v1.CalculateGoodsReceiptRequest;
import com.acme.procurement.v1.CalculateGoodsReceiptResponse;
import com.acme.procurement.v1.CountPurchaseOrdersRequest;
import com.acme.procurement.v1.CountPurchaseOrdersResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptCalculationServiceGrpc;
import com.acme.procurement.v1.GoodsReceiptLine;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class DeductionCalculationApplicationTest {
    @Test
    void recalculateDeductionsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var application = new DeductionCalculationApplication(
                DepreciationQueryServiceGrpc.newBlockingStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                LoanApplicationServiceGrpc.newBlockingStub(channel),
                new DeductionCalculationRequestMapperImpl(),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                PayRunValidationServiceGrpc.newBlockingStub(channel),
                GoodsReceiptCalculationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(RecalculateDeductionsRequest.getDefaultInstance());
        var response = application.recalculateDeductions(request);
        var sent0 = channel.request("acme.procurement.v1.GoodsReceiptCalculationService/CalculateGoodsReceipt");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent0, "goods_receipt", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "lines"),
                ProbeSupport.value(sent0, "goods_receipt", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "discount", "amount"),
                ProbeSupport.value(sent0, "goods_receipt", "lines", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "recipient"),
                ProbeSupport.value(sent0, "goods_receipt", "billing_address", "recipient"));
        var sent1 = channel.request("acme.payroll.v1.PayRunValidationService/ValidatePayRun");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent1, "pay_run", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "owner", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptCalculationService/CalculateGoodsReceipt"), "goods_receipt", "billing_address", "lines"),
                ProbeSupport.value(sent1, "pay_run", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptCalculationService/CalculateGoodsReceipt"), "goods_receipt", "audit", "created_by"),
                ProbeSupport.value(response, "deduction", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunValidationService/ValidatePayRun"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunValidationService/ValidatePayRun"), "pay_run", "audit", "updated_by"),
                ProbeSupport.value(response, "deduction", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction_id"),
                ProbeSupport.value(response, "deduction", "deduction_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "effective"),
                ProbeSupport.value(response, "deduction", "effective"));
    }
}
