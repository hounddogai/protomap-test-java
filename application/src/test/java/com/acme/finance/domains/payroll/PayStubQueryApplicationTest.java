package com.acme.finance.domains.payroll;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.AssetTransferLine;
import com.acme.assets.v1.CalculateAssetTransferRequest;
import com.acme.assets.v1.CalculateAssetTransferResponse;
import com.acme.assets.v1.QuoteAssetTransferRequest;
import com.acme.assets.v1.QuoteAssetTransferResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.banking.v1.SummarizeBankAccountsRequest;
import com.acme.banking.v1.SummarizeBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.expenses.v1.ExplainExpensePolicyDecisionRequest;
import com.acme.expenses.v1.ExplainExpensePolicyDecisionResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.ShareFiscalPeriodExportRequest;
import com.acme.ledger.v1.ShareFiscalPeriodExportResponse;
import com.acme.payroll.v1.BrowsePayStubsRequest;
import com.acme.payroll.v1.BrowsePayStubsResponse;
import com.acme.payroll.v1.CheckPayRunEligibilityRequest;
import com.acme.payroll.v1.CheckPayRunEligibilityResponse;
import com.acme.payroll.v1.CountPayStubsRequest;
import com.acme.payroll.v1.CountPayStubsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.EstimateTaxWithholdingRequest;
import com.acme.payroll.v1.EstimateTaxWithholdingResponse;
import com.acme.payroll.v1.ExportPayStubsRequest;
import com.acme.payroll.v1.ExportPayStubsResponse;
import com.acme.payroll.v1.FindPayStubsRequest;
import com.acme.payroll.v1.FindPayStubsResponse;
import com.acme.payroll.v1.LookupPayStubRequest;
import com.acme.payroll.v1.LookupPayStubResponse;
import com.acme.payroll.v1.NormalizeTaxWithholdingRequest;
import com.acme.payroll.v1.NormalizeTaxWithholdingResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.PeekPayStubRequest;
import com.acme.payroll.v1.PeekPayStubResponse;
import com.acme.payroll.v1.ResolvePayStubsRequest;
import com.acme.payroll.v1.ResolvePayStubsResponse;
import com.acme.payroll.v1.SummarizePayStubsRequest;
import com.acme.payroll.v1.SummarizePayStubsResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingBlockAction;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.payroll.v1.ValidatePayRunRequest;
import com.acme.payroll.v1.ValidatePayRunResponse;
import com.acme.payroll.v1.ValidatePayStubRequest;
import com.acme.payroll.v1.ValidatePayStubResponse;
import com.acme.payroll.v1.ValidateTaxWithholdingRequest;
import com.acme.payroll.v1.ValidateTaxWithholdingResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class PayStubQueryApplicationTest {
    @Test
    void resolvePayStubsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var application = new PayStubQueryApplication(
                AssetTransferCalculationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BankAccountQueryServiceGrpc.newBlockingStub(channel),
                BankAccountServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                ExpensePolicyValidationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                PayRunValidationServiceGrpc.newBlockingStub(channel),
                PayStubValidationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingCalculationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingValidationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ResolvePayStubsRequest.getDefaultInstance());
        var response = application.resolvePayStubs(request);
        var sent0 = channel.request("acme.ledger.v1.FiscalPeriodExportService/ShareFiscalPeriodExport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "baggage"),
                ProbeSupport.value(sent0, "details", "variables"));
        var sent1 = channel.request("acme.payroll.v1.TaxWithholdingValidationService/ValidateTaxWithholding");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodExportService/ShareFiscalPeriodExport"), "fiscal_period", "audit", "etag"),
                ProbeSupport.value(sent1, "tax_withholding", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodExportService/ShareFiscalPeriodExport"), "fiscal_period", "organizer", "user_id"),
                ProbeSupport.value(sent1, "tax_withholding", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodExportService/ShareFiscalPeriodExport"), "fiscal_period", "location", "region"),
                ProbeSupport.value(response, "pay_stubs", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingValidationService/ValidateTaxWithholding"), "tax_withholding", "audit", "updated_at"),
                ProbeSupport.value(response, "pay_stubs", "audit", "updated_at"));
    }

    @Test
    void exportPayStubsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var application = new PayStubQueryApplication(
                AssetTransferCalculationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BankAccountQueryServiceGrpc.newBlockingStub(channel),
                BankAccountServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                ExpensePolicyValidationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                PayRunValidationServiceGrpc.newBlockingStub(channel),
                PayStubValidationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingCalculationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingValidationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ExportPayStubsRequest.getDefaultInstance());
        var response = application.exportPayStubs(request);
        var sent0 = channel.request("acme.expenses.v1.ExpensePolicyValidationService/ExplainExpensePolicyDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent0, "expense_policy", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        var sent1 = channel.request("acme.assets.v1.AssetTransferCalculationService/QuoteAssetTransfer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyValidationService/ExplainExpensePolicyDecision"), "expense_policy", "audit", "etag"),
                ProbeSupport.value(sent1, "asset_transfer", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyValidationService/ExplainExpensePolicyDecision"), "expense_policy", "audit", "updated_by"),
                ProbeSupport.value(sent1, "asset_transfer", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyValidationService/ExplainExpensePolicyDecision"), "expense_policy", "description"),
                ProbeSupport.value(response, "pay_stub", "lines", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyValidationService/ExplainExpensePolicyDecision"), "expense_policy", "audit", "updated_at"),
                ProbeSupport.value(response, "pay_stub", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferCalculationService/QuoteAssetTransfer"), "asset_transfer", "total"),
                ProbeSupport.value(response, "pay_stub", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "pay_stub", "billing_address", "region"));
        var endpoint = new PayStubQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportPayStubsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportPayStubsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportPayStubs(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
