package com.acme.finance.domains.expenses;

import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.assets.v1.ScoreFixedAssetRequest;
import com.acme.assets.v1.ScoreFixedAssetResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.ValidateAuditEventRequest;
import com.acme.audit.v1.ValidateAuditEventResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.ResolveBankAccountsRequest;
import com.acme.banking.v1.ResolveBankAccountsResponse;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.expenses.v1.CalculateMileageClaimRequest;
import com.acme.expenses.v1.CalculateMileageClaimResponse;
import com.acme.expenses.v1.CalculateReceiptRequest;
import com.acme.expenses.v1.CalculateReceiptResponse;
import com.acme.expenses.v1.CompareExpenseReportOptionsRequest;
import com.acme.expenses.v1.CompareExpenseReportOptionsResponse;
import com.acme.expenses.v1.CompareMileageClaimOptionsRequest;
import com.acme.expenses.v1.CompareMileageClaimOptionsResponse;
import com.acme.expenses.v1.EstimateExpenseReportRequest;
import com.acme.expenses.v1.EstimateExpenseReportResponse;
import com.acme.expenses.v1.EstimateMileageClaimRequest;
import com.acme.expenses.v1.EstimateMileageClaimResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportBankSettlement;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExplainMileageClaimCalculationRequest;
import com.acme.expenses.v1.ExplainMileageClaimCalculationResponse;
import com.acme.expenses.v1.GetMileageClaimRatesRequest;
import com.acme.expenses.v1.GetMileageClaimRatesResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.QuoteMileageClaimRequest;
import com.acme.expenses.v1.QuoteMileageClaimResponse;
import com.acme.expenses.v1.RecalculateExpenseReportsRequest;
import com.acme.expenses.v1.RecalculateExpenseReportsResponse;
import com.acme.expenses.v1.RecalculateMileageClaimsRequest;
import com.acme.expenses.v1.RecalculateMileageClaimsResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptBankSettlement;
import com.acme.expenses.v1.ReceiptCalculationServiceGrpc;
import com.acme.expenses.v1.ReceiptRef;
import com.acme.expenses.v1.SimulateExpenseReportRequest;
import com.acme.expenses.v1.SimulateExpenseReportResponse;
import com.acme.expenses.v1.SimulateReceiptRequest;
import com.acme.expenses.v1.SimulateReceiptResponse;
import com.acme.featurestore.v1.FeatureViewQueryServiceGrpc;
import com.acme.featurestore.v1.SuggestFeatureViewsRequest;
import com.acme.featurestore.v1.SuggestFeatureViewsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.lending.v1.ExplainLoanDecisionRequest;
import com.acme.lending.v1.ExplainLoanDecisionResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.licensing.v1.CreateSoftwareLicenseRequest;
import com.acme.licensing.v1.CreateSoftwareLicenseResponse;
import com.acme.licensing.v1.LicenseAssignmentRef;
import com.acme.licensing.v1.SoftwareLicense;
import com.acme.licensing.v1.SoftwareLicenseBankFunding;
import com.acme.licensing.v1.SoftwareLicenseServiceGrpc;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.UpdateMetricSeriesRequest;
import com.acme.metrics.v1.UpdateMetricSeriesResponse;
import com.acme.ratelimiting.v1.ListRateLimitsRequest;
import com.acme.ratelimiting.v1.ListRateLimitsResponse;
import com.acme.ratelimiting.v1.RateLimitServiceGrpc;
import com.acme.risk.v1.ScoreStressTestRequest;
import com.acme.risk.v1.ScoreStressTestResponse;
import com.acme.risk.v1.StressTest;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
import org.springframework.stereotype.Service;

class MileageClaimCalculationApplicationTest {
    @Test
    void explainMileageClaimCalculationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new MileageClaimCalculationApplication(
                FixedAssetCalculationServiceGrpc.newBlockingStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BankAccountQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseServiceGrpc.newStub(channel),
                ExpenseReportCalculationServiceGrpc.newBlockingStub(channel),
                ReceiptCalculationServiceGrpc.newBlockingStub(channel),
                FeatureViewQueryServiceGrpc.newBlockingStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                SoftwareLicenseServiceGrpc.newBlockingStub(channel),
                RateLimitServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(ExplainMileageClaimCalculationRequest.getDefaultInstance());
        var response = application.explainMileageClaimCalculation(request);
        var sent0 = channel.request("acme.featurestore.v1.FeatureViewQueryService/SuggestFeatureViews");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        var sent1 = channel.request("acme.assets.v1.FixedAssetValidationService/ScoreFixedAsset");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "billing_address", "country_code"),
                ProbeSupport.value(sent1, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "billing_address", "locality"),
                ProbeSupport.value(sent1, "fixed_asset", "address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "billing_address", "postal_code"),
                ProbeSupport.value(sent1, "details", "destination", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "audit", "updated_at"),
                ProbeSupport.value(sent1, "fixed_asset", "audit", "updated_at"));
        var sent2 = channel.request("acme.expenses.v1.ReceiptCalculationService/SimulateReceipt");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "billing_address", "lines"),
                ProbeSupport.value(sent2, "receipt", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "bank", "iban"),
                ProbeSupport.value(sent2, "receipt", "bank", "iban"));
        var sent3 = channel.request("acme.expenses.v1.ReceiptCalculationService/CalculateReceipt");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "bank", "bic"),
                ProbeSupport.value(sent3, "receipt", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetValidationService/ScoreFixedAsset"), "fixed_asset", "address", "locality"),
                ProbeSupport.value(sent3, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featurestore.v1.FeatureViewQueryService/SuggestFeatureViews"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featurestore.v1.FeatureViewQueryService/SuggestFeatureViews"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetValidationService/ScoreFixedAsset"), "fixed_asset", "audit", "updated_by"),
                ProbeSupport.value(response, "mileage_claim", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptCalculationService/SimulateReceipt"), "receipt", "counterparty", "number"),
                ProbeSupport.value(response, "mileage_claim", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptCalculationService/CalculateReceipt"), "receipt", "due_at"),
                ProbeSupport.value(response, "mileage_claim", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptCalculationService/CalculateReceipt"), "receipt", "billing_address", "lines"),
                ProbeSupport.value(response, "mileage_claim", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim_id"),
                ProbeSupport.value(response, "mileage_claim", "mileage_claim_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "counterparty"),
                ProbeSupport.value(response, "mileage_claim", "counterparty"));
    }
}
