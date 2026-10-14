package com.acme.finance.domains.expenses;

import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.BatchCreateExpenseReportsRequest;
import com.acme.expenses.v1.BatchCreateExpenseReportsResponse;
import com.acme.expenses.v1.BatchGetExpenseReportsRequest;
import com.acme.expenses.v1.BatchGetExpenseReportsResponse;
import com.acme.expenses.v1.CalculateExpensePolicyRequest;
import com.acme.expenses.v1.CalculateExpensePolicyResponse;
import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.CheckExpenseReportEligibilityRequest;
import com.acme.expenses.v1.CheckExpenseReportEligibilityResponse;
import com.acme.expenses.v1.CloneExpenseReportRequest;
import com.acme.expenses.v1.CloneExpenseReportResponse;
import com.acme.expenses.v1.CompareMileageClaimOptionsRequest;
import com.acme.expenses.v1.CompareMileageClaimOptionsResponse;
import com.acme.expenses.v1.CountExpenseReportsRequest;
import com.acme.expenses.v1.CountExpenseReportsResponse;
import com.acme.expenses.v1.CreateExpenseReportRequest;
import com.acme.expenses.v1.CreateExpenseReportResponse;
import com.acme.expenses.v1.DeleteExpenseReportRequest;
import com.acme.expenses.v1.DeleteExpenseReportResponse;
import com.acme.expenses.v1.EstimateExpensePolicyRequest;
import com.acme.expenses.v1.EstimateExpensePolicyResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportQueryServiceGrpc;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.expenses.v1.GetExpenseReportRequest;
import com.acme.expenses.v1.GetExpenseReportResponse;
import com.acme.expenses.v1.ListExpenseReportsRequest;
import com.acme.expenses.v1.ListExpenseReportsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimCalculationServiceGrpc;
import com.acme.expenses.v1.MileageClaimLine;
import com.acme.expenses.v1.MileageClaimLineTax;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.SearchExpenseReportsRequest;
import com.acme.expenses.v1.SearchExpenseReportsResponse;
import com.acme.expenses.v1.UpdateExpenseReportRequest;
import com.acme.expenses.v1.UpdateExpenseReportResponse;
import com.acme.expenses.v1.WatchExpenseReportsRequest;
import com.acme.expenses.v1.WatchExpenseReportsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ScoreUnderwritingRequest;
import com.acme.insurance.v1.ScoreUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.lending.v1.EstimateRepaymentRequest;
import com.acme.lending.v1.EstimateRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.procurement.v1.CompareRfqOptionsRequest;
import com.acme.procurement.v1.CompareRfqOptionsResponse;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.suppliers.v1.CountSupplierContractsRequest;
import com.acme.suppliers.v1.CountSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class ExpenseReportApplicationTest {
    @Test
    void createExpenseReportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new ExpenseReportApplication(
                FixedAssetCalculationServiceGrpc.newBlockingStub(channel),
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                ExpensePolicyCalculationServiceGrpc.newBlockingStub(channel),
                ExpenseReportCalculationServiceGrpc.newBlockingStub(channel),
                ExpenseReportQueryServiceGrpc.newBlockingStub(channel),
                ExpenseReportValidationServiceGrpc.newBlockingStub(channel),
                MileageClaimCalculationServiceGrpc.newBlockingStub(channel),
                UnderwritingValidationServiceGrpc.newFutureStub(channel),
                RepaymentCalculationServiceGrpc.newBlockingStub(channel),
                new ExpenseReportRequestMapperImpl(),
                RfqCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel),
                SupplierContractQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CreateExpenseReportRequest.getDefaultInstance());
        var response = application.createExpenseReport(request);
        var sent0 = channel.request("acme.expenses.v1.ExpenseReportQueryService/CountExpenseReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        var sent1 = channel.request("acme.expenses.v1.ExpensePolicyCalculationService/CalculateExpensePolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "counterparty", "email"),
                ProbeSupport.value(sent1, "expense_policy", "owner", "email"));
        var sent2 = channel.request("acme.expenses.v1.MileageClaimCalculationService/CompareMileageClaimOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyCalculationService/CalculateExpensePolicy"), "expense_policy", "discount", "amount"),
                ProbeSupport.value(sent2, "mileage_claim", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportQueryService/CountExpenseReports"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyCalculationService/CalculateExpensePolicy"), "breakdown", "total"),
                ProbeSupport.value(response, "expense_report", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyCalculationService/CalculateExpensePolicy"), "expense_policy", "audit", "updated_by"),
                ProbeSupport.value(response, "expense_report", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.MileageClaimCalculationService/CompareMileageClaimOptions"), "mileage_claim", "counterparty", "display_name"),
                ProbeSupport.value(response, "expense_report", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "due_at"),
                ProbeSupport.value(response, "expense_report", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "status"),
                ProbeSupport.value(response, "expense_report", "status"));
    }

    @Test
    void watchExpenseReportsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new ExpenseReportApplication(
                FixedAssetCalculationServiceGrpc.newBlockingStub(channel),
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                ExpensePolicyCalculationServiceGrpc.newBlockingStub(channel),
                ExpenseReportCalculationServiceGrpc.newBlockingStub(channel),
                ExpenseReportQueryServiceGrpc.newBlockingStub(channel),
                ExpenseReportValidationServiceGrpc.newBlockingStub(channel),
                MileageClaimCalculationServiceGrpc.newBlockingStub(channel),
                UnderwritingValidationServiceGrpc.newFutureStub(channel),
                RepaymentCalculationServiceGrpc.newBlockingStub(channel),
                new ExpenseReportRequestMapperImpl(),
                RfqCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel),
                SupplierContractQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(WatchExpenseReportsRequest.getDefaultInstance());
        var response = application.watchExpenseReports(request);
        var sent0 = channel.request("acme.expenses.v1.ExpenseReportCalculationService/CalculateExpenseReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "expense_report", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "details", "destination", "region"));
        var sent1 = channel.request("acme.assets.v1.FixedAssetCalculationService/EstimateFixedAsset");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportCalculationService/CalculateExpenseReport"), "expense_report", "billing_address", "locality"),
                ProbeSupport.value(sent1, "fixed_asset", "address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportCalculationService/CalculateExpenseReport"), "expense_report", "billing_address", "country_code"),
                ProbeSupport.value(sent1, "fixed_asset", "address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportCalculationService/CalculateExpenseReport"), "expense_report", "audit", "updated_by"),
                ProbeSupport.value(sent1, "fixed_asset", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportCalculationService/CalculateExpenseReport"), "expense_report", "billing_address", "region"),
                ProbeSupport.value(sent1, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportCalculationService/CalculateExpenseReport"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetCalculationService/EstimateFixedAsset"), "fixed_asset", "address", "postal_code"),
                ProbeSupport.value(response, "expense_report", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetCalculationService/EstimateFixedAsset"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "expense_report", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "expense_report", "billing_address", "region"));
        var endpoint = new ExpenseReportEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<WatchExpenseReportsResponse> observer = new StreamObserver<>() {
            public void onNext(WatchExpenseReportsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.watchExpenseReports(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void batchCreateExpenseReportsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new ExpenseReportApplication(
                FixedAssetCalculationServiceGrpc.newBlockingStub(channel),
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                ExpensePolicyCalculationServiceGrpc.newBlockingStub(channel),
                ExpenseReportCalculationServiceGrpc.newBlockingStub(channel),
                ExpenseReportQueryServiceGrpc.newBlockingStub(channel),
                ExpenseReportValidationServiceGrpc.newBlockingStub(channel),
                MileageClaimCalculationServiceGrpc.newBlockingStub(channel),
                UnderwritingValidationServiceGrpc.newFutureStub(channel),
                RepaymentCalculationServiceGrpc.newBlockingStub(channel),
                new ExpenseReportRequestMapperImpl(),
                RfqCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel),
                SupplierContractQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(BatchCreateExpenseReportsRequest.getDefaultInstance());
        var response = application.batchCreateExpenseReports(request);
        var sent0 = channel.request("acme.procurement.v1.RfqCalculationService/CompareRfqOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "billing_address", "country_code"),
                ProbeSupport.value(sent0, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqCalculationService/CompareRfqOptions"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        var endpoint = new ExpenseReportEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<BatchCreateExpenseReportsResponse> observer = new StreamObserver<>() {
            public void onNext(BatchCreateExpenseReportsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.batchCreateExpenseReports(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
