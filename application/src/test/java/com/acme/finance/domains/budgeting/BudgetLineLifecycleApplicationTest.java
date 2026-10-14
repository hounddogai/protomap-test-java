package com.acme.finance.domains.budgeting;

import com.acme.analytics.v1.AnalyticsEvent;
import com.acme.analytics.v1.AnalyticsEventServiceGrpc;
import com.acme.analytics.v1.FunnelRef;
import com.acme.analytics.v1.UpdateAnalyticsEventRequest;
import com.acme.analytics.v1.UpdateAnalyticsEventResponse;
import com.acme.budgeting.v1.ActivateBudgetLineRequest;
import com.acme.budgeting.v1.ActivateBudgetLineResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.ExpireBudgetLineRequest;
import com.acme.budgeting.v1.ExpireBudgetLineResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.GetBudgetLineLifecycleRequest;
import com.acme.budgeting.v1.GetBudgetLineLifecycleResponse;
import com.acme.budgeting.v1.ListBudgetLineReportsRequest;
import com.acme.budgeting.v1.ListBudgetLineReportsResponse;
import com.acme.budgeting.v1.ReconcileVariancesRequest;
import com.acme.budgeting.v1.ReconcileVariancesResponse;
import com.acme.budgeting.v1.ReinstateBudgetLineRequest;
import com.acme.budgeting.v1.ReinstateBudgetLineResponse;
import com.acme.budgeting.v1.ResolveBudgetLinesRequest;
import com.acme.budgeting.v1.ResolveBudgetLinesResponse;
import com.acme.budgeting.v1.ScheduleBudgetLineExpiryRequest;
import com.acme.budgeting.v1.ScheduleBudgetLineExpiryResponse;
import com.acme.budgeting.v1.ScheduleForecastReportRequest;
import com.acme.budgeting.v1.ScheduleForecastReportResponse;
import com.acme.budgeting.v1.SuggestBudgetLinesRequest;
import com.acme.budgeting.v1.SuggestBudgetLinesResponse;
import com.acme.budgeting.v1.SuspendBudgetLineRequest;
import com.acme.budgeting.v1.SuspendBudgetLineResponse;
import com.acme.budgeting.v1.TerminateBudgetLineRequest;
import com.acme.budgeting.v1.TerminateBudgetLineResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.CreateDocumentRequest;
import com.acme.documents.v1.CreateDocumentResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentFolderRef;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ListPremiumConflictsRequest;
import com.acme.insurance.v1.ListPremiumConflictsResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.SyncPremiumsRequest;
import com.acme.insurance.v1.SyncPremiumsResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentApprovalServiceGrpc;
import com.acme.lending.v1.SubmitRepaymentRequest;
import com.acme.lending.v1.SubmitRepaymentResponse;
import com.acme.payments.v1.PaymentQueryServiceGrpc;
import com.acme.payments.v1.SuggestPaymentsRequest;
import com.acme.payments.v1.SuggestPaymentsResponse;
import com.acme.suppliers.v1.LintSupplierRatingsRequest;
import com.acme.suppliers.v1.LintSupplierRatingsResponse;
import com.acme.suppliers.v1.LintSupplierRisksRequest;
import com.acme.suppliers.v1.LintSupplierRisksResponse;
import com.acme.suppliers.v1.PreviewSupplierRiskRequest;
import com.acme.suppliers.v1.PreviewSupplierRiskResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class BudgetLineLifecycleApplicationTest {
    @Test
    void scheduleBudgetLineExpiryPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var application = new BudgetLineLifecycleApplication(
                AnalyticsEventServiceGrpc.newFutureStub(channel),
                BudgetLineQueryServiceGrpc.newBlockingStub(channel),
                BudgetLineReportingServiceGrpc.newBlockingStub(channel),
                ForecastReportingServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                RepaymentApprovalServiceGrpc.newBlockingStub(channel),
                new BudgetLineLifecycleRequestMapperImpl(),
                PaymentQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel),
                SupplierRiskValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ScheduleBudgetLineExpiryRequest.getDefaultInstance());
        var response = application.scheduleBudgetLineExpiry(request);
        var sent0 = channel.request("acme.budgeting.v1.VarianceSyncService/ReconcileVariances");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "attachments", "checksum"),
                ProbeSupport.value(sent0, "details", "checksum"));
        var sent1 = channel.request("acme.budgeting.v1.ForecastReportingService/ScheduleForecastReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent1, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent1, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "metadata"),
                ProbeSupport.value(sent1, "details", "filters"));
        var sent2 = channel.request("acme.budgeting.v1.BudgetLineQueryService/SuggestBudgetLines");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceSyncService/ReconcileVariances"), "variance", "audit", "updated_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        var sent3 = channel.request("acme.insurance.v1.PremiumSyncService/ListPremiumConflicts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastReportingService/ScheduleForecastReport"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line_id"),
                ProbeSupport.value(response, "budget_line", "budget_line_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "issued_at"),
                ProbeSupport.value(response, "budget_line", "issued_at"));
    }
}
