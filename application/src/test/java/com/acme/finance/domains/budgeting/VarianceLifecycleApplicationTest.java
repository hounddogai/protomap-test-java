package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ActivateVarianceRequest;
import com.acme.budgeting.v1.ActivateVarianceResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.EstimateBudgetRequest;
import com.acme.budgeting.v1.EstimateBudgetResponse;
import com.acme.budgeting.v1.ExpireVarianceRequest;
import com.acme.budgeting.v1.ExpireVarianceResponse;
import com.acme.budgeting.v1.FetchVarianceReportRequest;
import com.acme.budgeting.v1.FetchVarianceReportResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.GetVarianceLifecycleRequest;
import com.acme.budgeting.v1.GetVarianceLifecycleResponse;
import com.acme.budgeting.v1.ListForecastConflictsRequest;
import com.acme.budgeting.v1.ListForecastConflictsResponse;
import com.acme.budgeting.v1.ListVarianceReportsRequest;
import com.acme.budgeting.v1.ListVarianceReportsResponse;
import com.acme.budgeting.v1.RenewVarianceRequest;
import com.acme.budgeting.v1.RenewVarianceResponse;
import com.acme.budgeting.v1.ScheduleVarianceExpiryRequest;
import com.acme.budgeting.v1.ScheduleVarianceExpiryResponse;
import com.acme.budgeting.v1.ScoreVarianceRequest;
import com.acme.budgeting.v1.ScoreVarianceResponse;
import com.acme.budgeting.v1.SuspendVarianceRequest;
import com.acme.budgeting.v1.SuspendVarianceResponse;
import com.acme.budgeting.v1.TerminateVarianceRequest;
import com.acme.budgeting.v1.TerminateVarianceResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.budgeting.v1.VarianceReportingServiceGrpc;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.ExportFeatureFlagsRequest;
import com.acme.featureflags.v1.ExportFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesResponse;
import com.acme.risk.v1.AdvanceRiskRegisterWorkflowRequest;
import com.acme.risk.v1.AdvanceRiskRegisterWorkflowResponse;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import com.acme.risk.v1.PauseExposureWorkflowRequest;
import com.acme.risk.v1.PauseExposureWorkflowResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionResponse;
import com.acme.suppliers.v1.LintSupplierRatingsRequest;
import com.acme.suppliers.v1.LintSupplierRatingsResponse;
import com.acme.suppliers.v1.PreviewSupplierRatingRequest;
import com.acme.suppliers.v1.PreviewSupplierRatingResponse;
import com.acme.suppliers.v1.SuggestSupplierContractsRequest;
import com.acme.suppliers.v1.SuggestSupplierContractsResponse;
import com.acme.suppliers.v1.SummarizeSupplierContractsRequest;
import com.acme.suppliers.v1.SummarizeSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingCardSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
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

class VarianceLifecycleApplicationTest {
    @Test
    void scheduleVarianceExpiryPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var application = new VarianceLifecycleApplication(
                BudgetCalculationServiceGrpc.newFutureStub(channel),
                ForecastSyncServiceGrpc.newBlockingStub(channel),
                VarianceReportingServiceGrpc.newBlockingStub(channel),
                VarianceValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                FeatureFlagQueryServiceGrpc.newBlockingStub(channel),
                JournalEntryNotificationServiceGrpc.newBlockingStub(channel),
                new VarianceLifecycleRequestMapperImpl(),
                repository,
                ExposureWorkflowServiceGrpc.newBlockingStub(channel),
                RiskRegisterWorkflowServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierContractQueryServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(ScheduleVarianceExpiryRequest.getDefaultInstance());
        var response = application.scheduleVarianceExpiry(request);
        var sent0 = channel.request("acme.suppliers.v1.SupplierContractQueryService/SuggestSupplierContracts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "sla", "due_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "variance_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.budgeting.v1.VarianceValidationService/ScoreVariance");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "assignee", "email"),
                ProbeSupport.value(sent1, "variance", "assignee", "email"));
        var sent2 = channel.request("acme.suppliers.v1.SupplierRatingValidationService/PreviewSupplierRating");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "assignee", "display_name"),
                ProbeSupport.value(sent2, "supplier_rating", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceValidationService/ScoreVariance"), "variance", "requester", "email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        var sent3 = channel.request("acme.budgeting.v1.ForecastSyncService/ListForecastConflicts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/PreviewSupplierRating"), "supplier_rating", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "audit", "created_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierContractQueryService/SuggestSupplierContracts"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierContractQueryService/SuggestSupplierContracts"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceValidationService/ScoreVariance"), "variance", "assignee"),
                ProbeSupport.value(response, "variance", "assignee"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceValidationService/ScoreVariance"), "variance", "requester", "name"),
                ProbeSupport.value(response, "variance", "requester", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/PreviewSupplierRating"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance_id"),
                ProbeSupport.value(response, "variance", "variance_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "custom_fields"),
                ProbeSupport.value(response, "variance", "custom_fields"));
    }
}
