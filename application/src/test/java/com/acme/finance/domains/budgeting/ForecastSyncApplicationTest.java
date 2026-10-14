package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.BrowseBudgetsRequest;
import com.acme.budgeting.v1.BrowseBudgetsResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineAttachment;
import com.acme.budgeting.v1.BudgetLineBankSettlement;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineRef;
import com.acme.budgeting.v1.BudgetQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.CalculateBudgetLineRequest;
import com.acme.budgeting.v1.CalculateBudgetLineResponse;
import com.acme.budgeting.v1.CompareBudgetOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetOptionsResponse;
import com.acme.budgeting.v1.CompareForecastOptionsRequest;
import com.acme.budgeting.v1.CompareForecastOptionsResponse;
import com.acme.budgeting.v1.EstimateBudgetLineRequest;
import com.acme.budgeting.v1.EstimateBudgetLineResponse;
import com.acme.budgeting.v1.ExplainForecastCalculationRequest;
import com.acme.budgeting.v1.ExplainForecastCalculationResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastBankSettlement;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastValidationServiceGrpc;
import com.acme.budgeting.v1.GetForecastSyncStateRequest;
import com.acme.budgeting.v1.GetForecastSyncStateResponse;
import com.acme.budgeting.v1.ListForecastConflictsRequest;
import com.acme.budgeting.v1.ListForecastConflictsResponse;
import com.acme.budgeting.v1.PullForecastsRequest;
import com.acme.budgeting.v1.PullForecastsResponse;
import com.acme.budgeting.v1.PushForecastRequest;
import com.acme.budgeting.v1.PushForecastResponse;
import com.acme.budgeting.v1.ReconcileForecastsRequest;
import com.acme.budgeting.v1.ReconcileForecastsResponse;
import com.acme.budgeting.v1.ResolveForecastConflictRequest;
import com.acme.budgeting.v1.ResolveForecastConflictResponse;
import com.acme.budgeting.v1.RetryVarianceImportRequest;
import com.acme.budgeting.v1.RetryVarianceImportResponse;
import com.acme.budgeting.v1.StreamForecastChangesRequest;
import com.acme.budgeting.v1.StreamForecastChangesResponse;
import com.acme.budgeting.v1.SyncForecastsRequest;
import com.acme.budgeting.v1.SyncForecastsResponse;
import com.acme.budgeting.v1.ValidateForecastRequest;
import com.acme.budgeting.v1.ValidateForecastResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.community.v1.Forum;
import com.acme.community.v1.ForumServiceGrpc;
import com.acme.community.v1.UpdateForumRequest;
import com.acme.community.v1.UpdateForumResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseWorkflowServiceGrpc;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ListUnderwritingConflictsRequest;
import com.acme.insurance.v1.ListUnderwritingConflictsResponse;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.keymanagement.v1.CryptoKeyServiceGrpc;
import com.acme.keymanagement.v1.WatchCryptoKeysRequest;
import com.acme.keymanagement.v1.WatchCryptoKeysResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.StreamFiscalPeriodExportRequest;
import com.acme.ledger.v1.StreamFiscalPeriodExportResponse;
import com.acme.orders.v1.EstimateOrderRequest;
import com.acme.orders.v1.EstimateOrderResponse;
import com.acme.orders.v1.Order;
import com.acme.orders.v1.OrderCalculationServiceGrpc;
import com.acme.risk.v1.ApproveRiskScenarioRequest;
import com.acme.risk.v1.ApproveRiskScenarioResponse;
import com.acme.risk.v1.RejectRiskRegisterRequest;
import com.acme.risk.v1.RejectRiskRegisterResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.SubmitRiskRegisterRequest;
import com.acme.risk.v1.SubmitRiskRegisterResponse;
import com.acme.suppliers.v1.FindSuppliersRequest;
import com.acme.suppliers.v1.FindSuppliersResponse;
import com.acme.suppliers.v1.ListSuppliersRequest;
import com.acme.suppliers.v1.ListSuppliersResponse;
import com.acme.suppliers.v1.PreviewSupplierRatingRequest;
import com.acme.suppliers.v1.PreviewSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.venues.v1.CloneVenueRequest;
import com.acme.venues.v1.CloneVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ForecastSyncApplicationTest {
    @Test
    void streamForecastChangesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var listener = new ForecastSyncApplicationEvents(SupplierQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ForecastSyncApplication.ResolveForecastConflictStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ForecastSyncApplication(
                BudgetCalculationServiceGrpc.newBlockingStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                BudgetQueryServiceGrpc.newBlockingStub(channel),
                ForecastCalculationServiceGrpc.newBlockingStub(channel),
                ForecastCalculationServiceGrpc.newFutureStub(channel),
                ForecastValidationServiceGrpc.newFutureStub(channel),
                VarianceImportServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ForumServiceGrpc.newBlockingStub(channel),
                ComplianceCaseWorkflowServiceGrpc.newBlockingStub(channel),
                events,
                UnderwritingSyncServiceGrpc.newBlockingStub(channel),
                CryptoKeyServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                OrderCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterApprovalServiceGrpc.newBlockingStub(channel),
                RiskScenarioApprovalServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel),
                SupplierServiceGrpc.newBlockingStub(channel),
                VenueServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StreamForecastChangesRequest.getDefaultInstance());
        var response = application.streamForecastChanges(request);
        var sent0 = channel.request("acme.budgeting.v1.BudgetLineCalculationService/EstimateBudgetLine");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "attachments", "uri"),
                ProbeSupport.value(sent0, "budget_line", "attachments", "uri"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "attachments", "content_type"),
                ProbeSupport.value(sent0, "budget_line", "attachments", "content_type"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "attachments", "checksum"),
                ProbeSupport.value(sent0, "budget_line", "attachments", "checksum"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "attachments", "file_name"),
                ProbeSupport.value(sent0, "budget_line", "attachments", "file_name"));
        var sent1 = channel.request("acme.ledger.v1.FiscalPeriodExportService/StreamFiscalPeriodExport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "due_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "forecast_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        var sent2 = channel.request("acme.keymanagement.v1.CryptoKeyService/WatchCryptoKeys");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/EstimateBudgetLine"), "budget_line", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        var sent3 = channel.request("acme.risk.v1.RiskScenarioApprovalService/ApproveRiskScenario");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "counterparty", "display_name"),
                ProbeSupport.value(sent3, "risk_scenario", "customer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioApprovalService/ApproveRiskScenario"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioApprovalService/ApproveRiskScenario"), "risk_scenario", "customer", "display_name"),
                ProbeSupport.value(response, "forecast", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "status"),
                ProbeSupport.value(response, "forecast", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "notes"),
                ProbeSupport.value(response, "forecast", "notes"));
        var endpoint = new ForecastSyncEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamForecastChangesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamForecastChangesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.streamForecastChanges(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ForecastSyncApplicationEvents(SupplierQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<FindSuppliersResponse>();
        listener.on(new ForecastSyncApplication.ResolveForecastConflictStage1Requested(ProbeSupport.sample(FindSuppliersRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
