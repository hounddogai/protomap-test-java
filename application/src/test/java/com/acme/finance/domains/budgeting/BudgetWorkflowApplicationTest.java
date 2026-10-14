package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.SubmitFixedAssetRequest;
import com.acme.assets.v1.SubmitFixedAssetResponse;
import com.acme.budgeting.v1.ActivateForecastRequest;
import com.acme.budgeting.v1.ActivateForecastResponse;
import com.acme.budgeting.v1.AdvanceBudgetWorkflowRequest;
import com.acme.budgeting.v1.AdvanceBudgetWorkflowResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetLineBankSettlement;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineCardSettlement;
import com.acme.budgeting.v1.BudgetLineLifecycleServiceGrpc;
import com.acme.budgeting.v1.BudgetLineLine;
import com.acme.budgeting.v1.BudgetLineLineTax;
import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.CancelBudgetWorkflowRequest;
import com.acme.budgeting.v1.CancelBudgetWorkflowResponse;
import com.acme.budgeting.v1.CountBudgetLineTotalsRequest;
import com.acme.budgeting.v1.CountBudgetLineTotalsResponse;
import com.acme.budgeting.v1.EstimateBudgetLineRequest;
import com.acme.budgeting.v1.EstimateBudgetLineResponse;
import com.acme.budgeting.v1.ExplainForecastDecisionRequest;
import com.acme.budgeting.v1.ExplainForecastDecisionResponse;
import com.acme.budgeting.v1.ExportBudgetLinesRequest;
import com.acme.budgeting.v1.ExportBudgetLinesResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastLifecycleServiceGrpc;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.ForecastValidationServiceGrpc;
import com.acme.budgeting.v1.GenerateVarianceReportRequest;
import com.acme.budgeting.v1.GenerateVarianceReportResponse;
import com.acme.budgeting.v1.GetForecastSyncStateRequest;
import com.acme.budgeting.v1.GetForecastSyncStateResponse;
import com.acme.budgeting.v1.InspectBudgetWorkflowRequest;
import com.acme.budgeting.v1.InspectBudgetWorkflowResponse;
import com.acme.budgeting.v1.ListBudgetLineReportsRequest;
import com.acme.budgeting.v1.ListBudgetLineReportsResponse;
import com.acme.budgeting.v1.ListBudgetWorkflowsRequest;
import com.acme.budgeting.v1.ListBudgetWorkflowsResponse;
import com.acme.budgeting.v1.PauseBudgetWorkflowRequest;
import com.acme.budgeting.v1.PauseBudgetWorkflowResponse;
import com.acme.budgeting.v1.RecallBudgetLineRequest;
import com.acme.budgeting.v1.RecallBudgetLineResponse;
import com.acme.budgeting.v1.ResumeBudgetWorkflowRequest;
import com.acme.budgeting.v1.ResumeBudgetWorkflowResponse;
import com.acme.budgeting.v1.RetryBudgetWorkflowRequest;
import com.acme.budgeting.v1.RetryBudgetWorkflowResponse;
import com.acme.budgeting.v1.ScheduleBudgetLineExpiryRequest;
import com.acme.budgeting.v1.ScheduleBudgetLineExpiryResponse;
import com.acme.budgeting.v1.ScheduleVarianceReportRequest;
import com.acme.budgeting.v1.ScheduleVarianceReportResponse;
import com.acme.budgeting.v1.ShareBudgetLineReportRequest;
import com.acme.budgeting.v1.ShareBudgetLineReportResponse;
import com.acme.budgeting.v1.ShareVarianceReportRequest;
import com.acme.budgeting.v1.ShareVarianceReportResponse;
import com.acme.budgeting.v1.SignalBudgetWorkflowRequest;
import com.acme.budgeting.v1.SignalBudgetWorkflowResponse;
import com.acme.budgeting.v1.StartBudgetWorkflowRequest;
import com.acme.budgeting.v1.StartBudgetWorkflowResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceReportingServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.featureflags.v1.BrowseFeatureFlagsRequest;
import com.acme.featureflags.v1.BrowseFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CountCoverageTotalsRequest;
import com.acme.insurance.v1.CountCoverageTotalsResponse;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.ListClaimWorkflowsRequest;
import com.acme.insurance.v1.ListClaimWorkflowsResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionResponse;
import com.acme.suppliers.v1.SummarizeSuppliersRequest;
import com.acme.suppliers.v1.SummarizeSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.travel.v1.Trip;
import com.acme.travel.v1.TripCardSettlement;
import com.acme.travel.v1.TripServiceGrpc;
import com.acme.travel.v1.UpdateTripRequest;
import com.acme.travel.v1.UpdateTripResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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

class BudgetWorkflowApplicationTest {
    @Test
    void retryBudgetWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var listener = new BudgetWorkflowApplicationEvents(BudgetLineReportingServiceGrpc.newStub(channel), FeatureFlagQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof BudgetWorkflowApplication.AdvanceBudgetWorkflowStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof BudgetWorkflowApplication.SignalBudgetWorkflowStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new BudgetWorkflowApplication(
                FixedAssetApprovalServiceGrpc.newBlockingStub(channel),
                BudgetLineApprovalServiceGrpc.newBlockingStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                BudgetLineLifecycleServiceGrpc.newBlockingStub(channel),
                BudgetLineQueryServiceGrpc.newBlockingStub(channel),
                BudgetLineReportingServiceGrpc.newBlockingStub(channel),
                ForecastLifecycleServiceGrpc.newBlockingStub(channel),
                ForecastSyncServiceGrpc.newBlockingStub(channel),
                VarianceReportingServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                CoverageReportingServiceGrpc.newBlockingStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                new BudgetWorkflowRequestMapperImpl(),
                repository,
                Runnable::run,
                SupplierQueryServiceGrpc.newBlockingStub(channel),
                SupplierRiskValidationServiceGrpc.newBlockingStub(channel),
                TripServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RetryBudgetWorkflowRequest.getDefaultInstance());
        var response = application.retryBudgetWorkflow(request);
        var sent0 = channel.request("acme.budgeting.v1.BudgetLineReportingService/ListBudgetLineReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget", "labels"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.budgeting.v1.BudgetLineApprovalService/RecallBudgetLine");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget", "audit", "etag"),
                ProbeSupport.value(sent1, "budget_line", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget", "card", "holder_name"),
                ProbeSupport.value(sent1, "budget_line", "card", "holder_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent1, "details", "approver", "email"));
        var sent2 = channel.request("acme.budgeting.v1.ForecastLifecycleService/ActivateForecast");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget", "holder", "forecast_id"),
                ProbeSupport.value(sent2, "forecast_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget", "contact", "phone"),
                ProbeSupport.value(sent2, "details", "notify_contact", "phone"));
        var sent3 = channel.request("acme.travel.v1.TripService/UpdateTrip");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget", "audit", "updated_by"),
                ProbeSupport.value(sent3, "trip", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineApprovalService/RecallBudgetLine"), "budget_line", "card", "last4"),
                ProbeSupport.value(sent3, "trip", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastLifecycleService/ActivateForecast"), "forecast", "billing_address", "lines"),
                ProbeSupport.value(sent3, "trip", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineReportingService/ListBudgetLineReports"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineApprovalService/RecallBudgetLine"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastLifecycleService/ActivateForecast"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_id"),
                ProbeSupport.value(response, "budget", "budget_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget", "available"),
                ProbeSupport.value(response, "budget", "available"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new BudgetWorkflowApplicationEvents(BudgetLineReportingServiceGrpc.newStub(channel), FeatureFlagQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CountBudgetLineTotalsResponse>();
        listener.on(new BudgetWorkflowApplication.AdvanceBudgetWorkflowStage0Requested(ProbeSupport.sample(CountBudgetLineTotalsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<BrowseFeatureFlagsResponse>();
        listener.on(new BudgetWorkflowApplication.SignalBudgetWorkflowStage0Requested(ProbeSupport.sample(BrowseFeatureFlagsRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
