package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ActivateForecastRequest;
import com.acme.budgeting.v1.ActivateForecastResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.ExpireForecastRequest;
import com.acme.budgeting.v1.ExpireForecastResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.GetForecastLifecycleRequest;
import com.acme.budgeting.v1.GetForecastLifecycleResponse;
import com.acme.budgeting.v1.ReconcileForecastsRequest;
import com.acme.budgeting.v1.ReconcileForecastsResponse;
import com.acme.budgeting.v1.ReinstateForecastRequest;
import com.acme.budgeting.v1.ReinstateForecastResponse;
import com.acme.budgeting.v1.RenewForecastRequest;
import com.acme.budgeting.v1.RenewForecastResponse;
import com.acme.budgeting.v1.ScheduleForecastExpiryRequest;
import com.acme.budgeting.v1.ScheduleForecastExpiryResponse;
import com.acme.budgeting.v1.ShareVarianceReportRequest;
import com.acme.budgeting.v1.ShareVarianceReportResponse;
import com.acme.budgeting.v1.SuspendForecastRequest;
import com.acme.budgeting.v1.SuspendForecastResponse;
import com.acme.budgeting.v1.TerminateForecastRequest;
import com.acme.budgeting.v1.TerminateForecastResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceReportingServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.compliance.v1.StreamSanctionMetricsRequest;
import com.acme.compliance.v1.StreamSanctionMetricsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.fulfillment.v1.BrowseFulfillmentsRequest;
import com.acme.fulfillment.v1.BrowseFulfillmentsResponse;
import com.acme.fulfillment.v1.FulfillmentQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.risk.v1.DelegateStressTestApprovalRequest;
import com.acme.risk.v1.DelegateStressTestApprovalResponse;
import com.acme.risk.v1.StressTestApprovalServiceGrpc;
import com.acme.suppliers.v1.BatchGetSuppliersRequest;
import com.acme.suppliers.v1.BatchGetSuppliersResponse;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.CountSuppliersRequest;
import com.acme.suppliers.v1.CountSuppliersResponse;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionResponse;
import com.acme.suppliers.v1.ScoreSupplierRatingRequest;
import com.acme.suppliers.v1.ScoreSupplierRatingResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.UpdateSupplierRequest;
import com.acme.suppliers.v1.UpdateSupplierResponse;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ForecastLifecycleApplicationTest {
    @Test
    void suspendForecastPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var listener = new ForecastLifecycleApplicationEvents(SupplierRatingValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ForecastLifecycleApplication.RenewForecastStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ForecastLifecycleApplication(
                ForecastSyncServiceGrpc.newBlockingStub(channel),
                VarianceReportingServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                SanctionReportingServiceGrpc.newBlockingStub(channel),
                events,
                FulfillmentQueryServiceGrpc.newBlockingStub(channel),
                new ForecastLifecycleRequestMapperImpl(),
                repository,
                StressTestApprovalServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierContractQueryServiceGrpc.newBlockingStub(channel),
                SupplierQueryServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newFutureStub(channel),
                SupplierServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SuspendForecastRequest.getDefaultInstance());
        var response = application.suspendForecast(request);
        var sent0 = channel.request("acme.budgeting.v1.VarianceReportingService/ShareVarianceReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "recipient", "preferred_channel"));
        var sent1 = channel.request("acme.fulfillment.v1.FulfillmentQueryService/BrowseFulfillments");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceReportingService/ShareVarianceReport"), "variance", "sla", "due_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "user_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceReportingService/ShareVarianceReport"), "variance", "custom_fields"),
                ProbeSupport.value(sent1, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceReportingService/ShareVarianceReport"), "variance", "sla", "due_at"),
                ProbeSupport.value(response, "forecast", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.fulfillment.v1.FulfillmentQueryService/BrowseFulfillments"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast_id"),
                ProbeSupport.value(response, "forecast", "forecast_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ForecastLifecycleApplicationEvents(SupplierRatingValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ValidateSupplierRatingResponse>();
        listener.on(new ForecastLifecycleApplication.RenewForecastStage0Requested(ProbeSupport.sample(ValidateSupplierRatingRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
