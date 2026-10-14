package com.acme.finance.domains.treasury;

import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.StreamBudgetLineMetricsRequest;
import com.acme.budgeting.v1.StreamBudgetLineMetricsResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.RejectCollectionCaseRequest;
import com.acme.collections.v1.RejectCollectionCaseResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.EstimateHouseholdRequest;
import com.acme.customers.v1.EstimateHouseholdResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.WatchDocumentsRequest;
import com.acme.documents.v1.WatchDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionQueryServiceGrpc;
import com.acme.treasury.v1.CountFxHedgeTotalsRequest;
import com.acme.treasury.v1.CountFxHedgeTotalsResponse;
import com.acme.treasury.v1.DeleteFxHedgeReportRequest;
import com.acme.treasury.v1.DeleteFxHedgeReportResponse;
import com.acme.treasury.v1.FetchFxHedgeReportRequest;
import com.acme.treasury.v1.FetchFxHedgeReportResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.GenerateFxHedgeReportRequest;
import com.acme.treasury.v1.GenerateFxHedgeReportResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.ListFxHedgeReportsRequest;
import com.acme.treasury.v1.ListFxHedgeReportsResponse;
import com.acme.treasury.v1.ResolveCashPositionsRequest;
import com.acme.treasury.v1.ResolveCashPositionsResponse;
import com.acme.treasury.v1.StreamFxHedgeMetricsRequest;
import com.acme.treasury.v1.StreamFxHedgeMetricsResponse;
import com.acme.treasury.v1.ValidateLiquidityForecastImportRequest;
import com.acme.treasury.v1.ValidateLiquidityForecastImportResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class FxHedgeReportingApplicationTest {
    @Test
    void streamFxHedgeMetricsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var listener = new FxHedgeReportingApplicationEvents(CashPositionQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof FxHedgeReportingApplication.DeleteFxHedgeReportStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new FxHedgeReportingApplication(
                BudgetLineReportingServiceGrpc.newBlockingStub(channel),
                CollectionCaseApprovalServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                events,
                repository,
                LiquidityForecastImportServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StreamFxHedgeMetricsRequest.getDefaultInstance());
        var response = application.streamFxHedgeMetrics(request);
        var sent0 = channel.request("acme.budgeting.v1.BudgetLineReportingService/StreamBudgetLineMetrics");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "fx_hedge", "billing_address", "region"));
        var endpoint = new FxHedgeReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamFxHedgeMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamFxHedgeMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamFxHedgeMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new FxHedgeReportingApplicationEvents(CashPositionQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ResolveCashPositionsResponse>();
        listener.on(new FxHedgeReportingApplication.DeleteFxHedgeReportStage0Requested(ProbeSupport.sample(ResolveCashPositionsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
