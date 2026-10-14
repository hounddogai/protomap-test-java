package com.acme.finance.domains.budgeting;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.ListAuditEventSubscriptionsRequest;
import com.acme.audit.v1.ListAuditEventSubscriptionsResponse;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.PreviewAccessPolicyRequest;
import com.acme.authorization.v1.PreviewAccessPolicyResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.budgeting.v1.CancelVarianceImportRequest;
import com.acme.budgeting.v1.CancelVarianceImportResponse;
import com.acme.budgeting.v1.EstimateForecastRequest;
import com.acme.budgeting.v1.EstimateForecastResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.GetVarianceImportStatusRequest;
import com.acme.budgeting.v1.GetVarianceImportStatusResponse;
import com.acme.budgeting.v1.ImportVariancesRequest;
import com.acme.budgeting.v1.ImportVariancesResponse;
import com.acme.budgeting.v1.ListVarianceImportsRequest;
import com.acme.budgeting.v1.ListVarianceImportsResponse;
import com.acme.budgeting.v1.MapVarianceImportColumnsRequest;
import com.acme.budgeting.v1.MapVarianceImportColumnsResponse;
import com.acme.budgeting.v1.RecalculateForecastsRequest;
import com.acme.budgeting.v1.RecalculateForecastsResponse;
import com.acme.budgeting.v1.ResolveBudgetLinesRequest;
import com.acme.budgeting.v1.ResolveBudgetLinesResponse;
import com.acme.budgeting.v1.RetryVarianceImportRequest;
import com.acme.budgeting.v1.RetryVarianceImportResponse;
import com.acme.budgeting.v1.ValidateVarianceImportRequest;
import com.acme.budgeting.v1.ValidateVarianceImportResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.search.v1.ExplainSearchIndexDecisionRequest;
import com.acme.search.v1.ExplainSearchIndexDecisionResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.suppliers.v1.BrowseSuppliersRequest;
import com.acme.suppliers.v1.BrowseSuppliersResponse;
import com.acme.suppliers.v1.ExportSuppliersRequest;
import com.acme.suppliers.v1.ExportSuppliersResponse;
import com.acme.suppliers.v1.FindSupplierContractsRequest;
import com.acme.suppliers.v1.FindSupplierContractsResponse;
import com.acme.suppliers.v1.ResolveSuppliersRequest;
import com.acme.suppliers.v1.ResolveSuppliersResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class VarianceImportApplicationTest {
    @Test
    void importVariancesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var listener = new VarianceImportApplicationEvents(BudgetLineQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof VarianceImportApplication.MapVarianceImportColumnsStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new VarianceImportApplication(
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                ForecastCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                repository,
                SearchIndexValidationServiceGrpc.newBlockingStub(channel),
                SupplierContractQueryServiceGrpc.newBlockingStub(channel),
                SupplierQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ImportVariancesRequest.getDefaultInstance());
        var response = application.importVariances(request);
        var sent0 = channel.request("acme.budgeting.v1.ForecastCalculationService/EstimateForecast");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "comments", "created_at"),
                ProbeSupport.value(sent0, "forecast", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "updated_by"),
                ProbeSupport.value(sent0, "forecast", "audit", "updated_by"));
        var sent1 = channel.request("acme.authorization.v1.AccessPolicyValidationService/PreviewAccessPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "requester", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "header", "submitted_by", "preferred_channel"),
                ProbeSupport.value(sent1, "details", "contact", "preferred_channel"));
        var sent2 = channel.request("acme.suppliers.v1.SupplierQueryService/ResolveSuppliers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        var sent3 = channel.request("acme.audit.v1.AuditEventNotificationService/ListAuditEventSubscriptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastCalculationService/EstimateForecast"), "forecast", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "sla", "due_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyValidationService/PreviewAccessPolicy"), "access_policy", "block", "reason"),
                ProbeSupport.value(response, "rejections", "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyValidationService/PreviewAccessPolicy"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierQueryService/ResolveSuppliers"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierQueryService/ResolveSuppliers"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventNotificationService/ListAuditEventSubscriptions"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        var endpoint = new VarianceImportEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ImportVariancesResponse> observer = new StreamObserver<>() {
            public void onNext(ImportVariancesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.importVariances(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new VarianceImportApplicationEvents(BudgetLineQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ResolveBudgetLinesResponse>();
        listener.on(new VarianceImportApplication.MapVarianceImportColumnsStage0Requested(ProbeSupport.sample(ResolveBudgetLinesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
