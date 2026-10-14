package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ApproveBudgetLineRequest;
import com.acme.budgeting.v1.ApproveBudgetLineResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.CheckVarianceEligibilityRequest;
import com.acme.budgeting.v1.CheckVarianceEligibilityResponse;
import com.acme.budgeting.v1.CommentBudgetLineApprovalRequest;
import com.acme.budgeting.v1.CommentBudgetLineApprovalResponse;
import com.acme.budgeting.v1.CountVarianceTotalsRequest;
import com.acme.budgeting.v1.CountVarianceTotalsResponse;
import com.acme.budgeting.v1.DelegateBudgetLineApprovalRequest;
import com.acme.budgeting.v1.DelegateBudgetLineApprovalResponse;
import com.acme.budgeting.v1.DeleteBudgetLineReportRequest;
import com.acme.budgeting.v1.DeleteBudgetLineReportResponse;
import com.acme.budgeting.v1.EscalateBudgetLineRequest;
import com.acme.budgeting.v1.EscalateBudgetLineResponse;
import com.acme.budgeting.v1.FetchBudgetLineReportRequest;
import com.acme.budgeting.v1.FetchBudgetLineReportResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastBankSettlement;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastCardSettlement;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.GetBudgetLineApprovalStatusRequest;
import com.acme.budgeting.v1.GetBudgetLineApprovalStatusResponse;
import com.acme.budgeting.v1.ListPendingBudgetLinesRequest;
import com.acme.budgeting.v1.ListPendingBudgetLinesResponse;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.budgeting.v1.RecallBudgetLineRequest;
import com.acme.budgeting.v1.RecallBudgetLineResponse;
import com.acme.budgeting.v1.RecallBudgetRequest;
import com.acme.budgeting.v1.RecallBudgetResponse;
import com.acme.budgeting.v1.RejectBudgetLineRequest;
import com.acme.budgeting.v1.RejectBudgetLineResponse;
import com.acme.budgeting.v1.ShareForecastReportRequest;
import com.acme.budgeting.v1.ShareForecastReportResponse;
import com.acme.budgeting.v1.SubmitBudgetLineRequest;
import com.acme.budgeting.v1.SubmitBudgetLineResponse;
import com.acme.budgeting.v1.SyncForecastsRequest;
import com.acme.budgeting.v1.SyncForecastsResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceReportingServiceGrpc;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.cart.v1.Cart;
import com.acme.cart.v1.CartCalculationServiceGrpc;
import com.acme.cart.v1.EstimateCartRequest;
import com.acme.cart.v1.EstimateCartResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerCalculationRequest;
import com.acme.customers.v1.ExplainCustomerCalculationResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersRequest;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersResponse;
import com.acme.merchandising.v1.CollectionQueryServiceGrpc;
import com.acme.merchandising.v1.SuggestCollectionsRequest;
import com.acme.merchandising.v1.SuggestCollectionsResponse;
import com.acme.suppliers.v1.CountSuppliersRequest;
import com.acme.suppliers.v1.CountSuppliersResponse;
import com.acme.suppliers.v1.ExportSuppliersRequest;
import com.acme.suppliers.v1.ExportSuppliersResponse;
import com.acme.suppliers.v1.SummarizeSuppliersRequest;
import com.acme.suppliers.v1.SummarizeSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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

class BudgetLineApprovalApplicationTest {
    @Test
    void recallBudgetLinePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var listener = new BudgetLineApprovalApplicationEvents(BudgetLineReportingServiceGrpc.newStub(channel), CartCalculationServiceGrpc.newStub(channel), CollectionQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof BudgetLineApprovalApplication.ListPendingBudgetLinesStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof BudgetLineApprovalApplication.RecallBudgetLineStage2Requested command) {
                listener.on(command);
            }
            else if (event instanceof BudgetLineApprovalApplication.CommentBudgetLineApprovalStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new BudgetLineApprovalApplication(
                BudgetLineReportingServiceGrpc.newBlockingStub(channel),
                ForecastCalculationServiceGrpc.newBlockingStub(channel),
                ForecastReportingServiceGrpc.newBlockingStub(channel),
                ForecastSyncServiceGrpc.newBlockingStub(channel),
                VarianceReportingServiceGrpc.newBlockingStub(channel),
                VarianceValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                events,
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RecallBudgetLineRequest.getDefaultInstance());
        var response = application.recallBudgetLine(request);
        var sent0 = channel.request("acme.budgeting.v1.ForecastReportingService/ShareForecastReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "counterparty", "email"),
                ProbeSupport.value(sent0, "details", "recipient", "email"));
        var sent1 = channel.request("acme.budgeting.v1.BudgetLineReportingService/DeleteBudgetLineReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "budget_line_id"),
                ProbeSupport.value(sent1, "budget_line_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastReportingService/ShareForecastReport"), "forecast", "audit", "etag"),
                ProbeSupport.value(sent1, "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent1, "reason"));
        var sent2 = channel.request("acme.budgeting.v1.BudgetLineReportingService/FetchBudgetLineReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "budget_line_id"),
                ProbeSupport.value(sent2, "budget_line_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastReportingService/ShareForecastReport"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastReportingService/ShareForecastReport"), "forecast", "audit", "created_by"),
                ProbeSupport.value(response, "budget_line", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineReportingService/DeleteBudgetLineReport"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineReportingService/FetchBudgetLineReport"), "budget_line", "audit", "updated_by"),
                ProbeSupport.value(response, "budget_line", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line_id"),
                ProbeSupport.value(response, "budget_line", "budget_line_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_line", "number"),
                ProbeSupport.value(response, "budget_line", "number"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new BudgetLineApprovalApplicationEvents(BudgetLineReportingServiceGrpc.newStub(channel), CartCalculationServiceGrpc.newStub(channel), CollectionQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<SuggestCollectionsResponse>();
        listener.on(new BudgetLineApprovalApplication.ListPendingBudgetLinesStage1Requested(ProbeSupport.sample(SuggestCollectionsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<FetchBudgetLineReportResponse>();
        listener.on(new BudgetLineApprovalApplication.RecallBudgetLineStage2Requested(ProbeSupport.sample(FetchBudgetLineReportRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        var completion2 = new CompletableFuture<EstimateCartResponse>();
        listener.on(new BudgetLineApprovalApplication.CommentBudgetLineApprovalStage1Requested(ProbeSupport.sample(EstimateCartRequest.getDefaultInstance()), completion2, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion2.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion2.getNow(null));
    }
}
