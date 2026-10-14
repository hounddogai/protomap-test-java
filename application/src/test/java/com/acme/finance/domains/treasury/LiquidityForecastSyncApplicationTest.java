package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.StreamAuditTrailExportRequest;
import com.acme.audit.v1.StreamAuditTrailExportResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.CountAccessPoliciesRequest;
import com.acme.authorization.v1.CountAccessPoliciesResponse;
import com.acme.authorization.v1.SummarizeAccessPoliciesRequest;
import com.acme.authorization.v1.SummarizeAccessPoliciesResponse;
import com.acme.billing.v1.BatchCreateBillingAccountsRequest;
import com.acme.billing.v1.BatchCreateBillingAccountsResponse;
import com.acme.billing.v1.BillingAccountServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.customers.v1.RecalculateCustomerSegmentsRequest;
import com.acme.customers.v1.RecalculateCustomerSegmentsResponse;
import com.acme.customers.v1.ScoreConsentRecordRequest;
import com.acme.customers.v1.ScoreConsentRecordResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyLifecycleServiceGrpc;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryRequest;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryResponse;
import com.acme.expenses.v1.StreamExpensePolicyChangesRequest;
import com.acme.expenses.v1.StreamExpensePolicyChangesResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.CommentLoanApprovalRequest;
import com.acme.lending.v1.CommentLoanApprovalResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanLimit;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferCardSettlement;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCardFunding;
import com.acme.treasury.v1.CashPositionLifecycleServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CheckCashPositionEligibilityRequest;
import com.acme.treasury.v1.CheckCashPositionEligibilityResponse;
import com.acme.treasury.v1.CompareLiquidityForecastOptionsRequest;
import com.acme.treasury.v1.CompareLiquidityForecastOptionsResponse;
import com.acme.treasury.v1.ExplainBankTransferCalculationRequest;
import com.acme.treasury.v1.ExplainBankTransferCalculationResponse;
import com.acme.treasury.v1.ExplainFxHedgeDecisionRequest;
import com.acme.treasury.v1.ExplainFxHedgeDecisionResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.FxHedgeValidationServiceGrpc;
import com.acme.treasury.v1.GetLiquidityForecastSyncStateRequest;
import com.acme.treasury.v1.GetLiquidityForecastSyncStateResponse;
import com.acme.treasury.v1.LintCashPositionsRequest;
import com.acme.treasury.v1.LintCashPositionsResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastCalculationServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastLine;
import com.acme.treasury.v1.ListFxHedgeConflictsRequest;
import com.acme.treasury.v1.ListFxHedgeConflictsResponse;
import com.acme.treasury.v1.ListLiquidityForecastConflictsRequest;
import com.acme.treasury.v1.ListLiquidityForecastConflictsResponse;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsRequest;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsResponse;
import com.acme.treasury.v1.PullLiquidityForecastsRequest;
import com.acme.treasury.v1.PullLiquidityForecastsResponse;
import com.acme.treasury.v1.PushLiquidityForecastRequest;
import com.acme.treasury.v1.PushLiquidityForecastResponse;
import com.acme.treasury.v1.RecalculateFxHedgesRequest;
import com.acme.treasury.v1.RecalculateFxHedgesResponse;
import com.acme.treasury.v1.ResolveLiquidityForecastConflictRequest;
import com.acme.treasury.v1.ResolveLiquidityForecastConflictResponse;
import com.acme.treasury.v1.StreamLiquidityForecastChangesRequest;
import com.acme.treasury.v1.StreamLiquidityForecastChangesResponse;
import com.acme.treasury.v1.SyncLiquidityForecastsRequest;
import com.acme.treasury.v1.SyncLiquidityForecastsResponse;
import com.acme.treasury.v1.TerminateCashPositionRequest;
import com.acme.treasury.v1.TerminateCashPositionResponse;
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

class LiquidityForecastSyncApplicationTest {
    @Test
    void syncLiquidityForecastsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var listener = new LiquidityForecastSyncApplicationEvents(AccessPolicyQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof LiquidityForecastSyncApplication.PullLiquidityForecastsStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new LiquidityForecastSyncApplication(
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                BillingAccountServiceGrpc.newStub(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                PreferenceCalculationServiceGrpc.newBlockingStub(channel),
                events,
                ExpensePolicyLifecycleServiceGrpc.newBlockingStub(channel),
                ExpensePolicySyncServiceGrpc.newStub(channel),
                LoanApprovalServiceGrpc.newBlockingStub(channel),
                new LiquidityForecastSyncRequestMapperImpl(),
                repository,
                Runnable::run,
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionLifecycleServiceGrpc.newBlockingStub(channel),
                CashPositionValidationServiceGrpc.newBlockingStub(channel),
                FxHedgeCalculationServiceGrpc.newBlockingStub(channel),
                FxHedgeSyncServiceGrpc.newBlockingStub(channel),
                FxHedgeValidationServiceGrpc.newBlockingStub(channel),
                LiquidityForecastCalculationServiceGrpc.newBlockingStub(channel),
                LiquidityForecastImportServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SyncLiquidityForecastsRequest.getDefaultInstance());
        var response = application.syncLiquidityForecasts(request);
        var sent0 = channel.request("acme.lending.v1.LoanApprovalService/CommentLoanApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "lines", "tax", "amount"),
                ProbeSupport.value(sent0, "loan", "limits", "amount"));
        var sent1 = channel.request("acme.treasury.v1.LiquidityForecastCalculationService/CompareLiquidityForecastOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApprovalService/CommentLoanApproval"), "loan", "audit", "created_by"),
                ProbeSupport.value(sent1, "liquidity_forecast", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "audit", "etag"),
                ProbeSupport.value(sent1, "liquidity_forecast", "audit", "etag"));
        var sent2 = channel.request("acme.expenses.v1.ExpensePolicyLifecycleService/ScheduleExpensePolicyExpiry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApprovalService/CommentLoanApproval"), "loan", "audit", "updated_at"),
                ProbeSupport.value(sent2, "expense_policy", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.LiquidityForecastCalculationService/CompareLiquidityForecastOptions"), "liquidity_forecast", "audit", "updated_by"),
                ProbeSupport.value(sent2, "expense_policy", "audit", "updated_by"));
        var sent3 = channel.request("acme.treasury.v1.CashPositionLifecycleService/TerminateCashPosition");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "counterparty", "cash_position_id"),
                ProbeSupport.value(sent3, "cash_position_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApprovalService/CommentLoanApproval"), "loan", "contact", "preferred_channel"),
                ProbeSupport.value(sent3, "details", "notify_contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.LiquidityForecastCalculationService/CompareLiquidityForecastOptions"), "liquidity_forecast", "audit", "created_by"),
                ProbeSupport.value(response, "liquidity_forecast", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyLifecycleService/ScheduleExpensePolicyExpiry"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyLifecycleService/ScheduleExpensePolicyExpiry"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionLifecycleService/TerminateCashPosition"), "outcome", "reference_id"),
                ProbeSupport.value(response, "outcome", "reference_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast_id"),
                ProbeSupport.value(response, "liquidity_forecast", "liquidity_forecast_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "counterparty"),
                ProbeSupport.value(response, "liquidity_forecast", "counterparty"));
    }

    @Test
    void streamLiquidityForecastChangesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var listener = new LiquidityForecastSyncApplicationEvents(AccessPolicyQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof LiquidityForecastSyncApplication.PullLiquidityForecastsStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new LiquidityForecastSyncApplication(
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                BillingAccountServiceGrpc.newStub(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                PreferenceCalculationServiceGrpc.newBlockingStub(channel),
                events,
                ExpensePolicyLifecycleServiceGrpc.newBlockingStub(channel),
                ExpensePolicySyncServiceGrpc.newStub(channel),
                LoanApprovalServiceGrpc.newBlockingStub(channel),
                new LiquidityForecastSyncRequestMapperImpl(),
                repository,
                Runnable::run,
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionLifecycleServiceGrpc.newBlockingStub(channel),
                CashPositionValidationServiceGrpc.newBlockingStub(channel),
                FxHedgeCalculationServiceGrpc.newBlockingStub(channel),
                FxHedgeSyncServiceGrpc.newBlockingStub(channel),
                FxHedgeValidationServiceGrpc.newBlockingStub(channel),
                LiquidityForecastCalculationServiceGrpc.newBlockingStub(channel),
                LiquidityForecastImportServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StreamLiquidityForecastChangesRequest.getDefaultInstance());
        var response = application.streamLiquidityForecastChanges(request);
        var sent0 = channel.request("acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "billing_address", "recipient"),
                ProbeSupport.value(sent0, "details", "destination", "recipient"));
        var sent1 = channel.request("acme.audit.v1.AuditTrailExportService/StreamAuditTrailExport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord"), "breakdown", "currency_code"),
                ProbeSupport.value(response, "liquidity_forecast", "counterparty", "currency_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "liquidity_forecast_id"),
                ProbeSupport.value(response, "liquidity_forecast", "liquidity_forecast_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "issued_at"),
                ProbeSupport.value(response, "liquidity_forecast", "issued_at"));
        var endpoint = new LiquidityForecastSyncEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamLiquidityForecastChangesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamLiquidityForecastChangesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.streamLiquidityForecastChanges(observer);
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
        var listener = new LiquidityForecastSyncApplicationEvents(AccessPolicyQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CountAccessPoliciesResponse>();
        listener.on(new LiquidityForecastSyncApplication.PullLiquidityForecastsStage2Requested(ProbeSupport.sample(CountAccessPoliciesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
