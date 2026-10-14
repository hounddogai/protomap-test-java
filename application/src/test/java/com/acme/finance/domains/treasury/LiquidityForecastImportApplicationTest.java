package com.acme.finance.domains.treasury;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.BrowseCustomersRequest;
import com.acme.customers.v1.BrowseCustomersResponse;
import com.acme.customers.v1.CompareHouseholdOptionsRequest;
import com.acme.customers.v1.CompareHouseholdOptionsResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ExplainHouseholdDecisionRequest;
import com.acme.customers.v1.ExplainHouseholdDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.BrowseUsersRequest;
import com.acme.identity.v1.BrowseUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.learning.v1.Course;
import com.acme.learning.v1.CreateCourseRequest;
import com.acme.learning.v1.CreateCourseResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.ShareFiscalPeriodExportRequest;
import com.acme.ledger.v1.ShareFiscalPeriodExportResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.ScoreMetricSeriesRequest;
import com.acme.metrics.v1.ScoreMetricSeriesResponse;
import com.acme.payroll.v1.ScheduleTaxWithholdingExpiryRequest;
import com.acme.payroll.v1.ScheduleTaxWithholdingExpiryResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingLifecycleServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferAttachment;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferQueryServiceGrpc;
import com.acme.treasury.v1.CancelLiquidityForecastImportRequest;
import com.acme.treasury.v1.CancelLiquidityForecastImportResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.acme.treasury.v1.CompareBankTransferOptionsRequest;
import com.acme.treasury.v1.CompareBankTransferOptionsResponse;
import com.acme.treasury.v1.CompareCashPositionOptionsRequest;
import com.acme.treasury.v1.CompareCashPositionOptionsResponse;
import com.acme.treasury.v1.CountBankTransfersRequest;
import com.acme.treasury.v1.CountBankTransfersResponse;
import com.acme.treasury.v1.CreateCashPositionRequest;
import com.acme.treasury.v1.CreateCashPositionResponse;
import com.acme.treasury.v1.ExplainCashPositionCalculationRequest;
import com.acme.treasury.v1.ExplainCashPositionCalculationResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.GetLiquidityForecastImportStatusRequest;
import com.acme.treasury.v1.GetLiquidityForecastImportStatusResponse;
import com.acme.treasury.v1.ImportLiquidityForecastsRequest;
import com.acme.treasury.v1.ImportLiquidityForecastsResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.ListLiquidityForecastImportsRequest;
import com.acme.treasury.v1.ListLiquidityForecastImportsResponse;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsRequest;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsResponse;
import com.acme.treasury.v1.RecalculateBankTransfersRequest;
import com.acme.treasury.v1.RecalculateBankTransfersResponse;
import com.acme.treasury.v1.RetryLiquidityForecastImportRequest;
import com.acme.treasury.v1.RetryLiquidityForecastImportResponse;
import com.acme.treasury.v1.ValidateLiquidityForecastImportRequest;
import com.acme.treasury.v1.ValidateLiquidityForecastImportResponse;
import com.acme.venues.v1.CountVenuesRequest;
import com.acme.venues.v1.CountVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class LiquidityForecastImportApplicationTest {
    @Test
    void importLiquidityForecastsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var listener = new LiquidityForecastImportApplicationEvents(VenueQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof LiquidityForecastImportApplication.RetryLiquidityForecastImportStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new LiquidityForecastImportApplication(
                ChannelRegistry.fixed(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newFutureStub(channel),
                events,
                UserQueryServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingLifecycleServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                BankTransferQueryServiceGrpc.newBlockingStub(channel),
                CashPositionCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ImportLiquidityForecastsRequest.getDefaultInstance());
        var response = application.importLiquidityForecasts(request);
        var sent0 = channel.request("acme.treasury.v1.CashPositionCalculationService/CompareCashPositionOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "billing_address", "recipient"),
                ProbeSupport.value(sent0, "details", "destination", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "header", "submitted_by", "phone"),
                ProbeSupport.value(sent0, "cash_position", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "updated_by"),
                ProbeSupport.value(sent0, "cash_position", "audit", "updated_by"));
        var sent1 = channel.request("acme.customers.v1.HouseholdCalculationService/CompareHouseholdOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/CompareCashPositionOptions"), "cash_position", "audit", "updated_by"),
                ProbeSupport.value(sent1, "household", "children", "children", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/CompareCashPositionOptions"), "cash_position", "audit", "etag"),
                ProbeSupport.value(sent1, "household", "children", "children", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "billing_address", "recipient"),
                ProbeSupport.value(sent1, "household", "children", "children", "location", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "created_at"),
                ProbeSupport.value(sent1, "household", "children", "children", "audit", "created_at"));
        var sent2 = channel.request("acme.treasury.v1.BankTransferCalculationService/RecalculateBankTransfers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdCalculationService/CompareHouseholdOptions"), "household", "location", "country_code"),
                ProbeSupport.value(sent2, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/CompareCashPositionOptions"), "cash_position", "audit", "updated_by"),
                ProbeSupport.value(sent2, "bank_transfer", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/CompareCashPositionOptions"), "cash_position", "contact", "email"),
                ProbeSupport.value(sent2, "bank_transfer", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/CompareCashPositionOptions"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/CompareCashPositionOptions"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdCalculationService/CompareHouseholdOptions"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdCalculationService/CompareHouseholdOptions"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/RecalculateBankTransfers"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        var endpoint = new LiquidityForecastImportEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ImportLiquidityForecastsResponse> observer = new StreamObserver<>() {
            public void onNext(ImportLiquidityForecastsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.importLiquidityForecasts(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void cancelLiquidityForecastImportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var listener = new LiquidityForecastImportApplicationEvents(VenueQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof LiquidityForecastImportApplication.RetryLiquidityForecastImportStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new LiquidityForecastImportApplication(
                ChannelRegistry.fixed(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newFutureStub(channel),
                events,
                UserQueryServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingLifecycleServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                BankTransferQueryServiceGrpc.newBlockingStub(channel),
                CashPositionCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CancelLiquidityForecastImportRequest.getDefaultInstance());
        var response = application.cancelLiquidityForecastImport(request);
        var sent0 = channel.request("acme.payroll.v1.TaxWithholdingLifecycleService/ScheduleTaxWithholdingExpiry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "user_id"),
                ProbeSupport.value(sent0, "tax_withholding", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "phone"),
                ProbeSupport.value(sent0, "details", "notify", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent0, "details", "notify", "email"));
        var sent1 = channel.request("acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent1, "household", "children", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingLifecycleService/ScheduleTaxWithholdingExpiry"), "tax_withholding", "owner", "display_name"),
                ProbeSupport.value(sent1, "household", "children", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingLifecycleService/ScheduleTaxWithholdingExpiry"), "tax_withholding", "owner", "user_id"),
                ProbeSupport.value(sent1, "household", "children", "manager", "user_id"));
        var sent2 = channel.request("acme.treasury.v1.BankTransferQueryService/CountBankTransfers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision"), "household", "location", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingLifecycleService/ScheduleTaxWithholdingExpiry"), "tax_withholding", "audit", "updated_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision"), "household", "parent_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingLifecycleService/ScheduleTaxWithholdingExpiry"), "tax_withholding", "parameters"),
                ProbeSupport.value(sent2, "filter", "labels"));
        var sent3 = channel.request("acme.learning.v1.CourseService/CreateCourse");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision"), "household", "audit", "updated_by"),
                ProbeSupport.value(sent3, "course", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision"), "household", "location", "recipient"),
                ProbeSupport.value(sent3, "course", "address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingLifecycleService/ScheduleTaxWithholdingExpiry"), "tax_withholding", "audit", "etag"),
                ProbeSupport.value(response, "liquidity_forecast", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision"), "household", "audit", "updated_by"),
                ProbeSupport.value(response, "liquidity_forecast", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferQueryService/CountBankTransfers"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.learning.v1.CourseService/CreateCourse"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast_id"),
                ProbeSupport.value(response, "liquidity_forecast", "liquidity_forecast_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new LiquidityForecastImportApplicationEvents(VenueQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CountVenuesResponse>();
        listener.on(new LiquidityForecastImportApplication.RetryLiquidityForecastImportStage0Requested(ProbeSupport.sample(CountVenuesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
