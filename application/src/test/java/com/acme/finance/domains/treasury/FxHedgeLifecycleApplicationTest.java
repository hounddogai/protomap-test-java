package com.acme.finance.domains.treasury;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlApprovalServiceGrpc;
import com.acme.compliance.v1.CountSanctionTotalsRequest;
import com.acme.compliance.v1.CountSanctionTotalsResponse;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.compliance.v1.SubmitControlRequest;
import com.acme.compliance.v1.SubmitControlResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerCalculationRequest;
import com.acme.customers.v1.ExplainCustomerCalculationResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.RecalculateConsentRecordsRequest;
import com.acme.customers.v1.RecalculateConsentRecordsResponse;
import com.acme.customers.v1.RecalculateCustomersRequest;
import com.acme.customers.v1.RecalculateCustomersResponse;
import com.acme.customers.v1.ScoreHouseholdRequest;
import com.acme.customers.v1.ScoreHouseholdResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.CountUsersRequest;
import com.acme.identity.v1.CountUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralWorkflowServiceGrpc;
import com.acme.lending.v1.PauseCollateralWorkflowRequest;
import com.acme.lending.v1.PauseCollateralWorkflowResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.ValidateMetricSeriesRequest;
import com.acme.metrics.v1.ValidateMetricSeriesResponse;
import com.acme.treasury.v1.ActivateFxHedgeRequest;
import com.acme.treasury.v1.ActivateFxHedgeResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionQueryServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CountCashPositionsRequest;
import com.acme.treasury.v1.CountCashPositionsResponse;
import com.acme.treasury.v1.ExpireFxHedgeRequest;
import com.acme.treasury.v1.ExpireFxHedgeResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.GetFxHedgeLifecycleRequest;
import com.acme.treasury.v1.GetFxHedgeLifecycleResponse;
import com.acme.treasury.v1.GetFxHedgeSyncStateRequest;
import com.acme.treasury.v1.GetFxHedgeSyncStateResponse;
import com.acme.treasury.v1.ImportLiquidityForecastsRequest;
import com.acme.treasury.v1.ImportLiquidityForecastsResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastSyncServiceGrpc;
import com.acme.treasury.v1.PushFxHedgeRequest;
import com.acme.treasury.v1.PushFxHedgeResponse;
import com.acme.treasury.v1.ReinstateFxHedgeRequest;
import com.acme.treasury.v1.ReinstateFxHedgeResponse;
import com.acme.treasury.v1.RenewFxHedgeRequest;
import com.acme.treasury.v1.RenewFxHedgeResponse;
import com.acme.treasury.v1.SuspendFxHedgeRequest;
import com.acme.treasury.v1.SuspendFxHedgeResponse;
import com.acme.treasury.v1.SyncLiquidityForecastsRequest;
import com.acme.treasury.v1.SyncLiquidityForecastsResponse;
import com.acme.treasury.v1.TerminateFxHedgeRequest;
import com.acme.treasury.v1.TerminateFxHedgeResponse;
import com.acme.venues.v1.SuggestVenuesRequest;
import com.acme.venues.v1.SuggestVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
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

class FxHedgeLifecycleApplicationTest {
    @Test
    void activateFxHedgePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new FxHedgeLifecycleApplication(
                ChannelRegistry.fixed(channel),
                ControlApprovalServiceGrpc.newBlockingStub(channel),
                SanctionReportingServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                UserQueryServiceGrpc.newBlockingStub(channel),
                CollateralWorkflowServiceGrpc.newBlockingStub(channel),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                CashPositionQueryServiceGrpc.newBlockingStub(channel),
                FxHedgeSyncServiceGrpc.newBlockingStub(channel),
                LiquidityForecastImportServiceGrpc.newStub(channel),
                LiquidityForecastSyncServiceGrpc.newBlockingStub(channel),
                VenueQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ActivateFxHedgeRequest.getDefaultInstance());
        var response = application.activateFxHedge(request);
        var sent0 = channel.request("acme.metrics.v1.MetricSeriesValidationService/ValidateMetricSeries");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "display_name"),
                ProbeSupport.value(sent0, "metric_series", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent0, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        var sent1 = channel.request("acme.treasury.v1.LiquidityForecastSyncService/SyncLiquidityForecasts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "display_name"),
                ProbeSupport.value(sent1, "liquidity_forecast", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesValidationService/ValidateMetricSeries"), "metric_series", "audit", "created_by"),
                ProbeSupport.value(sent1, "liquidity_forecast", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesValidationService/ValidateMetricSeries"), "metric_series", "audit", "created_at"),
                ProbeSupport.value(sent1, "liquidity_forecast", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesValidationService/ValidateMetricSeries"), "metric_series", "audit", "updated_at"),
                ProbeSupport.value(sent1, "liquidity_forecast", "audit", "updated_at"));
        var sent2 = channel.request("acme.customers.v1.CustomerCalculationService/RecalculateCustomers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.LiquidityForecastSyncService/SyncLiquidityForecasts"), "liquidity_forecast", "audit", "created_by"),
                ProbeSupport.value(sent2, "customer", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.LiquidityForecastSyncService/SyncLiquidityForecasts"), "liquidity_forecast", "billing_address", "locality"),
                ProbeSupport.value(sent2, "details", "destination", "locality"));
        var sent3 = channel.request("acme.lending.v1.CollateralWorkflowService/PauseCollateralWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesValidationService/ValidateMetricSeries"), "metric_series", "audit", "created_at"),
                ProbeSupport.value(sent3, "collateral", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesValidationService/ValidateMetricSeries"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/RecalculateCustomers"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/RecalculateCustomers"), "customer", "audit", "etag"),
                ProbeSupport.value(response, "fx_hedge", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralWorkflowService/PauseCollateralWorkflow"), "collateral", "address", "locality"),
                ProbeSupport.value(response, "fx_hedge", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge_id"),
                ProbeSupport.value(response, "fx_hedge", "fx_hedge_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
