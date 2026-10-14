package com.acme.finance.domains.treasury;

import com.acme.audiences.v1.AudienceQueryServiceGrpc;
import com.acme.audiences.v1.ResolveAudiencesRequest;
import com.acme.audiences.v1.ResolveAudiencesResponse;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersRequest;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersResponse;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.featurestore.v1.BrowseFeatureViewsRequest;
import com.acme.featurestore.v1.BrowseFeatureViewsResponse;
import com.acme.featurestore.v1.FeatureViewQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.CheckPostingBatchEligibilityRequest;
import com.acme.ledger.v1.CheckPostingBatchEligibilityResponse;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsRequest;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsResponse;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralComponent;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.LintCollateralsRequest;
import com.acme.lending.v1.LintCollateralsResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferValidationServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionBankFunding;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CheckCashPositionEligibilityRequest;
import com.acme.treasury.v1.CheckCashPositionEligibilityResponse;
import com.acme.treasury.v1.CheckLiquidityForecastEligibilityRequest;
import com.acme.treasury.v1.CheckLiquidityForecastEligibilityResponse;
import com.acme.treasury.v1.CompareFxHedgeOptionsRequest;
import com.acme.treasury.v1.CompareFxHedgeOptionsResponse;
import com.acme.treasury.v1.CompareLiquidityForecastOptionsRequest;
import com.acme.treasury.v1.CompareLiquidityForecastOptionsResponse;
import com.acme.treasury.v1.EstimateFxHedgeRequest;
import com.acme.treasury.v1.EstimateFxHedgeResponse;
import com.acme.treasury.v1.ExplainFxHedgeCalculationRequest;
import com.acme.treasury.v1.ExplainFxHedgeCalculationResponse;
import com.acme.treasury.v1.ExplainLiquidityForecastDecisionRequest;
import com.acme.treasury.v1.ExplainLiquidityForecastDecisionResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeBankSettlement;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeCardSettlement;
import com.acme.treasury.v1.FxHedgeLine;
import com.acme.treasury.v1.GetLiquidityForecastRulesRequest;
import com.acme.treasury.v1.GetLiquidityForecastRulesResponse;
import com.acme.treasury.v1.LintLiquidityForecastsRequest;
import com.acme.treasury.v1.LintLiquidityForecastsResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastCalculationServiceGrpc;
import com.acme.treasury.v1.PreviewCashPositionRequest;
import com.acme.treasury.v1.PreviewCashPositionResponse;
import com.acme.treasury.v1.PreviewLiquidityForecastRequest;
import com.acme.treasury.v1.PreviewLiquidityForecastResponse;
import com.acme.treasury.v1.ScoreLiquidityForecastRequest;
import com.acme.treasury.v1.ScoreLiquidityForecastResponse;
import com.acme.treasury.v1.SimulateFxHedgeRequest;
import com.acme.treasury.v1.SimulateFxHedgeResponse;
import com.acme.treasury.v1.ValidateBankTransferRequest;
import com.acme.treasury.v1.ValidateBankTransferResponse;
import com.acme.treasury.v1.ValidateLiquidityForecastRequest;
import com.acme.treasury.v1.ValidateLiquidityForecastResponse;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class LiquidityForecastValidationApplicationTest {
    @Test
    void previewLiquidityForecastPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new LiquidityForecastValidationApplication(
                AudienceQueryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                FeatureViewQueryServiceGrpc.newBlockingStub(channel),
                JournalEntryNotificationServiceGrpc.newBlockingStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                CollateralValidationServiceGrpc.newFutureStub(channel),
                new LiquidityForecastValidationRequestMapperImpl(),
                repository,
                Runnable::run,
                BankTransferValidationServiceGrpc.newBlockingStub(channel),
                CashPositionValidationServiceGrpc.newBlockingStub(channel),
                FxHedgeCalculationServiceGrpc.newBlockingStub(channel),
                LiquidityForecastCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(PreviewLiquidityForecastRequest.getDefaultInstance());
        var response = application.previewLiquidityForecast(request);
        var sent0 = channel.request("acme.lending.v1.CollateralValidationService/LintCollaterals");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "audit", "updated_at"),
                ProbeSupport.value(sent0, "collateral", "audit", "updated_at"));
        var sent1 = channel.request("acme.treasury.v1.FxHedgeCalculationService/CompareFxHedgeOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/LintCollaterals"), "collateral", "owner", "display_name"),
                ProbeSupport.value(sent1, "fx_hedge", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/LintCollaterals"), "collateral", "audit", "created_by"),
                ProbeSupport.value(sent1, "fx_hedge", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/LintCollaterals"), "collateral", "components", "description"),
                ProbeSupport.value(sent1, "fx_hedge", "lines", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "bank", "iban"),
                ProbeSupport.value(sent1, "fx_hedge", "bank", "iban"));
        var sent2 = channel.request("acme.treasury.v1.LiquidityForecastCalculationService/CompareLiquidityForecastOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/LintCollaterals"), "collateral", "address", "recipient"),
                ProbeSupport.value(sent2, "liquidity_forecast", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeCalculationService/CompareFxHedgeOptions"), "fx_hedge", "audit", "created_at"),
                ProbeSupport.value(sent2, "liquidity_forecast", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/LintCollaterals"), "collateral", "address", "region"),
                ProbeSupport.value(sent2, "details", "destination", "region"));
        var sent3 = channel.request("acme.audit.v1.RetentionPolicyNotificationService/NotifyRetentionPolicyWatchers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "phone"),
                ProbeSupport.value(sent3, "details", "recipient", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/LintCollaterals"), "collateral", "owner", "email"),
                ProbeSupport.value(sent3, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/LintCollaterals"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/LintCollaterals"), "collateral", "address", "region"),
                ProbeSupport.value(response, "liquidity_forecast", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeCalculationService/CompareFxHedgeOptions"), "breakdown", "currency_code"),
                ProbeSupport.value(response, "liquidity_forecast", "counterparty", "currency_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.LiquidityForecastCalculationService/CompareLiquidityForecastOptions"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyNotificationService/NotifyRetentionPolicyWatchers"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyNotificationService/NotifyRetentionPolicyWatchers"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast_id"),
                ProbeSupport.value(response, "liquidity_forecast", "liquidity_forecast_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "notes"),
                ProbeSupport.value(response, "liquidity_forecast", "notes"));
    }
}
