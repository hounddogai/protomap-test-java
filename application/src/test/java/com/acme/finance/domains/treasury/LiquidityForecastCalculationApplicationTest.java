package com.acme.finance.domains.treasury;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.NormalizeDisposalRequest;
import com.acme.assets.v1.NormalizeDisposalResponse;
import com.acme.assets.v1.ScoreDisposalRequest;
import com.acme.assets.v1.ScoreDisposalResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.fuel.v1.FuelCardQueryServiceGrpc;
import com.acme.fuel.v1.ResolveFuelCardsRequest;
import com.acme.fuel.v1.ResolveFuelCardsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.metrics.v1.ExplainMetricSeriesDecisionRequest;
import com.acme.metrics.v1.ExplainMetricSeriesDecisionResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferAttachment;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferValidationServiceGrpc;
import com.acme.treasury.v1.CalculateLiquidityForecastRequest;
import com.acme.treasury.v1.CalculateLiquidityForecastResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionBankFunding;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionCardFunding;
import com.acme.treasury.v1.CashPositionLimit;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CompareBankTransferOptionsRequest;
import com.acme.treasury.v1.CompareBankTransferOptionsResponse;
import com.acme.treasury.v1.CompareLiquidityForecastOptionsRequest;
import com.acme.treasury.v1.CompareLiquidityForecastOptionsResponse;
import com.acme.treasury.v1.EstimateFxHedgeRequest;
import com.acme.treasury.v1.EstimateFxHedgeResponse;
import com.acme.treasury.v1.EstimateLiquidityForecastRequest;
import com.acme.treasury.v1.EstimateLiquidityForecastResponse;
import com.acme.treasury.v1.ExplainCashPositionCalculationRequest;
import com.acme.treasury.v1.ExplainCashPositionCalculationResponse;
import com.acme.treasury.v1.ExplainCashPositionDecisionRequest;
import com.acme.treasury.v1.ExplainCashPositionDecisionResponse;
import com.acme.treasury.v1.ExplainLiquidityForecastCalculationRequest;
import com.acme.treasury.v1.ExplainLiquidityForecastCalculationResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeCardSettlement;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.GetCashPositionRatesRequest;
import com.acme.treasury.v1.GetCashPositionRatesResponse;
import com.acme.treasury.v1.GetLiquidityForecastRatesRequest;
import com.acme.treasury.v1.GetLiquidityForecastRatesResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.QuoteLiquidityForecastRequest;
import com.acme.treasury.v1.QuoteLiquidityForecastResponse;
import com.acme.treasury.v1.RecalculateLiquidityForecastsRequest;
import com.acme.treasury.v1.RecalculateLiquidityForecastsResponse;
import com.acme.treasury.v1.ScoreCashPositionRequest;
import com.acme.treasury.v1.ScoreCashPositionResponse;
import com.acme.treasury.v1.SimulateBankTransferRequest;
import com.acme.treasury.v1.SimulateBankTransferResponse;
import com.acme.treasury.v1.SimulateLiquidityForecastRequest;
import com.acme.treasury.v1.SimulateLiquidityForecastResponse;
import com.acme.treasury.v1.ValidateBankTransferRequest;
import com.acme.treasury.v1.ValidateBankTransferResponse;
import com.acme.treasury.v1.ValidateCashPositionRequest;
import com.acme.treasury.v1.ValidateCashPositionResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class LiquidityForecastCalculationApplicationTest {
    @Test
    void simulateLiquidityForecastPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new LiquidityForecastCalculationApplication(
                DisposalValidationServiceGrpc.newBlockingStub(channel),
                FixedAssetServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                FuelCardQueryServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                repository,
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                BankTransferValidationServiceGrpc.newBlockingStub(channel),
                CashPositionCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionValidationServiceGrpc.newBlockingStub(channel),
                CashPositionValidationServiceGrpc.newFutureStub(channel),
                FxHedgeCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SimulateLiquidityForecastRequest.getDefaultInstance());
        var response = application.simulateLiquidityForecast(request);
        var sent0 = channel.request("acme.treasury.v1.CashPositionValidationService/ScoreCashPosition");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "billing_address", "country_code"),
                ProbeSupport.value(sent0, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "card", "last4"),
                ProbeSupport.value(sent0, "cash_position", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "audit", "created_by"),
                ProbeSupport.value(sent0, "cash_position", "audit", "created_by"));
        var sent1 = channel.request("acme.treasury.v1.CashPositionValidationService/ValidateCashPosition");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "audit", "created_by"),
                ProbeSupport.value(sent1, "cash_position", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionValidationService/ScoreCashPosition"), "cash_position", "holder", "fx_hedge_id"),
                ProbeSupport.value(sent1, "cash_position", "holder", "fx_hedge_id"));
        var sent2 = channel.request("acme.treasury.v1.CashPositionCalculationService/GetCashPositionRates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "counterparty", "cash_position_id"),
                ProbeSupport.value(sent2, "cash_position_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionValidationService/ScoreCashPosition"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionValidationService/ValidateCashPosition"), "cash_position", "audit", "etag"),
                ProbeSupport.value(response, "liquidity_forecast", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/GetCashPositionRates"), "cash_position", "audit", "created_by"),
                ProbeSupport.value(response, "liquidity_forecast", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast_id"),
                ProbeSupport.value(response, "liquidity_forecast", "liquidity_forecast_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "liquidity_forecast", "issued_at"),
                ProbeSupport.value(response, "liquidity_forecast", "issued_at"));
    }
}
