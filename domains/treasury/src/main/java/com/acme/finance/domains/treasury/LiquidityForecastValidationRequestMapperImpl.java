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
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class LiquidityForecastValidationRequestMapperImpl implements LiquidityForecastValidationRequestMapper {
    @Override
    public EstimateFxHedgeRequest validateLiquidityForecastStage0(ValidateLiquidityForecastRequest request) {
        return EstimateFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateFxHedgeRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getLiquidityForecast().getBillingAddress().getPostalCode())
                                .setRegion(request.getLiquidityForecast().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setFxHedge(FxHedge.newBuilder()
                        .setCard(FxHedgeCardSettlement.newBuilder()
                                .setToken(request.getLiquidityForecast().getCard().getToken())
                                .build())
                        .build())
                .build();
    }
}
