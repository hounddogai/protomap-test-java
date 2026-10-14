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
import org.springframework.stereotype.Service;

/**
 * Checks liquidity forecasts against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class LiquidityForecastValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(LiquidityForecastValidationApplication.class);

    private final AudienceQueryServiceGrpc.AudienceQueryServiceBlockingStub audiencesAudienceQueryServiceBlocking;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final FeatureViewQueryServiceGrpc.FeatureViewQueryServiceBlockingStub featurestoreFeatureViewQueryServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final CollateralValidationServiceGrpc.CollateralValidationServiceFutureStub lendingCollateralValidationServiceFuture;

    private final LiquidityForecastValidationRequestMapper mapper;

    private final TreasuryRepository repository;

    private final Executor rpcExecutor;

    private final BankTransferValidationServiceGrpc.BankTransferValidationServiceBlockingStub treasuryBankTransferValidationServiceBlocking;

    private final CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking;

    private final FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking;

    private final LiquidityForecastCalculationServiceGrpc.LiquidityForecastCalculationServiceBlockingStub treasuryLiquidityForecastCalculationServiceBlocking;

    public LiquidityForecastValidationApplication(
            AudienceQueryServiceGrpc.AudienceQueryServiceBlockingStub audiencesAudienceQueryServiceBlocking,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            FeatureViewQueryServiceGrpc.FeatureViewQueryServiceBlockingStub featurestoreFeatureViewQueryServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            CollateralValidationServiceGrpc.CollateralValidationServiceFutureStub lendingCollateralValidationServiceFuture,
            LiquidityForecastValidationRequestMapper mapper,
            TreasuryRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            BankTransferValidationServiceGrpc.BankTransferValidationServiceBlockingStub treasuryBankTransferValidationServiceBlocking,
            CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking,
            FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking,
            LiquidityForecastCalculationServiceGrpc.LiquidityForecastCalculationServiceBlockingStub treasuryLiquidityForecastCalculationServiceBlocking) {
        this.audiencesAudienceQueryServiceBlocking = audiencesAudienceQueryServiceBlocking;
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.featurestoreFeatureViewQueryServiceBlocking = featurestoreFeatureViewQueryServiceBlocking;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.lendingCollateralValidationServiceFuture = lendingCollateralValidationServiceFuture;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.treasuryBankTransferValidationServiceBlocking = treasuryBankTransferValidationServiceBlocking;
        this.treasuryCashPositionValidationServiceBlocking = treasuryCashPositionValidationServiceBlocking;
        this.treasuryFxHedgeCalculationServiceBlocking = treasuryFxHedgeCalculationServiceBlocking;
        this.treasuryLiquidityForecastCalculationServiceBlocking = treasuryLiquidityForecastCalculationServiceBlocking;
    }

    /**
     * Validates a liquidity forecast.
     */
    public ValidateLiquidityForecastResponse validateLiquidityForecast(ValidateLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = mapper.validateLiquidityForecastStage0(request);
        var reply0 = treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFxHedge(outbound0);

        var outbound1 = CompareFxHedgeOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareFxHedgeOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply0.getFxHedge().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setFxHedge(FxHedge.newBuilder()
                        .setCard(FxHedgeCardSettlement.newBuilder()
                                .setLast4(switch (reply0.getFxHedge().getSettlementCase()) {
                                    case CARD -> reply0.getFxHedge().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareFxHedgeOptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateLiquidityForecast", request.getLiquidityForecast().getLiquidityForecastId());
        return ValidateLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCurrencyCode(reply1.getBreakdown().getCurrencyCode())
                                .build())
                        .setDueAt(request.getLiquidityForecast().getDueAt())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Previews a liquidity forecast.
     */
    public PreviewLiquidityForecastResponse previewLiquidityForecast(PreviewLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = LintCollateralsRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getLiquidityForecast().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = lendingCollateralValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).lintCollaterals(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = CompareFxHedgeOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCollateral().getAudit().getCreatedBy())
                                .build())
                        .setBank(FxHedgeBankSettlement.newBuilder()
                                .setIban(switch (request.getLiquidityForecast().getSettlementCase()) {
                                    case BANK -> request.getLiquidityForecast().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(reply0.getCollateral().getOwner().getDisplayName())
                                .build())
                        .addLines(FxHedgeLine.newBuilder()
                                .setDescription(reply0.getCollateral().getComponentsOrDefault("primary", CollateralComponent.getDefaultInstance()).getDescription())
                                .build())
                        .build())
                .build();
        var reply1 = treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareFxHedgeOptions(outbound1);

        var outbound2 = CompareLiquidityForecastOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareLiquidityForecastOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(switch (reply0.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> reply0.getCollateral().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getFxHedge().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(switch (reply0.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> reply0.getCollateral().getAddress().getRecipient();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = treasuryLiquidityForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareLiquidityForecastOptions(outbound2);

        var outbound3 = NotifyRetentionPolicyWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyRetentionPolicyWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getCollateral().getOwner().getEmail())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        RpcPort<NotifyRetentionPolicyWatchersRequest, NotifyRetentionPolicyWatchersResponse> port3 = RpcPort.adapt(auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::notifyRetentionPolicyWatchers);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewLiquidityForecast", request.getLiquidityForecast().getLiquidityForecastId());
        return PreviewLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getCollateral().getAddress().getRegion())
                                .build())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCurrencyCode(reply1.getBreakdown().getCurrencyCode())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .setNotes(request.getLiquidityForecast().getNotes())
                        .build())
                .setOutcome(PreviewLiquidityForecastResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Checks a liquidity forecast.
     */
    public CheckLiquidityForecastEligibilityResponse checkLiquidityForecastEligibility(CheckLiquidityForecastEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckLiquidityForecastEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = SimulateFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getLiquidityForecast().getBillingAddress().getPostalCode())
                                .setRecipient(request.getLiquidityForecast().getBillingAddress().getRecipient())
                                .build())
                        .setCard(FxHedgeCardSettlement.newBuilder()
                                .setHolderName(switch (request.getLiquidityForecast().getSettlementCase()) {
                                    case CARD -> request.getLiquidityForecast().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateFxHedge(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckLiquidityForecastEligibility", request.getLiquidityForecast().getLiquidityForecastId());
        return CheckLiquidityForecastEligibilityResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .setNotes(reply0.getFxHedge().getNotes())
                        .setNumber(request.getLiquidityForecast().getNumber())
                        .build())
                .setOutcome(CheckLiquidityForecastEligibilityResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a liquidity forecast.
     */
    public ExplainLiquidityForecastDecisionResponse explainLiquidityForecastDecision(ExplainLiquidityForecastDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainLiquidityForecastDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = CheckCashPositionEligibilityRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setBank(CashPositionBankFunding.newBuilder()
                                .setAccountHolder(request.getLiquidityForecast().getBank().getAccountHolder())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CheckCashPositionEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCashPositionEligibility(outbound0), rpcExecutor);

        var outbound1 = ExplainFxHedgeCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getLiquidityForecast().getBillingAddress().getLinesList())
                                .setRegion(request.getLiquidityForecast().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainFxHedgeCalculation(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainLiquidityForecastDecision", request.getLiquidityForecast().getLiquidityForecastId());
        return ExplainLiquidityForecastDecisionResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setBillingAddress(reply1.getFxHedge().getBillingAddress())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCurrencyCode(reply0.getCashPosition().getCurrencyCode())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .putAllMetadata(request.getLiquidityForecast().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Scores a liquidity forecast.
     */
    public ScoreLiquidityForecastResponse scoreLiquidityForecast(ScoreLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = ValidateBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLiquidityForecast().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getLiquidityForecast().getBillingAddress().getRecipient())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getLiquidityForecast().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command0 = new DispatchCommand<>("acme.treasury.v1.BankTransferValidationService/ValidateBankTransfer", outbound0);
        var reply0 = treasuryBankTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateBankTransfer(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreLiquidityForecast", request.getLiquidityForecast().getLiquidityForecastId());
        return ScoreLiquidityForecastResponse.newBuilder()
                .setBreakdown(ScoreLiquidityForecastResponse.Breakdown.newBuilder()
                        .setTotal(reply0.getBankTransfer().getTotal())
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setDueAt(reply0.getBankTransfer().getDueAt())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .setNumber(request.getLiquidityForecast().getNumber())
                        .build())
                .build();
    }

    /**
     * Lints a liquidity forecast.
     */
    public LintLiquidityForecastsResponse lintLiquidityForecasts(LintLiquidityForecastsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintLiquidityForecasts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = PreviewCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getLiquidityForecast().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getLiquidityForecast().getAudit().getUpdatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command0 = new DispatchCommand<>("acme.treasury.v1.CashPositionValidationService/PreviewCashPosition", outbound0);
        var reply0 = treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewCashPosition(command0.payload());

        var outbound1 = ResolveAudiencesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> audiencesAudienceQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAudiences(value)).orElseThrow();

        var outbound2 = BrowseFeatureViewsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseFeatureViewsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getCashPosition().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getCashPosition().getLabelsMap())
                        .setOwnerId(reply0.getCashPosition().getCashPositionId())
                        .setRegion(request.getLiquidityForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var adapter2 = new Function<BrowseFeatureViewsRequest, BrowseFeatureViewsResponse>() {
            @Override
            public BrowseFeatureViewsResponse apply(BrowseFeatureViewsRequest input) {
                return featurestoreFeatureViewQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseFeatureViews(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);

        var outbound3 = RpcSupport.unpack(Any.pack(ListJournalEntrySubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListJournalEntrySubscriptionsRequest.Filter.newBuilder()
                        .setRegion(request.getLiquidityForecast().getBillingAddress().getRegion())
                        .build())
                .build()), ListJournalEntrySubscriptionsRequest.class);
        var reply3 = ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listJournalEntrySubscriptions(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintLiquidityForecasts", request.getLiquidityForecast().getLiquidityForecastId());
        return LintLiquidityForecastsResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .setNumber(request.getLiquidityForecast().getNumber())
                        .build())
                .setOutcome(LintLiquidityForecastsResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a liquidity forecast.
     */
    public GetLiquidityForecastRulesResponse getLiquidityForecastRules(GetLiquidityForecastRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLiquidityForecastRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "GetLiquidityForecastRules");

        var outbound0 = CheckPostingBatchEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckPostingBatchEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.ledger.v1.PostingBatchValidationService/CheckPostingBatchEligibility", outbound0);
        var reply0 = ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPostingBatchEligibility(command0.payload());
        return GetLiquidityForecastRulesResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
