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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a liquidity forecast, such as totals, taxes, and estimates. Coordinates validation,
 * durable audit writes, and downstream operations.
 */
@Service
public class LiquidityForecastCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(LiquidityForecastCalculationApplication.class);

    private final DisposalValidationServiceGrpc.DisposalValidationServiceBlockingStub assetsDisposalValidationServiceBlocking;

    private final FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final FuelCardQueryServiceGrpc.FuelCardQueryServiceBlockingStub fuelFuelCardQueryServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final TreasuryRepository repository;

    private final BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking;

    private final BankTransferValidationServiceGrpc.BankTransferValidationServiceBlockingStub treasuryBankTransferValidationServiceBlocking;

    private final CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking;

    private final CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking;

    private final CashPositionValidationServiceGrpc.CashPositionValidationServiceFutureStub treasuryCashPositionValidationServiceFuture;

    private final FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking;

    public LiquidityForecastCalculationApplication(
            DisposalValidationServiceGrpc.DisposalValidationServiceBlockingStub assetsDisposalValidationServiceBlocking,
            FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            FuelCardQueryServiceGrpc.FuelCardQueryServiceBlockingStub fuelFuelCardQueryServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            TreasuryRepository repository,
            BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking,
            BankTransferValidationServiceGrpc.BankTransferValidationServiceBlockingStub treasuryBankTransferValidationServiceBlocking,
            CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking,
            CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking,
            CashPositionValidationServiceGrpc.CashPositionValidationServiceFutureStub treasuryCashPositionValidationServiceFuture,
            FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking) {
        this.assetsDisposalValidationServiceBlocking = assetsDisposalValidationServiceBlocking;
        this.assetsFixedAssetServiceAsync = assetsFixedAssetServiceAsync;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.fuelFuelCardQueryServiceBlocking = fuelFuelCardQueryServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.repository = repository;
        this.treasuryBankTransferCalculationServiceBlocking = treasuryBankTransferCalculationServiceBlocking;
        this.treasuryBankTransferValidationServiceBlocking = treasuryBankTransferValidationServiceBlocking;
        this.treasuryCashPositionCalculationServiceBlocking = treasuryCashPositionCalculationServiceBlocking;
        this.treasuryCashPositionValidationServiceBlocking = treasuryCashPositionValidationServiceBlocking;
        this.treasuryCashPositionValidationServiceFuture = treasuryCashPositionValidationServiceFuture;
        this.treasuryFxHedgeCalculationServiceBlocking = treasuryFxHedgeCalculationServiceBlocking;
    }

    /**
     * Calculates a liquidity forecast.
     */
    public CalculateLiquidityForecastResponse calculateLiquidityForecast(CalculateLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = (ScoreDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreDisposalRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .addAllScenarioIds(request.getDetails().getScenarioIdsList())
                        .build())
                .setDisposal(Disposal.newBuilder()
                        .setCustomer(FixedAssetRef.newBuilder()
                                .setDisplayName(request.getLiquidityForecast().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = assetsDisposalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreDisposal(outbound0);

        var outbound1 = ResolveFuelCardsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> fuelFuelCardQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveFuelCards(value)).orElseThrow();

        var outbound2 = BatchCreateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateFixedAssetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateFixedAssetsResponse.getDefaultInstance());
        var input2 = assetsFixedAssetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateFixedAssets(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLiquidityForecast().getAudit().getCreatedAt())
                                .setCreatedBy(request.getLiquidityForecast().getAudit().getCreatedBy())
                                .setEtag(request.getLiquidityForecast().getAudit().getEtag())
                                .setUpdatedAt(request.getLiquidityForecast().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateLiquidityForecast", request.getLiquidityForecast().getLiquidityForecastId());
        return CalculateLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getDisposal().getAudit().getEtag())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .setStatus(request.getLiquidityForecast().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Estimates a liquidity forecast.
     */
    public EstimateLiquidityForecastResponse estimateLiquidityForecast(EstimateLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = ExplainCashPositionDecisionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getLiquidityForecast().getAudit().getCreatedBy())
                                .build())
                        .setHolder(FxHedgeRef.newBuilder()
                                .setDisplayName(request.getLiquidityForecast().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.treasury.v1.CashPositionValidationService");
        var serviceName0 = "acme.treasury.v1" + "." + "CashPositionValidationService";
        var methodName0 = "Exp" + "lainCashPositionDecision";
        var descriptor0 = MethodDescriptor.<ExplainCashPositionDecisionRequest, ExplainCashPositionDecisionResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ExplainCashPositionDecisionRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ExplainCashPositionDecisionResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateLiquidityForecast", request.getLiquidityForecast().getLiquidityForecastId());
        return EstimateLiquidityForecastResponse.newBuilder()
                .setBreakdown(EstimateLiquidityForecastResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply0.getCashPosition().getCurrencyCode())
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .setTotal(request.getLiquidityForecast().getTotal())
                        .build())
                .build();
    }

    /**
     * Quotes a liquidity forecast.
     */
    public QuoteLiquidityForecastResponse quoteLiquidityForecast(QuoteLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var results0 = new ArrayList<SubscribePostingBatchUpdatesResponse>();
        for (var element : request.getLiquidityForecast().getLinesList()) {
            SubscribePostingBatchUpdatesRequest itemRequest = SubscribePostingBatchUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribePostingBatchUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(element.getAttributesMap())
                        .build())
                .setReason(request.getReason())
                .build();
            results0.add(ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribePostingBatchUpdates(itemRequest));
        }
        var reply0 = results0.isEmpty() ? SubscribePostingBatchUpdatesResponse.getDefaultInstance()
                : results0.getLast();

        var results1 = new ArrayList<ValidateCashPositionResponse>();
        for (var element : request.getLiquidityForecast().getLinesList()) {
            ValidateCashPositionRequest itemRequest = ValidateCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPostingBatch().getAudit().getEtag())
                                .build())
                        .setCard(CashPositionCardFunding.newBuilder()
                                .setNetwork(request.getLiquidityForecast().getCard().getNetwork())
                                .setToken(reply0.getPostingBatch().getCard().getToken())
                                .build())
                        .addLimits(CashPositionLimit.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results1.add(treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateCashPosition(itemRequest));
        }
        var reply1 = results1.isEmpty() ? ValidateCashPositionResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = EstimateFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getLiquidityForecast().getBillingAddress().getLinesList())
                                .build())
                        .setCard(FxHedgeCardSettlement.newBuilder()
                                .setToken(switch (reply0.getPostingBatch().getSettlementCase()) {
                                    case CARD -> reply0.getPostingBatch().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCashPositionId(request.getLiquidityForecast().getCounterparty().getCashPositionId())
                                .build())
                        .build())
                .build();
        var reply2 = treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFxHedge(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteLiquidityForecast", request.getLiquidityForecast().getLiquidityForecastId());
        return QuoteLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getFxHedge().getAudit().getCreatedAt())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .putAllMetadata(request.getLiquidityForecast().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Compares a liquidity forecast.
     */
    public CompareLiquidityForecastOptionsResponse compareLiquidityForecastOptions(CompareLiquidityForecastOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareLiquidityForecastOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = ExplainMetricSeriesDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getLiquidityForecast().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getLiquidityForecast().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainMetricSeriesDecision(value));
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareLiquidityForecastOptions", request.getLiquidityForecast().getLiquidityForecastId());
        return CompareLiquidityForecastOptionsResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .setStatus(request.getLiquidityForecast().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Recalculates a liquidity forecast.
     */
    public RecalculateLiquidityForecastsResponse recalculateLiquidityForecasts(RecalculateLiquidityForecastsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateLiquidityForecasts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var results0 = new ArrayList<SimulateBankTransferResponse>();
        for (var element : request.getLiquidityForecast().getAttachmentsList()) {
            SimulateBankTransferRequest itemRequest = SimulateBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .addAttachments(BankTransferAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setSizeBytes(element.getSizeBytes())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results0.add(treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateBankTransfer(itemRequest));
        }
        var reply0 = results0.isEmpty() ? SimulateBankTransferResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CompareBankTransferOptionsRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getBankTransfer().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareBankTransferOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getBankTransfer().getBillingAddress().getCountryCode())
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.treasury.v1.BankTransferCalculationService");
        var selected1 = BankTransferCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.compareBankTransferOptions(outbound1);

        var results2 = new ArrayList<ValidateBankTransferResponse>();
        for (var element : request.getLiquidityForecast().getAttachmentsList()) {
            ValidateBankTransferRequest itemRequest = ValidateBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .addAttachments(BankTransferAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setFileName(element.getFileName())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results2.add(treasuryBankTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateBankTransfer(itemRequest));
        }
        var reply2 = results2.isEmpty() ? ValidateBankTransferResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateLiquidityForecasts", request.getLiquidityForecast().getLiquidityForecastId());
        return RecalculateLiquidityForecastsResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setBillingAddress(request.getLiquidityForecast().getBillingAddress())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(reply1.getBankTransfer().getCounterparty().getDisplayName())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .build();
    }

    /**
     * Simulates a liquidity forecast.
     */
    public SimulateLiquidityForecastResponse simulateLiquidityForecast(SimulateLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = ScoreCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getLiquidityForecast().getAudit().getCreatedBy())
                                .build())
                        .setCard(CashPositionCardFunding.newBuilder()
                                .setLast4(request.getLiquidityForecast().getCard().getLast4())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreCashPositionRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getLiquidityForecast().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var pending0 = treasuryCashPositionValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).scoreCashPosition(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = RpcSupport.unpack(Any.pack(ValidateCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getLiquidityForecast().getAudit().getCreatedBy())
                                .build())
                        .setHolder(FxHedgeRef.newBuilder()
                                .setFxHedgeId(reply0.getCashPosition().getHolder().getFxHedgeId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()), ValidateCashPositionRequest.class);
        var reply1 = treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateCashPosition(outbound1);

        var outbound2 = GetCashPositionRatesRequest.newBuilder()
                .setCashPositionId(request.getLiquidityForecast().getCounterparty().getCashPositionId())
                .setContext(request.getContext())
                .build();
        RpcPort<GetCashPositionRatesRequest, GetCashPositionRatesResponse> port2 = RpcPort.adapt(treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::getCashPositionRates);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateLiquidityForecast", request.getLiquidityForecast().getLiquidityForecastId());
        return SimulateLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getCashPosition().getAudit().getCreatedBy())
                                .setEtag(reply1.getCashPosition().getAudit().getEtag())
                                .build())
                        .setIssuedAt(request.getLiquidityForecast().getIssuedAt())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a liquidity forecast.
     */
    public ExplainLiquidityForecastCalculationResponse explainLiquidityForecastCalculation(ExplainLiquidityForecastCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainLiquidityForecastCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = NormalizeDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposal(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getLiquidityForecast().getAudit().getEtag())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> assetsDisposalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeDisposal(value));

        var outbound1 = ExplainCashPositionCalculationRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getDisposal().getAudit().getEtag())
                                .setUpdatedAt(request.getLiquidityForecast().getAudit().getUpdatedAt())
                                .build())
                        .setBank(CashPositionBankFunding.newBuilder()
                                .setIban(request.getLiquidityForecast().getBank().getIban())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainCashPositionCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply0.getDisposal().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.treasury.v1.CashPositionCalculationService");
        var selected1 = CashPositionCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.explainCashPositionCalculation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainLiquidityForecastCalculation", request.getLiquidityForecast().getLiquidityForecastId());
        return ExplainLiquidityForecastCalculationResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getCashPosition().getAudit().getUpdatedBy())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .putAllMetadata(request.getLiquidityForecast().getMetadataMap())
                        .build())
                .setOutcome(ExplainLiquidityForecastCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a liquidity forecast.
     */
    public GetLiquidityForecastRatesResponse getLiquidityForecastRates(GetLiquidityForecastRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLiquidityForecastRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "GetLiquidityForecastRates");
        return GetLiquidityForecastRatesResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .build();
    }
}
