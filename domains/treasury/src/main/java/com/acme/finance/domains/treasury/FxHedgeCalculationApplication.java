package com.acme.finance.domains.treasury;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.RecordFiscalPeriodVersionRequest;
import com.acme.ledger.v1.RecordFiscalPeriodVersionResponse;
import com.acme.procurement.v1.ExportPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ExportPurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionQueryServiceGrpc;
import com.acme.recommendations.v1.CloneRecommendationSetRequest;
import com.acme.recommendations.v1.CloneRecommendationSetResponse;
import com.acme.recommendations.v1.RecommendationSet;
import com.acme.recommendations.v1.RecommendationSetCardSettlement;
import com.acme.recommendations.v1.RecommendationSetServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferBankSettlement;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferCardSettlement;
import com.acme.treasury.v1.CalculateBankTransferRequest;
import com.acme.treasury.v1.CalculateBankTransferResponse;
import com.acme.treasury.v1.CalculateFxHedgeRequest;
import com.acme.treasury.v1.CalculateFxHedgeResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionCardFunding;
import com.acme.treasury.v1.CashPositionLimit;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CompareFxHedgeOptionsRequest;
import com.acme.treasury.v1.CompareFxHedgeOptionsResponse;
import com.acme.treasury.v1.EstimateFxHedgeRequest;
import com.acme.treasury.v1.EstimateFxHedgeResponse;
import com.acme.treasury.v1.ExplainBankTransferCalculationRequest;
import com.acme.treasury.v1.ExplainBankTransferCalculationResponse;
import com.acme.treasury.v1.ExplainCashPositionDecisionRequest;
import com.acme.treasury.v1.ExplainCashPositionDecisionResponse;
import com.acme.treasury.v1.ExplainFxHedgeCalculationRequest;
import com.acme.treasury.v1.ExplainFxHedgeCalculationResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.GetFxHedgeRatesRequest;
import com.acme.treasury.v1.GetFxHedgeRatesResponse;
import com.acme.treasury.v1.QuoteCashPositionRequest;
import com.acme.treasury.v1.QuoteCashPositionResponse;
import com.acme.treasury.v1.RecalculateBankTransfersRequest;
import com.acme.treasury.v1.RecalculateBankTransfersResponse;
import com.acme.treasury.v1.RecalculateFxHedgesRequest;
import com.acme.treasury.v1.RecalculateFxHedgesResponse;
import com.acme.treasury.v1.ScoreCashPositionRequest;
import com.acme.treasury.v1.ScoreCashPositionResponse;
import com.acme.treasury.v1.SimulateFxHedgeRequest;
import com.acme.treasury.v1.SimulateFxHedgeResponse;
import com.acme.venues.v1.SummarizeVenuesRequest;
import com.acme.venues.v1.SummarizeVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a fx hedge, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class FxHedgeCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(FxHedgeCalculationApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync;

    private final FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking;

    private final PurchaseRequisitionQueryServiceGrpc.PurchaseRequisitionQueryServiceBlockingStub procurementPurchaseRequisitionQueryServiceBlocking;

    private final RecommendationSetServiceGrpc.RecommendationSetServiceBlockingStub recommendationsRecommendationSetServiceBlocking;

    private final TreasuryRepository repository;

    private final Executor rpcExecutor;

    private final BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking;

    private final CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking;

    private final CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public FxHedgeCalculationApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BudgetServiceGrpc.BudgetServiceStub budgetingBudgetServiceAsync,
            FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking,
            PurchaseRequisitionQueryServiceGrpc.PurchaseRequisitionQueryServiceBlockingStub procurementPurchaseRequisitionQueryServiceBlocking,
            RecommendationSetServiceGrpc.RecommendationSetServiceBlockingStub recommendationsRecommendationSetServiceBlocking,
            TreasuryRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking,
            CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking,
            CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.budgetingBudgetServiceAsync = budgetingBudgetServiceAsync;
        this.ledgerFiscalPeriodHistoryServiceBlocking = ledgerFiscalPeriodHistoryServiceBlocking;
        this.procurementPurchaseRequisitionQueryServiceBlocking = procurementPurchaseRequisitionQueryServiceBlocking;
        this.recommendationsRecommendationSetServiceBlocking = recommendationsRecommendationSetServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.treasuryBankTransferCalculationServiceBlocking = treasuryBankTransferCalculationServiceBlocking;
        this.treasuryCashPositionCalculationServiceBlocking = treasuryCashPositionCalculationServiceBlocking;
        this.treasuryCashPositionValidationServiceBlocking = treasuryCashPositionValidationServiceBlocking;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    /**
     * Calculates a fx hedge.
     */
    public CalculateFxHedgeResponse calculateFxHedge(CalculateFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = RecordFiscalPeriodVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordFiscalPeriodVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.ledger.v1.FiscalPeriodHistoryService/RecordFiscalPeriodVersion", outbound0);
        var reply0 = ledgerFiscalPeriodHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordFiscalPeriodVersion(command0.payload());

        var outbound1 = CalculateBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBank(BankTransferBankSettlement.newBuilder()
                                .setBic(switch (request.getFxHedge().getSettlementCase()) {
                                    case BANK -> request.getFxHedge().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateBankTransfer(outbound1);

        var outbound2 = ExplainBankTransferCalculationRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getFiscalPeriod().getAudit().getCreatedAt())
                                .setUpdatedBy(reply0.getFiscalPeriod().getAudit().getUpdatedBy())
                                .build())
                        .setBank(BankTransferBankSettlement.newBuilder()
                                .setBic(switch (reply1.getBankTransfer().getSettlementCase()) {
                                    case BANK -> reply1.getBankTransfer().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(reply1.getBankTransfer().getCounterparty().getCustomerId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBankTransferCalculation(outbound2);

        var outbound3 = BatchCreateBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBudgetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateBudgetsResponse.getDefaultInstance());
        var input3 = budgetingBudgetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBudgets(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFxHedge().getAudit().getCreatedAt())
                                .setCreatedBy(request.getFxHedge().getAudit().getCreatedBy())
                                .setEtag(request.getFxHedge().getAudit().getEtag())
                                .setUpdatedAt(request.getFxHedge().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input4 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateFxHedge", request.getFxHedge().getFxHedgeId());
        return CalculateFxHedgeResponse.newBuilder()
                .setBreakdown(CalculateFxHedgeResponse.Breakdown.newBuilder()
                        .setTotal(request.getFxHedge().getTotal())
                        .build())
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getFiscalPeriod().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getBankTransfer().getBillingAddress().getCountryCode())
                                .build())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCurrencyCode(reply1.getBreakdown().getCurrencyCode())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .setNotes(reply2.getBankTransfer().getNotes())
                        .setNumber(reply2.getBankTransfer().getNumber())
                        .build())
                .build();
    }

    /**
     * Estimates a fx hedge.
     */
    public EstimateFxHedgeResponse estimateFxHedge(EstimateFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var results0 = new ArrayList<ScoreCashPositionResponse>();
        for (var element : request.getFxHedge().getLinesList()) {
            ScoreCashPositionRequest itemRequest = ScoreCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .addLimits(CashPositionLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreCashPositionRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getFxHedge().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
            results0.add(treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreCashPosition(itemRequest));
        }
        var reply0 = results0.isEmpty() ? ScoreCashPositionResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateFxHedge", request.getFxHedge().getFxHedgeId());
        return EstimateFxHedgeResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setFxHedgeId(request.getFxHedgeId())
                        .setStatus(request.getFxHedge().getStatus())
                        .build())
                .build();
    }

    /**
     * Compares a fx hedge.
     */
    public CompareFxHedgeOptionsResponse compareFxHedgeOptions(CompareFxHedgeOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareFxHedgeOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = RecalculateBankTransfersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateBankTransfersRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getFxHedge().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        RpcPort<RecalculateBankTransfersRequest, RecalculateBankTransfersResponse> port0 = RpcPort.adapt(treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculateBankTransfers);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = CloneRecommendationSetRequest.newBuilder()
                .setContext(request.getContext())
                .setRecommendationSet(RecommendationSet.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getBankTransfer().getAudit().getUpdatedAt())
                                .build())
                        .setCard(RecommendationSetCardSettlement.newBuilder()
                                .setHolderName(switch (request.getFxHedge().getSettlementCase()) {
                                    case CARD -> request.getFxHedge().getCard().getHolderName();
                                    default -> "";
                                })
                                .setNetwork(switch (request.getFxHedge().getSettlementCase()) {
                                    case CARD -> request.getFxHedge().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getBankTransfer().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = recommendationsRecommendationSetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneRecommendationSet(outbound1);

        var outbound2 = CalculateBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getRecommendationSet().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getFxHedge().getBillingAddress().getLocality())
                                .build())
                        .setCard(BankTransferCardSettlement.newBuilder()
                                .setHolderName(switch (reply0.getBankTransfer().getSettlementCase()) {
                                    case CARD -> reply0.getBankTransfer().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CalculateBankTransferRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFxHedge().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply2 = treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateBankTransfer(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareFxHedgeOptions", request.getFxHedge().getFxHedgeId());
        return CompareFxHedgeOptionsResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply2.getBankTransfer().getBillingAddress().getLocality())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .setNotes(request.getFxHedge().getNotes())
                        .setNumber(reply2.getBankTransfer().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalculates a fx hedge.
     */
    public RecalculateFxHedgesResponse recalculateFxHedges(RecalculateFxHedgesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateFxHedges")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = ExportPurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportPurchaseRequisitionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFxHedge().getAudit().getUpdatedAt())
                        .setOwnerId(request.getFxHedge().getCounterparty().getCashPositionId())
                        .setRegion(request.getFxHedge().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = procurementPurchaseRequisitionQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportPurchaseRequisitions(outbound0);
        var reply0 = ExportPurchaseRequisitionsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateFxHedges", request.getFxHedge().getFxHedgeId());
        return RecalculateFxHedgesResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCurrencyCode(request.getDetails().getCurrencyCode())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .build();
    }

    /**
     * Simulates a fx hedge.
     */
    public SimulateFxHedgeResponse simulateFxHedge(SimulateFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = RecalculateBankTransfersRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getFxHedge().getBillingAddress().getLocality())
                                .setRegion(request.getFxHedge().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecalculateBankTransfersRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateBankTransfers(value)).orElseThrow();

        var outbound1 = ExplainCashPositionDecisionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getFxHedge().getAudit().getUpdatedBy())
                                .build())
                        .setCard(CashPositionCardFunding.newBuilder()
                                .setLast4(switch (request.getFxHedge().getSettlementCase()) {
                                    case CARD -> request.getFxHedge().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setCurrencyCode(request.getFxHedge().getCounterparty().getCurrencyCode())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCashPositionDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateFxHedge", request.getFxHedge().getFxHedgeId());
        return SimulateFxHedgeResponse.newBuilder()
                .setBreakdown(SimulateFxHedgeResponse.Breakdown.newBuilder()
                        .setTotal(reply0.getBankTransfer().getTotal())
                        .build())
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getCashPosition().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCurrencyCode(request.getDetails().getCurrencyCode())
                                .setDisplayName(reply0.getBankTransfer().getCounterparty().getDisplayName())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .build();
    }

    /**
     * Explains a fx hedge.
     */
    public ExplainFxHedgeCalculationResponse explainFxHedgeCalculation(ExplainFxHedgeCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainFxHedgeCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = CalculateBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFxHedge().getAudit().getCreatedAt())
                                .setCreatedBy(request.getFxHedge().getAudit().getCreatedBy())
                                .build())
                        .setBank(BankTransferBankSettlement.newBuilder()
                                .setIban(request.getFxHedge().getBank().getIban())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CalculateBankTransferRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFxHedge().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateBankTransfer(outbound0), rpcExecutor);

        var outbound1 = SummarizeVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeVenues(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainFxHedgeCalculation", request.getFxHedge().getFxHedgeId());
        return ExplainFxHedgeCalculationResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setFxHedgeId(request.getFxHedgeId())
                        .putAllMetadata(reply0.getBankTransfer().getMetadataMap())
                        .setTotal(request.getFxHedge().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a fx hedge.
     */
    public GetFxHedgeRatesResponse getFxHedgeRates(GetFxHedgeRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFxHedgeRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getFxHedgeId(), "GetFxHedgeRates");

        var outbound0 = BeanCopies.copy(QuoteCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setHolder(FxHedgeRef.newBuilder()
                                .setFxHedgeId(request.getFxHedgeId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), QuoteCashPositionRequest.class);
        var reply0 = treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCashPosition(outbound0);
        return GetFxHedgeRatesResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCashPosition().getAudit().getCreatedBy())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .setNumber(reply0.getCashPosition().getHolder().getNumber())
                        .build())
                .build();
    }
}
