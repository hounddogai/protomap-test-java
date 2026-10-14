package com.acme.finance.domains.treasury;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationCalculationServiceGrpc;
import com.acme.contracts.v1.ObligationComment;
import com.acme.contracts.v1.QuoteObligationRequest;
import com.acme.contracts.v1.QuoteObligationResponse;
import com.acme.deliveries.v1.DeliveryServiceGrpc;
import com.acme.deliveries.v1.WatchDeliveriesRequest;
import com.acme.deliveries.v1.WatchDeliveriesResponse;
import com.acme.expenses.v1.CheckReceiptEligibilityRequest;
import com.acme.expenses.v1.CheckReceiptEligibilityResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptCardSettlement;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.CountPurchaseOrdersRequest;
import com.acme.procurement.v1.CountPurchaseOrdersResponse;
import com.acme.procurement.v1.FindPurchaseOrdersRequest;
import com.acme.procurement.v1.FindPurchaseOrdersResponse;
import com.acme.procurement.v1.PurchaseOrderQueryServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferValidationServiceGrpc;
import com.acme.treasury.v1.BatchCreateCashPositionsRequest;
import com.acme.treasury.v1.BatchCreateCashPositionsResponse;
import com.acme.treasury.v1.CalculateFxHedgeRequest;
import com.acme.treasury.v1.CalculateFxHedgeResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionQueryServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CheckBankTransferEligibilityRequest;
import com.acme.treasury.v1.CheckBankTransferEligibilityResponse;
import com.acme.treasury.v1.CloneCashPositionRequest;
import com.acme.treasury.v1.CloneCashPositionResponse;
import com.acme.treasury.v1.CreateCashPositionRequest;
import com.acme.treasury.v1.CreateCashPositionResponse;
import com.acme.treasury.v1.DeleteCashPositionRequest;
import com.acme.treasury.v1.DeleteCashPositionResponse;
import com.acme.treasury.v1.EstimateCashPositionRequest;
import com.acme.treasury.v1.EstimateCashPositionResponse;
import com.acme.treasury.v1.ExplainFxHedgeCalculationRequest;
import com.acme.treasury.v1.ExplainFxHedgeCalculationResponse;
import com.acme.treasury.v1.ExportCashPositionsRequest;
import com.acme.treasury.v1.ExportCashPositionsResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeBankSettlement;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeCardSettlement;
import com.acme.treasury.v1.GetCashPositionRequest;
import com.acme.treasury.v1.GetCashPositionResponse;
import com.acme.treasury.v1.GetFxHedgeRatesRequest;
import com.acme.treasury.v1.GetFxHedgeRatesResponse;
import com.acme.treasury.v1.RecalculateFxHedgesRequest;
import com.acme.treasury.v1.RecalculateFxHedgesResponse;
import com.acme.treasury.v1.SearchCashPositionsRequest;
import com.acme.treasury.v1.SearchCashPositionsResponse;
import com.acme.treasury.v1.UpdateCashPositionRequest;
import com.acme.treasury.v1.UpdateCashPositionResponse;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Manages cash positions: creates, reads, changes, and lists them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class CashPositionApplication {
    private static final Logger log = LoggerFactory.getLogger(CashPositionApplication.class);

    private final ChannelRegistry channels;

    private final ObligationCalculationServiceGrpc.ObligationCalculationServiceBlockingStub contractsObligationCalculationServiceBlocking;

    private final DeliveryServiceGrpc.DeliveryServiceBlockingStub deliveriesDeliveryServiceBlocking;

    private final ReceiptValidationServiceGrpc.ReceiptValidationServiceBlockingStub expensesReceiptValidationServiceBlocking;

    private final PurchaseOrderQueryServiceGrpc.PurchaseOrderQueryServiceBlockingStub procurementPurchaseOrderQueryServiceBlocking;

    private final TreasuryRepository repository;

    private final BankTransferValidationServiceGrpc.BankTransferValidationServiceBlockingStub treasuryBankTransferValidationServiceBlocking;

    private final CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking;

    private final CashPositionQueryServiceGrpc.CashPositionQueryServiceBlockingStub treasuryCashPositionQueryServiceBlocking;

    private final FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking;

    public CashPositionApplication(
            ChannelRegistry channels,
            ObligationCalculationServiceGrpc.ObligationCalculationServiceBlockingStub contractsObligationCalculationServiceBlocking,
            DeliveryServiceGrpc.DeliveryServiceBlockingStub deliveriesDeliveryServiceBlocking,
            ReceiptValidationServiceGrpc.ReceiptValidationServiceBlockingStub expensesReceiptValidationServiceBlocking,
            PurchaseOrderQueryServiceGrpc.PurchaseOrderQueryServiceBlockingStub procurementPurchaseOrderQueryServiceBlocking,
            TreasuryRepository repository,
            BankTransferValidationServiceGrpc.BankTransferValidationServiceBlockingStub treasuryBankTransferValidationServiceBlocking,
            CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking,
            CashPositionQueryServiceGrpc.CashPositionQueryServiceBlockingStub treasuryCashPositionQueryServiceBlocking,
            FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking) {
        this.channels = channels;
        this.contractsObligationCalculationServiceBlocking = contractsObligationCalculationServiceBlocking;
        this.deliveriesDeliveryServiceBlocking = deliveriesDeliveryServiceBlocking;
        this.expensesReceiptValidationServiceBlocking = expensesReceiptValidationServiceBlocking;
        this.procurementPurchaseOrderQueryServiceBlocking = procurementPurchaseOrderQueryServiceBlocking;
        this.repository = repository;
        this.treasuryBankTransferValidationServiceBlocking = treasuryBankTransferValidationServiceBlocking;
        this.treasuryCashPositionCalculationServiceBlocking = treasuryCashPositionCalculationServiceBlocking;
        this.treasuryCashPositionQueryServiceBlocking = treasuryCashPositionQueryServiceBlocking;
        this.treasuryFxHedgeCalculationServiceBlocking = treasuryFxHedgeCalculationServiceBlocking;
    }

    /**
     * Creates a cash position.
     */
    public CreateCashPositionResponse createCashPosition(CreateCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = QuoteObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCashPosition().getAudit().getCreatedBy())
                                .setEtag(request.getCashPosition().getAudit().getEtag())
                                .build())
                        .addComments(ObligationComment.newBuilder()
                                .setCreatedAt(request.getCashPosition().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getCashPosition().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<QuoteObligationRequest, QuoteObligationResponse>() {
            @Override
            public QuoteObligationResponse apply(QuoteObligationRequest input) {
                return contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteObligation(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(GetFxHedgeRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedgeId(request.getCashPosition().getHolder().getFxHedgeId())
                .build()), GetFxHedgeRatesRequest.class);
        var reply1 = treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getFxHedgeRates(outbound1);

        var outbound2 = RecalculateFxHedgesRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getFxHedge().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getCashPosition().getAudit().getUpdatedBy())
                                .build())
                        .setBank(FxHedgeBankSettlement.newBuilder()
                                .setBic(switch (reply1.getFxHedge().getSettlementCase()) {
                                    case BANK -> reply1.getFxHedge().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCashPositionId(reply1.getFxHedge().getCounterparty().getCashPositionId())
                                .build())
                        .build())
                .build();
        var reply2 = treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateFxHedges(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateCashPosition", request.getCashPosition().getCashPositionId());
        return CreateCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getFxHedge().getAudit().getCreatedAt())
                                .setCreatedBy(reply2.getFxHedge().getAudit().getCreatedBy())
                                .build())
                        .setCashPositionId(reply2.getFxHedge().getCounterparty().getCashPositionId())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getObligation().getRequester().getEmail())
                                .setPhone(reply0.getObligation().getRequester().getPhone())
                                .build())
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .setStatus(request.getCashPosition().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a cash position.
     */
    public GetCashPositionResponse getCashPosition(GetCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "GetCashPosition");

        var outbound0 = CheckBankTransferEligibilityRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CheckBankTransferEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var reply0 = treasuryBankTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkBankTransferEligibility(outbound0);
        return GetCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Changes a cash position.
     */
    public UpdateCashPositionResponse updateCashPosition(UpdateCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdateCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = ExplainFxHedgeCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getCashPosition().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.treasury.v1.FxHedgeCalculationService");
        var selected0 = FxHedgeCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.explainFxHedgeCalculation(outbound0);

        var outbound1 = WatchDeliveriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchDeliveriesRequest.Filter.newBuilder()
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getCashPosition().getContact().getEmail())
                        .setPreferredChannel(request.getCashPosition().getContact().getPreferredChannel())
                        .build())
                .build();
        var stream1 = deliveriesDeliveryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchDeliveries(outbound1);
        var reply1 = WatchDeliveriesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdateCashPosition", request.getCashPosition().getCashPositionId());
        return UpdateCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setContact(request.getCashPosition().getContact())
                        .setHolder(request.getCashPosition().getHolder())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Deletes a cash position.
     */
    public DeleteCashPositionResponse deleteCashPosition(DeleteCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "DeleteCashPosition");

        var outbound0 = ExportCashPositionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportCashPositionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getCashPositionId())
                        .build())
                .build();
        var stream0 = treasuryCashPositionQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportCashPositions(outbound0);
        var reply0 = ExportCashPositionsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = EstimateCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .setCashPositionId(request.getCashPositionId())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command1 = new DispatchCommand<>("acme.treasury.v1.CashPositionCalculationService/EstimateCashPosition", outbound1);
        var reply1 = treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCashPosition(command1.payload());

        var outbound2 = CalculateFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setCard(FxHedgeCardSettlement.newBuilder()
                                .setLast4(switch (reply1.getCashPosition().getFundingCase()) {
                                    case CARD -> reply1.getCashPosition().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateFxHedge(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getReason()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteCashPosition", request.getReason());
        return DeleteCashPositionResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists cash positions a page at a time.
     */
    public SearchCashPositionsResponse searchCashPositions(SearchCashPositionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchCashPositions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getContext().getTraceId(), "SearchCashPositions");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchCashPositions", request.getPage().getPageToken());
        return SearchCashPositionsResponse.getDefaultInstance();
    }

    /**
     * Takes a stream of cash positions and answers once at the end.
     */
    public BatchCreateCashPositionsResponse batchCreateCashPositions(BatchCreateCashPositionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreateCashPositions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveCashPosition(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = CalculateFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRecord().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getRecord().getAudit().getUpdatedAt())
                                .build())
                        .setCard(FxHedgeCardSettlement.newBuilder()
                                .setHolderName(switch (request.getRecord().getFundingCase()) {
                                    case CARD -> request.getRecord().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateFxHedge(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreateCashPositions", request.getHeader().getSource());
        return BatchCreateCashPositionsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Creates a cash position.
     */
    public CloneCashPositionResponse cloneCashPosition(CloneCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CloneCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = CheckReceiptEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckReceiptEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getCashPosition().getContact().getPhone())
                                .build())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCashPosition().getAudit().getUpdatedAt())
                                .build())
                        .setCard(ReceiptCardSettlement.newBuilder()
                                .setNetwork(switch (request.getCashPosition().getFundingCase()) {
                                    case CARD -> request.getCashPosition().getCard().getNetwork();
                                    default -> "";
                                })
                                .setToken(switch (request.getCashPosition().getFundingCase()) {
                                    case CARD -> request.getCashPosition().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = expensesReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkReceiptEligibility(outbound0);

        var outbound1 = (CountPurchaseOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountPurchaseOrdersRequest.Filter.newBuilder()
                        .setRegion(reply0.getReceipt().getBillingAddress().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply1 = procurementPurchaseOrderQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countPurchaseOrders(outbound1);

        var outbound2 = FindPurchaseOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindPurchaseOrdersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getReceipt().getAudit().getCreatedAt())
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .setRegion(reply0.getReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.retryRead(() -> procurementPurchaseOrderQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findPurchaseOrders(outbound2));
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CloneCashPosition", request.getCashPosition().getCashPositionId());
        return CloneCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getReceipt().getAudit().getEtag())
                                .build())
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .setStatus(request.getCashPosition().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
