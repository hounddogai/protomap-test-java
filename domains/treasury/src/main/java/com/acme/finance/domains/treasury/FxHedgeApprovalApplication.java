package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.NormalizeAuditEventRequest;
import com.acme.audit.v1.NormalizeAuditEventResponse;
import com.acme.audit.v1.PreviewAuditEventRequest;
import com.acme.audit.v1.PreviewAuditEventResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.SimulateHouseholdRequest;
import com.acme.customers.v1.SimulateHouseholdResponse;
import com.acme.documents.v1.BatchGetDocumentsRequest;
import com.acme.documents.v1.BatchGetDocumentsResponse;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.ListDocumentsRequest;
import com.acme.documents.v1.ListDocumentsResponse;
import com.acme.documents.v1.WatchDocumentsRequest;
import com.acme.documents.v1.WatchDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.RevertFiscalPeriodRequest;
import com.acme.ledger.v1.RevertFiscalPeriodResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderCardSettlement;
import com.acme.procurement.v1.SubmitPurchaseOrderRequest;
import com.acme.procurement.v1.SubmitPurchaseOrderResponse;
import com.acme.treasury.v1.ApproveFxHedgeRequest;
import com.acme.treasury.v1.ApproveFxHedgeResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCardFunding;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CashPositionWorkflowServiceGrpc;
import com.acme.treasury.v1.CommentFxHedgeApprovalRequest;
import com.acme.treasury.v1.CommentFxHedgeApprovalResponse;
import com.acme.treasury.v1.CountFxHedgeTotalsRequest;
import com.acme.treasury.v1.CountFxHedgeTotalsResponse;
import com.acme.treasury.v1.EscalateFxHedgeRequest;
import com.acme.treasury.v1.EscalateFxHedgeResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeCardSettlement;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.FxHedgeReportingServiceGrpc;
import com.acme.treasury.v1.GetFxHedgeApprovalStatusRequest;
import com.acme.treasury.v1.GetFxHedgeApprovalStatusResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastCalculationServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastCardSettlement;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.ListPendingFxHedgesRequest;
import com.acme.treasury.v1.ListPendingFxHedgesResponse;
import com.acme.treasury.v1.RecalculateFxHedgesRequest;
import com.acme.treasury.v1.RecalculateFxHedgesResponse;
import com.acme.treasury.v1.RecalculateLiquidityForecastsRequest;
import com.acme.treasury.v1.RecalculateLiquidityForecastsResponse;
import com.acme.treasury.v1.RetryLiquidityForecastImportRequest;
import com.acme.treasury.v1.RetryLiquidityForecastImportResponse;
import com.acme.treasury.v1.SignalCashPositionWorkflowRequest;
import com.acme.treasury.v1.SignalCashPositionWorkflowResponse;
import com.acme.treasury.v1.SubmitFxHedgeRequest;
import com.acme.treasury.v1.SubmitFxHedgeResponse;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Routes fx hedges through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class FxHedgeApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(FxHedgeApprovalApplication.class);

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking;

    private final FxHedgeApprovalRequestMapper mapper;

    private final PurchaseOrderApprovalServiceGrpc.PurchaseOrderApprovalServiceBlockingStub procurementPurchaseOrderApprovalServiceBlocking;

    private final TreasuryRepository repository;

    private final CashPositionWorkflowServiceGrpc.CashPositionWorkflowServiceBlockingStub treasuryCashPositionWorkflowServiceBlocking;

    private final FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking;

    private final FxHedgeReportingServiceGrpc.FxHedgeReportingServiceBlockingStub treasuryFxHedgeReportingServiceBlocking;

    private final LiquidityForecastCalculationServiceGrpc.LiquidityForecastCalculationServiceBlockingStub treasuryLiquidityForecastCalculationServiceBlocking;

    private final LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking;

    public FxHedgeApprovalApplication(
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking,
            FxHedgeApprovalRequestMapper mapper,
            PurchaseOrderApprovalServiceGrpc.PurchaseOrderApprovalServiceBlockingStub procurementPurchaseOrderApprovalServiceBlocking,
            TreasuryRepository repository,
            CashPositionWorkflowServiceGrpc.CashPositionWorkflowServiceBlockingStub treasuryCashPositionWorkflowServiceBlocking,
            FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking,
            FxHedgeReportingServiceGrpc.FxHedgeReportingServiceBlockingStub treasuryFxHedgeReportingServiceBlocking,
            LiquidityForecastCalculationServiceGrpc.LiquidityForecastCalculationServiceBlockingStub treasuryLiquidityForecastCalculationServiceBlocking,
            LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking) {
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.ledgerFiscalPeriodHistoryServiceBlocking = ledgerFiscalPeriodHistoryServiceBlocking;
        this.mapper = mapper;
        this.procurementPurchaseOrderApprovalServiceBlocking = procurementPurchaseOrderApprovalServiceBlocking;
        this.repository = repository;
        this.treasuryCashPositionWorkflowServiceBlocking = treasuryCashPositionWorkflowServiceBlocking;
        this.treasuryFxHedgeCalculationServiceBlocking = treasuryFxHedgeCalculationServiceBlocking;
        this.treasuryFxHedgeReportingServiceBlocking = treasuryFxHedgeReportingServiceBlocking;
        this.treasuryLiquidityForecastCalculationServiceBlocking = treasuryLiquidityForecastCalculationServiceBlocking;
        this.treasuryLiquidityForecastImportServiceBlocking = treasuryLiquidityForecastImportServiceBlocking;
    }

    /**
     * Submits a fx hedge.
     */
    public SubmitFxHedgeResponse submitFxHedge(SubmitFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = NormalizeAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getFxHedge().getCounterparty().getDisplayName())
                                .setNumber(request.getFxHedge().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeAuditEventRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        final class Stage0Adapter {
            NormalizeAuditEventResponse fetch(NormalizeAuditEventRequest input) {
                return auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAuditEvent(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = SignalCashPositionWorkflowRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFxHedge().getAudit().getCreatedAt())
                                .build())
                        .setCard(CashPositionCardFunding.newBuilder()
                                .setHolderName(switch (request.getFxHedge().getSettlementCase()) {
                                    case CARD -> request.getFxHedge().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .setHolder(FxHedgeRef.newBuilder()
                                .setDisplayName(reply0.getAuditEvent().getSubject().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = treasuryCashPositionWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalCashPositionWorkflow(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitFxHedge", request.getFxHedge().getFxHedgeId());
        return SubmitFxHedgeResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getCashPosition().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(request.getFxHedge().getBillingAddress())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .setOutcome(SubmitFxHedgeResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves a fx hedge.
     */
    public ApproveFxHedgeResponse approveFxHedge(ApproveFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = mapper.approveFxHedgeStage0(request);
        var reply0 = documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listDocuments(outbound0);

        var outbound1 = BeanCopies.copy(PreviewAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getFxHedge().getCounterparty().getDisplayName())
                                .setNumber(request.getFxHedge().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewAuditEventRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build(), PreviewAuditEventRequest.class);
        var reply1 = auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAuditEvent(outbound1);

        var outbound2 = RevertFiscalPeriodRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertFiscalPeriodRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRecipient(request.getFxHedge().getBillingAddress().getRecipient())
                                .build())
                        .addParticipants(FiscalPeriodParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getApprover().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            RevertFiscalPeriodResponse fetch(RevertFiscalPeriodRequest input) {
                return ledgerFiscalPeriodHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertFiscalPeriod(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = RetryLiquidityForecastImportRequest.newBuilder()
                .setContext(request.getContext())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getFiscalPeriod().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply2.getFiscalPeriod().getLocation().getLinesList())
                                .build())
                        .build())
                .build();
        var reply3 = Stream.of(outbound3).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(treasuryLiquidityForecastImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryLiquidityForecastImport(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveFxHedge", request.getFxHedge().getFxHedgeId());
        return ApproveFxHedgeResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply3.getLiquidityForecast().getAudit().getCreatedBy())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .setIssuedAt(request.getFxHedge().getIssuedAt())
                        .putAllMetadata(reply2.getFiscalPeriod().getMetadataMap())
                        .build())
                .setOutcome(ApproveFxHedgeResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply3.getOutcome().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Escalates a fx hedge.
     */
    public EscalateFxHedgeResponse escalateFxHedge(EscalateFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = JsonCodec.roundTrip(SubmitPurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getFxHedge().getAudit().getUpdatedBy())
                                .build())
                        .setCard(PurchaseOrderCardSettlement.newBuilder()
                                .setToken(request.getFxHedge().getCard().getToken())
                                .build())
                        .build())
                .build(), SubmitPurchaseOrderRequest.class);
        var reply0 = procurementPurchaseOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitPurchaseOrder(outbound0);

        var outbound1 = RecalculateLiquidityForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateLiquidityForecastsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply0.getPurchaseOrder().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setCard(LiquidityForecastCardSettlement.newBuilder()
                                .setHolderName(switch (request.getFxHedge().getSettlementCase()) {
                                    case CARD -> request.getFxHedge().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = treasuryLiquidityForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateLiquidityForecasts(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateFxHedge", request.getFxHedge().getFxHedgeId());
        return EscalateFxHedgeResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCashPositionId(reply1.getLiquidityForecast().getCounterparty().getCashPositionId())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .putAllMetadata(request.getFxHedge().getMetadataMap())
                        .setTotal(reply1.getLiquidityForecast().getTotal())
                        .build())
                .setOutcome(EscalateFxHedgeResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists fx hedges a page at a time.
     */
    public ListPendingFxHedgesResponse listPendingFxHedges(ListPendingFxHedgesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingFxHedges")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getContext().getTraceId(), "ListPendingFxHedges");

        var outbound0 = BatchGetDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            BatchGetDocumentsResponse fetch(BatchGetDocumentsRequest input) {
                return documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetDocuments(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingFxHedges", request.getPage().getPageToken());
        return ListPendingFxHedgesResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setTotalSize(reply0.getPageInfo().getTotalSize())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a fx hedge.
     */
    public GetFxHedgeApprovalStatusResponse getFxHedgeApprovalStatus(GetFxHedgeApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFxHedgeApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getFxHedgeId(), "GetFxHedgeApprovalStatus");

        var outbound0 = JsonCodec.roundTrip(SimulateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(request.getContext().getActor().getUserId())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setEmail(request.getContext().getActor().getEmail())
                                        .build())
                                .build())
                        .build())
                .build(), SimulateHouseholdRequest.class);
        var reply0 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateHousehold(outbound0);

        var outbound1 = mapper.getFxHedgeApprovalStatusStage1(request, reply0);
        var reply1 = treasuryFxHedgeReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countFxHedgeTotals(outbound1);
        return GetFxHedgeApprovalStatusResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCurrencyCode(reply0.getBreakdown().getCurrencyCode())
                                .setDisplayName(reply0.getHousehold().getManager().getDisplayName())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Comments a fx hedge.
     */
    public CommentFxHedgeApprovalResponse commentFxHedgeApproval(CommentFxHedgeApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentFxHedgeApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = WatchDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchDocumentsRequest.Filter.newBuilder()
                        .setRegion(request.getFxHedge().getBillingAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .build())
                .build();
        var stream0 = documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchDocuments(outbound0);
        var reply0 = WatchDocumentsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = RecalculateFxHedgesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateFxHedgesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getFxHedge().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFxHedge().getAudit().getUpdatedAt())
                                .build())
                        .setCard(FxHedgeCardSettlement.newBuilder()
                                .setLast4(switch (request.getFxHedge().getSettlementCase()) {
                                    case CARD -> request.getFxHedge().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateFxHedges(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentFxHedgeApproval", request.getFxHedge().getFxHedgeId());
        return CommentFxHedgeApprovalResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(reply1.getFxHedge().getCounterparty().getDisplayName())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .setIssuedAt(request.getFxHedge().getIssuedAt())
                        .build())
                .build();
    }
}
