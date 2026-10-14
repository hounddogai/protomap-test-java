package com.acme.finance.domains.procurement;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.LintAssetTransfersRequest;
import com.acme.assets.v1.LintAssetTransfersResponse;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.CountAuditEventsRequest;
import com.acme.audit.v1.CountAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationValidationServiceGrpc;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityRequest;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.ValidateAttestationRequest;
import com.acme.compliance.v1.ValidateAttestationResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.BrowseLedgerAccountsRequest;
import com.acme.ledger.v1.BrowseLedgerAccountsResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesResponse;
import com.acme.lending.v1.CountLoansRequest;
import com.acme.lending.v1.CountLoansResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanQueryServiceGrpc;
import com.acme.lending.v1.QuoteLoanApplicationRequest;
import com.acme.lending.v1.QuoteLoanApplicationResponse;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.BatchGetPurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchGetPurchaseRequisitionsResponse;
import com.acme.procurement.v1.ClonePurchaseRequisitionRequest;
import com.acme.procurement.v1.ClonePurchaseRequisitionResponse;
import com.acme.procurement.v1.CreatePurchaseRequisitionRequest;
import com.acme.procurement.v1.CreatePurchaseRequisitionResponse;
import com.acme.procurement.v1.DeletePurchaseRequisitionRequest;
import com.acme.procurement.v1.DeletePurchaseRequisitionResponse;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionRequest;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionResponse;
import com.acme.procurement.v1.ExplainPurchaseRequisitionCalculationRequest;
import com.acme.procurement.v1.ExplainPurchaseRequisitionCalculationResponse;
import com.acme.procurement.v1.ExplainRfqDecisionRequest;
import com.acme.procurement.v1.ExplainRfqDecisionResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptCalculationServiceGrpc;
import com.acme.procurement.v1.GoodsReceiptLine;
import com.acme.procurement.v1.GoodsReceiptLineTax;
import com.acme.procurement.v1.ListPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ListPurchaseRequisitionsResponse;
import com.acme.procurement.v1.NormalizePurchaseRequisitionRequest;
import com.acme.procurement.v1.NormalizePurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderCardSettlement;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionCardSettlement;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.QuoteGoodsReceiptRequest;
import com.acme.procurement.v1.QuoteGoodsReceiptResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.procurement.v1.SearchPurchaseRequisitionsRequest;
import com.acme.procurement.v1.SearchPurchaseRequisitionsResponse;
import com.acme.procurement.v1.SimulateGoodsReceiptRequest;
import com.acme.procurement.v1.SimulateGoodsReceiptResponse;
import com.acme.procurement.v1.UpdatePurchaseRequisitionRequest;
import com.acme.procurement.v1.UpdatePurchaseRequisitionResponse;
import com.acme.procurement.v1.WatchPurchaseRequisitionsRequest;
import com.acme.procurement.v1.WatchPurchaseRequisitionsResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Manages purchase requisitions: creates, reads, changes, and lists them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PurchaseRequisitionApplication {
    private static final Logger log = LoggerFactory.getLogger(PurchaseRequisitionApplication.class);

    private final AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking;

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationValidationServiceGrpc.AttestationValidationServiceBlockingStub complianceAttestationValidationServiceBlocking;

    private final ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking;

    private final LoanQueryServiceGrpc.LoanQueryServiceBlockingStub lendingLoanQueryServiceBlocking;

    private final PurchaseRequisitionRequestMapper mapper;

    private final GoodsReceiptCalculationServiceGrpc.GoodsReceiptCalculationServiceBlockingStub procurementGoodsReceiptCalculationServiceBlocking;

    private final PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking;

    private final PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking;

    private final PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking;

    private final RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking;

    private final ProcurementRepository repository;

    private final Executor rpcExecutor;

    public PurchaseRequisitionApplication(
            AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking,
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            ChannelRegistry channels,
            AttestationValidationServiceGrpc.AttestationValidationServiceBlockingStub complianceAttestationValidationServiceBlocking,
            ComplianceCaseValidationServiceGrpc.ComplianceCaseValidationServiceBlockingStub complianceComplianceCaseValidationServiceBlocking,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking,
            LoanQueryServiceGrpc.LoanQueryServiceBlockingStub lendingLoanQueryServiceBlocking,
            PurchaseRequisitionRequestMapper mapper,
            GoodsReceiptCalculationServiceGrpc.GoodsReceiptCalculationServiceBlockingStub procurementGoodsReceiptCalculationServiceBlocking,
            PurchaseOrderValidationServiceGrpc.PurchaseOrderValidationServiceBlockingStub procurementPurchaseOrderValidationServiceBlocking,
            PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking,
            PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking,
            RfqValidationServiceGrpc.RfqValidationServiceBlockingStub procurementRfqValidationServiceBlocking,
            ProcurementRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsAssetTransferValidationServiceBlocking = assetsAssetTransferValidationServiceBlocking;
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.channels = channels;
        this.complianceAttestationValidationServiceBlocking = complianceAttestationValidationServiceBlocking;
        this.complianceComplianceCaseValidationServiceBlocking = complianceComplianceCaseValidationServiceBlocking;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.lendingLoanApplicationCalculationServiceBlocking = lendingLoanApplicationCalculationServiceBlocking;
        this.lendingLoanQueryServiceBlocking = lendingLoanQueryServiceBlocking;
        this.mapper = mapper;
        this.procurementGoodsReceiptCalculationServiceBlocking = procurementGoodsReceiptCalculationServiceBlocking;
        this.procurementPurchaseOrderValidationServiceBlocking = procurementPurchaseOrderValidationServiceBlocking;
        this.procurementPurchaseRequisitionCalculationServiceBlocking = procurementPurchaseRequisitionCalculationServiceBlocking;
        this.procurementPurchaseRequisitionValidationServiceBlocking = procurementPurchaseRequisitionValidationServiceBlocking;
        this.procurementRfqValidationServiceBlocking = procurementRfqValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Creates a purchase requisition.
     */
    public CreatePurchaseRequisitionResponse createPurchaseRequisition(CreatePurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreatePurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var futures0 = request.getPurchaseRequisition().getLinesList().stream()
                .map(element -> SimulateGoodsReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .addLines(GoodsReceiptLine.newBuilder()
                                .setTax(GoodsReceiptLineTax.newBuilder()
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> procurementGoodsReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateGoodsReceipt(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? SimulateGoodsReceiptResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = QuoteLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setCard(LoanApplicationCardSettlement.newBuilder()
                                .setNetwork(switch (request.getPurchaseRequisition().getSettlementCase()) {
                                    case CARD -> request.getPurchaseRequisition().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteLoanApplication(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(UnsubscribeFiscalPeriodUpdatesRequest.newBuilder()
                .putAllAnnotations(request.getPurchaseRequisition().getMetadataMap())
                .setContext(request.getContext())
                .setDetails(UnsubscribeFiscalPeriodUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply1.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .putAllVariables(reply0.getResult().getDetailsMap())
                        .build())
                .build()), UnsubscribeFiscalPeriodUpdatesRequest.class);
        var reply2 = ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeFiscalPeriodUpdates(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreatePurchaseRequisition", request.getPurchaseRequisition().getNumber());
        return CreatePurchaseRequisitionResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getFiscalPeriod().getAudit().getEtag())
                                .build())
                        .putAllMetadata(reply1.getLoanApplication().getMetadataMap())
                        .setNotes(request.getPurchaseRequisition().getNotes())
                        .setPurchaseRequisitionId(request.getPurchaseRequisition().getPurchaseRequisitionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a purchase requisition.
     */
    public GetPurchaseRequisitionResponse getPurchaseRequisition(GetPurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getPurchaseRequisitionId(), "GetPurchaseRequisition");

        var outbound0 = LintAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setDueAt(request.getContext().getDeadline())
                        .build())
                .setContext(request.getContext())
                .setDetails(LintAssetTransfersRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        Function<LintAssetTransfersRequest, LintAssetTransfersResponse> operation0 = assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lintAssetTransfers;
        var reply0 = operation0.apply(outbound0);
        return GetPurchaseRequisitionResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .putAllMetadata(reply0.getAssetTransfer().getMetadataMap())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .build();
    }

    /**
     * Changes a purchase requisition.
     */
    public UpdatePurchaseRequisitionResponse updatePurchaseRequisition(UpdatePurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdatePurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = ExplainPurchaseOrderDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPurchaseRequisition().getAudit().getCreatedBy())
                                .build())
                        .setCard(PurchaseOrderCardSettlement.newBuilder()
                                .setHolderName(request.getPurchaseRequisition().getCard().getHolderName())
                                .setLast4(request.getPurchaseRequisition().getCard().getLast4())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> procurementPurchaseOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPurchaseOrderDecision(outbound0), rpcExecutor);

        var outbound1 = QuoteGoodsReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(request.getPurchaseRequisition().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> procurementGoodsReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteGoodsReceipt(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = NormalizePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getPurchaseRequisition().getBillingAddress().getRegion())
                                .build())
                        .setCard(PurchaseRequisitionCardSettlement.newBuilder()
                                .setToken(switch (reply0.getPurchaseOrder().getSettlementCase()) {
                                    case CARD -> reply0.getPurchaseOrder().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePurchaseRequisition(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdatePurchaseRequisition", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return UpdatePurchaseRequisitionResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(request.getPurchaseRequisition().getBillingAddress())
                        .setCounterparty(request.getPurchaseRequisition().getCounterparty())
                        .setPurchaseRequisitionId(reply2.getPurchaseRequisition().getPurchaseRequisitionId())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Deletes a purchase requisition.
     */
    public DeletePurchaseRequisitionResponse deletePurchaseRequisition(DeletePurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeletePurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getPurchaseRequisitionId(), "DeletePurchaseRequisition");

        var outbound0 = SimulateGoodsReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            SimulateGoodsReceiptResponse fetch(SimulateGoodsReceiptRequest input) {
                return procurementGoodsReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateGoodsReceipt(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = BrowseLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getGoodsReceipt().getAudit().getUpdatedAt())
                        .setOwnerId(reply0.getGoodsReceipt().getCounterparty().getPurchaseRequisitionId())
                        .setRegion(reply0.getGoodsReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.ledger.v1.LedgerAccountQueryService");
        var selected1 = LedgerAccountQueryServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.browseLedgerAccounts(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeletePurchaseRequisition", request.getPurchaseRequisitionId());
        return DeletePurchaseRequisitionResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists purchase requisitions a page at a time.
     */
    public ListPurchaseRequisitionsResponse listPurchaseRequisitions(ListPurchaseRequisitionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPurchaseRequisitions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getContext().getTraceId(), "ListPurchaseRequisitions");

        var outbound0 = CountAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAuditEventsRequest.Filter.newBuilder()
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageToken(request.getPage().getPageToken())
                        .build())
                .build();
        var reply0 = auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAuditEvents(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getOwnerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPurchaseRequisitions", request.getFilter().getOwnerId());
        return ListPurchaseRequisitionsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns several purchase requisitions at once.
     */
    public BatchGetPurchaseRequisitionsResponse batchGetPurchaseRequisitions(BatchGetPurchaseRequisitionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetPurchaseRequisitions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getContext().getTraceId(), "BatchGetPurchaseRequisitions");

        var outbound0 = mapper.batchGetPurchaseRequisitionsStage0(request);
        var reply0 = lendingLoanQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countLoans(outbound0);
        return BatchGetPurchaseRequisitionsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists purchase requisitions a page at a time.
     */
    public SearchPurchaseRequisitionsResponse searchPurchaseRequisitions(SearchPurchaseRequisitionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchPurchaseRequisitions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getContext().getTraceId(), "SearchPurchaseRequisitions");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchPurchaseRequisitions", request.getPage().getPageToken());
        return SearchPurchaseRequisitionsResponse.getDefaultInstance();
    }

    /**
     * Streams the changes of purchase requisitions as they happen.
     */
    public WatchPurchaseRequisitionsResponse watchPurchaseRequisitions(WatchPurchaseRequisitionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchPurchaseRequisitions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getContext().getTraceId(), "WatchPurchaseRequisitions");

        var outbound0 = ValidateAttestationRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ValidateAttestationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .setPhone(request.getSubscriber().getPhone())
                                .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(complianceAttestationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateAttestation(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchPurchaseRequisitions", request.getFilter().getQuery());
        return WatchPurchaseRequisitionsResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getAttestation().getBillingAddress().getRecipient())
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Takes a stream of purchase requisitions and answers once at the end.
     */
    public BatchCreatePurchaseRequisitionsResponse batchCreatePurchaseRequisitions(BatchCreatePurchaseRequisitionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreatePurchaseRequisitions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = ExplainRfqDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainRfqDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(switch (request.getChunkCase()) {
                                    case HEADER -> request.getHeader().getSubmittedBy().getEmail();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getCounterparty().getDisplayName();
                                    default -> "";
                                })
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getHeader().getSubmittedBy().getName())
                                .build())
                        .build())
                .build();
        var reply0 = procurementRfqValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainRfqDecision(outbound0);

        var outbound1 = ExplainPurchaseRequisitionCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPurchaseRequisitionCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getRfq().getRequester().getEmail())
                                .build())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getBillingAddress().getRecipient();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setNumber(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getCounterparty().getNumber();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPurchaseRequisitionCalculation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreatePurchaseRequisitions", request.getHeader().getSource());
        return BatchCreatePurchaseRequisitionsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Creates a purchase requisition.
     */
    public ClonePurchaseRequisitionResponse clonePurchaseRequisition(ClonePurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ClonePurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = CheckComplianceCaseEligibilityRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPurchaseRequisition().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            CheckComplianceCaseEligibilityResponse fetch(CheckComplianceCaseEligibilityRequest input) {
                return complianceComplianceCaseValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkComplianceCaseEligibility(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ClonePurchaseRequisition", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return ClonePurchaseRequisitionResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getComplianceCase().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(request.getPurchaseRequisition().getBillingAddress())
                        .setDueAt(request.getPurchaseRequisition().getDueAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
