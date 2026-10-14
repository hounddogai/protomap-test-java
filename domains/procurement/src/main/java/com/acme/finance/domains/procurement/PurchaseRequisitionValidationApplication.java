package com.acme.finance.domains.procurement;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailAdminServiceGrpc;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.ListAuditEventsRequest;
import com.acme.audit.v1.ListAuditEventsResponse;
import com.acme.audit.v1.RepairAuditTrailRequest;
import com.acme.audit.v1.RepairAuditTrailResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.ScoreMandateRequest;
import com.acme.banking.v1.ScoreMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ContractQueryServiceGrpc;
import com.acme.contracts.v1.ExplainObligationDecisionRequest;
import com.acme.contracts.v1.ExplainObligationDecisionResponse;
import com.acme.contracts.v1.NormalizeRenewalRequest;
import com.acme.contracts.v1.NormalizeRenewalResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.SummarizeContractsRequest;
import com.acme.contracts.v1.SummarizeContractsResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryCardSettlement;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.RecordJournalEntryVersionRequest;
import com.acme.ledger.v1.RecordJournalEntryVersionResponse;
import com.acme.localization.v1.CountTranslationKeysRequest;
import com.acme.localization.v1.CountTranslationKeysResponse;
import com.acme.localization.v1.TranslationKeyQueryServiceGrpc;
import com.acme.procurement.v1.CalculatePurchaseOrderRequest;
import com.acme.procurement.v1.CalculatePurchaseOrderResponse;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityRequest;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityResponse;
import com.acme.procurement.v1.ComparePurchaseOrderOptionsRequest;
import com.acme.procurement.v1.ComparePurchaseOrderOptionsResponse;
import com.acme.procurement.v1.EstimatePurchaseOrderRequest;
import com.acme.procurement.v1.EstimatePurchaseOrderResponse;
import com.acme.procurement.v1.EstimatePurchaseRequisitionRequest;
import com.acme.procurement.v1.EstimatePurchaseRequisitionResponse;
import com.acme.procurement.v1.ExplainPurchaseRequisitionDecisionRequest;
import com.acme.procurement.v1.ExplainPurchaseRequisitionDecisionResponse;
import com.acme.procurement.v1.GetPurchaseOrderRatesRequest;
import com.acme.procurement.v1.GetPurchaseOrderRatesResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionRulesRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionRulesResponse;
import com.acme.procurement.v1.LintPurchaseRequisitionsRequest;
import com.acme.procurement.v1.LintPurchaseRequisitionsResponse;
import com.acme.procurement.v1.NormalizePurchaseRequisitionRequest;
import com.acme.procurement.v1.NormalizePurchaseRequisitionResponse;
import com.acme.procurement.v1.PreviewPurchaseRequisitionRequest;
import com.acme.procurement.v1.PreviewPurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderBankSettlement;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderCardSettlement;
import com.acme.procurement.v1.PurchaseOrderLine;
import com.acme.procurement.v1.PurchaseOrderLineTax;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionCardSettlement;
import com.acme.procurement.v1.QuotePurchaseOrderRequest;
import com.acme.procurement.v1.QuotePurchaseOrderResponse;
import com.acme.procurement.v1.ScorePurchaseRequisitionRequest;
import com.acme.procurement.v1.ScorePurchaseRequisitionResponse;
import com.acme.procurement.v1.SimulatePurchaseOrderRequest;
import com.acme.procurement.v1.SimulatePurchaseOrderResponse;
import com.acme.procurement.v1.ValidatePurchaseRequisitionRequest;
import com.acme.procurement.v1.ValidatePurchaseRequisitionResponse;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityResponse;
import com.acme.suppliers.v1.SupplierContractRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.venues.v1.CheckVenueEligibilityRequest;
import com.acme.venues.v1.CheckVenueEligibilityResponse;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import java.util.ArrayList;
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
 * Checks purchase requisitions against the rules before anything acts on them. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class PurchaseRequisitionValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(PurchaseRequisitionValidationApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final AuditTrailAdminServiceGrpc.AuditTrailAdminServiceBlockingStub auditAuditTrailAdminServiceBlocking;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final ContractQueryServiceGrpc.ContractQueryServiceBlockingStub contractsContractQueryServiceBlocking;

    private final ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking;

    private final RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking;

    private final TranslationKeyQueryServiceGrpc.TranslationKeyQueryServiceFutureStub localizationTranslationKeyQueryServiceFuture;

    private final PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceBlockingStub procurementPurchaseOrderCalculationServiceBlocking;

    private final PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking;

    private final ProcurementRepository repository;

    private final Executor rpcExecutor;

    private final SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public PurchaseRequisitionValidationApplication(
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            AuditTrailAdminServiceGrpc.AuditTrailAdminServiceBlockingStub auditAuditTrailAdminServiceBlocking,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            ContractQueryServiceGrpc.ContractQueryServiceBlockingStub contractsContractQueryServiceBlocking,
            ObligationValidationServiceGrpc.ObligationValidationServiceBlockingStub contractsObligationValidationServiceBlocking,
            RenewalValidationServiceGrpc.RenewalValidationServiceBlockingStub contractsRenewalValidationServiceBlocking,
            JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking,
            TranslationKeyQueryServiceGrpc.TranslationKeyQueryServiceFutureStub localizationTranslationKeyQueryServiceFuture,
            PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceBlockingStub procurementPurchaseOrderCalculationServiceBlocking,
            PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking,
            ProcurementRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.auditAuditTrailAdminServiceBlocking = auditAuditTrailAdminServiceBlocking;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.contractsContractQueryServiceBlocking = contractsContractQueryServiceBlocking;
        this.contractsObligationValidationServiceBlocking = contractsObligationValidationServiceBlocking;
        this.contractsRenewalValidationServiceBlocking = contractsRenewalValidationServiceBlocking;
        this.ledgerJournalEntryHistoryServiceBlocking = ledgerJournalEntryHistoryServiceBlocking;
        this.localizationTranslationKeyQueryServiceFuture = localizationTranslationKeyQueryServiceFuture;
        this.procurementPurchaseOrderCalculationServiceBlocking = procurementPurchaseOrderCalculationServiceBlocking;
        this.procurementPurchaseRequisitionCalculationServiceBlocking = procurementPurchaseRequisitionCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierRiskValidationServiceBlocking = suppliersSupplierRiskValidationServiceBlocking;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    /**
     * Validates a purchase requisition.
     */
    public ValidatePurchaseRequisitionResponse validatePurchaseRequisition(ValidatePurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidatePurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = (SimulatePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulatePurchaseOrderRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getPurchaseRequisition().getBillingAddress().getCountryCode())
                                .setLocality(request.getPurchaseRequisition().getBillingAddress().getLocality())
                                .setRecipient(request.getPurchaseRequisition().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulatePurchaseOrder(outbound0);

        var outbound1 = ScoreMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setCard(MandateCardSettlement.newBuilder()
                                .setHolderName(reply0.getPurchaseOrder().getCard().getHolderName())
                                .build())
                        .build())
                .build();
        var reply1 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreMandate(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidatePurchaseRequisition", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return ValidatePurchaseRequisitionResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setCounterparty(request.getPurchaseRequisition().getCounterparty())
                        .putAllMetadata(reply1.getMandate().getMetadataMap())
                        .setNotes(reply0.getPurchaseOrder().getNotes())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .setTotal(reply1.getMandate().getTotal())
                        .build())
                .build();
    }

    /**
     * Previews a purchase requisition.
     */
    public PreviewPurchaseRequisitionResponse previewPurchaseRequisition(PreviewPurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewPurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var results0 = new ArrayList<QuotePurchaseOrderResponse>();
        for (var element : request.getPurchaseRequisition().getLinesList()) {
            QuotePurchaseOrderRequest itemRequest = QuotePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .addLines(PurchaseOrderLine.newBuilder()
                                .setTax(PurchaseOrderLineTax.newBuilder()
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build();
            results0.add(procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePurchaseOrder(itemRequest));
        }
        var reply0 = results0.isEmpty() ? QuotePurchaseOrderResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = EstimatePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPurchaseRequisition().getAudit().getUpdatedAt())
                                .build())
                        .setCard(PurchaseOrderCardSettlement.newBuilder()
                                .setNetwork(switch (request.getPurchaseRequisition().getSettlementCase()) {
                                    case CARD -> request.getPurchaseRequisition().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePurchaseOrder(outbound1);

        var outbound2 = RepairAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPurchaseOrder().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getPurchaseOrder().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getPurchaseRequisition().getBillingAddress().getRegion())
                                .build())
                        .setCard(AuditTrailCardSettlement.newBuilder()
                                .setNetwork(switch (reply0.getPurchaseOrder().getSettlementCase()) {
                                    case CARD -> reply0.getPurchaseOrder().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RepairAuditTrailRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply2 = auditAuditTrailAdminServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).repairAuditTrail(outbound2);

        var outbound3 = (NormalizeRenewalRequest.newBuilder()
                .setContext(request.getContext())
                .setRenewal(Renewal.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(reply2.getAuditTrail().getBillingAddress().getPostalCode())
                                .build())
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getContact().getName())
                                        .build())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply3 = contractsRenewalValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeRenewal(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewPurchaseRequisition", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return PreviewPurchaseRequisitionResponse.newBuilder()
                .setOutcome(PreviewPurchaseRequisitionResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getPurchaseOrder().getBillingAddress().getRecipient())
                                .build())
                        .putAllMetadata(reply1.getPurchaseOrder().getMetadataMap())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .setStatus(request.getPurchaseRequisition().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks a purchase requisition.
     */
    public CheckPurchaseRequisitionEligibilityResponse checkPurchaseRequisitionEligibility(CheckPurchaseRequisitionEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckPurchaseRequisitionEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = SummarizeContractsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeContractsRequest.Details.newBuilder()
                        .putAllFilters(request.getPurchaseRequisition().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> contractsContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeContracts(outbound0), rpcExecutor);

        var outbound1 = GetPurchaseOrderRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrderId(request.getPurchaseRequisition().getCounterparty().getPurchaseOrderId())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getPurchaseOrderRates(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckPurchaseRequisitionEligibility", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return CheckPurchaseRequisitionEligibilityResponse.newBuilder()
                .setOutcome(CheckPurchaseRequisitionEligibilityResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setIssuedAt(reply1.getPurchaseOrder().getIssuedAt())
                        .setNotes(request.getPurchaseRequisition().getNotes())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Normalizes a purchase requisition.
     */
    public NormalizePurchaseRequisitionResponse normalizePurchaseRequisition(NormalizePurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizePurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = GetPurchaseOrderRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrderId(request.getPurchaseRequisition().getCounterparty().getPurchaseOrderId())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getPurchaseOrderRates(value));

        var outbound1 = CalculatePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculatePurchaseOrderRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getPurchaseRequisition().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getPurchaseRequisition().getAudit().getUpdatedBy())
                                .build())
                        .setBank(PurchaseOrderBankSettlement.newBuilder()
                                .setAccountHolder(reply0.getPurchaseOrder().getBank().getAccountHolder())
                                .setBic(request.getPurchaseRequisition().getBank().getBic())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePurchaseOrder(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = CheckSupplierRiskEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckSupplierRiskEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPurchaseOrder().getAudit().getCreatedBy())
                                .build())
                        .setCustomer(SupplierContractRef.newBuilder()
                                .setDisplayName(reply0.getPurchaseOrder().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        Function<CheckSupplierRiskEligibilityRequest, CheckSupplierRiskEligibilityResponse> operation2 = suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::checkSupplierRiskEligibility;
        var reply2 = operation2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizePurchaseRequisition", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return NormalizePurchaseRequisitionResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPurchaseOrder().getBillingAddress().getCountryCode())
                                .build())
                        .setCounterparty(request.getPurchaseRequisition().getCounterparty())
                        .setDueAt(reply2.getSupplierRisk().getSla().getDueAt())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .setTotal(reply1.getPurchaseOrder().getTotal())
                        .build())
                .build();
    }

    /**
     * Explains a purchase requisition.
     */
    public ExplainPurchaseRequisitionDecisionResponse explainPurchaseRequisitionDecision(ExplainPurchaseRequisitionDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainPurchaseRequisitionDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = ComparePurchaseOrderOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getPurchaseRequisition().getBillingAddress().getLocality())
                                .build())
                        .setCard(PurchaseOrderCardSettlement.newBuilder()
                                .setLast4(switch (request.getPurchaseRequisition().getSettlementCase()) {
                                    case CARD -> request.getPurchaseRequisition().getCard().getLast4();
                                    default -> "";
                                })
                                .setNetwork(switch (request.getPurchaseRequisition().getSettlementCase()) {
                                    case CARD -> request.getPurchaseRequisition().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePurchaseOrderOptions(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainPurchaseRequisitionDecision", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return ExplainPurchaseRequisitionDecisionResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPurchaseOrder().getBillingAddress().getCountryCode())
                                .build())
                        .setCounterparty(request.getPurchaseRequisition().getCounterparty())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .build();
    }

    /**
     * Scores a purchase requisition.
     */
    public ScorePurchaseRequisitionResponse scorePurchaseRequisition(ScorePurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScorePurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = EstimatePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .setCard(PurchaseRequisitionCardSettlement.newBuilder()
                                .setLast4(switch (request.getPurchaseRequisition().getSettlementCase()) {
                                    case CARD -> request.getPurchaseRequisition().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePurchaseRequisition(outbound0);

        var outbound1 = RecordJournalEntryVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordJournalEntryVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPurchaseRequisition().getBillingAddress().getCountryCode())
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .setCard(JournalEntryCardSettlement.newBuilder()
                                .setHolderName(switch (request.getPurchaseRequisition().getSettlementCase()) {
                                    case CARD -> request.getPurchaseRequisition().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordJournalEntryVersion(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScorePurchaseRequisition", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return ScorePurchaseRequisitionResponse.newBuilder()
                .setBreakdown(ScorePurchaseRequisitionResponse.Breakdown.newBuilder()
                        .setTotal(reply0.getPurchaseRequisition().getTotal())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setCounterparty(request.getPurchaseRequisition().getCounterparty())
                        .setNotes(reply0.getPurchaseRequisition().getNotes())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .setTotal(reply1.getJournalEntry().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Lints a purchase requisition.
     */
    public LintPurchaseRequisitionsResponse lintPurchaseRequisitions(LintPurchaseRequisitionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintPurchaseRequisitions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        List<ComparePurchaseOrderOptionsResponse> results0 = request.getPurchaseRequisition().getLinesList().stream()
                .map(element -> ComparePurchaseOrderOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .addLines(PurchaseOrderLine.newBuilder()
                                .setTax(PurchaseOrderLineTax.newBuilder()
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePurchaseOrderOptions(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ComparePurchaseOrderOptionsResponse.getDefaultInstance()
                : results0.getLast();

        var builder1 = ListAuditEventsRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setFilter(ListAuditEventsRequest.Filter.newBuilder()
                .setCreatedAfter(request.getPurchaseRequisition().getAudit().getUpdatedAt())
                .putAllLabels(request.getAnnotationsMap())
                .setRegion(request.getPurchaseRequisition().getBillingAddress().getRegion())
                .build());
        var outbound1 = builder1.build();
        var reply1 = auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditEvents(outbound1);

        var builder2 = ExplainObligationDecisionRequest.newBuilder();
        builder2.setContext(request.getContext());
        builder2.setObligation(Obligation.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedAt(request.getPurchaseRequisition().getAudit().getUpdatedAt())
                        .build())
                .setRequester(ContactInfo.newBuilder()
                        .setName(request.getDetails().getContact().getName())
                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                        .build())
                .build());
        var outbound2 = builder2.build();
        var reply2 = contractsObligationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainObligationDecision(outbound2);

        var outbound3 = CountTranslationKeysRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountTranslationKeysRequest.Filter.newBuilder()
                        .setCreatedAfter(reply2.getObligation().getSla().getDueAt())
                        .setRegion(request.getPurchaseRequisition().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending3 = localizationTranslationKeyQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).countTranslationKeys(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintPurchaseRequisitions", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return LintPurchaseRequisitionsResponse.newBuilder()
                .setOutcome(LintPurchaseRequisitionsResponse.Outcome.newBuilder()
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getObligation().getAudit().getEtag())
                                .build())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .setStatus(request.getPurchaseRequisition().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a purchase requisition.
     */
    public GetPurchaseRequisitionRulesResponse getPurchaseRequisitionRules(GetPurchaseRequisitionRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPurchaseRequisitionRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getPurchaseRequisitionId(), "GetPurchaseRequisitionRules");

        var outbound0 = CheckVenueEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckVenueEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkVenueEligibility(value));
        return GetPurchaseRequisitionRulesResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
