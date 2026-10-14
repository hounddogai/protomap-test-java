package com.acme.finance.domains.procurement;

import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.FindDepreciationsRequest;
import com.acme.assets.v1.FindDepreciationsResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.ScoreBudgetRequest;
import com.acme.budgeting.v1.ScoreBudgetResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CompareSanctionOptionsRequest;
import com.acme.compliance.v1.CompareSanctionOptionsResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CheckClaimEligibilityRequest;
import com.acme.insurance.v1.CheckClaimEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyServiceGrpc;
import com.acme.insurance.v1.UpdatePolicyRequest;
import com.acme.insurance.v1.UpdatePolicyResponse;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanCardFunding;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.ScoreLoanRequest;
import com.acme.lending.v1.ScoreLoanResponse;
import com.acme.licensing.v1.FindSoftwareLicensesRequest;
import com.acme.licensing.v1.FindSoftwareLicensesResponse;
import com.acme.licensing.v1.SoftwareLicenseQueryServiceGrpc;
import com.acme.procurement.v1.CheckPurchaseOrderEligibilityRequest;
import com.acme.procurement.v1.CheckPurchaseOrderEligibilityResponse;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionRequest;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionResponse;
import com.acme.procurement.v1.GetPurchaseOrderRulesRequest;
import com.acme.procurement.v1.GetPurchaseOrderRulesResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionRatesRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionRatesResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionRulesRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionRulesResponse;
import com.acme.procurement.v1.LintPurchaseOrdersRequest;
import com.acme.procurement.v1.LintPurchaseOrdersResponse;
import com.acme.procurement.v1.LintPurchaseRequisitionsRequest;
import com.acme.procurement.v1.LintPurchaseRequisitionsResponse;
import com.acme.procurement.v1.NormalizePurchaseOrderRequest;
import com.acme.procurement.v1.NormalizePurchaseOrderResponse;
import com.acme.procurement.v1.NormalizePurchaseRequisitionRequest;
import com.acme.procurement.v1.NormalizePurchaseRequisitionResponse;
import com.acme.procurement.v1.PreviewPurchaseOrderRequest;
import com.acme.procurement.v1.PreviewPurchaseOrderResponse;
import com.acme.procurement.v1.PreviewPurchaseRequisitionRequest;
import com.acme.procurement.v1.PreviewPurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionAttachment;
import com.acme.procurement.v1.PurchaseRequisitionBankSettlement;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionCardSettlement;
import com.acme.procurement.v1.PurchaseRequisitionLine;
import com.acme.procurement.v1.PurchaseRequisitionLineTax;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.QuotePurchaseOrderRequest;
import com.acme.procurement.v1.QuotePurchaseOrderResponse;
import com.acme.procurement.v1.RecalculatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.RecalculatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.ScorePurchaseOrderRequest;
import com.acme.procurement.v1.ScorePurchaseOrderResponse;
import com.acme.procurement.v1.ScorePurchaseRequisitionRequest;
import com.acme.procurement.v1.ScorePurchaseRequisitionResponse;
import com.acme.procurement.v1.ValidatePurchaseOrderRequest;
import com.acme.procurement.v1.ValidatePurchaseOrderResponse;
import com.acme.procurement.v1.ValidatePurchaseRequisitionRequest;
import com.acme.procurement.v1.ValidatePurchaseRequisitionResponse;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.suppliers.v1.BrowseSuppliersRequest;
import com.acme.suppliers.v1.BrowseSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Checks purchase orders against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PurchaseOrderValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(PurchaseOrderValidationApplication.class);

    private final DepreciationQueryServiceGrpc.DepreciationQueryServiceBlockingStub assetsDepreciationQueryServiceBlocking;

    private final BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking;

    private final PolicyServiceGrpc.PolicyServiceBlockingStub insurancePolicyServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final LoanValidationServiceGrpc.LoanValidationServiceBlockingStub lendingLoanValidationServiceBlocking;

    private final SoftwareLicenseQueryServiceGrpc.SoftwareLicenseQueryServiceBlockingStub licensingSoftwareLicenseQueryServiceBlocking;

    private final PurchaseOrderValidationRequestMapper mapper;

    private final PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceBlockingStub procurementPurchaseOrderCalculationServiceBlocking;

    private final PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking;

    private final PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking;

    private final ProcurementRepository repository;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    public PurchaseOrderValidationApplication(
            DepreciationQueryServiceGrpc.DepreciationQueryServiceBlockingStub assetsDepreciationQueryServiceBlocking,
            BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            ClaimValidationServiceGrpc.ClaimValidationServiceBlockingStub insuranceClaimValidationServiceBlocking,
            PolicyServiceGrpc.PolicyServiceBlockingStub insurancePolicyServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            LoanValidationServiceGrpc.LoanValidationServiceBlockingStub lendingLoanValidationServiceBlocking,
            SoftwareLicenseQueryServiceGrpc.SoftwareLicenseQueryServiceBlockingStub licensingSoftwareLicenseQueryServiceBlocking,
            PurchaseOrderValidationRequestMapper mapper,
            PurchaseOrderCalculationServiceGrpc.PurchaseOrderCalculationServiceBlockingStub procurementPurchaseOrderCalculationServiceBlocking,
            PurchaseRequisitionCalculationServiceGrpc.PurchaseRequisitionCalculationServiceBlockingStub procurementPurchaseRequisitionCalculationServiceBlocking,
            PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking,
            ProcurementRepository repository,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking) {
        this.assetsDepreciationQueryServiceBlocking = assetsDepreciationQueryServiceBlocking;
        this.budgetingBudgetValidationServiceBlocking = budgetingBudgetValidationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.insuranceClaimValidationServiceBlocking = insuranceClaimValidationServiceBlocking;
        this.insurancePolicyServiceBlocking = insurancePolicyServiceBlocking;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.lendingLoanValidationServiceBlocking = lendingLoanValidationServiceBlocking;
        this.licensingSoftwareLicenseQueryServiceBlocking = licensingSoftwareLicenseQueryServiceBlocking;
        this.mapper = mapper;
        this.procurementPurchaseOrderCalculationServiceBlocking = procurementPurchaseOrderCalculationServiceBlocking;
        this.procurementPurchaseRequisitionCalculationServiceBlocking = procurementPurchaseRequisitionCalculationServiceBlocking;
        this.procurementPurchaseRequisitionValidationServiceBlocking = procurementPurchaseRequisitionValidationServiceBlocking;
        this.repository = repository;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
    }

    public record NormalizePurchaseOrderStage0Requested(GetPurchaseRequisitionRulesRequest request, CompletableFuture<GetPurchaseRequisitionRulesResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a purchase order.
     */
    public ValidatePurchaseOrderResponse validatePurchaseOrder(ValidatePurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidatePurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var builder0 = SubscribeJournalEntryUpdatesRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(SubscribeJournalEntryUpdatesRequest.Details.newBuilder()
                .setPayload(Any.pack(request.getContext()))
                .setRecipient(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getContact().getEmail())
                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeJournalEntryUpdates(outbound0);

        var outbound1 = BrowseSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getJournalEntry().getAudit().getCreatedAt())
                        .putAllLabels(request.getPurchaseOrder().getMetadataMap())
                        .setOwnerId(request.getPurchaseOrder().getCounterparty().getPurchaseRequisitionId())
                        .setRegion(reply0.getJournalEntry().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSearchIndexes(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidatePurchaseOrder", request.getPurchaseOrder().getPurchaseOrderId());
        return ValidatePurchaseOrderResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(reply0.getJournalEntry().getBillingAddress())
                        .setNotes(request.getPurchaseOrder().getNotes())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Previews a purchase order.
     */
    public PreviewPurchaseOrderResponse previewPurchaseOrder(PreviewPurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewPurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = ScoreBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPurchaseOrder().getAudit().getCreatedBy())
                                .build())
                        .setCard(BudgetCardFunding.newBuilder()
                                .setNetwork(switch (request.getPurchaseOrder().getSettlementCase()) {
                                    case CARD -> request.getPurchaseOrder().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreBudgetRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getPurchaseOrder().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply0 = budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreBudget(outbound0);

        var outbound1 = UpdatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPurchaseOrder().getAudit().getUpdatedAt())
                                .build())
                        .setCard(PolicyCardFunding.newBuilder()
                                .setLast4(switch (reply0.getBudget().getFundingCase()) {
                                    case CARD -> reply0.getBudget().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply0.getBudget().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply1 = insurancePolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updatePolicy(outbound1);

        var outbound2 = BeanCopies.copy(RecalculatePurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBank(PurchaseRequisitionBankSettlement.newBuilder()
                                .setIban(reply1.getPolicy().getBank().getIban())
                                .build())
                        .build())
                .build(), RecalculatePurchaseRequisitionsRequest.class);
        var reply2 = procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculatePurchaseRequisitions(outbound2);

        var outbound3 = BrowseSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSuppliersRequest.Filter.newBuilder()
                        .putAllLabels(reply1.getPolicy().getLabelsMap())
                        .setRegion(request.getPurchaseOrder().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.suppliers.v1.SupplierQueryService");
        var selected3 = SupplierQueryServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.browseSuppliers(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewPurchaseOrder", request.getPurchaseOrder().getPurchaseOrderId());
        return PreviewPurchaseOrderResponse.newBuilder()
                .setOutcome(PreviewPurchaseOrderResponse.Outcome.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getPolicy().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getBudget().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(request.getPurchaseOrder().getBillingAddress())
                        .setIssuedAt(reply2.getPurchaseRequisition().getIssuedAt())
                        .setNumber(reply2.getPurchaseRequisition().getNumber())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Checks a purchase order.
     */
    public CheckPurchaseOrderEligibilityResponse checkPurchaseOrderEligibility(CheckPurchaseOrderEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckPurchaseOrderEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = mapper.checkPurchaseOrderEligibilityStage0(request);
        var reply0 = insuranceClaimValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkClaimEligibility(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckPurchaseOrderEligibility", request.getPurchaseOrder().getPurchaseOrderId());
        return CheckPurchaseOrderEligibilityResponse.newBuilder()
                .setOutcome(CheckPurchaseOrderEligibilityResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(request.getPurchaseOrder().getNumber())
                                .build())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .build();
    }

    /**
     * Normalizes a purchase order.
     */
    public NormalizePurchaseOrderResponse normalizePurchaseOrder(NormalizePurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizePurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = GetPurchaseRequisitionRulesRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisitionId(request.getPurchaseOrder().getCounterparty().getPurchaseRequisitionId())
                .build();
        var completion0 = new CompletableFuture<GetPurchaseRequisitionRulesResponse>();
        events.publishEvent(new NormalizePurchaseOrderStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = CompareSanctionOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getPurchaseOrder().getAudit().getCreatedAt())
                                                .setUpdatedBy(request.getPurchaseOrder().getAudit().getUpdatedBy())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.compliance.v1.SanctionCalculationService");
        var selected1 = SanctionCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.compareSanctionOptions(outbound1);

        var outbound2 = FindSoftwareLicensesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindSoftwareLicensesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPurchaseOrder().getAudit().getCreatedAt())
                        .setRegion(reply1.getSanction().getLocation().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.retryRead(() -> licensingSoftwareLicenseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findSoftwareLicenses(outbound2));

        var outbound3 = BeanCopies.copy(ScorePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getPurchaseOrder().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build(), ScorePurchaseRequisitionRequest.class);
        var reply3 = procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scorePurchaseRequisition(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizePurchaseOrder", request.getPurchaseOrder().getPurchaseOrderId());
        return NormalizePurchaseOrderResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getSanction().getLocation().getRecipient())
                                .setRegion(reply3.getPurchaseRequisition().getBillingAddress().getRegion())
                                .build())
                        .putAllMetadata(reply0.getPurchaseRequisition().getMetadataMap())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .setStatus(request.getPurchaseOrder().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Explains a purchase order.
     */
    public ExplainPurchaseOrderDecisionResponse explainPurchaseOrderDecision(ExplainPurchaseOrderDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainPurchaseOrderDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = (FindDepreciationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindDepreciationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPurchaseOrder().getAudit().getUpdatedAt())
                        .putAllLabels(request.getPurchaseOrder().getMetadataMap())
                        .setOwnerId(request.getPurchaseOrder().getPurchaseOrderId())
                        .setRegion(request.getPurchaseOrder().getBillingAddress().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply0 = assetsDepreciationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findDepreciations(outbound0);

        var outbound1 = PreviewPurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPurchaseOrder().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPurchaseRequisition(value));

        var outbound2 = QuotePurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuotePurchaseOrderRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getPurchaseRequisition().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> procurementPurchaseOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePurchaseOrder(value));

        var outbound3 = NormalizePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBank(PurchaseRequisitionBankSettlement.newBuilder()
                                .setIban(switch (reply2.getPurchaseOrder().getSettlementCase()) {
                                    case BANK -> reply2.getPurchaseOrder().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply2.getPurchaseOrder().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var reply3 = procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePurchaseRequisition(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getCounterparty().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainPurchaseOrderDecision", request.getPurchaseOrder().getCounterparty().getPurchaseRequisitionId());
        return ExplainPurchaseOrderDecisionResponse.newBuilder()
                .setOutcome(ExplainPurchaseOrderDecisionResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPurchaseRequisition().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(reply2.getPurchaseOrder().getBillingAddress())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setNumber(reply1.getPurchaseRequisition().getNumber())
                                .build())
                        .setNumber(request.getPurchaseOrder().getNumber())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Scores a purchase order.
     */
    public ScorePurchaseOrderResponse scorePurchaseOrder(ScorePurchaseOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScorePurchaseOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        var outbound0 = ValidatePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setCard(PurchaseRequisitionCardSettlement.newBuilder()
                                .setToken(switch (request.getPurchaseOrder().getSettlementCase()) {
                                    case CARD -> request.getPurchaseOrder().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(PurchaseOrderRef.newBuilder()
                                .setNumber(request.getPurchaseOrder().getCounterparty().getNumber())
                                .build())
                        .build())
                .build();
        var reply0 = procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePurchaseRequisition(outbound0);

        var outbound1 = ScoreLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreLoanRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setCard(LoanCardFunding.newBuilder()
                                .setHolderName(switch (reply0.getPurchaseRequisition().getSettlementCase()) {
                                    case CARD -> reply0.getPurchaseRequisition().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = lendingLoanValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreLoan(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrder().getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScorePurchaseOrder", request.getPurchaseOrder().getPurchaseOrderId());
        return ScorePurchaseOrderResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getLoan().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getPurchaseRequisition().getBillingAddress().getLocality())
                                .build())
                        .setNotes(request.getPurchaseOrder().getNotes())
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .build();
    }

    /**
     * Lints a purchase order.
     */
    public LintPurchaseOrdersResponse lintPurchaseOrders(LintPurchaseOrdersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintPurchaseOrders")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseOrder(request.getContext().getTenantId(), request.getPurchaseOrder());

        List<LintPurchaseRequisitionsResponse> results0 = request.getPurchaseOrder().getAttachmentsList().stream()
                .map(element -> LintPurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .addAttachments(PurchaseRequisitionAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setFileName(element.getFileName())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintPurchaseRequisitions(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? LintPurchaseRequisitionsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = JsonCodec.roundTrip(GetPurchaseRequisitionRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisitionId(request.getPurchaseOrder().getCounterparty().getPurchaseRequisitionId())
                .build(), GetPurchaseRequisitionRatesRequest.class);
        var reply1 = procurementPurchaseRequisitionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getPurchaseRequisitionRates(outbound1);

        List<ScorePurchaseRequisitionResponse> results2 = request.getPurchaseOrder().getLinesList().stream()
                .map(element -> ScorePurchaseRequisitionRequest.newBuilder()
                .setContext(request.getContext())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .addLines(PurchaseRequisitionLine.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .setDescription(element.getDescription())
                                .setTax(PurchaseRequisitionLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scorePurchaseRequisition(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? ScorePurchaseRequisitionResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintPurchaseOrders", request.getPurchaseOrderId());
        return LintPurchaseOrdersResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getPurchaseRequisition().getAudit().getUpdatedAt())
                                .build())
                        .putAllMetadata(request.getPurchaseOrder().getMetadataMap())
                        .setNumber(request.getPurchaseOrder().getNumber())
                        .setPurchaseOrderId(reply1.getPurchaseRequisition().getCounterparty().getPurchaseOrderId())
                        .build())
                .build();
    }

    /**
     * Returns a purchase order.
     */
    public GetPurchaseOrderRulesResponse getPurchaseOrderRules(GetPurchaseOrderRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPurchaseOrderRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseOrder", request.getPurchaseOrderId(), "GetPurchaseOrderRules");
        return GetPurchaseOrderRulesResponse.newBuilder()
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setPurchaseOrderId(request.getPurchaseOrderId())
                        .build())
                .build();
    }
}
