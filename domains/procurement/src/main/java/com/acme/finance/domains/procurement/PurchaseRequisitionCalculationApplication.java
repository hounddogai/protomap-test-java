package com.acme.finance.domains.procurement;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesResponse;
import com.acme.banking.v1.ExplainMandateCalculationRequest;
import com.acme.banking.v1.ExplainMandateCalculationResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetBankFunding;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineAttachment;
import com.acme.budgeting.v1.BudgetLineBankSettlement;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.CalculateBudgetRequest;
import com.acme.budgeting.v1.CalculateBudgetResponse;
import com.acme.budgeting.v1.CompareBudgetLineOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetLineOptionsResponse;
import com.acme.budgeting.v1.NormalizeBudgetLineRequest;
import com.acme.budgeting.v1.NormalizeBudgetLineResponse;
import com.acme.budgeting.v1.ScoreBudgetLineRequest;
import com.acme.budgeting.v1.ScoreBudgetLineResponse;
import com.acme.certificates.v1.Certificate;
import com.acme.certificates.v1.CertificateAttachment;
import com.acme.certificates.v1.CertificateCalculationServiceGrpc;
import com.acme.certificates.v1.CompareCertificateOptionsRequest;
import com.acme.certificates.v1.CompareCertificateOptionsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CompareControlOptionsRequest;
import com.acme.compliance.v1.CompareControlOptionsResponse;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagDiscountAction;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.LintFeatureFlagsRequest;
import com.acme.featureflags.v1.LintFeatureFlagsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.fuel.v1.CountFuelCardsRequest;
import com.acme.fuel.v1.CountFuelCardsResponse;
import com.acme.fuel.v1.EstimateFuelCardRequest;
import com.acme.fuel.v1.EstimateFuelCardResponse;
import com.acme.fuel.v1.FuelCard;
import com.acme.fuel.v1.FuelCardBankFunding;
import com.acme.fuel.v1.FuelCardCalculationServiceGrpc;
import com.acme.fuel.v1.FuelCardLimit;
import com.acme.fuel.v1.FuelCardQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryLine;
import com.acme.ledger.v1.JournalEntryLineTax;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LintJournalEntriesRequest;
import com.acme.ledger.v1.LintJournalEntriesResponse;
import com.acme.ledger.v1.PreviewJournalEntryRequest;
import com.acme.ledger.v1.PreviewJournalEntryResponse;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.metrics.v1.BrowseMetricSeriesesRequest;
import com.acme.metrics.v1.BrowseMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.procurement.v1.CalculatePurchaseRequisitionRequest;
import com.acme.procurement.v1.CalculatePurchaseRequisitionResponse;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsRequest;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsResponse;
import com.acme.procurement.v1.EstimatePurchaseRequisitionRequest;
import com.acme.procurement.v1.EstimatePurchaseRequisitionResponse;
import com.acme.procurement.v1.ExplainPurchaseRequisitionCalculationRequest;
import com.acme.procurement.v1.ExplainPurchaseRequisitionCalculationResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionRatesRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionRatesResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.QuotePurchaseRequisitionRequest;
import com.acme.procurement.v1.QuotePurchaseRequisitionResponse;
import com.acme.procurement.v1.RecalculatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.RecalculatePurchaseRequisitionsResponse;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierNationalId;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.UpdateSupplierRequest;
import com.acme.suppliers.v1.UpdateSupplierResponse;
import com.acme.webhooks.v1.FindWebhookEndpointsRequest;
import com.acme.webhooks.v1.FindWebhookEndpointsResponse;
import com.acme.webhooks.v1.WebhookEndpointQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
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
 * Computes the figures of a purchase requisition, such as totals, taxes, and estimates. Coordinates validation,
 * durable audit writes, and downstream operations.
 */
@Service
public class PurchaseRequisitionCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(PurchaseRequisitionCalculationApplication.class);

    private final FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync;

    private final AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking;

    private final BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking;

    private final BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking;

    private final CertificateCalculationServiceGrpc.CertificateCalculationServiceBlockingStub certificatesCertificateCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking;

    private final FuelCardCalculationServiceGrpc.FuelCardCalculationServiceBlockingStub fuelFuelCardCalculationServiceBlocking;

    private final FuelCardQueryServiceGrpc.FuelCardQueryServiceBlockingStub fuelFuelCardQueryServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking;

    private final PurchaseRequisitionCalculationRequestMapper mapper;

    private final ProcurementRepository repository;

    private final Executor rpcExecutor;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    private final WebhookEndpointQueryServiceGrpc.WebhookEndpointQueryServiceBlockingStub webhooksWebhookEndpointQueryServiceBlocking;

    public PurchaseRequisitionCalculationApplication(
            FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync,
            AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking,
            BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking,
            BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking,
            CertificateCalculationServiceGrpc.CertificateCalculationServiceBlockingStub certificatesCertificateCalculationServiceBlocking,
            ChannelRegistry channels,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking,
            FuelCardCalculationServiceGrpc.FuelCardCalculationServiceBlockingStub fuelFuelCardCalculationServiceBlocking,
            FuelCardQueryServiceGrpc.FuelCardQueryServiceBlockingStub fuelFuelCardQueryServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking,
            PurchaseRequisitionCalculationRequestMapper mapper,
            ProcurementRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking,
            WebhookEndpointQueryServiceGrpc.WebhookEndpointQueryServiceBlockingStub webhooksWebhookEndpointQueryServiceBlocking) {
        this.assetsFixedAssetServiceAsync = assetsFixedAssetServiceAsync;
        this.auditAuditTrailNotificationServiceBlocking = auditAuditTrailNotificationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.budgetingBudgetCalculationServiceBlocking = budgetingBudgetCalculationServiceBlocking;
        this.budgetingBudgetLineCalculationServiceBlocking = budgetingBudgetLineCalculationServiceBlocking;
        this.budgetingBudgetLineValidationServiceBlocking = budgetingBudgetLineValidationServiceBlocking;
        this.certificatesCertificateCalculationServiceBlocking = certificatesCertificateCalculationServiceBlocking;
        this.channels = channels;
        this.featureflagsFeatureFlagValidationServiceBlocking = featureflagsFeatureFlagValidationServiceBlocking;
        this.fuelFuelCardCalculationServiceBlocking = fuelFuelCardCalculationServiceBlocking;
        this.fuelFuelCardQueryServiceBlocking = fuelFuelCardQueryServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.ledgerJournalEntryValidationServiceBlocking = ledgerJournalEntryValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
        this.webhooksWebhookEndpointQueryServiceBlocking = webhooksWebhookEndpointQueryServiceBlocking;
    }

    /**
     * Calculates a purchase requisition.
     */
    public CalculatePurchaseRequisitionResponse calculatePurchaseRequisition(CalculatePurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculatePurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = CreateSupplierRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplier(Supplier.newBuilder()
                        .setNationalId(SupplierNationalId.newBuilder()
                                .setNumber(request.getPurchaseRequisition().getCounterparty().getNumber())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getPurchaseRequisition().getBillingAddress().getLinesList())
                                .setPostalCode(request.getPurchaseRequisition().getBillingAddress().getPostalCode())
                                .setRegion(request.getPurchaseRequisition().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.suppliers.v1.SupplierService/CreateSupplier", outbound0);
        var reply0 = suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createSupplier(command0.payload());

        var outbound1 = CalculateBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBank(BudgetBankFunding.newBuilder()
                                .setBic(switch (request.getPurchaseRequisition().getSettlementCase()) {
                                    case BANK -> request.getPurchaseRequisition().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CalculateBudgetRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getPurchaseRequisition().getBillingAddress().getCountryCode())
                                .setPostalCode(reply0.getSupplier().getPrimaryAddress().getPostalCode())
                                .setRegion(reply0.getSupplier().getPrimaryAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply1 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateBudget(outbound1);

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
                                .setCreatedAt(request.getPurchaseRequisition().getAudit().getCreatedAt())
                                .setCreatedBy(request.getPurchaseRequisition().getAudit().getCreatedBy())
                                .setEtag(request.getPurchaseRequisition().getAudit().getEtag())
                                .setUpdatedAt(request.getPurchaseRequisition().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculatePurchaseRequisition", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return CalculatePurchaseRequisitionResponse.newBuilder()
                .setBreakdown(CalculatePurchaseRequisitionResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getSupplier().getAudit().getEtag())
                                .build())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .build();
    }

    /**
     * Estimates a purchase requisition.
     */
    public EstimatePurchaseRequisitionResponse estimatePurchaseRequisition(EstimatePurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimatePurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        List<PreviewJournalEntryResponse> results0 = request.getPurchaseRequisition().getLinesList().stream()
                .map(element -> PreviewJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setJournalEntry(JournalEntry.newBuilder()
                        .addLines(JournalEntryLine.newBuilder()
                                .setTax(JournalEntryLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewJournalEntry(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? PreviewJournalEntryResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CompareBudgetLineOptionsRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPurchaseRequisition().getAudit().getUpdatedAt())
                                .build())
                        .setBank(BudgetLineBankSettlement.newBuilder()
                                .setBic(request.getPurchaseRequisition().getBank().getBic())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getPurchaseRequisition().getBillingAddress().getLinesList())
                                .setPostalCode(reply0.getJournalEntry().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareBudgetLineOptions(value));

        var outbound2 = (ExplainMandateCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getBudgetLine().getAudit().getUpdatedBy())
                                .build())
                        .setCard(MandateCardSettlement.newBuilder()
                                .setNetwork(reply0.getJournalEntry().getCard().getNetwork())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainMandateCalculation(outbound2);

        var futures3 = request.getPurchaseRequisition().getLinesList().stream()
                .map(element -> FindWebhookEndpointsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindWebhookEndpointsRequest.Filter.newBuilder()
                        .setOwnerId(element.getLineId())
                        .setRegion(reply2.getMandate().getBillingAddress().getRegion())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> webhooksWebhookEndpointQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findWebhookEndpoints(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? FindWebhookEndpointsResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimatePurchaseRequisition", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return EstimatePurchaseRequisitionResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .putAllMetadata(request.getPurchaseRequisition().getMetadataMap())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Quotes a purchase requisition.
     */
    public QuotePurchaseRequisitionResponse quotePurchaseRequisition(QuotePurchaseRequisitionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuotePurchaseRequisition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var futures0 = request.getPurchaseRequisition().getLinesList().stream()
                .map(element -> EstimateFuelCardRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateFuelCardRequest.Details.newBuilder()
                        .setBudget(element.getTax().getAmount())
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .setFuelCard(FuelCard.newBuilder()
                        .setBank(FuelCardBankFunding.newBuilder()
                                .setBic(request.getPurchaseRequisition().getBank().getBic())
                                .build())
                        .addLimits(FuelCardLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> fuelFuelCardCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFuelCard(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? EstimateFuelCardResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CountFuelCardsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountFuelCardsRequest.Filter.newBuilder()
                        .putAllLabels(reply0.getFuelCard().getLabelsMap())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> fuelFuelCardQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countFuelCards(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuotePurchaseRequisition", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return QuotePurchaseRequisitionResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setNotes(request.getPurchaseRequisition().getNotes())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Compares a purchase requisition.
     */
    public ComparePurchaseRequisitionOptionsResponse comparePurchaseRequisitionOptions(ComparePurchaseRequisitionOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ComparePurchaseRequisitionOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        List<LintFeatureFlagsResponse> results0 = request.getPurchaseRequisition().getLinesList().stream()
                .map(element -> LintFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPurchaseRequisition().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getPurchaseRequisition().getAudit().getUpdatedAt())
                                .build())
                        .setDescription(element.getDescription())
                        .setDiscount(FeatureFlagDiscountAction.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintFeatureFlags(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? LintFeatureFlagsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CompareControlOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareControlOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getPurchaseRequisition().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.compliance.v1.ControlCalculationService");
        var selected1 = ControlCalculationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.compareControlOptions(outbound1);

        var outbound2 = mapper.comparePurchaseRequisitionOptionsStage2(request, reply0);
        var reply2 = suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateSupplier(outbound2);

        var outbound3 = LintJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getControl().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getFeatureFlag().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getPurchaseRequisition().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getPurchaseRequisition().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var adapter3 = new Function<LintJournalEntriesRequest, LintJournalEntriesResponse>() {
            @Override
            public LintJournalEntriesResponse apply(LintJournalEntriesRequest input) {
                return ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintJournalEntries(input);
            }
        };
        var reply3 = adapter3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ComparePurchaseRequisitionOptions", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return ComparePurchaseRequisitionOptionsResponse.newBuilder()
                .setBreakdown(ComparePurchaseRequisitionOptionsResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply2.getSupplier().getPrimaryAddress().getPostalCode())
                                .build())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setMessage(reply3.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalculates a purchase requisition.
     */
    public RecalculatePurchaseRequisitionsResponse recalculatePurchaseRequisitions(RecalculatePurchaseRequisitionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculatePurchaseRequisitions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = BrowseMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPurchaseRequisition().getAudit().getUpdatedAt())
                        .setOwnerId(request.getPurchaseRequisition().getPurchaseRequisitionId())
                        .setRegion(request.getPurchaseRequisition().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.metrics.v1.MetricSeriesQueryService");
        var selected0 = MetricSeriesQueryServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.browseMetricSerieses(outbound0);

        var outbound1 = ScoreBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBank(BudgetLineBankSettlement.newBuilder()
                                .setAccountHolder(request.getPurchaseRequisition().getBank().getAccountHolder())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getPurchaseRequisition().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var adapter1 = new Function<ScoreBudgetLineRequest, ScoreBudgetLineResponse>() {
            @Override
            public ScoreBudgetLineResponse apply(ScoreBudgetLineRequest input) {
                return budgetingBudgetLineValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreBudgetLine(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var futures2 = request.getPurchaseRequisition().getAttachmentsList().stream()
                .map(element -> CompareCertificateOptionsRequest.newBuilder()
                .setCertificate(Certificate.newBuilder()
                        .addAttachments(CertificateAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setFileName(element.getFileName())
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> certificatesCertificateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCertificateOptions(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? CompareCertificateOptionsResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculatePurchaseRequisitions", request.getPurchaseRequisition().getNumber());
        return RecalculatePurchaseRequisitionsResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getBudgetLine().getAudit().getCreatedAt())
                                .build())
                        .putAllMetadata(request.getPurchaseRequisition().getMetadataMap())
                        .setNumber(reply1.getBudgetLine().getNumber())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Explains a purchase requisition.
     */
    public ExplainPurchaseRequisitionCalculationResponse explainPurchaseRequisitionCalculation(ExplainPurchaseRequisitionCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainPurchaseRequisitionCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePurchaseRequisition(request.getContext().getTenantId(), request.getPurchaseRequisition());

        var outbound0 = ScheduleFiscalPeriodExportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ScheduleFiscalPeriodExportRequest.Details.newBuilder()
                        .putAllFilters(request.getPurchaseRequisition().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleFiscalPeriodExport(outbound0), rpcExecutor);

        var outbound1 = SubscribeAuditTrailUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeAuditTrailUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setName(request.getDetails().getContact().getName())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> auditAuditTrailNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeAuditTrailUpdates(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var futures2 = request.getPurchaseRequisition().getAttachmentsList().stream()
                .map(element -> NormalizeBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .addAttachments(BudgetLineAttachment.newBuilder()
                                .setFileName(element.getFileName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> budgetingBudgetLineValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeBudgetLine(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? NormalizeBudgetLineResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPurchaseRequisition().getPurchaseRequisitionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainPurchaseRequisitionCalculation", request.getPurchaseRequisition().getPurchaseRequisitionId());
        return ExplainPurchaseRequisitionCalculationResponse.newBuilder()
                .setOutcome(ExplainPurchaseRequisitionCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setIssuedAt(request.getPurchaseRequisition().getIssuedAt())
                        .putAllMetadata(reply1.getAuditTrail().getMetadataMap())
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a purchase requisition.
     */
    public GetPurchaseRequisitionRatesResponse getPurchaseRequisitionRates(GetPurchaseRequisitionRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPurchaseRequisitionRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PurchaseRequisition", request.getPurchaseRequisitionId(), "GetPurchaseRequisitionRates");
        return GetPurchaseRequisitionRatesResponse.newBuilder()
                .setPurchaseRequisition(PurchaseRequisition.newBuilder()
                        .setPurchaseRequisitionId(request.getPurchaseRequisitionId())
                        .build())
                .build();
    }
}
