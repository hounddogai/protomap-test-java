package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditEventMeasurement;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.RecordAuditEventVersionRequest;
import com.acme.audit.v1.RecordAuditEventVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineAttachment;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.NormalizeBudgetLineRequest;
import com.acme.budgeting.v1.NormalizeBudgetLineResponse;
import com.acme.budgeting.v1.ScoreBudgetLineRequest;
import com.acme.budgeting.v1.ScoreBudgetLineResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.SimulateUnderwritingRequest;
import com.acme.insurance.v1.SimulateUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.ledger.v1.CompareLedgerAccountVersionsRequest;
import com.acme.ledger.v1.CompareLedgerAccountVersionsResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.localization.v1.ExplainTranslationKeyCalculationRequest;
import com.acme.localization.v1.ExplainTranslationKeyCalculationResponse;
import com.acme.localization.v1.TranslationKey;
import com.acme.localization.v1.TranslationKeyCalculationServiceGrpc;
import com.acme.metrics.v1.ListMetricSeriesesRequest;
import com.acme.metrics.v1.ListMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.routing.v1.Route;
import com.acme.routing.v1.RouteBankSettlement;
import com.acme.routing.v1.RouteCalculationServiceGrpc;
import com.acme.routing.v1.SimulateRouteRequest;
import com.acme.routing.v1.SimulateRouteResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.CalculateBankTransferRequest;
import com.acme.treasury.v1.CalculateBankTransferResponse;
import com.acme.treasury.v1.CalculateCashPositionRequest;
import com.acme.treasury.v1.CalculateCashPositionResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionBankFunding;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionCardFunding;
import com.acme.treasury.v1.CashPositionLimit;
import com.acme.treasury.v1.CompareBankTransferOptionsRequest;
import com.acme.treasury.v1.CompareBankTransferOptionsResponse;
import com.acme.treasury.v1.EstimateBankTransferRequest;
import com.acme.treasury.v1.EstimateBankTransferResponse;
import com.acme.treasury.v1.EstimateCashPositionRequest;
import com.acme.treasury.v1.EstimateCashPositionResponse;
import com.acme.treasury.v1.ExplainBankTransferCalculationRequest;
import com.acme.treasury.v1.ExplainBankTransferCalculationResponse;
import com.acme.treasury.v1.ExplainCashPositionCalculationRequest;
import com.acme.treasury.v1.ExplainCashPositionCalculationResponse;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.GetBankTransferRatesRequest;
import com.acme.treasury.v1.GetBankTransferRatesResponse;
import com.acme.treasury.v1.QuoteCashPositionRequest;
import com.acme.treasury.v1.QuoteCashPositionResponse;
import com.acme.treasury.v1.RecalculateBankTransfersRequest;
import com.acme.treasury.v1.RecalculateBankTransfersResponse;
import com.acme.treasury.v1.SimulateBankTransferRequest;
import com.acme.treasury.v1.SimulateBankTransferResponse;
import com.acme.venues.v1.CheckVenueEligibilityRequest;
import com.acme.venues.v1.CheckVenueEligibilityResponse;
import com.acme.venues.v1.NormalizeVenueRequest;
import com.acme.venues.v1.NormalizeVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a bank transfer, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class BankTransferCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(BankTransferCalculationApplication.class);

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking;

    private final ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync;

    private final UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final TranslationKeyCalculationServiceGrpc.TranslationKeyCalculationServiceBlockingStub localizationTranslationKeyCalculationServiceBlocking;

    private final BankTransferCalculationRequestMapper mapper;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final TreasuryRepository repository;

    private final RouteCalculationServiceGrpc.RouteCalculationServiceBlockingStub routingRouteCalculationServiceBlocking;

    private final Executor rpcExecutor;

    private final CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public BankTransferCalculationApplication(
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking,
            ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync,
            UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            TranslationKeyCalculationServiceGrpc.TranslationKeyCalculationServiceBlockingStub localizationTranslationKeyCalculationServiceBlocking,
            BankTransferCalculationRequestMapper mapper,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            TreasuryRepository repository,
            RouteCalculationServiceGrpc.RouteCalculationServiceBlockingStub routingRouteCalculationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.budgetingBudgetLineValidationServiceBlocking = budgetingBudgetLineValidationServiceBlocking;
        this.contractsContractServiceAsync = contractsContractServiceAsync;
        this.insuranceUnderwritingCalculationServiceBlocking = insuranceUnderwritingCalculationServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.localizationTranslationKeyCalculationServiceBlocking = localizationTranslationKeyCalculationServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.repository = repository;
        this.routingRouteCalculationServiceBlocking = routingRouteCalculationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.treasuryCashPositionCalculationServiceBlocking = treasuryCashPositionCalculationServiceBlocking;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    /**
     * Calculates a bank transfer.
     */
    public CalculateBankTransferResponse calculateBankTransfer(CalculateBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = (NormalizeVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getBankTransfer().getBillingAddress().getLinesList())
                                .setLocality(request.getBankTransfer().getBillingAddress().getLocality())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getBankTransfer().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeVenue(outbound0);

        var outbound1 = CalculateCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setHolder(FxHedgeRef.newBuilder()
                                .setDisplayName(request.getBankTransfer().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CalculateCashPositionRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getBankTransfer().getBillingAddress().getCountryCode())
                                .setRegion(switch (reply0.getVenue().getLocatorCase()) {
                                    case ADDRESS -> reply0.getVenue().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCashPosition(outbound1);

        var outbound2 = SimulateUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getVenue().getOwner().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getBankTransfer().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var reply2 = insuranceUnderwritingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateUnderwriting(outbound2);

        var outbound3 = BatchCreateContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateContractsRequest.StreamHeader.newBuilder()
                        .setSource(request.getBankTransfer().getBankTransferId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getBankTransfer().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateContractsResponse.getDefaultInstance());
        var input3 = contractsContractServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateContracts(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBankTransfer().getAudit().getCreatedAt())
                                .setCreatedBy(request.getBankTransfer().getAudit().getCreatedBy())
                                .setEtag(request.getBankTransfer().getAudit().getEtag())
                                .setUpdatedAt(request.getBankTransfer().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input4 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateBankTransfer", request.getBankTransfer().getNumber());
        return CalculateBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getUnderwriting().getAudit().getUpdatedBy())
                                .build())
                        .setBankTransferId(request.getBankTransferId())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getVenue().getOwner().getEmail())
                                .build())
                        .setDueAt(request.getBankTransfer().getDueAt())
                        .build())
                .setBreakdown(CalculateBankTransferResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply1.getBreakdown().getCurrencyCode())
                        .setTotal(reply2.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Estimates a bank transfer.
     */
    public EstimateBankTransferResponse estimateBankTransfer(EstimateBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = mapper.estimateBankTransferStage0(request);
        var reply0 = treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCashPositionCalculation(outbound0);

        var outbound1 = (ListMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListMetricSeriesesRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply1 = metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listMetricSerieses(outbound1);

        var outbound2 = ExplainTranslationKeyCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setTranslationKey(TranslationKey.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .setLocality(request.getBankTransfer().getBillingAddress().getLocality())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getCashPosition().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply2 = localizationTranslationKeyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainTranslationKeyCalculation(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateBankTransfer", request.getBankTransfer().getBankTransferId());
        return EstimateBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getTranslationKey().getAudit().getUpdatedBy())
                                .build())
                        .setBankTransferId(request.getBankTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply2.getTranslationKey().getAddress().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getCashPosition().getHolder().getDisplayName())
                                .build())
                        .build())
                .setBreakdown(EstimateBankTransferResponse.Breakdown.newBuilder()
                        .setTotal(request.getBankTransfer().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Compares a bank transfer.
     */
    public CompareBankTransferOptionsResponse compareBankTransferOptions(CompareBankTransferOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareBankTransferOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = mapper.compareBankTransferOptionsStage0(request);
        var reply0 = treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCashPosition(outbound0);

        var outbound1 = SimulateRouteRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateRouteRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getBankTransfer().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setRoute(Route.newBuilder()
                        .setBank(RouteBankSettlement.newBuilder()
                                .setAccountHolder(switch (request.getBankTransfer().getSettlementCase()) {
                                    case BANK -> request.getBankTransfer().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply1 = routingRouteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateRoute(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareBankTransferOptions", request.getBankTransfer().getBankTransferId());
        return CompareBankTransferOptionsResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getCashPosition().getContact().getEmail())
                                .build())
                        .setNotes(reply1.getRoute().getNotes())
                        .setTotal(request.getBankTransfer().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalculates a bank transfer.
     */
    public RecalculateBankTransfersResponse recalculateBankTransfers(RecalculateBankTransfersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateBankTransfers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = mapper.recalculateBankTransfersStage0(request);
        var reply0 = auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordAuditEventVersion(outbound0);

        List<NormalizeBudgetLineResponse> results1 = request.getBankTransfer().getAttachmentsList().stream()
                .map(element -> NormalizeBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .addAttachments(BudgetLineAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(budgetingBudgetLineValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeBudgetLine(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? NormalizeBudgetLineResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = CompareLedgerAccountVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareLedgerAccountVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getBudgetLine().getAudit().getEtag())
                                .setUpdatedAt(request.getBankTransfer().getAudit().getUpdatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getBankTransfer().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareLedgerAccountVersions(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = JsonCodec.roundTrip(ScoreBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getBudgetLine().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getBankTransfer().getBillingAddress().getCountryCode())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply2.getLedgerAccount().getHolder().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), ScoreBudgetLineRequest.class);
        var reply3 = budgetingBudgetLineValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreBudgetLine(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateBankTransfers", request.getBankTransfer().getBankTransferId());
        return RecalculateBankTransfersResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getLedgerAccount().getAudit().getEtag())
                                .build())
                        .setBankTransferId(request.getBankTransferId())
                        .putAllMetadata(request.getBankTransfer().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Simulates a bank transfer.
     */
    public SimulateBankTransferResponse simulateBankTransfer(SimulateBankTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateBankTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        List<ListAuditTrailExportsResponse> results0 = request.getBankTransfer().getLinesList().stream()
                .map(element -> ListAuditTrailExportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditTrailExportsRequest.Filter.newBuilder()
                        .putAllLabels(element.getAttributesMap())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build())
                .flatMap(value -> Stream.of(auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditTrailExports(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ListAuditTrailExportsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = JsonCodec.roundTrip(CheckVenueEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setLocality(request.getBankTransfer().getBillingAddress().getLocality())
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getBankTransfer().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build(), CheckVenueEligibilityRequest.class);
        var reply1 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkVenueEligibility(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateBankTransfer", request.getBankTransfer().getBankTransferId());
        return SimulateBankTransferResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply1.getVenue().getOwner().getEmail())
                                .build())
                        .build())
                .setBreakdown(SimulateBankTransferResponse.Breakdown.newBuilder()
                        .setTotal(request.getBankTransfer().getTotal())
                        .build())
                .build();
    }

    /**
     * Explains a bank transfer.
     */
    public ExplainBankTransferCalculationResponse explainBankTransferCalculation(ExplainBankTransferCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainBankTransferCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankTransfer(request.getContext().getTenantId(), request.getBankTransfer());

        var outbound0 = QuoteCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCard(CashPositionCardFunding.newBuilder()
                                .setHolderName(switch (request.getBankTransfer().getSettlementCase()) {
                                    case CARD -> request.getBankTransfer().getCard().getHolderName();
                                    default -> "";
                                })
                                .setNetwork(switch (request.getBankTransfer().getSettlementCase()) {
                                    case CARD -> request.getBankTransfer().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCashPosition(outbound0);

        var futures1 = request.getBankTransfer().getLinesList().stream()
                .map(element -> EstimateCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getBankTransfer().getAudit().getCreatedBy())
                                .build())
                        .setBank(CashPositionBankFunding.newBuilder()
                                .setBic(reply0.getCashPosition().getBank().getBic())
                                .build())
                        .addLimits(CashPositionLimit.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCashPosition(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? EstimateCashPositionResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = ExplainCashPositionCalculationRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getCashPosition().getAudit().getCreatedBy())
                                .setUpdatedAt(reply1.getCashPosition().getAudit().getUpdatedAt())
                                .build())
                        .setCard(CashPositionCardFunding.newBuilder()
                                .setLast4(switch (reply1.getCashPosition().getFundingCase()) {
                                    case CARD -> reply1.getCashPosition().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainCashPositionCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply1.getCashPosition().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply2 = treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCashPositionCalculation(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBankTransfer().getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainBankTransferCalculation", request.getBankTransfer().getBankTransferId());
        return ExplainBankTransferCalculationResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .setIssuedAt(request.getBankTransfer().getIssuedAt())
                        .build())
                .setOutcome(ExplainBankTransferCalculationResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a bank transfer.
     */
    public GetBankTransferRatesResponse getBankTransferRates(GetBankTransferRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetBankTransferRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "GetBankTransferRates");

        var outbound0 = RecordAuditEventVersionRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setCorrelationId(request.getContext().getActor().getUserId())
                        .setDetail(Any.pack(request.getContext()))
                        .addMeasurements(AuditEventMeasurement.newBuilder()
                                .setObservedAt(request.getContext().getDeadline())
                                .build())
                        .setSource(request.getBankTransferId())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecordAuditEventVersionRequest.Details.newBuilder()
                        .setAuthorEmail(request.getContext().getActor().getEmail())
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordAuditEventVersion(value)))
                .collect(Collectors.toList()).getFirst();
        return GetBankTransferRatesResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
