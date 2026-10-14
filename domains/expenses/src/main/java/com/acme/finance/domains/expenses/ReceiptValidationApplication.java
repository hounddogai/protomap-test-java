package com.acme.finance.domains.expenses;

import com.acme.analytics.v1.AnalyticsEvent;
import com.acme.analytics.v1.AnalyticsEventMeasurement;
import com.acme.analytics.v1.AnalyticsEventServiceGrpc;
import com.acme.analytics.v1.FunnelRef;
import com.acme.analytics.v1.UpdateAnalyticsEventRequest;
import com.acme.analytics.v1.UpdateAnalyticsEventResponse;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.ScheduleAuditEventExportRequest;
import com.acme.audit.v1.ScheduleAuditEventExportResponse;
import com.acme.boxoffice.v1.BoxOfficeSale;
import com.acme.boxoffice.v1.BoxOfficeSaleCalculationServiceGrpc;
import com.acme.boxoffice.v1.CashDrawerRef;
import com.acme.boxoffice.v1.ExplainBoxOfficeSaleCalculationRequest;
import com.acme.boxoffice.v1.ExplainBoxOfficeSaleCalculationResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.BrowseComplianceCasesRequest;
import com.acme.compliance.v1.BrowseComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseQueryServiceGrpc;
import com.acme.compliance.v1.ControlQueryServiceGrpc;
import com.acme.compliance.v1.CountComplianceCasesRequest;
import com.acme.compliance.v1.CountComplianceCasesResponse;
import com.acme.compliance.v1.CountControlsRequest;
import com.acme.compliance.v1.CountControlsResponse;
import com.acme.concessions.v1.ConcessionStandQueryServiceGrpc;
import com.acme.concessions.v1.FindConcessionStandsRequest;
import com.acme.concessions.v1.FindConcessionStandsResponse;
import com.acme.expenses.v1.CalculateReceiptRequest;
import com.acme.expenses.v1.CalculateReceiptResponse;
import com.acme.expenses.v1.CheckReceiptEligibilityRequest;
import com.acme.expenses.v1.CheckReceiptEligibilityResponse;
import com.acme.expenses.v1.EstimateMileageClaimRequest;
import com.acme.expenses.v1.EstimateMileageClaimResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportCardSettlement;
import com.acme.expenses.v1.ExplainReceiptDecisionRequest;
import com.acme.expenses.v1.ExplainReceiptDecisionResponse;
import com.acme.expenses.v1.GetReceiptRatesRequest;
import com.acme.expenses.v1.GetReceiptRatesResponse;
import com.acme.expenses.v1.GetReceiptRulesRequest;
import com.acme.expenses.v1.GetReceiptRulesResponse;
import com.acme.expenses.v1.LintReceiptsRequest;
import com.acme.expenses.v1.LintReceiptsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimBankSettlement;
import com.acme.expenses.v1.MileageClaimCalculationServiceGrpc;
import com.acme.expenses.v1.NormalizeReceiptRequest;
import com.acme.expenses.v1.NormalizeReceiptResponse;
import com.acme.expenses.v1.PreviewReceiptRequest;
import com.acme.expenses.v1.PreviewReceiptResponse;
import com.acme.expenses.v1.QuoteExpenseReportRequest;
import com.acme.expenses.v1.QuoteExpenseReportResponse;
import com.acme.expenses.v1.RecalculateReceiptsRequest;
import com.acme.expenses.v1.RecalculateReceiptsResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptBankSettlement;
import com.acme.expenses.v1.ReceiptCalculationServiceGrpc;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptLineTax;
import com.acme.expenses.v1.ScoreReceiptRequest;
import com.acme.expenses.v1.ScoreReceiptResponse;
import com.acme.expenses.v1.SimulateReceiptRequest;
import com.acme.expenses.v1.SimulateReceiptResponse;
import com.acme.expenses.v1.ValidateReceiptRequest;
import com.acme.expenses.v1.ValidateReceiptResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.CompareLedgerAccountVersionsRequest;
import com.acme.ledger.v1.CompareLedgerAccountVersionsResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.metrics.v1.BrowseMetricSeriesesRequest;
import com.acme.metrics.v1.BrowseMetricSeriesesResponse;
import com.acme.metrics.v1.CountMetricSeriesesRequest;
import com.acme.metrics.v1.CountMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.venues.v1.FindVenuesRequest;
import com.acme.venues.v1.FindVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
 * Checks receipts against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class ReceiptValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(ReceiptValidationApplication.class);

    private final AnalyticsEventServiceGrpc.AnalyticsEventServiceBlockingStub analyticsAnalyticsEventServiceBlocking;

    private final AuditEventExportServiceGrpc.AuditEventExportServiceFutureStub auditAuditEventExportServiceFuture;

    private final BoxOfficeSaleCalculationServiceGrpc.BoxOfficeSaleCalculationServiceBlockingStub boxofficeBoxOfficeSaleCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceBlockingStub complianceComplianceCaseQueryServiceBlocking;

    private final ControlQueryServiceGrpc.ControlQueryServiceBlockingStub complianceControlQueryServiceBlocking;

    private final ConcessionStandQueryServiceGrpc.ConcessionStandQueryServiceBlockingStub concessionsConcessionStandQueryServiceBlocking;

    private final ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking;

    private final ReceiptCalculationServiceGrpc.ReceiptCalculationServiceBlockingStub expensesReceiptCalculationServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final ReceiptValidationRequestMapper mapper;

    private final MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking;

    private final ExpensesRepository repository;

    private final Executor rpcExecutor;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public ReceiptValidationApplication(
            AnalyticsEventServiceGrpc.AnalyticsEventServiceBlockingStub analyticsAnalyticsEventServiceBlocking,
            AuditEventExportServiceGrpc.AuditEventExportServiceFutureStub auditAuditEventExportServiceFuture,
            BoxOfficeSaleCalculationServiceGrpc.BoxOfficeSaleCalculationServiceBlockingStub boxofficeBoxOfficeSaleCalculationServiceBlocking,
            ChannelRegistry channels,
            ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceBlockingStub complianceComplianceCaseQueryServiceBlocking,
            ControlQueryServiceGrpc.ControlQueryServiceBlockingStub complianceControlQueryServiceBlocking,
            ConcessionStandQueryServiceGrpc.ConcessionStandQueryServiceBlockingStub concessionsConcessionStandQueryServiceBlocking,
            ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking,
            ReceiptCalculationServiceGrpc.ReceiptCalculationServiceBlockingStub expensesReceiptCalculationServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            ReceiptValidationRequestMapper mapper,
            MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking,
            ExpensesRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.analyticsAnalyticsEventServiceBlocking = analyticsAnalyticsEventServiceBlocking;
        this.auditAuditEventExportServiceFuture = auditAuditEventExportServiceFuture;
        this.boxofficeBoxOfficeSaleCalculationServiceBlocking = boxofficeBoxOfficeSaleCalculationServiceBlocking;
        this.channels = channels;
        this.complianceComplianceCaseQueryServiceBlocking = complianceComplianceCaseQueryServiceBlocking;
        this.complianceControlQueryServiceBlocking = complianceControlQueryServiceBlocking;
        this.concessionsConcessionStandQueryServiceBlocking = concessionsConcessionStandQueryServiceBlocking;
        this.expensesExpenseReportCalculationServiceBlocking = expensesExpenseReportCalculationServiceBlocking;
        this.expensesReceiptCalculationServiceBlocking = expensesReceiptCalculationServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesQueryServiceBlocking = metricsMetricSeriesQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    /**
     * Validates a receipt.
     */
    public ValidateReceiptResponse validateReceipt(ValidateReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = CountControlsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountControlsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getReceipt().getAudit().getUpdatedAt())
                        .putAllLabels(request.getReceipt().getMetadataMap())
                        .setOwnerId(request.getReceipt().getCounterparty().getMileageClaimId())
                        .setRegion(request.getReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> complianceControlQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countControls(outbound0), rpcExecutor);

        var outbound1 = FindConcessionStandsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindConcessionStandsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getReceipt().getDueAt())
                        .setOwnerId(request.getReceipt().getCounterparty().getMileageClaimId())
                        .setRegion(request.getReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> concessionsConcessionStandQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findConcessionStands(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateReceipt", request.getReceipt().getReceiptId());
        return ValidateReceiptResponse.newBuilder()
                .setOutcome(ValidateReceiptResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .putAllMetadata(request.getReceipt().getMetadataMap())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Previews a receipt.
     */
    public PreviewReceiptResponse previewReceipt(PreviewReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = (UpdateAnalyticsEventRequest.newBuilder()
                .setAnalyticsEvent(AnalyticsEvent.newBuilder()
                        .setCorrelationId(request.getReceipt().getReceiptId())
                        .setDetail(Any.pack(request.getContext()))
                        .addMeasurements(AnalyticsEventMeasurement.newBuilder()
                                .setObservedAt(request.getReceipt().getIssuedAt())
                                .build())
                        .setOccurredAt(request.getReceipt().getAudit().getUpdatedAt())
                        .setSubject(FunnelRef.newBuilder()
                                .setDisplayName(request.getReceipt().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply0 = analyticsAnalyticsEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateAnalyticsEvent(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewReceipt", request.getReceipt().getNumber());
        return PreviewReceiptResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setReceiptId(request.getReceiptId())
                        .setStatus(request.getReceipt().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getAnalyticsEvent().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Checks a receipt.
     */
    public CheckReceiptEligibilityResponse checkReceiptEligibility(CheckReceiptEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckReceiptEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = QuoteExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setCard(ExpenseReportCardSettlement.newBuilder()
                                .setHolderName(switch (request.getReceipt().getSettlementCase()) {
                                    case CARD -> request.getReceipt().getCard().getHolderName();
                                    default -> "";
                                })
                                .setLast4(switch (request.getReceipt().getSettlementCase()) {
                                    case CARD -> request.getReceipt().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteExpenseReport(outbound0);

        var outbound1 = ExplainBoxOfficeSaleCalculationRequest.newBuilder()
                .setBoxOfficeSale(BoxOfficeSale.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getReceipt().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getExpenseReport().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getReceipt().getBillingAddress().getLinesList())
                                .build())
                        .setCounterparty(CashDrawerRef.newBuilder()
                                .setDisplayName(reply0.getExpenseReport().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> boxofficeBoxOfficeSaleCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBoxOfficeSaleCalculation(value)).orElseThrow();

        var outbound2 = BrowseComplianceCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseComplianceCasesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getBoxOfficeSale().getAudit().getCreatedAt())
                        .setRegion(reply1.getBoxOfficeSale().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.retryRead(() -> complianceComplianceCaseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseComplianceCases(outbound2));

        var outbound3 = GetReceiptRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setReceiptId(request.getReceipt().getReceiptId())
                .build();
        var reply3 = RpcSupport.call(outbound3, value -> expensesReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getReceiptRates(value));
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getCounterparty().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckReceiptEligibility", request.getReceipt().getCounterparty().getMileageClaimId());
        return CheckReceiptEligibilityResponse.newBuilder()
                .setOutcome(CheckReceiptEligibilityResponse.Outcome.newBuilder()
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setBillingAddress(reply1.getBoxOfficeSale().getBillingAddress())
                        .setNumber(request.getReceipt().getNumber())
                        .setReceiptId(request.getReceiptId())
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Normalizes a receipt.
     */
    public NormalizeReceiptResponse normalizeReceipt(NormalizeReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = (CalculateReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .setBank(ReceiptBankSettlement.newBuilder()
                                .setBic(request.getReceipt().getBank().getBic())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = expensesReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateReceipt(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeReceipt", request.getReceipt().getReceiptId());
        return NormalizeReceiptResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getReceipt().getBillingAddress().getPostalCode())
                                .build())
                        .setIssuedAt(reply0.getReceipt().getIssuedAt())
                        .setReceiptId(request.getReceiptId())
                        .setStatus(request.getReceipt().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a receipt.
     */
    public ExplainReceiptDecisionResponse explainReceiptDecision(ExplainReceiptDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainReceiptDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = EstimateMileageClaimRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateMileageClaimRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getReceipt().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getReceipt().getAudit().getEtag())
                                .build())
                        .setBank(MileageClaimBankSettlement.newBuilder()
                                .setIban(request.getReceipt().getBank().getIban())
                                .build())
                        .setNumber(request.getReceipt().getCounterparty().getNumber())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.expenses.v1.MileageClaimCalculationService");
        var selected0 = MileageClaimCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.estimateMileageClaim(outbound0);

        var outbound1 = CountMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getReceipt().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(request.getReceiptId())
                        .setRegion(request.getReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.metrics.v1.MetricSeriesQueryService");
        var serviceName1 = "acme.metrics.v1" + "." + "MetricSeriesQueryService";
        var methodName1 = "Cou" + "ntMetricSerieses";
        var descriptor1 = MethodDescriptor.<CountMetricSeriesesRequest, CountMetricSeriesesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(CountMetricSeriesesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CountMetricSeriesesResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainReceiptDecision", request.getReceipt().getReceiptId());
        return ExplainReceiptDecisionResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setNumber(request.getReceipt().getNumber())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Scores a receipt.
     */
    public ScoreReceiptResponse scoreReceipt(ScoreReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = ScheduleAuditEventExportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ScheduleAuditEventExportRequest.Details.newBuilder()
                        .putAllFilters(request.getReceipt().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = auditAuditEventExportServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleAuditEventExport(outbound0);
        var reply0 = RpcSupport.await(pending0);

        List<RecalculateReceiptsResponse> results1 = request.getReceipt().getLinesList().stream()
                .map(element -> RecalculateReceiptsRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .addLines(ReceiptLine.newBuilder()
                                .setQuantity(element.getQuantity())
                                .setTax(ReceiptLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(expensesReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateReceipts(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? RecalculateReceiptsResponse.getDefaultInstance()
                : results1.getLast();

        var results2 = new ArrayList<CalculateReceiptResponse>();
        for (var element : request.getReceipt().getLinesList()) {
            CalculateReceiptRequest itemRequest = CalculateReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .addLines(ReceiptLine.newBuilder()
                                .setQuantity(element.getQuantity())
                                .setTax(ReceiptLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build();
            results2.add(expensesReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateReceipt(itemRequest));
        }
        var reply2 = results2.isEmpty() ? CalculateReceiptResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = CountComplianceCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountComplianceCasesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply2.getReceipt().getAudit().getCreatedAt())
                        .setRegion(reply2.getReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        RpcPort<CountComplianceCasesRequest, CountComplianceCasesResponse> port3 = RpcPort.adapt(complianceComplianceCaseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::countComplianceCases);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreReceipt", request.getReceipt().getReceiptId());
        return ScoreReceiptResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setNumber(request.getReceipt().getNumber())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lints a receipt.
     */
    public LintReceiptsResponse lintReceipts(LintReceiptsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintReceipts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = FindVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindVenuesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getReceipt().getAudit().getCreatedAt())
                        .putAllLabels(request.getReceipt().getMetadataMap())
                        .setOwnerId(request.getReceipt().getCounterparty().getMileageClaimId())
                        .setRegion(request.getReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findVenues(value)).orElseThrow();

        var results1 = new ArrayList<SimulateReceiptResponse>();
        for (var element : request.getReceipt().getLinesList()) {
            SimulateReceiptRequest itemRequest = SimulateReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .addLines(ReceiptLine.newBuilder()
                                .setTax(ReceiptLineTax.newBuilder()
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .build())
                                .build())
                        .build())
                .build();
            results1.add(expensesReceiptCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateReceipt(itemRequest));
        }
        var reply1 = results1.isEmpty() ? SimulateReceiptResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = mapper.lintReceiptsStage2(request);
        var reply2 = ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareLedgerAccountVersions(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintReceipts", request.getReceipt().getReceiptId());
        return LintReceiptsResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getLedgerAccount().getAudit().getUpdatedAt())
                                .build())
                        .setDueAt(request.getReceipt().getDueAt())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns a receipt.
     */
    public GetReceiptRulesResponse getReceiptRules(GetReceiptRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetReceiptRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getReceiptId(), "GetReceiptRules");

        var outbound0 = BrowseMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getReceiptId())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses", outbound0);
        var reply0 = metricsMetricSeriesQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseMetricSerieses(command0.payload());
        return GetReceiptRulesResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
