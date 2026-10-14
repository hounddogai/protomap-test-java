package com.acme.finance.domains.payroll;

import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastBankSettlement;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.PushForecastRequest;
import com.acme.budgeting.v1.PushForecastResponse;
import com.acme.budgeting.v1.StreamForecastChangesRequest;
import com.acme.budgeting.v1.StreamForecastChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AttestationWorkflowServiceGrpc;
import com.acme.compliance.v1.SignalAttestationWorkflowRequest;
import com.acme.compliance.v1.SignalAttestationWorkflowResponse;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractApprovalServiceGrpc;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.contracts.v1.RejectContractRequest;
import com.acme.contracts.v1.RejectContractResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.StreamUnderwritingMetricsRequest;
import com.acme.insurance.v1.StreamUnderwritingMetricsResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.ledger.v1.AnnotateJournalEntryVersionRequest;
import com.acme.ledger.v1.AnnotateJournalEntryVersionResponse;
import com.acme.ledger.v1.CompareJournalEntryVersionsRequest;
import com.acme.ledger.v1.CompareJournalEntryVersionsResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryBankSettlement;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsRequest;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsResponse;
import com.acme.payroll.v1.CalculatePayRunRequest;
import com.acme.payroll.v1.CalculatePayRunResponse;
import com.acme.payroll.v1.CountDeductionTotalsRequest;
import com.acme.payroll.v1.CountDeductionTotalsResponse;
import com.acme.payroll.v1.CountPayStubsRequest;
import com.acme.payroll.v1.CountPayStubsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionDiscountAction;
import com.acme.payroll.v1.DeleteDeductionReportRequest;
import com.acme.payroll.v1.DeleteDeductionReportResponse;
import com.acme.payroll.v1.EstimateTaxWithholdingRequest;
import com.acme.payroll.v1.EstimateTaxWithholdingResponse;
import com.acme.payroll.v1.FetchDeductionReportRequest;
import com.acme.payroll.v1.FetchDeductionReportResponse;
import com.acme.payroll.v1.GenerateDeductionReportRequest;
import com.acme.payroll.v1.GenerateDeductionReportResponse;
import com.acme.payroll.v1.GetDeductionRatesRequest;
import com.acme.payroll.v1.GetDeductionRatesResponse;
import com.acme.payroll.v1.ListDeductionReportsRequest;
import com.acme.payroll.v1.ListDeductionReportsResponse;
import com.acme.payroll.v1.NormalizeDeductionRequest;
import com.acme.payroll.v1.NormalizeDeductionResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.PayRunQueryServiceGrpc;
import com.acme.payroll.v1.PayRunServiceGrpc;
import com.acme.payroll.v1.PayStubQueryServiceGrpc;
import com.acme.payroll.v1.ResolvePayRunsRequest;
import com.acme.payroll.v1.ResolvePayRunsResponse;
import com.acme.payroll.v1.ScheduleDeductionReportRequest;
import com.acme.payroll.v1.ScheduleDeductionReportResponse;
import com.acme.payroll.v1.SearchPayRunsRequest;
import com.acme.payroll.v1.SearchPayRunsResponse;
import com.acme.payroll.v1.ShareDeductionReportRequest;
import com.acme.payroll.v1.ShareDeductionReportResponse;
import com.acme.payroll.v1.StreamDeductionMetricsRequest;
import com.acme.payroll.v1.StreamDeductionMetricsResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingBlockAction;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about deductions. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class DeductionReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(DeductionReportingApplication.class);

    private final ForecastSyncServiceGrpc.ForecastSyncServiceStub budgetingForecastSyncServiceAsync;

    private final ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationWorkflowServiceGrpc.AttestationWorkflowServiceBlockingStub complianceAttestationWorkflowServiceBlocking;

    private final ContractApprovalServiceGrpc.ContractApprovalServiceBlockingStub contractsContractApprovalServiceBlocking;

    private final ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync;

    private final ApplicationEventPublisher events;

    private final UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking;

    private final PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking;

    private final PayRunQueryServiceGrpc.PayRunQueryServiceBlockingStub payrollPayRunQueryServiceBlocking;

    private final PayRunServiceGrpc.PayRunServiceBlockingStub payrollPayRunServiceBlocking;

    private final PayStubQueryServiceGrpc.PayStubQueryServiceBlockingStub payrollPayStubQueryServiceBlocking;

    private final TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking;

    private final PayrollRepository repository;

    private final Executor rpcExecutor;

    public DeductionReportingApplication(
            ForecastSyncServiceGrpc.ForecastSyncServiceStub budgetingForecastSyncServiceAsync,
            ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking,
            ChannelRegistry channels,
            AttestationWorkflowServiceGrpc.AttestationWorkflowServiceBlockingStub complianceAttestationWorkflowServiceBlocking,
            ContractApprovalServiceGrpc.ContractApprovalServiceBlockingStub contractsContractApprovalServiceBlocking,
            ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync,
            ApplicationEventPublisher events,
            UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking,
            JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking,
            PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking,
            PayRunQueryServiceGrpc.PayRunQueryServiceBlockingStub payrollPayRunQueryServiceBlocking,
            PayRunServiceGrpc.PayRunServiceBlockingStub payrollPayRunServiceBlocking,
            PayStubQueryServiceGrpc.PayStubQueryServiceBlockingStub payrollPayStubQueryServiceBlocking,
            TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking,
            PayrollRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.budgetingForecastSyncServiceAsync = budgetingForecastSyncServiceAsync;
        this.budgetingForecastSyncServiceBlocking = budgetingForecastSyncServiceBlocking;
        this.channels = channels;
        this.complianceAttestationWorkflowServiceBlocking = complianceAttestationWorkflowServiceBlocking;
        this.contractsContractApprovalServiceBlocking = contractsContractApprovalServiceBlocking;
        this.contractsContractServiceAsync = contractsContractServiceAsync;
        this.events = events;
        this.insuranceUnderwritingReportingServiceBlocking = insuranceUnderwritingReportingServiceBlocking;
        this.ledgerJournalEntryHistoryServiceBlocking = ledgerJournalEntryHistoryServiceBlocking;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.payrollDeductionCalculationServiceBlocking = payrollDeductionCalculationServiceBlocking;
        this.payrollPayRunCalculationServiceBlocking = payrollPayRunCalculationServiceBlocking;
        this.payrollPayRunQueryServiceBlocking = payrollPayRunQueryServiceBlocking;
        this.payrollPayRunServiceBlocking = payrollPayRunServiceBlocking;
        this.payrollPayStubQueryServiceBlocking = payrollPayStubQueryServiceBlocking;
        this.payrollTaxWithholdingCalculationServiceBlocking = payrollTaxWithholdingCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record ScheduleDeductionReportStage1Requested(NormalizeDeductionRequest request, CompletableFuture<NormalizeDeductionResponse> completion, io.grpc.Context context) {}

    /**
     * Generates a deduction.
     */
    public GenerateDeductionReportResponse generateDeductionReport(GenerateDeductionReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateDeductionReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getDeductionId(), "GenerateDeductionReport");

        var outbound0 = AnnotateJournalEntryVersionRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(AnnotateJournalEntryVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setReason(request.getReason())
                .build();
        var reply0 = ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateJournalEntryVersion(outbound0);

        var outbound1 = PushForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushForecastRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getJournalEntry().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getJournalEntry().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getJournalEntry().getAudit().getUpdatedBy())
                                .build())
                        .setBank(ForecastBankSettlement.newBuilder()
                                .setBic(switch (reply0.getJournalEntry().getSettlementCase()) {
                                    case BANK -> reply0.getJournalEntry().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = budgetingForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushForecast(outbound1);

        var outbound2 = StreamUnderwritingMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamUnderwritingMetricsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getForecast().getAudit().getUpdatedAt())
                        .setRegion(reply1.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream2 = insuranceUnderwritingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamUnderwritingMetrics(outbound2);
        var reply2 = StreamUnderwritingMetricsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = BatchCreateContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateContractsRequest.StreamHeader.newBuilder()
                        .setFormat(request.getDetails().getFormat())
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateContractsResponse.getDefaultInstance());
        var input3 = contractsContractServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateContracts(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamForecastChangesRequest.newBuilder()
                .setChange(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setDueAt(request.getContext().getDeadline())
                        .setIssuedAt(request.getContext().getDeadline())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamForecastChangesResponse.getDefaultInstance());
        var input4 = budgetingForecastSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamForecastChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateDeductionReport", request.getDeductionId());
        return GenerateDeductionReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a deduction.
     */
    public FetchDeductionReportResponse fetchDeductionReport(FetchDeductionReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchDeductionReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getDeductionId(), "FetchDeductionReport");

        var outbound0 = CountPayStubsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountPayStubsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .setOwnerId(request.getDeductionId())
                        .build())
                .build();
        Function<CountPayStubsRequest, CountPayStubsResponse> operation0 = payrollPayStubQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::countPayStubs;
        var reply0 = operation0.apply(outbound0);
        return FetchDeductionReportResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Schedules a deduction.
     */
    public ScheduleDeductionReportResponse scheduleDeductionReport(ScheduleDeductionReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleDeductionReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getDeductionId(), "ScheduleDeductionReport");

        var outbound0 = EstimateTaxWithholdingRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setBlock(TaxWithholdingBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateTaxWithholding(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = NormalizeDeductionRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getTaxWithholding().getAudit().getCreatedBy())
                                .build())
                        .setDiscount(DeductionDiscountAction.newBuilder()
                                .setAmount(reply0.getTaxWithholding().getDiscount().getAmount())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getTaxWithholding().getOwner().getUserId())
                                .build())
                        .build())
                .setDetails(NormalizeDeductionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getTaxWithholding().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<NormalizeDeductionResponse>();
        events.publishEvent(new ScheduleDeductionReportStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = ListLedgerAccountSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLedgerAccountSubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getTaxWithholding().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getTaxWithholding().getParametersMap())
                        .setOwnerId(reply0.getTaxWithholding().getOwner().getUserId())
                        .build())
                .build();
        var reply2 = RpcSupport.retryRead(() -> ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLedgerAccountSubscriptions(outbound2));

        var outbound3 = BeanCopies.copy(SearchPayRunsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchPayRunsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getTaxWithholding().getAudit().getUpdatedAt())
                        .build())
                .build(), SearchPayRunsRequest.class);
        var reply3 = payrollPayRunServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchPayRuns(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleDeductionReport", request.getDeductionId());
        return ScheduleDeductionReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply3.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams the changes of deductions as they happen.
     */
    public StreamDeductionMetricsResponse streamDeductionMetrics(StreamDeductionMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamDeductionMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getContext().getTraceId(), "StreamDeductionMetrics");

        var outbound0 = (SignalAttestationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SignalAttestationWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getSubscriber().getName())
                                .setPhone(request.getSubscriber().getPhone())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = complianceAttestationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalAttestationWorkflow(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamDeductionMetrics", request.getFilter().getQuery());
        return StreamDeductionMetricsResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getAttestation().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getAttestation().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists deductions a page at a time.
     */
    public ListDeductionReportsResponse listDeductionReports(ListDeductionReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListDeductionReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getContext().getTraceId(), "ListDeductionReports");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListDeductionReports", request.getPage().getPageToken());
        return ListDeductionReportsResponse.getDefaultInstance();
    }

    /**
     * Shares a deduction.
     */
    public ShareDeductionReportResponse shareDeductionReport(ShareDeductionReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareDeductionReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getDeductionId(), "ShareDeductionReport");

        var outbound0 = ListLedgerAccountSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLedgerAccountSubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getDetails().getVariablesMap())
                        .setOwnerId(request.getDetails().getTemplateId())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.ledger.v1.LedgerAccountNotificationService");
        var selected0 = LedgerAccountNotificationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.listLedgerAccountSubscriptions(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareDeductionReport", request.getDetails().getRecipient().getPhone());
        return ShareDeductionReportResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .build())
                .setOutcome(ShareDeductionReportResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Counts deductions by status.
     */
    public CountDeductionTotalsResponse countDeductionTotals(CountDeductionTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountDeductionTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getContext().getTraceId(), "CountDeductionTotals");

        var outbound0 = RpcSupport.unpack(Any.pack(RejectContractRequest.newBuilder()
                .setContext(request.getContext())
                .setContract(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setDetails(RejectContractRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build()), RejectContractRequest.class);
        var reply0 = contractsContractApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectContract(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountDeductionTotals", request.getPage().getPageToken());
        return CountDeductionTotalsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Deletes a deduction.
     */
    public DeleteDeductionReportResponse deleteDeductionReport(DeleteDeductionReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteDeductionReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getDeductionId(), "DeleteDeductionReport");

        var outbound0 = GetDeductionRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDeductionId(request.getDeductionId())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> payrollDeductionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getDeductionRates(outbound0), rpcExecutor);

        var outbound1 = ResolvePayRunsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> payrollPayRunQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolvePayRuns(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = CalculatePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .addLines(PayRunLine.newBuilder()
                                .setTax(PayRunLineTax.newBuilder()
                                        .setAmount(reply0.getDeduction().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePayRun(value));

        var outbound3 = CompareJournalEntryVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareJournalEntryVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setBank(JournalEntryBankSettlement.newBuilder()
                                .setBic(reply2.getPayRun().getBank().getBic())
                                .build())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.ledger.v1.JournalEntryHistoryService");
        var serviceName3 = "acme.ledger.v1" + "." + "JournalEntryHistoryService";
        var methodName3 = "Com" + "pareJournalEntryVersions";
        var descriptor3 = MethodDescriptor.<CompareJournalEntryVersionsRequest, CompareJournalEntryVersionsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(CompareJournalEntryVersionsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CompareJournalEntryVersionsResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getReason()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteDeductionReport", request.getReason());
        return DeleteDeductionReportResponse.newBuilder()
                .setEffectiveAt(reply3.getOutcome().getEffectiveAt())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
