package com.acme.finance.domains.payroll;

import com.acme.assets.v1.BrowseDepreciationsRequest;
import com.acme.assets.v1.BrowseDepreciationsResponse;
import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.AuditTrailLineTax;
import com.acme.audit.v1.FindAuditEventsRequest;
import com.acme.audit.v1.FindAuditEventsResponse;
import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.SuggestJournalEntriesRequest;
import com.acme.ledger.v1.SuggestJournalEntriesResponse;
import com.acme.metrics.v1.BrowseMetricSeriesesRequest;
import com.acme.metrics.v1.BrowseMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.payroll.v1.CalculatePayStubRequest;
import com.acme.payroll.v1.CalculatePayStubResponse;
import com.acme.payroll.v1.CalculateTaxWithholdingRequest;
import com.acme.payroll.v1.CalculateTaxWithholdingResponse;
import com.acme.payroll.v1.CheckPayRunEligibilityRequest;
import com.acme.payroll.v1.CheckPayRunEligibilityResponse;
import com.acme.payroll.v1.CheckTaxWithholdingEligibilityRequest;
import com.acme.payroll.v1.CheckTaxWithholdingEligibilityResponse;
import com.acme.payroll.v1.CompareTaxWithholdingOptionsRequest;
import com.acme.payroll.v1.CompareTaxWithholdingOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionCondition;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.EstimateDeductionRequest;
import com.acme.payroll.v1.EstimateDeductionResponse;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.ExplainTaxWithholdingCalculationRequest;
import com.acme.payroll.v1.ExplainTaxWithholdingCalculationResponse;
import com.acme.payroll.v1.ExplainTaxWithholdingDecisionRequest;
import com.acme.payroll.v1.ExplainTaxWithholdingDecisionResponse;
import com.acme.payroll.v1.LintTaxWithholdingsRequest;
import com.acme.payroll.v1.LintTaxWithholdingsResponse;
import com.acme.payroll.v1.NormalizeTaxWithholdingRequest;
import com.acme.payroll.v1.NormalizeTaxWithholdingResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubLineTax;
import com.acme.payroll.v1.PreviewTaxWithholdingRequest;
import com.acme.payroll.v1.PreviewTaxWithholdingResponse;
import com.acme.payroll.v1.ScoreTaxWithholdingRequest;
import com.acme.payroll.v1.ScoreTaxWithholdingResponse;
import com.acme.payroll.v1.SimulateTaxWithholdingRequest;
import com.acme.payroll.v1.SimulateTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingCondition;
import com.acme.payroll.v1.ValidateTaxWithholdingRequest;
import com.acme.payroll.v1.ValidateTaxWithholdingResponse;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.SearchSearchIndexesRequest;
import com.acme.search.v1.SearchSearchIndexesResponse;
import com.acme.suppliers.v1.ListSuppliersRequest;
import com.acme.suppliers.v1.ListSuppliersResponse;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Checks tax withholdings against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class TaxWithholdingValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(TaxWithholdingValidationApplication.class);

    private final DepreciationQueryServiceGrpc.DepreciationQueryServiceFutureStub assetsDepreciationQueryServiceFuture;

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking;

    private final TaxWithholdingValidationRequestMapper mapper;

    private final MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking;

    private final DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking;

    private final PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking;

    private final TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking;

    private final PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceStub procurementPurchaseRequisitionServiceAsync;

    private final PayrollRepository repository;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    public TaxWithholdingValidationApplication(
            DepreciationQueryServiceGrpc.DepreciationQueryServiceFutureStub assetsDepreciationQueryServiceFuture,
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking,
            TaxWithholdingValidationRequestMapper mapper,
            MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking,
            DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking,
            PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking,
            TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking,
            PurchaseRequisitionServiceGrpc.PurchaseRequisitionServiceStub procurementPurchaseRequisitionServiceAsync,
            PayrollRepository repository,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking) {
        this.assetsDepreciationQueryServiceFuture = assetsDepreciationQueryServiceFuture;
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.events = events;
        this.ledgerJournalEntryQueryServiceBlocking = ledgerJournalEntryQueryServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesQueryServiceBlocking = metricsMetricSeriesQueryServiceBlocking;
        this.payrollDeductionCalculationServiceBlocking = payrollDeductionCalculationServiceBlocking;
        this.payrollPayStubCalculationServiceBlocking = payrollPayStubCalculationServiceBlocking;
        this.payrollTaxWithholdingCalculationServiceBlocking = payrollTaxWithholdingCalculationServiceBlocking;
        this.procurementPurchaseRequisitionServiceAsync = procurementPurchaseRequisitionServiceAsync;
        this.repository = repository;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
    }

    public record PreviewTaxWithholdingStage0Requested(ExplainTaxWithholdingCalculationRequest request, CompletableFuture<ExplainTaxWithholdingCalculationResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a tax withholding.
     */
    public ValidateTaxWithholdingResponse validateTaxWithholding(ValidateTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = SearchSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getTaxWithholding().getAudit().getUpdatedAt())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchSearchIndexes(value)).orElseThrow();

        var outbound1 = EstimatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getTaxWithholding().getAudit().getEtag())
                                .build())
                        .addLines(PayStubLine.newBuilder()
                                .setTax(PayStubLineTax.newBuilder()
                                        .setAmount(request.getTaxWithholding().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.payroll.v1.PayStubCalculationService/EstimatePayStub", outbound1);
        var reply1 = payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePayStub(command1.payload());

        var outbound2 = BatchCreatePurchaseRequisitionsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreatePurchaseRequisitionsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreatePurchaseRequisitionsResponse.getDefaultInstance());
        var input2 = procurementPurchaseRequisitionServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreatePurchaseRequisitions(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getTaxWithholding().getAudit().getCreatedAt())
                                .setCreatedBy(request.getTaxWithholding().getAudit().getCreatedBy())
                                .setEtag(request.getTaxWithholding().getAudit().getEtag())
                                .setUpdatedAt(request.getTaxWithholding().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateTaxWithholding", request.getTaxWithholding().getTaxWithholdingId());
        return ValidateTaxWithholdingResponse.newBuilder()
                .setOutcome(ValidateTaxWithholdingResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setDescription(request.getTaxWithholding().getDescription())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Previews a tax withholding.
     */
    public PreviewTaxWithholdingResponse previewTaxWithholding(PreviewTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = ExplainTaxWithholdingCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ExplainTaxWithholdingCalculationResponse>();
        events.publishEvent(new PreviewTaxWithholdingStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = RecordRetentionPolicyVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordRetentionPolicyVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getTaxWithholding().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getTaxWithholding().getAudit().getUpdatedAt())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(switch (reply0.getTaxWithholding().getActionCase()) {
                                    case BLOCK -> reply0.getTaxWithholding().getBlock().getNotifyEmail();
                                    default -> "";
                                })
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getTaxWithholding().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordRetentionPolicyVersion(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewTaxWithholding", request.getTaxWithholding().getTaxWithholdingId());
        return PreviewTaxWithholdingResponse.newBuilder()
                .setOutcome(PreviewTaxWithholdingResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setEffective(reply0.getTaxWithholding().getEffective())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .setVersion(request.getTaxWithholding().getVersion())
                        .build())
                .build();
    }

    /**
     * Checks a tax withholding.
     */
    public CheckTaxWithholdingEligibilityResponse checkTaxWithholdingEligibility(CheckTaxWithholdingEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckTaxWithholdingEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var results0 = new ArrayList<CompareTaxWithholdingOptionsResponse>();
        for (var element : request.getTaxWithholding().getConditionsList()) {
            CompareTaxWithholdingOptionsRequest itemRequest = CompareTaxWithholdingOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .addConditions(TaxWithholdingCondition.newBuilder()
                                .setNegate(element.getNegate())
                                .build())
                        .build())
                .build();
            results0.add(payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareTaxWithholdingOptions(itemRequest));
        }
        var reply0 = results0.isEmpty() ? CompareTaxWithholdingOptionsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ResolveAuditTrailConflictRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getTaxWithholding().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(RetentionPolicyRef.newBuilder()
                                .setDisplayName(request.getTaxWithholding().getOwner().getDisplayName())
                                .build())
                        .addLines(AuditTrailLine.newBuilder()
                                .setTax(AuditTrailLineTax.newBuilder()
                                        .setAmount(request.getTaxWithholding().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ResolveAuditTrailConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var channel1 = channels.lookup("acme.audit.v1.AuditTrailSyncService");
        var serviceName1 = "acme.audit.v1" + "." + "AuditTrailSyncService";
        var methodName1 = "Res" + "olveAuditTrailConflict";
        var descriptor1 = MethodDescriptor.<ResolveAuditTrailConflictRequest, ResolveAuditTrailConflictResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ResolveAuditTrailConflictRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ResolveAuditTrailConflictResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckTaxWithholdingEligibility", request.getTaxWithholding().getTaxWithholdingId());
        return CheckTaxWithholdingEligibilityResponse.newBuilder()
                .setOutcome(CheckTaxWithholdingEligibilityResponse.Outcome.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setEffective(request.getTaxWithholding().getEffective())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Normalizes a tax withholding.
     */
    public NormalizeTaxWithholdingResponse normalizeTaxWithholding(NormalizeTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = CheckPayRunEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getTaxWithholding().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.payroll.v1.PayRunValidationService");
        var serviceName0 = "acme.payroll.v1" + "." + "PayRunValidationService";
        var methodName0 = "Che" + "ckPayRunEligibility";
        var descriptor0 = MethodDescriptor.<CheckPayRunEligibilityRequest, CheckPayRunEligibilityResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(CheckPayRunEligibilityRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CheckPayRunEligibilityResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = mapper.normalizeTaxWithholdingStage1(request, reply0);
        var reply1 = ledgerJournalEntryQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestJournalEntries(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeTaxWithholding", request.getTaxWithholding().getTaxWithholdingId());
        return NormalizeTaxWithholdingResponse.newBuilder()
                .setOutcome(NormalizeTaxWithholdingResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .setVersion(request.getTaxWithholding().getVersion())
                        .build())
                .build();
    }

    /**
     * Explains a tax withholding.
     */
    public ExplainTaxWithholdingDecisionResponse explainTaxWithholdingDecision(ExplainTaxWithholdingDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainTaxWithholdingDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = SimulateTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getTaxWithholding().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getTaxWithholding().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.payroll.v1.TaxWithholdingCalculationService/SimulateTaxWithholding", outbound0);
        var reply0 = payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateTaxWithholding(command0.payload());

        List<CalculateTaxWithholdingResponse> results1 = request.getTaxWithholding().getConditionsList().stream()
                .map(element -> CalculateTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .addConditions(TaxWithholdingCondition.newBuilder()
                                .setAttribute(element.getAttribute())
                                .setNegate(element.getNegate())
                                .setOperator(element.getOperator())
                                .setValue(element.getValue())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateTaxWithholding(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? CalculateTaxWithholdingResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainTaxWithholdingDecision", request.getTaxWithholding().getTaxWithholdingId());
        return ExplainTaxWithholdingDecisionResponse.newBuilder()
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getTaxWithholding().getAudit().getCreatedBy())
                                .build())
                        .setStatus(request.getTaxWithholding().getStatus())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Scores a tax withholding.
     */
    public ScoreTaxWithholdingResponse scoreTaxWithholding(ScoreTaxWithholdingRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreTaxWithholding")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        List<CalculateTaxWithholdingResponse> results0 = request.getTaxWithholding().getConditionsList().stream()
                .map(element -> CalculateTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .addConditions(TaxWithholdingCondition.newBuilder()
                                .setNegate(element.getNegate())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateTaxWithholding(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? CalculateTaxWithholdingResponse.getDefaultInstance()
                : results0.getLast();

        List<EstimateDeductionResponse> results1 = request.getTaxWithholding().getConditionsList().stream()
                .map(element -> EstimateDeductionRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .addConditions(DeductionCondition.newBuilder()
                                .setAttribute(element.getAttribute())
                                .setOperator(element.getOperator())
                                .setValue(element.getValue())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(payrollDeductionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateDeduction(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? EstimateDeductionResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = BrowseMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getTaxWithholding().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getTaxWithholding().getParametersMap())
                        .setOwnerId(reply0.getTaxWithholding().getOwner().getUserId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        final class Stage2Adapter {
            BrowseMetricSeriesesResponse fetch(BrowseMetricSeriesesRequest input) {
                return metricsMetricSeriesQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseMetricSerieses(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);

        var outbound3 = ListSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListSuppliersRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.suppliers.v1.SupplierService");
        var selected3 = SupplierServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.listSuppliers(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreTaxWithholding", request.getTaxWithholding().getTaxWithholdingId());
        return ScoreTaxWithholdingResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setName(request.getTaxWithholding().getName())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }

    /**
     * Lints a tax withholding.
     */
    public LintTaxWithholdingsResponse lintTaxWithholdings(LintTaxWithholdingsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintTaxWithholdings")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveTaxWithholding(request.getContext().getTenantId(), request.getTaxWithholding());

        var outbound0 = BrowseDepreciationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseDepreciationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getTaxWithholding().getAudit().getCreatedAt())
                        .build())
                .build();
        var pending0 = assetsDepreciationQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).browseDepreciations(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = (CalculatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getTaxWithholding().getAudit().getCreatedAt())
                                .setCreatedBy(request.getTaxWithholding().getAudit().getCreatedBy())
                                .setEtag(request.getTaxWithholding().getAudit().getEtag())
                                .build())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDisplayName(request.getTaxWithholding().getOwner().getDisplayName())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePayStub(outbound1);

        var outbound2 = (FindAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getPayStub().getAudit().getCreatedAt())
                        .setRegion(reply1.getPayStub().getBillingAddress().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply2 = auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findAuditEvents(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getTaxWithholding().getTaxWithholdingId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintTaxWithholdings", request.getTaxWithholding().getTaxWithholdingId());
        return LintTaxWithholdingsResponse.newBuilder()
                .setOutcome(LintTaxWithholdingsResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getPayStub().getAudit().getUpdatedBy())
                                .build())
                        .setStatus(request.getTaxWithholding().getStatus())
                        .setTaxWithholdingId(request.getTaxWithholdingId())
                        .build())
                .build();
    }
}
