package com.acme.finance.domains.payroll;

import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.ListAccessPoliciesRequest;
import com.acme.authorization.v1.ListAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.community.v1.BrowseForumsRequest;
import com.acme.community.v1.BrowseForumsResponse;
import com.acme.community.v1.ForumQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.SuggestJournalEntriesRequest;
import com.acme.ledger.v1.SuggestJournalEntriesResponse;
import com.acme.licensing.v1.ListSoftwareLicensesRequest;
import com.acme.licensing.v1.ListSoftwareLicensesResponse;
import com.acme.licensing.v1.SoftwareLicenseServiceGrpc;
import com.acme.metrics.v1.ListMetricSeriesesRequest;
import com.acme.metrics.v1.ListMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.payroll.v1.BatchCreatePayRunsRequest;
import com.acme.payroll.v1.BatchCreatePayRunsResponse;
import com.acme.payroll.v1.BatchGetPayRunsRequest;
import com.acme.payroll.v1.BatchGetPayRunsResponse;
import com.acme.payroll.v1.BrowsePayRunsRequest;
import com.acme.payroll.v1.BrowsePayRunsResponse;
import com.acme.payroll.v1.CalculateTaxWithholdingRequest;
import com.acme.payroll.v1.CalculateTaxWithholdingResponse;
import com.acme.payroll.v1.CheckTaxWithholdingEligibilityRequest;
import com.acme.payroll.v1.CheckTaxWithholdingEligibilityResponse;
import com.acme.payroll.v1.ClonePayRunRequest;
import com.acme.payroll.v1.ClonePayRunResponse;
import com.acme.payroll.v1.CreatePayRunRequest;
import com.acme.payroll.v1.CreatePayRunResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import com.acme.payroll.v1.ExplainPayStubDecisionRequest;
import com.acme.payroll.v1.ExplainPayStubDecisionResponse;
import com.acme.payroll.v1.GetPayRunRatesRequest;
import com.acme.payroll.v1.GetPayRunRatesResponse;
import com.acme.payroll.v1.GetPayRunRequest;
import com.acme.payroll.v1.GetPayRunResponse;
import com.acme.payroll.v1.LintTaxWithholdingsRequest;
import com.acme.payroll.v1.LintTaxWithholdingsResponse;
import com.acme.payroll.v1.ListPayRunsRequest;
import com.acme.payroll.v1.ListPayRunsResponse;
import com.acme.payroll.v1.NormalizeDeductionRequest;
import com.acme.payroll.v1.NormalizeDeductionResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunQueryServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubBankSettlement;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.ScoreDeductionRequest;
import com.acme.payroll.v1.ScoreDeductionResponse;
import com.acme.payroll.v1.SearchPayRunsRequest;
import com.acme.payroll.v1.SearchPayRunsResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingDiscountAction;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.payroll.v1.UpdatePayRunRequest;
import com.acme.payroll.v1.UpdatePayRunResponse;
import com.acme.procurement.v1.PreviewRfqRequest;
import com.acme.procurement.v1.PreviewRfqResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqComment;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Manages pay runs: creates, reads, changes, and lists them. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class PayRunApplication {
    private static final Logger log = LoggerFactory.getLogger(PayRunApplication.class);

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final ChannelRegistry channels;

    private final ForumQueryServiceGrpc.ForumQueryServiceBlockingStub communityForumQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking;

    private final SoftwareLicenseServiceGrpc.SoftwareLicenseServiceBlockingStub licensingSoftwareLicenseServiceBlocking;

    private final PayRunRequestMapper mapper;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final DeductionValidationServiceGrpc.DeductionValidationServiceBlockingStub payrollDeductionValidationServiceBlocking;

    private final PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking;

    private final PayRunQueryServiceGrpc.PayRunQueryServiceBlockingStub payrollPayRunQueryServiceBlocking;

    private final PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking;

    private final TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking;

    private final TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceBlockingStub payrollTaxWithholdingValidationServiceBlocking;

    private final PayrollRepository repository;

    public PayRunApplication(
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            ChannelRegistry channels,
            ForumQueryServiceGrpc.ForumQueryServiceBlockingStub communityForumQueryServiceBlocking,
            ApplicationEventPublisher events,
            JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking,
            SoftwareLicenseServiceGrpc.SoftwareLicenseServiceBlockingStub licensingSoftwareLicenseServiceBlocking,
            PayRunRequestMapper mapper,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            DeductionValidationServiceGrpc.DeductionValidationServiceBlockingStub payrollDeductionValidationServiceBlocking,
            PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking,
            PayRunQueryServiceGrpc.PayRunQueryServiceBlockingStub payrollPayRunQueryServiceBlocking,
            PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking,
            TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking,
            TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceBlockingStub payrollTaxWithholdingValidationServiceBlocking,
            PayrollRepository repository) {
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.channels = channels;
        this.communityForumQueryServiceBlocking = communityForumQueryServiceBlocking;
        this.events = events;
        this.ledgerJournalEntryQueryServiceBlocking = ledgerJournalEntryQueryServiceBlocking;
        this.licensingSoftwareLicenseServiceBlocking = licensingSoftwareLicenseServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.payrollDeductionValidationServiceBlocking = payrollDeductionValidationServiceBlocking;
        this.payrollPayRunCalculationServiceBlocking = payrollPayRunCalculationServiceBlocking;
        this.payrollPayRunQueryServiceBlocking = payrollPayRunQueryServiceBlocking;
        this.payrollPayStubValidationServiceBlocking = payrollPayStubValidationServiceBlocking;
        this.payrollTaxWithholdingCalculationServiceBlocking = payrollTaxWithholdingCalculationServiceBlocking;
        this.payrollTaxWithholdingValidationServiceBlocking = payrollTaxWithholdingValidationServiceBlocking;
        this.repository = repository;
    }

    public record BatchCreatePayRunsStage1Requested(ScoreDeductionRequest request, CompletableFuture<ScoreDeductionResponse> completion, io.grpc.Context context) {}

    /**
     * Creates a pay run.
     */
    public CreatePayRunResponse createPayRun(CreatePayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreatePayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = CheckTaxWithholdingEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPayRun().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getPayRun().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getPayRun().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.payroll.v1.TaxWithholdingValidationService/CheckTaxWithholdingEligibility", outbound0);
        var reply0 = payrollTaxWithholdingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkTaxWithholdingEligibility(command0.payload());

        var outbound1 = PreviewRfqRequest.newBuilder()
                .setContext(request.getContext())
                .setRfq(Rfq.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getPayRun().getCounterparty().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPayRun().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getPayRun().getAudit().getUpdatedBy())
                                .build())
                        .addComments(RfqComment.newBuilder()
                                .setCreatedAt(reply0.getTaxWithholding().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.procurement.v1.RfqValidationService");
        var serviceName1 = "acme.procurement.v1" + "." + "RfqValidationService";
        var methodName1 = "Pre" + "viewRfq";
        var descriptor1 = MethodDescriptor.<PreviewRfqRequest, PreviewRfqResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(PreviewRfqRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(PreviewRfqResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreatePayRun", request.getPayRun().getPayRunId());
        return CreatePayRunResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setDueAt(reply1.getRfq().getSla().getDueAt())
                        .setIssuedAt(request.getPayRun().getIssuedAt())
                        .addLines(PayRunLine.newBuilder()
                                .setDescription(reply0.getTaxWithholding().getDescription())
                                .build())
                        .setNumber(request.getPayRun().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a pay run.
     */
    public GetPayRunResponse getPayRun(GetPayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getPayRunId(), "GetPayRun");

        var outbound0 = GetPayRunRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRunId(request.getPayRunId())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getPayRunRates(value)))
                .collect(Collectors.toList()).getFirst();
        return GetPayRunResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPayRun().getAudit().getEtag())
                                .build())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Changes a pay run.
     */
    public UpdatePayRunResponse updatePayRun(UpdatePayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdatePayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = mapper.updatePayRunStage0(request);
        var reply0 = ledgerJournalEntryQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestJournalEntries(outbound0);

        var outbound1 = (ExplainPayStubDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPayRun().getAudit().getUpdatedAt())
                                .build())
                        .setBank(PayStubBankSettlement.newBuilder()
                                .setBic(request.getPayRun().getBank().getBic())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = payrollPayStubValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPayStubDecision(outbound1);

        var outbound2 = ListAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAccessPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getPayStub().getAudit().getCreatedAt())
                        .setRegion(request.getPayRun().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAccessPolicies(value));

        var outbound3 = GetPayRunRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRunId(request.getPayRun().getPayRunId())
                .build();
        var reply3 = Stream.of(outbound3).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getPayRunRates(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdatePayRun", request.getPayRun().getNumber());
        return UpdatePayRunResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setDueAt(request.getPayRun().getDueAt())
                        .setIssuedAt(reply1.getPayStub().getIssuedAt())
                        .setNotes(request.getPayRun().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists pay runs a page at a time.
     */
    public ListPayRunsResponse listPayRuns(ListPayRunsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPayRuns")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getContext().getTraceId(), "ListPayRuns");

        var outbound0 = BrowseForumsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseForumsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageSize(request.getPage().getPageSize())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> communityForumQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseForums(value));
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPayRuns", request.getPage().getPageToken());
        return ListPayRunsResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setTotalSize(reply0.getPageInfo().getTotalSize())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns several pay runs at once.
     */
    public BatchGetPayRunsResponse batchGetPayRuns(BatchGetPayRunsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetPayRuns")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getContext().getTraceId(), "BatchGetPayRuns");

        var outbound0 = RpcSupport.unpack(Any.pack(LintTaxWithholdingsRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build()), LintTaxWithholdingsRequest.class);
        var reply0 = payrollTaxWithholdingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintTaxWithholdings(outbound0);
        return BatchGetPayRunsResponse.newBuilder()
                .addPayRuns(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getTaxWithholding().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists pay runs a page at a time.
     */
    public SearchPayRunsResponse searchPayRuns(SearchPayRunsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchPayRuns")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getContext().getTraceId(), "SearchPayRuns");

        var outbound0 = (ListAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAccessPoliciesRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .setOrderBy(request.getOrderBy())
                .setPage(PageRequest.newBuilder()
                        .setPageToken(request.getPage().getPageToken())
                        .build())
                .build()).toBuilder().build();
        var reply0 = authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAccessPolicies(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getRegion()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchPayRuns", request.getFilter().getRegion());
        return SearchPayRunsResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setTotalSize(reply0.getPageInfo().getTotalSize())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Takes a stream of pay runs and answers once at the end.
     */
    public BatchCreatePayRunsResponse batchCreatePayRuns(BatchCreatePayRunsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreatePayRuns")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.savePayRun(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = ListMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRecord().getIssuedAt())
                        .setOwnerId(switch (request.getChunkCase()) {
                            case RECORD -> request.getRecord().getCounterparty().getDeductionId();
                            default -> "";
                        })
                        .setRegion(switch (request.getChunkCase()) {
                            case RECORD -> request.getRecord().getBillingAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply0 = metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listMetricSerieses(outbound0);

        var outbound1 = ScoreDeductionRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRecord().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<ScoreDeductionResponse>();
        events.publishEvent(new BatchCreatePayRunsStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = ListSoftwareLicensesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListSoftwareLicensesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getRecord().getAudit().getUpdatedAt())
                        .setRegion(switch (request.getChunkCase()) {
                            case RECORD -> request.getRecord().getBillingAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply2 = licensingSoftwareLicenseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listSoftwareLicenses(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreatePayRuns", request.getHeader().getSource());
        return BatchCreatePayRunsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Creates a pay run.
     */
    public ClonePayRunResponse clonePayRun(ClonePayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ClonePayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = JsonCodec.roundTrip(NormalizeDeductionRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getPayRun().getAudit().getCreatedAt())
                                .setCreatedBy(request.getPayRun().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getPayRun().getAudit().getUpdatedAt())
                                .build())
                        .setDeductionId(request.getPayRun().getCounterparty().getDeductionId())
                        .build())
                .build(), NormalizeDeductionRequest.class);
        var reply0 = payrollDeductionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeDeduction(outbound0);

        var outbound1 = mapper.clonePayRunStage1(request, reply0);
        var reply1 = payrollPayRunQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browsePayRuns(outbound1);

        var outbound2 = SuggestJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestJournalEntriesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPayRun().getAudit().getCreatedAt())
                        .setRegion(request.getPayRun().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(ledgerJournalEntryQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestJournalEntries(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = (CalculateTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateTaxWithholdingRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getPayRun().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setDiscount(TaxWithholdingDiscountAction.newBuilder()
                                .setAmount(reply0.getDeduction().getDiscount().getAmount())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply3 = payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateTaxWithholding(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ClonePayRun", request.getPayRun().getPayRunId());
        return ClonePayRunResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getDeduction().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getDeduction().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setVersion(reply3.getTaxWithholding().getVersion())
                                .build())
                        .setIssuedAt(request.getPayRun().getIssuedAt())
                        .setNotes(request.getPayRun().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }
}
