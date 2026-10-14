package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualLine;
import com.acme.accounting.v1.AccrualLineTax;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationRef;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.ExplainAccrualDecisionRequest;
import com.acme.accounting.v1.ExplainAccrualDecisionResponse;
import com.acme.accounting.v1.ExplainAllocationDecisionRequest;
import com.acme.accounting.v1.ExplainAllocationDecisionResponse;
import com.acme.accounting.v1.GetAllocationRulesRequest;
import com.acme.accounting.v1.GetAllocationRulesResponse;
import com.acme.accounting.v1.LintAccrualsRequest;
import com.acme.accounting.v1.LintAccrualsResponse;
import com.acme.accounting.v1.LintAllocationsRequest;
import com.acme.accounting.v1.LintAllocationsResponse;
import com.acme.accounting.v1.LintChartOfAccountsesRequest;
import com.acme.accounting.v1.LintChartOfAccountsesResponse;
import com.acme.accounting.v1.NormalizeAccrualRequest;
import com.acme.accounting.v1.NormalizeAccrualResponse;
import com.acme.accounting.v1.NormalizeAllocationRequest;
import com.acme.accounting.v1.NormalizeAllocationResponse;
import com.acme.accounting.v1.PreviewAllocationRequest;
import com.acme.accounting.v1.PreviewAllocationResponse;
import com.acme.accounting.v1.QuoteReconciliationRequest;
import com.acme.accounting.v1.QuoteReconciliationResponse;
import com.acme.accounting.v1.RecalculateReconciliationsRequest;
import com.acme.accounting.v1.RecalculateReconciliationsResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.ReconciliationComment;
import com.acme.accounting.v1.ScoreAllocationRequest;
import com.acme.accounting.v1.ScoreAllocationResponse;
import com.acme.accounting.v1.ScoreChartOfAccountsRequest;
import com.acme.accounting.v1.ScoreChartOfAccountsResponse;
import com.acme.accounting.v1.ValidateAccrualRequest;
import com.acme.accounting.v1.ValidateAccrualResponse;
import com.acme.accounting.v1.ValidateAllocationRequest;
import com.acme.accounting.v1.ValidateAllocationResponse;
import com.acme.audit.v1.PushRetentionPolicyRequest;
import com.acme.audit.v1.PushRetentionPolicyResponse;
import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyCondition;
import com.acme.audit.v1.RetentionPolicyDiscountAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningComment;
import com.acme.billing.v1.DunningValidationServiceGrpc;
import com.acme.billing.v1.NormalizeDunningRequest;
import com.acme.billing.v1.NormalizeDunningResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.employees.v1.BrowseEmployeesRequest;
import com.acme.employees.v1.BrowseEmployeesResponse;
import com.acme.employees.v1.EmployeeQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.payroll.v1.CompareDeductionOptionsRequest;
import com.acme.payroll.v1.CompareDeductionOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.QuoteDeductionRequest;
import com.acme.payroll.v1.QuoteDeductionResponse;
import com.acme.recruiting.v1.CalculateJobOpeningRequest;
import com.acme.recruiting.v1.CalculateJobOpeningResponse;
import com.acme.recruiting.v1.InterviewRef;
import com.acme.recruiting.v1.JobOpening;
import com.acme.recruiting.v1.JobOpeningCalculationServiceGrpc;
import com.acme.recruiting.v1.JobOpeningLine;
import com.acme.search.v1.BatchCreateSearchIndexesRequest;
import com.acme.search.v1.BatchCreateSearchIndexesResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Checks allocations against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class AllocationValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(AllocationValidationApplication.class);

    private final AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking;

    private final AccrualValidationServiceGrpc.AccrualValidationServiceFutureStub accountingAccrualValidationServiceFuture;

    private final ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking;

    private final ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final DunningValidationServiceGrpc.DunningValidationServiceBlockingStub billingDunningValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final EmployeeQueryServiceGrpc.EmployeeQueryServiceBlockingStub employeesEmployeeQueryServiceBlocking;

    private final DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking;

    private final JobOpeningCalculationServiceGrpc.JobOpeningCalculationServiceBlockingStub recruitingJobOpeningCalculationServiceBlocking;

    private final AccountingRepository repository;

    private final SearchIndexServiceGrpc.SearchIndexServiceStub searchSearchIndexServiceAsync;

    public AllocationValidationApplication(
            AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking,
            AccrualValidationServiceGrpc.AccrualValidationServiceFutureStub accountingAccrualValidationServiceFuture,
            ChartOfAccountsValidationServiceGrpc.ChartOfAccountsValidationServiceBlockingStub accountingChartOfAccountsValidationServiceBlocking,
            ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            DunningValidationServiceGrpc.DunningValidationServiceBlockingStub billingDunningValidationServiceBlocking,
            ChannelRegistry channels,
            EmployeeQueryServiceGrpc.EmployeeQueryServiceBlockingStub employeesEmployeeQueryServiceBlocking,
            DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking,
            JobOpeningCalculationServiceGrpc.JobOpeningCalculationServiceBlockingStub recruitingJobOpeningCalculationServiceBlocking,
            AccountingRepository repository,
            SearchIndexServiceGrpc.SearchIndexServiceStub searchSearchIndexServiceAsync) {
        this.accountingAccrualValidationServiceBlocking = accountingAccrualValidationServiceBlocking;
        this.accountingAccrualValidationServiceFuture = accountingAccrualValidationServiceFuture;
        this.accountingChartOfAccountsValidationServiceBlocking = accountingChartOfAccountsValidationServiceBlocking;
        this.accountingReconciliationCalculationServiceBlocking = accountingReconciliationCalculationServiceBlocking;
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.billingDunningValidationServiceBlocking = billingDunningValidationServiceBlocking;
        this.channels = channels;
        this.employeesEmployeeQueryServiceBlocking = employeesEmployeeQueryServiceBlocking;
        this.payrollDeductionCalculationServiceBlocking = payrollDeductionCalculationServiceBlocking;
        this.recruitingJobOpeningCalculationServiceBlocking = recruitingJobOpeningCalculationServiceBlocking;
        this.repository = repository;
        this.searchSearchIndexServiceAsync = searchSearchIndexServiceAsync;
    }

    /**
     * Validates an allocation.
     */
    public ValidateAllocationResponse validateAllocation(ValidateAllocationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateAllocation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = QuoteDeductionRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAllocation().getAudit().getCreatedAt())
                                .setCreatedBy(request.getAllocation().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.payroll.v1.DeductionCalculationService");
        var serviceName0 = "acme.payroll.v1" + "." + "DeductionCalculationService";
        var methodName0 = "Quo" + "teDeduction";
        var descriptor0 = MethodDescriptor.<QuoteDeductionRequest, QuoteDeductionResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(QuoteDeductionRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(QuoteDeductionResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var results1 = new ArrayList<PushRetentionPolicyResponse>();
        for (var element : request.getAllocation().getConditionsList()) {
            PushRetentionPolicyRequest itemRequest = PushRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushRetentionPolicyRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .addConditions(RetentionPolicyCondition.newBuilder()
                                .setOperator(element.getOperator())
                                .build())
                        .build())
                .build();
            results1.add(auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushRetentionPolicy(itemRequest));
        }
        var reply1 = results1.isEmpty() ? PushRetentionPolicyResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = ResolveRetentionPolicyConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolveRetentionPolicyConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getRetentionPolicy().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getDeduction().getAudit().getCreatedBy())
                                .build())
                        .setDiscount(RetentionPolicyDiscountAction.newBuilder()
                                .setPercent(reply0.getDeduction().getDiscount().getPercent())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.audit.v1.RetentionPolicySyncService");
        var selected2 = RetentionPolicySyncServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.resolveRetentionPolicyConflict(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateAllocation", request.getAllocation().getAllocationId());
        return ValidateAllocationResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply2.getRetentionPolicy().getOwner().getDisplayName())
                                .build())
                        .setVersion(request.getAllocation().getVersion())
                        .build())
                .setOutcome(ValidateAllocationResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Previews an allocation.
     */
    public PreviewAllocationResponse previewAllocation(PreviewAllocationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewAllocation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = NormalizeDunningRequest.newBuilder()
                .setContext(request.getContext())
                .setDunning(Dunning.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getAllocation().getAudit().getUpdatedAt())
                                .build())
                        .addComments(DunningComment.newBuilder()
                                .setCreatedAt(request.getAllocation().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> billingDunningValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeDunning(value));

        var outbound1 = NormalizeAccrualRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeAccrualRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getDunning().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.accounting.v1.AccrualValidationService/NormalizeAccrual", outbound1);
        var reply1 = accountingAccrualValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAccrual(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewAllocation", request.getAllocation().getAllocationId());
        return PreviewAllocationResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getAccrual().getAudit().getEtag())
                                .build())
                        .setOwner(request.getAllocation().getOwner())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Normalizes an allocation.
     */
    public NormalizeAllocationResponse normalizeAllocation(NormalizeAllocationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeAllocation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = ExplainAccrualDecisionRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setCounterparty(AllocationRef.newBuilder()
                                .setVersion(request.getAllocation().getVersion())
                                .build())
                        .addLines(AccrualLine.newBuilder()
                                .setDescription(request.getAllocation().getDescription())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.accounting.v1.AccrualValidationService");
        var serviceName0 = "acme.accounting.v1" + "." + "AccrualValidationService";
        var methodName0 = "Exp" + "lainAccrualDecision";
        var descriptor0 = MethodDescriptor.<ExplainAccrualDecisionRequest, ExplainAccrualDecisionResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ExplainAccrualDecisionRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ExplainAccrualDecisionResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeAllocation", request.getAllocation().getAllocationId());
        return NormalizeAllocationResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .putAllParameters(request.getAllocation().getParametersMap())
                        .build())
                .setOutcome(NormalizeAllocationResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Explains an allocation.
     */
    public ExplainAllocationDecisionResponse explainAllocationDecision(ExplainAllocationDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainAllocationDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = BatchCreateSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setRecord(SearchIndex.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAllocation().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getAllocation().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getAllocation().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getAllocation().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(BatchCreateSearchIndexesResponse.getDefaultInstance());
        var input0 = searchSearchIndexServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateSearchIndexes(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();

        var outbound1 = ValidateAccrualRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAllocation().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getAllocation().getAudit().getUpdatedAt())
                                .build())
                        .addLines(AccrualLine.newBuilder()
                                .setTax(AccrualLineTax.newBuilder()
                                        .setAmount(request.getAllocation().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ValidateAccrualRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> accountingAccrualValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateAccrual(value)).orElseThrow();

        var outbound2 = CompareDeductionOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAllocation().getAudit().getCreatedAt())
                                .setCreatedBy(request.getAllocation().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getAllocation().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setDetails(CompareDeductionOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getAccrual().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(payrollDeductionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareDeductionOptions(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = NormalizeAccrualRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getAccrual().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeAccrualRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getAllocation().getOwner().getEmail())
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .build();
        var pending3 = accountingAccrualValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAccrual(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainAllocationDecision", request.getAllocationId());
        return ExplainAllocationDecisionResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(reply3.getAccrual().getCounterparty().getAllocationId())
                        .setDescription(request.getAllocation().getDescription())
                        .setName(request.getAllocation().getName())
                        .build())
                .setOutcome(ExplainAllocationDecisionResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Scores an allocation.
     */
    public ScoreAllocationResponse scoreAllocation(ScoreAllocationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreAllocation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var outbound0 = RecalculateReconciliationsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateReconciliationsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .addComments(ReconciliationComment.newBuilder()
                                .setCreatedAt(request.getAllocation().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.accounting.v1.ReconciliationCalculationService");
        var serviceName0 = "acme.accounting.v1" + "." + "ReconciliationCalculationService";
        var methodName0 = "Rec" + "alculateReconciliations";
        var descriptor0 = MethodDescriptor.<RecalculateReconciliationsRequest, RecalculateReconciliationsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(RecalculateReconciliationsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RecalculateReconciliationsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ScoreChartOfAccountsRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(reply0.getReconciliation().getAssignee().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage1Adapter {
            ScoreChartOfAccountsResponse fetch(ScoreChartOfAccountsRequest input) {
                return accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreChartOfAccounts(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = JsonCodec.roundTrip(NormalizeDunningRequest.newBuilder()
                .setContext(request.getContext())
                .setDunning(Dunning.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getChartOfAccounts().getManager().getEmail())
                                .build())
                        .build())
                .build(), NormalizeDunningRequest.class);
        var reply2 = billingDunningValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeDunning(outbound2);

        var outbound3 = (LintAccrualsRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getDunning().getAudit().getCreatedAt())
                                .setEtag(reply0.getReconciliation().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(LintAccrualsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply2.getDunning().getAssignee().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply3 = accountingAccrualValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAccruals(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreAllocation", request.getAllocation().getAllocationId());
        return ScoreAllocationResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getChartOfAccounts().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getReconciliation().getAudit().getUpdatedAt())
                                .build())
                        .setPriority(request.getAllocation().getPriority())
                        .build())
                .setBreakdown(ScoreAllocationResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply2.getDunning().getCustomer().getCurrencyCode())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply3.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Lints an allocation.
     */
    public LintAllocationsResponse lintAllocations(LintAllocationsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintAllocations")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAllocation(request.getContext().getTenantId(), request.getAllocation());

        var builder0 = QuoteReconciliationRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setReconciliation(Reconciliation.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedBy(request.getAllocation().getAudit().getCreatedBy())
                        .build())
                .setRequester(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getContact().getEmail())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = accountingReconciliationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteReconciliation(outbound0);

        var outbound1 = BrowseEmployeesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseEmployeesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAllocation().getAudit().getCreatedAt())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> employeesEmployeeQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseEmployees(value));

        var outbound2 = JsonCodec.roundTrip(LintChartOfAccountsesRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(reply0.getReconciliation().getAudit().getCreatedBy())
                                                .setEtag(reply0.getReconciliation().getAudit().getEtag())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), LintChartOfAccountsesRequest.class);
        var reply2 = accountingChartOfAccountsValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintChartOfAccountses(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getAllocation().getAllocationId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintAllocations", request.getAllocation().getAllocationId());
        return LintAllocationsResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getChartOfAccounts().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getReconciliation().getAssignee().getDisplayName())
                                .build())
                        .setVersion(request.getAllocation().getVersion())
                        .build())
                .setOutcome(LintAllocationsResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns an allocation.
     */
    public GetAllocationRulesResponse getAllocationRules(GetAllocationRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAllocationRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Allocation", request.getAllocationId(), "GetAllocationRules");

        var outbound0 = CalculateJobOpeningRequest.newBuilder()
                .setContext(request.getContext())
                .setJobOpening(JobOpening.newBuilder()
                        .setCounterparty(InterviewRef.newBuilder()
                                .setStartsAt(request.getContext().getDeadline())
                                .build())
                        .addLines(JobOpeningLine.newBuilder()
                                .putAllAttributes(request.getContext().getBaggageMap())
                                .build())
                        .build())
                .build();
        var reply0 = recruitingJobOpeningCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateJobOpening(outbound0);
        return GetAllocationRulesResponse.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAllocationId(request.getAllocationId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
