package com.acme.finance.domains.payroll;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.UpdateCollectionCaseRequest;
import com.acme.collections.v1.UpdateCollectionCaseResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.QuoteComplianceCaseRequest;
import com.acme.compliance.v1.QuoteComplianceCaseResponse;
import com.acme.compliance.v1.SimulateComplianceCaseRequest;
import com.acme.compliance.v1.SimulateComplianceCaseResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyCondition;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.expenses.v1.NormalizeExpensePolicyRequest;
import com.acme.expenses.v1.NormalizeExpensePolicyResponse;
import com.acme.expenses.v1.PreviewExpensePolicyRequest;
import com.acme.expenses.v1.PreviewExpensePolicyResponse;
import com.acme.expenses.v1.ScoreExpensePolicyRequest;
import com.acme.expenses.v1.ScoreExpensePolicyResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.LintRepaymentsRequest;
import com.acme.lending.v1.LintRepaymentsResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.payroll.v1.CheckDeductionEligibilityRequest;
import com.acme.payroll.v1.CheckDeductionEligibilityResponse;
import com.acme.payroll.v1.ComparePayStubOptionsRequest;
import com.acme.payroll.v1.ComparePayStubOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionCondition;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.EstimateTaxWithholdingRequest;
import com.acme.payroll.v1.EstimateTaxWithholdingResponse;
import com.acme.payroll.v1.ExplainDeductionCalculationRequest;
import com.acme.payroll.v1.ExplainDeductionCalculationResponse;
import com.acme.payroll.v1.ExplainPayStubCalculationRequest;
import com.acme.payroll.v1.ExplainPayStubCalculationResponse;
import com.acme.payroll.v1.GetDeductionRulesRequest;
import com.acme.payroll.v1.GetDeductionRulesResponse;
import com.acme.payroll.v1.LintDeductionsRequest;
import com.acme.payroll.v1.LintDeductionsResponse;
import com.acme.payroll.v1.NormalizeDeductionRequest;
import com.acme.payroll.v1.NormalizeDeductionResponse;
import com.acme.payroll.v1.NormalizePayStubRequest;
import com.acme.payroll.v1.NormalizePayStubResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubLineTax;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.PreviewDeductionRequest;
import com.acme.payroll.v1.PreviewDeductionResponse;
import com.acme.payroll.v1.PreviewPayStubRequest;
import com.acme.payroll.v1.PreviewPayStubResponse;
import com.acme.payroll.v1.QuotePayRunRequest;
import com.acme.payroll.v1.QuotePayRunResponse;
import com.acme.payroll.v1.ScoreDeductionRequest;
import com.acme.payroll.v1.ScoreDeductionResponse;
import com.acme.payroll.v1.SimulateTaxWithholdingRequest;
import com.acme.payroll.v1.SimulateTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingCondition;
import com.acme.payroll.v1.TaxWithholdingDiscountAction;
import com.acme.payroll.v1.ValidateDeductionRequest;
import com.acme.payroll.v1.ValidateDeductionResponse;
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
 * Checks deductions against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class DeductionValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(DeductionValidationApplication.class);

    private final FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync;

    private final AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking;

    private final ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking;

    private final ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceBlockingStub expensesExpensePolicyValidationServiceBlocking;

    private final RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking;

    private final DeductionValidationRequestMapper mapper;

    private final DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking;

    private final PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking;

    private final PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking;

    private final PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking;

    private final TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking;

    private final PayrollRepository repository;

    private final Executor rpcExecutor;

    public DeductionValidationApplication(
            FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync,
            AuditTrailNotificationServiceGrpc.AuditTrailNotificationServiceBlockingStub auditAuditTrailNotificationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking,
            ChannelRegistry channels,
            CollectionCaseServiceGrpc.CollectionCaseServiceBlockingStub collectionsCollectionCaseServiceBlocking,
            ComplianceCaseCalculationServiceGrpc.ComplianceCaseCalculationServiceBlockingStub complianceComplianceCaseCalculationServiceBlocking,
            ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceBlockingStub expensesExpensePolicyValidationServiceBlocking,
            RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking,
            DeductionValidationRequestMapper mapper,
            DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking,
            PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking,
            PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking,
            PayStubValidationServiceGrpc.PayStubValidationServiceBlockingStub payrollPayStubValidationServiceBlocking,
            TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking,
            PayrollRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsFixedAssetServiceAsync = assetsFixedAssetServiceAsync;
        this.auditAuditTrailNotificationServiceBlocking = auditAuditTrailNotificationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.auditRetentionPolicyValidationServiceBlocking = auditRetentionPolicyValidationServiceBlocking;
        this.channels = channels;
        this.collectionsCollectionCaseServiceBlocking = collectionsCollectionCaseServiceBlocking;
        this.complianceComplianceCaseCalculationServiceBlocking = complianceComplianceCaseCalculationServiceBlocking;
        this.expensesExpensePolicyValidationServiceBlocking = expensesExpensePolicyValidationServiceBlocking;
        this.lendingRepaymentValidationServiceBlocking = lendingRepaymentValidationServiceBlocking;
        this.mapper = mapper;
        this.payrollDeductionCalculationServiceBlocking = payrollDeductionCalculationServiceBlocking;
        this.payrollPayRunCalculationServiceBlocking = payrollPayRunCalculationServiceBlocking;
        this.payrollPayStubCalculationServiceBlocking = payrollPayStubCalculationServiceBlocking;
        this.payrollPayStubValidationServiceBlocking = payrollPayStubValidationServiceBlocking;
        this.payrollTaxWithholdingCalculationServiceBlocking = payrollTaxWithholdingCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Validates a deduction.
     */
    public ValidateDeductionResponse validateDeduction(ValidateDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = SimulateTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setDiscount(TaxWithholdingDiscountAction.newBuilder()
                                .setAmount(request.getDeduction().getDiscount().getAmount())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDeduction().getOwner().getDisplayName())
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setUserId(request.getDeduction().getOwner().getUserId())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<SimulateTaxWithholdingRequest, SimulateTaxWithholdingResponse>() {
            @Override
            public SimulateTaxWithholdingResponse apply(SimulateTaxWithholdingRequest input) {
                return payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateTaxWithholding(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = BatchCreateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateFixedAssetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateFixedAssetsResponse.getDefaultInstance());
        var input1 = assetsFixedAssetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateFixedAssets(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDeduction().getAudit().getCreatedAt())
                                .setCreatedBy(request.getDeduction().getAudit().getCreatedBy())
                                .setEtag(request.getDeduction().getAudit().getEtag())
                                .setUpdatedAt(request.getDeduction().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateDeduction", request.getDeduction().getDeductionId());
        return ValidateDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getTaxWithholding().getAudit().getEtag())
                                .build())
                        .setDeductionId(request.getDeductionId())
                        .setDescription(request.getDeduction().getDescription())
                        .build())
                .build();
    }

    /**
     * Previews a deduction.
     */
    public PreviewDeductionResponse previewDeduction(PreviewDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        List<PreviewExpensePolicyResponse> results0 = request.getDeduction().getConditionsList().stream()
                .map(element -> PreviewExpensePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .addConditions(ExpensePolicyCondition.newBuilder()
                                .setOperator(element.getOperator())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(expensesExpensePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewExpensePolicy(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? PreviewExpensePolicyResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = UpdateCollectionCaseRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getExpensePolicy().getAudit().getCreatedAt())
                                .setCreatedBy(request.getDeduction().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(collectionsCollectionCaseServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateCollectionCase(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = mapper.previewDeductionStage2(request, reply0);
        var reply2 = payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePayStubOptions(outbound2);

        var futures3 = request.getDeduction().getConditionsList().stream()
                .map(element -> ScoreExpensePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .addConditions(ExpensePolicyCondition.newBuilder()
                                .setAttribute(element.getAttribute())
                                .setNegate(element.getNegate())
                                .setOperator(element.getOperator())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> expensesExpensePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreExpensePolicy(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? ScoreExpensePolicyResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewDeduction", request.getDeduction().getDeductionId());
        return PreviewDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getCollectionCase().getCustomer().getDisplayName())
                                .setUserId(reply1.getCollectionCase().getAssignee().getUserId())
                                .build())
                        .setVersion(request.getDeduction().getVersion())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Checks a deduction.
     */
    public CheckDeductionEligibilityResponse checkDeductionEligibility(CheckDeductionEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckDeductionEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = ExplainPayStubCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPayStubCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDeduction().getOwner().getEmail())
                                .build())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDeduction().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getDeduction().getAudit().getUpdatedAt())
                                .build())
                        .addLines(PayStubLine.newBuilder()
                                .setAmount(request.getDeduction().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        RpcPort<ExplainPayStubCalculationRequest, ExplainPayStubCalculationResponse> port0 = RpcPort.adapt(payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::explainPayStubCalculation);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = EstimatePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimatePayStubRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPayStub().getBillingAddress().getCountryCode())
                                .setRecipient(reply0.getPayStub().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPayStub().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.payroll.v1.PayStubCalculationService/EstimatePayStub", outbound1);
        var reply1 = payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePayStub(command1.payload());

        var outbound2 = (PreviewPayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getDeduction().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = payrollPayStubValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPayStub(outbound2);

        var outbound3 = mapper.checkDeductionEligibilityStage3(request, reply1);
        var reply3 = complianceComplianceCaseCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateComplianceCase(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckDeductionEligibility", request.getDeductionId());
        return CheckDeductionEligibilityResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getPayStub().getAudit().getCreatedAt())
                                .setEtag(reply3.getComplianceCase().getAudit().getEtag())
                                .build())
                        .setDeductionId(reply1.getPayStub().getCounterparty().getDeductionId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply3.getComplianceCase().getAssignee().getEmail())
                                .build())
                        .putAllParameters(request.getDeduction().getParametersMap())
                        .setPriority(request.getDeduction().getPriority())
                        .build())
                .setOutcome(CheckDeductionEligibilityResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Normalizes a deduction.
     */
    public NormalizeDeductionResponse normalizeDeduction(NormalizeDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = NormalizePayStubRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .addLines(PayStubLine.newBuilder()
                                .setTax(PayStubLineTax.newBuilder()
                                        .setAmount(request.getDeduction().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> payrollPayStubValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizePayStub(outbound0), rpcExecutor);

        var outbound1 = EstimateTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getDeduction().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDeduction().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateTaxWithholding(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        List<SimulateTaxWithholdingResponse> results2 = request.getDeduction().getConditionsList().stream()
                .map(element -> SimulateTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .addConditions(TaxWithholdingCondition.newBuilder()
                                .setAttribute(element.getAttribute())
                                .setOperator(element.getOperator())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateTaxWithholding(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? SimulateTaxWithholdingResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = (SubscribeAuditTrailUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeAuditTrailUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setName(request.getDetails().getContact().getName())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply3 = auditAuditTrailNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeAuditTrailUpdates(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeDeduction", request.getDeductionId());
        return NormalizeDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(reply0.getPayStub().getCounterparty().getDeductionId())
                        .setEffective(request.getDeduction().getEffective())
                        .setPriority(request.getDeduction().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Scores a deduction.
     */
    public ScoreDeductionResponse scoreDeduction(ScoreDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var builder0 = ScoreRetentionPolicyRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setRetentionPolicy(RetentionPolicy.newBuilder()
                .setOwner(UserRef.newBuilder()
                        .setUserId(request.getDeduction().getOwner().getUserId())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = auditRetentionPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRetentionPolicy(outbound0);

        List<ExplainDeductionCalculationResponse> results1 = request.getDeduction().getConditionsList().stream()
                .map(element -> ExplainDeductionCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .addConditions(DeductionCondition.newBuilder()
                                .setAttribute(element.getAttribute())
                                .setNegate(element.getNegate())
                                .setOperator(element.getOperator())
                                .setValue(element.getValue())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(payrollDeductionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainDeductionCalculation(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? ExplainDeductionCalculationResponse.getDefaultInstance()
                : results1.getLast();

        List<NormalizeExpensePolicyResponse> results2 = request.getDeduction().getConditionsList().stream()
                .map(element -> NormalizeExpensePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .addConditions(ExpensePolicyCondition.newBuilder()
                                .setAttribute(element.getAttribute())
                                .setOperator(element.getOperator())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(expensesExpensePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeExpensePolicy(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? NormalizeExpensePolicyResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = QuotePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRetentionPolicy().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getDeduction().getAudit().getUpdatedBy())
                                .build())
                        .addLines(PayRunLine.newBuilder()
                                .setTax(PayRunLineTax.newBuilder()
                                        .setAmount(reply1.getDeduction().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply3 = payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePayRun(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreDeduction", request.getDeduction().getDeductionId());
        return ScoreDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setOwner(reply0.getRetentionPolicy().getOwner())
                        .putAllParameters(reply0.getRetentionPolicy().getParametersMap())
                        .setPriority(request.getDeduction().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lints a deduction.
     */
    public LintDeductionsResponse lintDeductions(LintDeductionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintDeductions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = QuoteComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getDeduction().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getDeduction().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.compliance.v1.ComplianceCaseCalculationService");
        var selected0 = ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.quoteComplianceCase(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintDeductions", request.getDeduction().getDeductionId());
        return LintDeductionsResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getComplianceCase().getAudit().getCreatedBy())
                                .build())
                        .setDeductionId(request.getDeductionId())
                        .setDescription(request.getDeduction().getDescription())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getComplianceCase().getRequester().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns a deduction.
     */
    public GetDeductionRulesResponse getDeductionRules(GetDeductionRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDeductionRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getDeductionId(), "GetDeductionRules");

        var outbound0 = LintRepaymentsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintRepaymentsRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getContext().getActor().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = lendingRepaymentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintRepayments(outbound0);
        return GetDeductionRulesResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
