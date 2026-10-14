package com.acme.finance.domains.payroll;

import com.acme.audit.v1.LintRetentionPoliciesRequest;
import com.acme.audit.v1.LintRetentionPoliciesResponse;
import com.acme.audit.v1.NormalizeRetentionPolicyRequest;
import com.acme.audit.v1.NormalizeRetentionPolicyResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.ResolveAccessPoliciesRequest;
import com.acme.authorization.v1.ResolveAccessPoliciesResponse;
import com.acme.budgeting.v1.ListVarianceImportsRequest;
import com.acme.budgeting.v1.ListVarianceImportsResponse;
import com.acme.budgeting.v1.ValidateVarianceImportRequest;
import com.acme.budgeting.v1.ValidateVarianceImportResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.giftcards.v1.CompareGiftCardOptionsRequest;
import com.acme.giftcards.v1.CompareGiftCardOptionsResponse;
import com.acme.giftcards.v1.GiftCard;
import com.acme.giftcards.v1.GiftCardBankFunding;
import com.acme.giftcards.v1.GiftCardCalculationServiceGrpc;
import com.acme.giftcards.v1.GiftCardLimit;
import com.acme.identity.v1.UserRef;
import com.acme.patients.v1.BrowsePatientsRequest;
import com.acme.patients.v1.BrowsePatientsResponse;
import com.acme.patients.v1.PatientQueryServiceGrpc;
import com.acme.payroll.v1.ApprovePayStubRequest;
import com.acme.payroll.v1.ApprovePayStubResponse;
import com.acme.payroll.v1.CommentPayStubApprovalRequest;
import com.acme.payroll.v1.CommentPayStubApprovalResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionLifecycleServiceGrpc;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.DeductionReportingServiceGrpc;
import com.acme.payroll.v1.DelegatePayStubApprovalRequest;
import com.acme.payroll.v1.DelegatePayStubApprovalResponse;
import com.acme.payroll.v1.EscalatePayStubRequest;
import com.acme.payroll.v1.EscalatePayStubResponse;
import com.acme.payroll.v1.EstimatePayRunRequest;
import com.acme.payroll.v1.EstimatePayRunResponse;
import com.acme.payroll.v1.ExplainPayStubCalculationRequest;
import com.acme.payroll.v1.ExplainPayStubCalculationResponse;
import com.acme.payroll.v1.GetDeductionLifecycleRequest;
import com.acme.payroll.v1.GetDeductionLifecycleResponse;
import com.acme.payroll.v1.GetPayStubApprovalStatusRequest;
import com.acme.payroll.v1.GetPayStubApprovalStatusResponse;
import com.acme.payroll.v1.ListPendingPayRunsRequest;
import com.acme.payroll.v1.ListPendingPayRunsResponse;
import com.acme.payroll.v1.ListPendingPayStubsRequest;
import com.acme.payroll.v1.ListPendingPayStubsResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunApprovalServiceGrpc;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubCardSettlement;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PreviewPayRunRequest;
import com.acme.payroll.v1.PreviewPayRunResponse;
import com.acme.payroll.v1.RecalculateDeductionsRequest;
import com.acme.payroll.v1.RecalculateDeductionsResponse;
import com.acme.payroll.v1.RecallPayStubRequest;
import com.acme.payroll.v1.RecallPayStubResponse;
import com.acme.payroll.v1.RejectPayStubRequest;
import com.acme.payroll.v1.RejectPayStubResponse;
import com.acme.payroll.v1.ShareDeductionReportRequest;
import com.acme.payroll.v1.ShareDeductionReportResponse;
import com.acme.payroll.v1.ShareTaxWithholdingReportRequest;
import com.acme.payroll.v1.ShareTaxWithholdingReportResponse;
import com.acme.payroll.v1.SubmitPayStubRequest;
import com.acme.payroll.v1.SubmitPayStubResponse;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import com.acme.search.v1.CountSearchIndexesRequest;
import com.acme.search.v1.CountSearchIndexesResponse;
import com.acme.suppliers.v1.ListPendingSupplierContractsRequest;
import com.acme.suppliers.v1.ListPendingSupplierContractsResponse;
import com.acme.suppliers.v1.SubmitSupplierRiskRequest;
import com.acme.suppliers.v1.SubmitSupplierRiskResponse;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierRiskApprovalServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Routes pay stubs through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class PayStubApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(PayStubApprovalApplication.class);

    private final RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final VarianceImportServiceGrpc.VarianceImportServiceBlockingStub budgetingVarianceImportServiceBlocking;

    private final ChannelRegistry channels;

    private final GiftCardCalculationServiceGrpc.GiftCardCalculationServiceBlockingStub giftcardsGiftCardCalculationServiceBlocking;

    private final PayStubApprovalRequestMapper mapper;

    private final PatientQueryServiceGrpc.PatientQueryServiceBlockingStub patientsPatientQueryServiceBlocking;

    private final DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking;

    private final DeductionLifecycleServiceGrpc.DeductionLifecycleServiceBlockingStub payrollDeductionLifecycleServiceBlocking;

    private final DeductionReportingServiceGrpc.DeductionReportingServiceBlockingStub payrollDeductionReportingServiceBlocking;

    private final PayRunApprovalServiceGrpc.PayRunApprovalServiceBlockingStub payrollPayRunApprovalServiceBlocking;

    private final PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking;

    private final PayRunValidationServiceGrpc.PayRunValidationServiceBlockingStub payrollPayRunValidationServiceBlocking;

    private final PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking;

    private final TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceBlockingStub payrollTaxWithholdingReportingServiceBlocking;

    private final PayrollRepository repository;

    private final Executor rpcExecutor;

    private final SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceBlockingStub suppliersSupplierContractApprovalServiceBlocking;

    private final SupplierRiskApprovalServiceGrpc.SupplierRiskApprovalServiceFutureStub suppliersSupplierRiskApprovalServiceFuture;

    public PayStubApprovalApplication(
            RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            VarianceImportServiceGrpc.VarianceImportServiceBlockingStub budgetingVarianceImportServiceBlocking,
            ChannelRegistry channels,
            GiftCardCalculationServiceGrpc.GiftCardCalculationServiceBlockingStub giftcardsGiftCardCalculationServiceBlocking,
            PayStubApprovalRequestMapper mapper,
            PatientQueryServiceGrpc.PatientQueryServiceBlockingStub patientsPatientQueryServiceBlocking,
            DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking,
            DeductionLifecycleServiceGrpc.DeductionLifecycleServiceBlockingStub payrollDeductionLifecycleServiceBlocking,
            DeductionReportingServiceGrpc.DeductionReportingServiceBlockingStub payrollDeductionReportingServiceBlocking,
            PayRunApprovalServiceGrpc.PayRunApprovalServiceBlockingStub payrollPayRunApprovalServiceBlocking,
            PayRunCalculationServiceGrpc.PayRunCalculationServiceBlockingStub payrollPayRunCalculationServiceBlocking,
            PayRunValidationServiceGrpc.PayRunValidationServiceBlockingStub payrollPayRunValidationServiceBlocking,
            PayStubCalculationServiceGrpc.PayStubCalculationServiceBlockingStub payrollPayStubCalculationServiceBlocking,
            TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceBlockingStub payrollTaxWithholdingReportingServiceBlocking,
            PayrollRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceBlockingStub suppliersSupplierContractApprovalServiceBlocking,
            SupplierRiskApprovalServiceGrpc.SupplierRiskApprovalServiceFutureStub suppliersSupplierRiskApprovalServiceFuture) {
        this.auditRetentionPolicyValidationServiceBlocking = auditRetentionPolicyValidationServiceBlocking;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.budgetingVarianceImportServiceBlocking = budgetingVarianceImportServiceBlocking;
        this.channels = channels;
        this.giftcardsGiftCardCalculationServiceBlocking = giftcardsGiftCardCalculationServiceBlocking;
        this.mapper = mapper;
        this.patientsPatientQueryServiceBlocking = patientsPatientQueryServiceBlocking;
        this.payrollDeductionCalculationServiceBlocking = payrollDeductionCalculationServiceBlocking;
        this.payrollDeductionLifecycleServiceBlocking = payrollDeductionLifecycleServiceBlocking;
        this.payrollDeductionReportingServiceBlocking = payrollDeductionReportingServiceBlocking;
        this.payrollPayRunApprovalServiceBlocking = payrollPayRunApprovalServiceBlocking;
        this.payrollPayRunCalculationServiceBlocking = payrollPayRunCalculationServiceBlocking;
        this.payrollPayRunValidationServiceBlocking = payrollPayRunValidationServiceBlocking;
        this.payrollPayStubCalculationServiceBlocking = payrollPayStubCalculationServiceBlocking;
        this.payrollTaxWithholdingReportingServiceBlocking = payrollTaxWithholdingReportingServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractApprovalServiceBlocking = suppliersSupplierContractApprovalServiceBlocking;
        this.suppliersSupplierRiskApprovalServiceFuture = suppliersSupplierRiskApprovalServiceFuture;
    }

    /**
     * Submits a pay stub.
     */
    public SubmitPayStubResponse submitPayStub(SubmitPayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitPayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        List<ShareTaxWithholdingReportResponse> results0 = request.getPayStub().getLinesList().stream()
                .map(element -> ShareTaxWithholdingReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareTaxWithholdingReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .putAllVariables(element.getAttributesMap())
                        .build())
                .setReason(request.getReason())
                .build())
                .flatMap(value -> Stream.of(payrollTaxWithholdingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareTaxWithholdingReport(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ShareTaxWithholdingReportResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ListVarianceImportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListVarianceImportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPayStub().getAudit().getUpdatedAt())
                        .setRegion(request.getPayStub().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> budgetingVarianceImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listVarianceImports(value));
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitPayStub", request.getPayStub().getPayStubId());
        return SubmitPayStubResponse.newBuilder()
                .setOutcome(SubmitPayStubResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setIssuedAt(request.getPayStub().getIssuedAt())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .build();
    }

    /**
     * Approves a pay stub.
     */
    public ApprovePayStubResponse approvePayStub(ApprovePayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApprovePayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = BeanCopies.copy(GetDeductionLifecycleRequest.newBuilder()
                .setContext(request.getContext())
                .setDeductionId(request.getPayStub().getCounterparty().getDeductionId())
                .build(), GetDeductionLifecycleRequest.class);
        var reply0 = payrollDeductionLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getDeductionLifecycle(outbound0);

        var outbound1 = CompareGiftCardOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareGiftCardOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getPayStub().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setGiftCard(GiftCard.newBuilder()
                        .setBank(GiftCardBankFunding.newBuilder()
                                .setAccountHolder(switch (request.getPayStub().getSettlementCase()) {
                                    case BANK -> request.getPayStub().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .addLimits(GiftCardLimit.newBuilder()
                                .setAmount(reply0.getDeduction().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        var reply1 = giftcardsGiftCardCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareGiftCardOptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApprovePayStub", request.getPayStub().getPayStubId());
        return ApprovePayStubResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .putAllMetadata(request.getPayStub().getMetadataMap())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Rejects a pay stub.
     */
    public RejectPayStubResponse rejectPayStub(RejectPayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectPayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = CountSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPayStub().getAudit().getCreatedAt())
                        .setOwnerId(request.getPayStubId())
                        .setRegion(request.getPayStub().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.search.v1.SearchIndexQueryService");
        var serviceName0 = "acme.search.v1" + "." + "SearchIndexQueryService";
        var methodName0 = "Cou" + "ntSearchIndexes";
        var descriptor0 = MethodDescriptor.<CountSearchIndexesRequest, CountSearchIndexesResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(CountSearchIndexesRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CountSearchIndexesResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectPayStub", request.getPayStub().getPayStubId());
        return RejectPayStubResponse.newBuilder()
                .setOutcome(RejectPayStubResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setBillingAddress(request.getPayStub().getBillingAddress())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Escalates a pay stub.
     */
    public EscalatePayStubResponse escalatePayStub(EscalatePayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalatePayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = ExplainPayStubCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setCard(PayStubCardSettlement.newBuilder()
                                .setToken(switch (request.getPayStub().getSettlementCase()) {
                                    case CARD -> request.getPayStub().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = payrollPayStubCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPayStubCalculation(outbound0);

        var futures1 = request.getPayStub().getLinesList().stream()
                .map(element -> ListPendingSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingSupplierContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPayStub().getAudit().getUpdatedAt())
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(reply0.getPayStub().getBillingAddress().getRegion())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> suppliersSupplierContractApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingSupplierContracts(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? ListPendingSupplierContractsResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalatePayStub", request.getPayStubId());
        return EscalatePayStubResponse.newBuilder()
                .setOutcome(EscalatePayStubResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setDueAt(request.getPayStub().getDueAt())
                        .setPayStubId(reply0.getPayStub().getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists pay stubs a page at a time.
     */
    public ListPendingPayStubsResponse listPendingPayStubs(ListPendingPayStubsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingPayStubs")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getContext().getTraceId(), "ListPendingPayStubs");

        var outbound0 = EstimatePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .putAllMetadata(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(payrollPayRunCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimatePayRun(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = ScoreRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreRetentionPolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply0.getPayRun().getBillingAddress().getLinesList())
                                .setLocality(reply0.getPayRun().getBillingAddress().getLocality())
                                .setPostalCode(reply0.getPayRun().getBillingAddress().getPostalCode())
                                .setRecipient(reply0.getPayRun().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> auditRetentionPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRetentionPolicy(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingPayStubs", request.getPage().getPageToken());
        return ListPendingPayStubsResponse.newBuilder()
                .addPayStubs(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPayRun().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getPayRun().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
    }

    /**
     * Delegates a pay stub.
     */
    public DelegatePayStubApprovalResponse delegatePayStubApproval(DelegatePayStubApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegatePayStubApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = ShareRetentionPolicyExportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ShareRetentionPolicyExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.audit.v1.RetentionPolicyExportService");
        var selected0 = RetentionPolicyExportServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.shareRetentionPolicyExport(outbound0);

        var outbound1 = ResolveAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply1 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAccessPolicies(outbound1);

        var outbound2 = PreviewPayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getRetentionPolicy().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getPayStub().getBillingAddress().getLocality())
                                .setRegion(request.getPayStub().getBillingAddress().getRegion())
                                .build())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setDeductionId(request.getPayStub().getCounterparty().getDeductionId())
                                .build())
                        .build())
                .build();
        Function<PreviewPayRunRequest, PreviewPayRunResponse> operation2 = payrollPayRunValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewPayRun;
        var reply2 = operation2.apply(outbound2);

        var outbound3 = mapper.delegatePayStubApprovalStage3(request, reply2);
        var reply3 = auditRetentionPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeRetentionPolicy(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegatePayStubApproval", request.getPayStub().getPayStubId());
        return DelegatePayStubApprovalResponse.newBuilder()
                .setOutcome(DelegatePayStubApprovalResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply3.getOutcome().getNextStepsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setCounterparty(DeductionRef.newBuilder()
                                .setVersion(reply3.getRetentionPolicy().getVersion())
                                .build())
                        .setDueAt(request.getPayStub().getDueAt())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Recalls a pay stub.
     */
    public RecallPayStubResponse recallPayStub(RecallPayStubRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallPayStub")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = SubmitSupplierRiskRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitSupplierRiskRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending0 = suppliersSupplierRiskApprovalServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).submitSupplierRisk(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallPayStub", request.getPayStub().getPayStubId());
        return RecallPayStubResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getSupplierRisk().getAudit().getEtag())
                                .build())
                        .putAllMetadata(request.getPayStub().getMetadataMap())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a pay stub.
     */
    public GetPayStubApprovalStatusResponse getPayStubApprovalStatus(GetPayStubApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPayStubApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayStub", request.getPayStubId(), "GetPayStubApprovalStatus");

        var outbound0 = BeanCopies.copy(RecalculateDeductionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build(), RecalculateDeductionsRequest.class);
        var reply0 = payrollDeductionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateDeductions(outbound0);

        var outbound1 = ShareDeductionReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDeductionId(reply0.getDeduction().getDeductionId())
                .setDetails(ShareDeductionReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getDeduction().getOwner().getEmail())
                                .build())
                        .putAllVariables(reply0.getDeduction().getParametersMap())
                        .build())
                .setReason(switch (reply0.getDeduction().getActionCase()) {
                    case BLOCK -> reply0.getDeduction().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply1 = payrollDeductionReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareDeductionReport(outbound1);
        return GetPayStubApprovalStatusResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getDeduction().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(DeductionRef.newBuilder()
                                .setVersion(reply1.getDeduction().getVersion())
                                .build())
                        .setPayStubId(request.getPayStubId())
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Comments a pay stub.
     */
    public CommentPayStubApprovalResponse commentPayStubApproval(CommentPayStubApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentPayStubApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayStub(request.getContext().getTenantId(), request.getPayStub());

        var outbound0 = BrowsePatientsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowsePatientsRequest.Filter.newBuilder()
                        .setOwnerId(request.getDetails().getApprover().getUserId())
                        .setRegion(request.getPayStub().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> patientsPatientQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browsePatients(outbound0), rpcExecutor);

        var outbound1 = ListPendingPayRunsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingPayRunsRequest.Filter.newBuilder()
                        .setRegion(request.getPayStub().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> payrollPayRunApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingPayRuns(outbound1), rpcExecutor);

        var outbound2 = ValidateVarianceImportRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPayStub().getAudit().getEtag())
                                .setUpdatedAt(request.getPayStub().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> budgetingVarianceImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateVarianceImport(outbound2), rpcExecutor);

        var outbound3 = LintRetentionPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintRetentionPoliciesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var pending3 = CompletableFuture.supplyAsync(() -> auditRetentionPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintRetentionPolicies(outbound3), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2, pending3).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();
        var reply3 = pending3.join();
        log.atDebug().addKeyValue("auditValue", request.getPayStub().getPayStubId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentPayStubApproval", request.getPayStub().getPayStubId());
        return CommentPayStubApprovalResponse.newBuilder()
                .setPayStub(PayStub.newBuilder()
                        .setDueAt(request.getPayStub().getDueAt())
                        .addLines(PayStubLine.newBuilder()
                                .setDescription(reply3.getRetentionPolicy().getDescription())
                                .build())
                        .setPayStubId(request.getPayStubId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
