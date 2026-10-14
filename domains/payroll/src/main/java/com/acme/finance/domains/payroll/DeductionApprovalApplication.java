package com.acme.finance.domains.payroll;

import com.acme.assets.v1.CancelDepreciationWorkflowRequest;
import com.acme.assets.v1.CancelDepreciationWorkflowResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationWorkflowServiceGrpc;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.CreateAuditEventRequest;
import com.acme.audit.v1.CreateAuditEventResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.devices.v1.EstimateManagedDeviceRequest;
import com.acme.devices.v1.EstimateManagedDeviceResponse;
import com.acme.devices.v1.ManagedDevice;
import com.acme.devices.v1.ManagedDeviceCalculationServiceGrpc;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.LintDocumentsRequest;
import com.acme.documents.v1.LintDocumentsResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyLifecycleServiceGrpc;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryRequest;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.CheckUserEligibilityRequest;
import com.acme.identity.v1.CheckUserEligibilityResponse;
import com.acme.identity.v1.LintUsersRequest;
import com.acme.identity.v1.LintUsersResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.ListPolicyWorkflowsRequest;
import com.acme.insurance.v1.ListPolicyWorkflowsResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.ResumeClaimWorkflowRequest;
import com.acme.insurance.v1.ResumeClaimWorkflowResponse;
import com.acme.insurance.v1.StartPolicyWorkflowRequest;
import com.acme.insurance.v1.StartPolicyWorkflowResponse;
import com.acme.labeling.v1.LabelingTaskServiceGrpc;
import com.acme.labeling.v1.ListLabelingTasksRequest;
import com.acme.labeling.v1.ListLabelingTasksResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.RevertFiscalPeriodRequest;
import com.acme.ledger.v1.RevertFiscalPeriodResponse;
import com.acme.ledger.v1.SubscribeLedgerAccountUpdatesRequest;
import com.acme.ledger.v1.SubscribeLedgerAccountUpdatesResponse;
import com.acme.ledger.v1.ValidateLedgerAccountRequest;
import com.acme.ledger.v1.ValidateLedgerAccountResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.WatchMetricSeriesesRequest;
import com.acme.metrics.v1.WatchMetricSeriesesResponse;
import com.acme.payroll.v1.ApproveDeductionRequest;
import com.acme.payroll.v1.ApproveDeductionResponse;
import com.acme.payroll.v1.CommentDeductionApprovalRequest;
import com.acme.payroll.v1.CommentDeductionApprovalResponse;
import com.acme.payroll.v1.CommentPayStubApprovalRequest;
import com.acme.payroll.v1.CommentPayStubApprovalResponse;
import com.acme.payroll.v1.CompareDeductionOptionsRequest;
import com.acme.payroll.v1.CompareDeductionOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionDiscountAction;
import com.acme.payroll.v1.DelegateDeductionApprovalRequest;
import com.acme.payroll.v1.DelegateDeductionApprovalResponse;
import com.acme.payroll.v1.EscalateDeductionRequest;
import com.acme.payroll.v1.EscalateDeductionResponse;
import com.acme.payroll.v1.EstimateTaxWithholdingRequest;
import com.acme.payroll.v1.EstimateTaxWithholdingResponse;
import com.acme.payroll.v1.FindPayStubsRequest;
import com.acme.payroll.v1.FindPayStubsResponse;
import com.acme.payroll.v1.GetDeductionApprovalStatusRequest;
import com.acme.payroll.v1.GetDeductionApprovalStatusResponse;
import com.acme.payroll.v1.GetDeductionLifecycleRequest;
import com.acme.payroll.v1.GetDeductionLifecycleResponse;
import com.acme.payroll.v1.ListPendingDeductionsRequest;
import com.acme.payroll.v1.ListPendingDeductionsResponse;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubApprovalServiceGrpc;
import com.acme.payroll.v1.PayStubQueryServiceGrpc;
import com.acme.payroll.v1.RecallDeductionRequest;
import com.acme.payroll.v1.RecallDeductionResponse;
import com.acme.payroll.v1.RejectDeductionRequest;
import com.acme.payroll.v1.RejectDeductionResponse;
import com.acme.payroll.v1.SubmitDeductionRequest;
import com.acme.payroll.v1.SubmitDeductionResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.risk.v1.CancelExposureWorkflowRequest;
import com.acme.risk.v1.CancelExposureWorkflowResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Routes deductions through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class DeductionApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(DeductionApprovalApplication.class);

    private final DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceBlockingStub assetsDepreciationWorkflowServiceBlocking;

    private final ChannelRegistry channels;

    private final ManagedDeviceCalculationServiceGrpc.ManagedDeviceCalculationServiceBlockingStub devicesManagedDeviceCalculationServiceBlocking;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final ExpensePolicyLifecycleServiceGrpc.ExpensePolicyLifecycleServiceBlockingStub expensesExpensePolicyLifecycleServiceBlocking;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final UserValidationServiceGrpc.UserValidationServiceFutureStub identityUserValidationServiceFuture;

    private final ClaimWorkflowServiceGrpc.ClaimWorkflowServiceBlockingStub insuranceClaimWorkflowServiceBlocking;

    private final PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking;

    private final PolicyWorkflowServiceGrpc.PolicyWorkflowServiceFutureStub insurancePolicyWorkflowServiceFuture;

    private final LabelingTaskServiceGrpc.LabelingTaskServiceBlockingStub labelingLabelingTaskServiceBlocking;

    private final FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking;

    private final LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking;

    private final LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking;

    private final DeductionApprovalRequestMapper mapper;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking;

    private final PayStubApprovalServiceGrpc.PayStubApprovalServiceFutureStub payrollPayStubApprovalServiceFuture;

    private final PayStubQueryServiceGrpc.PayStubQueryServiceBlockingStub payrollPayStubQueryServiceBlocking;

    private final TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking;

    private final PayrollRepository repository;

    private final ExposureWorkflowServiceGrpc.ExposureWorkflowServiceBlockingStub riskExposureWorkflowServiceBlocking;

    private final Executor rpcExecutor;

    public DeductionApprovalApplication(
            DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceBlockingStub assetsDepreciationWorkflowServiceBlocking,
            ChannelRegistry channels,
            ManagedDeviceCalculationServiceGrpc.ManagedDeviceCalculationServiceBlockingStub devicesManagedDeviceCalculationServiceBlocking,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            ExpensePolicyLifecycleServiceGrpc.ExpensePolicyLifecycleServiceBlockingStub expensesExpensePolicyLifecycleServiceBlocking,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            UserValidationServiceGrpc.UserValidationServiceFutureStub identityUserValidationServiceFuture,
            ClaimWorkflowServiceGrpc.ClaimWorkflowServiceBlockingStub insuranceClaimWorkflowServiceBlocking,
            PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking,
            PolicyWorkflowServiceGrpc.PolicyWorkflowServiceFutureStub insurancePolicyWorkflowServiceFuture,
            LabelingTaskServiceGrpc.LabelingTaskServiceBlockingStub labelingLabelingTaskServiceBlocking,
            FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking,
            LedgerAccountNotificationServiceGrpc.LedgerAccountNotificationServiceBlockingStub ledgerLedgerAccountNotificationServiceBlocking,
            LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking,
            DeductionApprovalRequestMapper mapper,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            DeductionCalculationServiceGrpc.DeductionCalculationServiceBlockingStub payrollDeductionCalculationServiceBlocking,
            PayStubApprovalServiceGrpc.PayStubApprovalServiceFutureStub payrollPayStubApprovalServiceFuture,
            PayStubQueryServiceGrpc.PayStubQueryServiceBlockingStub payrollPayStubQueryServiceBlocking,
            TaxWithholdingCalculationServiceGrpc.TaxWithholdingCalculationServiceBlockingStub payrollTaxWithholdingCalculationServiceBlocking,
            PayrollRepository repository,
            ExposureWorkflowServiceGrpc.ExposureWorkflowServiceBlockingStub riskExposureWorkflowServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsDepreciationWorkflowServiceBlocking = assetsDepreciationWorkflowServiceBlocking;
        this.channels = channels;
        this.devicesManagedDeviceCalculationServiceBlocking = devicesManagedDeviceCalculationServiceBlocking;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.expensesExpensePolicyLifecycleServiceBlocking = expensesExpensePolicyLifecycleServiceBlocking;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.identityUserValidationServiceFuture = identityUserValidationServiceFuture;
        this.insuranceClaimWorkflowServiceBlocking = insuranceClaimWorkflowServiceBlocking;
        this.insurancePolicyWorkflowServiceBlocking = insurancePolicyWorkflowServiceBlocking;
        this.insurancePolicyWorkflowServiceFuture = insurancePolicyWorkflowServiceFuture;
        this.labelingLabelingTaskServiceBlocking = labelingLabelingTaskServiceBlocking;
        this.ledgerFiscalPeriodHistoryServiceBlocking = ledgerFiscalPeriodHistoryServiceBlocking;
        this.ledgerLedgerAccountNotificationServiceBlocking = ledgerLedgerAccountNotificationServiceBlocking;
        this.ledgerLedgerAccountValidationServiceBlocking = ledgerLedgerAccountValidationServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.payrollDeductionCalculationServiceBlocking = payrollDeductionCalculationServiceBlocking;
        this.payrollPayStubApprovalServiceFuture = payrollPayStubApprovalServiceFuture;
        this.payrollPayStubQueryServiceBlocking = payrollPayStubQueryServiceBlocking;
        this.payrollTaxWithholdingCalculationServiceBlocking = payrollTaxWithholdingCalculationServiceBlocking;
        this.repository = repository;
        this.riskExposureWorkflowServiceBlocking = riskExposureWorkflowServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Submits a deduction.
     */
    public SubmitDeductionResponse submitDeduction(SubmitDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = CancelDepreciationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDeduction().getAudit().getCreatedAt())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getDeduction().getOwner().getUserId())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDeduction().getOwner().getEmail())
                                        .build())
                                .build())
                        .build())
                .setDetails(CancelDepreciationWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<CancelDepreciationWorkflowRequest, CancelDepreciationWorkflowResponse> port0 = RpcPort.adapt(assetsDepreciationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::cancelDepreciationWorkflow);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = CreateAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(reply0.getDepreciation().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.audit.v1.AuditEventService");
        var serviceName1 = "acme.audit.v1" + "." + "AuditEventService";
        var methodName1 = "Cre" + "ateAuditEvent";
        var descriptor1 = MethodDescriptor.<CreateAuditEventRequest, CreateAuditEventResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(CreateAuditEventRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CreateAuditEventResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = mapper.submitDeductionStage2(request, reply0);
        var reply2 = expensesExpensePolicyLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleExpensePolicyExpiry(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitDeduction", request.getDeduction().getDeductionId());
        return SubmitDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getDepreciation().getAudit().getUpdatedAt())
                                .build())
                        .setDeductionId(request.getDeductionId())
                        .putAllParameters(reply2.getExpensePolicy().getParametersMap())
                        .setStatus(request.getDeduction().getStatus())
                        .build())
                .setOutcome(SubmitDeductionResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves a deduction.
     */
    public ApproveDeductionResponse approveDeduction(ApproveDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = ListLabelingTasksRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLabelingTasksRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDeduction().getAudit().getCreatedAt())
                        .putAllLabels(request.getDeduction().getParametersMap())
                        .setOwnerId(request.getDetails().getApprover().getUserId())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> labelingLabelingTaskServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLabelingTasks(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveDeduction", request.getDeduction().getDeductionId());
        return ApproveDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setOwner(request.getDeduction().getOwner())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Rejects a deduction.
     */
    public RejectDeductionResponse rejectDeduction(RejectDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = CommentPayStubApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentPayStubApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDeduction().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getDeduction().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var pending0 = payrollPayStubApprovalServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).commentPayStubApproval(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = WatchMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .build())
                .build();
        var stream1 = metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchMetricSerieses(outbound1);
        var reply1 = WatchMetricSeriesesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectDeduction", request.getDeduction().getDeductionId());
        return RejectDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setStatus(request.getDeduction().getStatus())
                        .build())
                .setOutcome(RejectDeductionResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Escalates a deduction.
     */
    public EscalateDeductionResponse escalateDeduction(EscalateDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = CancelExposureWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getDeduction().getAudit().getEtag())
                                .setUpdatedBy(request.getDeduction().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .setHolder(SupplierRef.newBuilder()
                                .setEmail(request.getDeduction().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> riskExposureWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelExposureWorkflow(outbound0), rpcExecutor);

        var outbound1 = LintDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintDocumentsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDeduction().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintDocuments(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = BeanCopies.copy(EstimateTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getDocument().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build(), EstimateTaxWithholdingRequest.class);
        var reply2 = payrollTaxWithholdingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateTaxWithholding(outbound2);

        var outbound3 = ListPolicyWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPolicyWorkflowsRequest.Filter.newBuilder()
                        .setRegion(reply1.getDocument().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending3 = insurancePolicyWorkflowServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).listPolicyWorkflows(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateDeduction", request.getDeduction().getDeductionId());
        return EscalateDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getTaxWithholding().getAudit().getUpdatedAt())
                                .build())
                        .setDeductionId(request.getDeductionId())
                        .setOwner(request.getDeduction().getOwner())
                        .setPriority(reply2.getTaxWithholding().getPriority())
                        .build())
                .setOutcome(EscalateDeductionResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply3.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists deductions a page at a time.
     */
    public ListPendingDeductionsResponse listPendingDeductions(ListPendingDeductionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingDeductions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getContext().getTraceId(), "ListPendingDeductions");

        var outbound0 = CheckUserEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckUserEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setUser(User.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setUserId(request.getContext().getActor().getUserId())
                .build();
        var pending0 = identityUserValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).checkUserEligibility(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingDeductions", request.getPage().getPageToken());
        return ListPendingDeductionsResponse.newBuilder()
                .addDeductions(Deduction.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply0.getUser().getUserId())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Delegates a deduction.
     */
    public DelegateDeductionApprovalResponse delegateDeductionApproval(DelegateDeductionApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateDeductionApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = CompareDeductionOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDeduction().getAudit().getCreatedAt())
                                .build())
                        .setDiscount(DeductionDiscountAction.newBuilder()
                                .setPercent(request.getDeduction().getDiscount().getPercent())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> payrollDeductionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareDeductionOptions(value));

        var outbound1 = GetDeductionLifecycleRequest.newBuilder()
                .setContext(request.getContext())
                .setDeductionId(request.getDeduction().getDeductionId())
                .build();
        var channel1 = channels.lookup("acme.payroll.v1.DeductionLifecycleService");
        var serviceName1 = "acme.payroll.v1" + "." + "DeductionLifecycleService";
        var methodName1 = "Get" + "DeductionLifecycle";
        var descriptor1 = MethodDescriptor.<GetDeductionLifecycleRequest, GetDeductionLifecycleResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(GetDeductionLifecycleRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(GetDeductionLifecycleResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = JsonCodec.roundTrip(RevertFiscalPeriodRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertFiscalPeriodRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getDeduction().getAudit().getUpdatedAt())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply1.getDeduction().getOwner().getDisplayName())
                                .build())
                        .build())
                .build(), RevertFiscalPeriodRequest.class);
        var reply2 = ledgerFiscalPeriodHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertFiscalPeriod(outbound2);

        var outbound3 = SubscribeLedgerAccountUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribeLedgerAccountUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDeduction().getOwner().getEmail())
                                .build())
                        .build())
                .setReason(switch (reply0.getDeduction().getActionCase()) {
                    case BLOCK -> reply0.getDeduction().getBlock().getReason();
                    default -> "";
                })
                .build();
        var reply3 = ledgerLedgerAccountNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribeLedgerAccountUpdates(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getName()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateDeductionApproval", request.getDeduction().getName());
        return DelegateDeductionApprovalResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setDeductionId(request.getDeductionId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply3.getLedgerAccount().getContact().getEmail())
                                .setUserId(reply1.getDeduction().getOwner().getUserId())
                                .build())
                        .setPriority(request.getDeduction().getPriority())
                        .build())
                .setOutcome(DelegateDeductionApprovalResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply3.getOutcome().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalls a deduction.
     */
    public RecallDeductionResponse recallDeduction(RecallDeductionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallDeduction")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = FindPayStubsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindPayStubsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDeduction().getOwner().getUserId())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> payrollPayStubQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findPayStubs(outbound0));

        var outbound1 = ValidateLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDeduction().getAudit().getCreatedAt())
                                .setEtag(request.getDeduction().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateLedgerAccount(value));
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallDeduction", request.getDeduction().getDeductionId());
        return RecallDeductionResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getLedgerAccount().getAudit().getUpdatedAt())
                                .build())
                        .setDeductionId(request.getDeductionId())
                        .setEffective(request.getDeduction().getEffective())
                        .build())
                .setOutcome(RecallDeductionResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a deduction.
     */
    public GetDeductionApprovalStatusResponse getDeductionApprovalStatus(GetDeductionApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDeductionApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Deduction", request.getDeductionId(), "GetDeductionApprovalStatus");

        var outbound0 = (StartPolicyWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setExternalAccountId(request.getDeductionId())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOpenedAt(request.getContext().getDeadline())
                        .build())
                .build()).toBuilder().build();
        var reply0 = insurancePolicyWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startPolicyWorkflow(outbound0);
        return GetDeductionApprovalStatusResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPolicy().getAudit().getCreatedAt())
                                .setUpdatedBy(reply0.getPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setDeductionId(request.getDeductionId())
                        .build())
                .build();
    }

    /**
     * Comments a deduction.
     */
    public CommentDeductionApprovalResponse commentDeductionApproval(CommentDeductionApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentDeductionApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDeduction(request.getContext().getTenantId(), request.getDeduction());

        var outbound0 = ResumeClaimWorkflowRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDeduction().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<ResumeClaimWorkflowRequest, ResumeClaimWorkflowResponse> operation0 = insuranceClaimWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::resumeClaimWorkflow;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = EstimateManagedDeviceRequest.newBuilder()
                .setContext(request.getContext())
                .setManagedDevice(ManagedDevice.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDeduction().getOwner().getEmail())
                                .setUserId(reply0.getClaim().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.devices.v1.ManagedDeviceCalculationService/EstimateManagedDevice", outbound1);
        var reply1 = devicesManagedDeviceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateManagedDevice(command1.payload());

        var outbound2 = LintUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRegion(switch (reply1.getManagedDevice().getLocatorCase()) {
                                    case ADDRESS -> reply1.getManagedDevice().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintUsers(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDeduction().getDeductionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentDeductionApproval", request.getDeduction().getDeductionId());
        return CommentDeductionApprovalResponse.newBuilder()
                .setDeduction(Deduction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getUser().getAudit().getUpdatedBy())
                                .build())
                        .setDeductionId(request.getDeductionId())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(reply1.getManagedDevice().getOwner().getUserId())
                                .build())
                        .setStatus(request.getDeduction().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }
}
