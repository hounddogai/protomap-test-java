package com.acme.finance.domains.expenses;

import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.ShareDisposalReportRequest;
import com.acme.assets.v1.ShareDisposalReportResponse;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersRequest;
import com.acme.audit.v1.NotifyRetentionPolicyWatchersResponse;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyNoticesRequest;
import com.acme.audit.v1.StreamRetentionPolicyNoticesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetLimit;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.CommentBudgetApprovalRequest;
import com.acme.budgeting.v1.CommentBudgetApprovalResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.PushForecastRequest;
import com.acme.budgeting.v1.PushForecastResponse;
import com.acme.budgeting.v1.ReconcileForecastsRequest;
import com.acme.budgeting.v1.ReconcileForecastsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ApproveAttestationRequest;
import com.acme.compliance.v1.ApproveAttestationResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.CancelControlWorkflowRequest;
import com.acme.compliance.v1.CancelControlWorkflowResponse;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlWorkflowServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.EstimateHouseholdRequest;
import com.acme.customers.v1.EstimateHouseholdResponse;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationRequest;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.PreviewCustomerSegmentRequest;
import com.acme.customers.v1.PreviewCustomerSegmentResponse;
import com.acme.dispatch.v1.BrowseDispatchOrdersRequest;
import com.acme.dispatch.v1.BrowseDispatchOrdersResponse;
import com.acme.expenses.v1.ActivateReceiptRequest;
import com.acme.expenses.v1.ActivateReceiptResponse;
import com.acme.expenses.v1.CheckReceiptEligibilityRequest;
import com.acme.expenses.v1.CheckReceiptEligibilityResponse;
import com.acme.expenses.v1.CountMileageClaimTotalsRequest;
import com.acme.expenses.v1.CountMileageClaimTotalsResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.expenses.v1.ExpireReceiptRequest;
import com.acme.expenses.v1.ExpireReceiptResponse;
import com.acme.expenses.v1.ExplainExpensePolicyDecisionRequest;
import com.acme.expenses.v1.ExplainExpensePolicyDecisionResponse;
import com.acme.expenses.v1.ExplainReceiptDecisionRequest;
import com.acme.expenses.v1.ExplainReceiptDecisionResponse;
import com.acme.expenses.v1.GetReceiptLifecycleRequest;
import com.acme.expenses.v1.GetReceiptLifecycleResponse;
import com.acme.expenses.v1.ListExpensePolicyConflictsRequest;
import com.acme.expenses.v1.ListExpensePolicyConflictsResponse;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.MileageClaimReportingServiceGrpc;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptBankSettlement;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ReinstateReceiptRequest;
import com.acme.expenses.v1.ReinstateReceiptResponse;
import com.acme.expenses.v1.RenewReceiptRequest;
import com.acme.expenses.v1.RenewReceiptResponse;
import com.acme.expenses.v1.ResolveExpensePolicyConflictRequest;
import com.acme.expenses.v1.ResolveExpensePolicyConflictResponse;
import com.acme.expenses.v1.ScheduleReceiptExpiryRequest;
import com.acme.expenses.v1.ScheduleReceiptExpiryResponse;
import com.acme.expenses.v1.SuspendReceiptRequest;
import com.acme.expenses.v1.SuspendReceiptResponse;
import com.acme.expenses.v1.TerminateReceiptRequest;
import com.acme.expenses.v1.TerminateReceiptResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CommentPolicyApprovalRequest;
import com.acme.insurance.v1.CommentPolicyApprovalResponse;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.SubmitPolicyRequest;
import com.acme.insurance.v1.SubmitPolicyResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Moves a receipt between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class ReceiptLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(ReceiptLifecycleApplication.class);

    private final DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final BudgetApprovalServiceGrpc.BudgetApprovalServiceBlockingStub budgetingBudgetApprovalServiceBlocking;

    private final ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationApprovalServiceGrpc.AttestationApprovalServiceBlockingStub complianceAttestationApprovalServiceBlocking;

    private final ControlWorkflowServiceGrpc.ControlWorkflowServiceBlockingStub complianceControlWorkflowServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceBlockingStub expensesExpensePolicySyncServiceBlocking;

    private final ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceBlockingStub expensesExpensePolicyValidationServiceBlocking;

    private final MileageClaimReportingServiceGrpc.MileageClaimReportingServiceFutureStub expensesMileageClaimReportingServiceFuture;

    private final ReceiptValidationServiceGrpc.ReceiptValidationServiceBlockingStub expensesReceiptValidationServiceBlocking;

    private final PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking;

    private final ExpensesRepository repository;

    private final Executor rpcExecutor;

    public ReceiptLifecycleApplication(
            DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            BudgetApprovalServiceGrpc.BudgetApprovalServiceBlockingStub budgetingBudgetApprovalServiceBlocking,
            ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking,
            ChannelRegistry channels,
            AttestationApprovalServiceGrpc.AttestationApprovalServiceBlockingStub complianceAttestationApprovalServiceBlocking,
            ControlWorkflowServiceGrpc.ControlWorkflowServiceBlockingStub complianceControlWorkflowServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            ApplicationEventPublisher events,
            ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceBlockingStub expensesExpensePolicySyncServiceBlocking,
            ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceBlockingStub expensesExpensePolicyValidationServiceBlocking,
            MileageClaimReportingServiceGrpc.MileageClaimReportingServiceFutureStub expensesMileageClaimReportingServiceFuture,
            ReceiptValidationServiceGrpc.ReceiptValidationServiceBlockingStub expensesReceiptValidationServiceBlocking,
            PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking,
            ExpensesRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsDisposalReportingServiceBlocking = assetsDisposalReportingServiceBlocking;
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.budgetingBudgetApprovalServiceBlocking = budgetingBudgetApprovalServiceBlocking;
        this.budgetingForecastSyncServiceBlocking = budgetingForecastSyncServiceBlocking;
        this.channels = channels;
        this.complianceAttestationApprovalServiceBlocking = complianceAttestationApprovalServiceBlocking;
        this.complianceControlWorkflowServiceBlocking = complianceControlWorkflowServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.events = events;
        this.expensesExpensePolicySyncServiceBlocking = expensesExpensePolicySyncServiceBlocking;
        this.expensesExpensePolicyValidationServiceBlocking = expensesExpensePolicyValidationServiceBlocking;
        this.expensesMileageClaimReportingServiceFuture = expensesMileageClaimReportingServiceFuture;
        this.expensesReceiptValidationServiceBlocking = expensesReceiptValidationServiceBlocking;
        this.insurancePolicyApprovalServiceBlocking = insurancePolicyApprovalServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record ExpireReceiptStage0Requested(BrowseDispatchOrdersRequest request, CompletableFuture<BrowseDispatchOrdersResponse> completion, io.grpc.Context context) {}

    /**
     * Activates a receipt.
     */
    public ActivateReceiptResponse activateReceipt(ActivateReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getReceiptId(), "ActivateReceipt");

        var outbound0 = RpcSupport.unpack(Any.pack(PushForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushForecastRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setCounterparty(BudgetRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build()), PushForecastRequest.class);
        var reply0 = budgetingForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pushForecast(outbound0);

        var outbound1 = ResolveExpensePolicyConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolveExpensePolicyConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getForecast().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply1 = expensesExpensePolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveExpensePolicyConflict(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateReceipt", request.getDetails().getReasonCode());
        return ActivateReceiptResponse.newBuilder()
                .setOutcome(ActivateReceiptResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getExpensePolicy().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getForecast().getBillingAddress().getCountryCode())
                                .addAllLines(reply0.getForecast().getBillingAddress().getLinesList())
                                .build())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .build();
    }

    /**
     * Suspends a receipt.
     */
    public SuspendReceiptResponse suspendReceipt(SuspendReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getReceiptId(), "SuspendReceipt");

        var builder0 = ReconcileForecastsRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(ReconcileForecastsRequest.Details.newBuilder()
                .setPayload(JsonCodec.encode(request.getContext()))
                .build());
        builder0.setForecast(Forecast.newBuilder()
                .setCounterparty(BudgetRef.newBuilder()
                        .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = budgetingForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileForecasts(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendReceipt", request.getDetails().getReasonCode());
        return SuspendReceiptResponse.newBuilder()
                .setOutcome(SuspendReceiptResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates a receipt.
     */
    public ReinstateReceiptResponse reinstateReceipt(ReinstateReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getReceiptId(), "ReinstateReceipt");

        var outbound0 = SubmitPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitPolicyRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.PolicyApprovalService");
        var serviceName0 = "acme.insurance.v1" + "." + "PolicyApprovalService";
        var methodName0 = "Sub" + "mitPolicy";
        var descriptor0 = MethodDescriptor.<SubmitPolicyRequest, SubmitPolicyResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(SubmitPolicyRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SubmitPolicyResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = BeanCopies.copy(ShareDisposalReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareDisposalReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setPhone(reply0.getPolicy().getContact().getPhone())
                                .setPreferredChannel(reply0.getPolicy().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build(), ShareDisposalReportRequest.class);
        var reply1 = assetsDisposalReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareDisposalReport(outbound1);

        var outbound2 = PreviewCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getDisposal().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setDetails(PreviewCustomerSegmentRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewCustomerSegment(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = NotifyRetentionPolicyWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyRetentionPolicyWatchersRequest.Details.newBuilder()
                        .setChannel(switch (reply1.getDisposal().getOriginCase()) {
                            case CHANNEL -> reply1.getDisposal().getChannel().getChannel();
                            default -> "";
                        })
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply2.getCustomerSegment().getOwner().getEmail())
                                .setName(reply1.getDisposal().getRequester().getName())
                                .setPhone(reply0.getPolicy().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply3 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).notifyRetentionPolicyWatchers(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateReceipt", request.getDetails().getReasonCode());
        return ReinstateReceiptResponse.newBuilder()
                .setOutcome(ReinstateReceiptResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getPolicy().getAudit().getEtag())
                                .setUpdatedBy(reply3.getRetentionPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Terminates a receipt.
     */
    public TerminateReceiptResponse terminateReceipt(TerminateReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getReceiptId(), "TerminateReceipt");

        var outbound0 = ApproveAttestationRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setCounterparty(ComplianceCaseRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ApproveAttestationRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(complianceAttestationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveAttestation(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CountMileageClaimTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountMileageClaimTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getAttestation().getIssuedAt())
                        .putAllLabels(reply0.getAttestation().getMetadataMap())
                        .setRegion(reply0.getAttestation().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = expensesMileageClaimReportingServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).countMileageClaimTotals(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateReceipt", request.getDetails().getReasonCode());
        return TerminateReceiptResponse.newBuilder()
                .setOutcome(TerminateReceiptResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .putAllMetadata(reply0.getAttestation().getMetadataMap())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Renews a receipt.
     */
    public RenewReceiptResponse renewReceipt(RenewReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getReceiptId(), "RenewReceipt");

        var outbound0 = CancelControlWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setControl(Control.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .setDetails(CancelControlWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            CancelControlWorkflowResponse fetch(CancelControlWorkflowRequest input) {
                return complianceControlWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelControlWorkflow(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = StreamRetentionPolicyNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                        .setPhone(request.getDetails().getNotifyContact().getPhone())
                        .build())
                .build();
        var stream1 = auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyNotices(outbound1);
        var reply1 = StreamRetentionPolicyNoticesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = CommentBudgetApprovalRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .addLimits(BudgetLimit.newBuilder()
                                .setAmount(reply0.getControl().getDiscount().getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CommentBudgetApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(budgetingBudgetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentBudgetApproval(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = CheckReceiptEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getBudget().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getControl().getAudit().getUpdatedAt())
                                .build())
                        .addLines(ReceiptLine.newBuilder()
                                .setAmount(reply0.getControl().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        var reply3 = Stream.of(outbound3).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(expensesReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkReceiptEligibility(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewReceipt", request.getDetails().getReasonCode());
        return RenewReceiptResponse.newBuilder()
                .setOutcome(RenewReceiptResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply3.getReceipt().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(MileageClaimRef.newBuilder()
                                .setDisplayName(reply2.getBudget().getHolder().getDisplayName())
                                .build())
                        .setReceiptId(request.getReceiptId())
                        .setStatus(reply3.getReceipt().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Expires a receipt.
     */
    public ExpireReceiptResponse expireReceipt(ExpireReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getReceiptId(), "ExpireReceipt");

        var outbound0 = BrowseDispatchOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseDispatchOrdersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var completion0 = new CompletableFuture<BrowseDispatchOrdersResponse>();
        events.publishEvent(new ExpireReceiptStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = BeanCopies.copy(CommentPolicyApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentPolicyApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build(), CommentPolicyApprovalRequest.class);
        var reply1 = insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentPolicyApproval(outbound1);

        var outbound2 = ExplainCustomerSegmentCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getPolicy().getAudit().getCreatedAt())
                                .setEtag(reply1.getPolicy().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getPolicy().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.customers.v1.CustomerSegmentCalculationService");
        var serviceName2 = "acme.customers.v1" + "." + "CustomerSegmentCalculationService";
        var methodName2 = "Exp" + "lainCustomerSegmentCalculation";
        var descriptor2 = MethodDescriptor.<ExplainCustomerSegmentCalculationRequest, ExplainCustomerSegmentCalculationResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ExplainCustomerSegmentCalculationRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ExplainCustomerSegmentCalculationResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireReceipt", request.getDetails().getReasonCode());
        return ExpireReceiptResponse.newBuilder()
                .setOutcome(ExpireReceiptResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setCounterparty(MileageClaimRef.newBuilder()
                                .setDisplayName(reply2.getCustomerSegment().getOwner().getDisplayName())
                                .build())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a receipt.
     */
    public GetReceiptLifecycleResponse getReceiptLifecycle(GetReceiptLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetReceiptLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getReceiptId(), "GetReceiptLifecycle");

        var outbound0 = ListExpensePolicyConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListExpensePolicyConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .setOwnerId(request.getReceiptId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> expensesExpensePolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listExpensePolicyConflicts(outbound0), rpcExecutor);

        var outbound1 = EstimateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .setManager(UserRef.newBuilder()
                                        .setUserId(request.getContext().getActor().getUserId())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateHousehold(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        return GetReceiptLifecycleResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getHousehold().getLocation().getCountryCode())
                                .build())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Schedules a receipt.
     */
    public ScheduleReceiptExpiryResponse scheduleReceiptExpiry(ScheduleReceiptExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleReceiptExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = ExplainReceiptDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainReceiptDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getReceipt().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getReceipt().getAudit().getUpdatedAt())
                                .build())
                        .setBank(ReceiptBankSettlement.newBuilder()
                                .setBic(request.getReceipt().getBank().getBic())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> expensesReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainReceiptDecision(outbound0), rpcExecutor);

        var outbound1 = ExplainExpensePolicyDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainExpensePolicyDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> expensesExpensePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainExpensePolicyDecision(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleReceiptExpiry", request.getReceipt().getReceiptId());
        return ScheduleReceiptExpiryResponse.newBuilder()
                .setOutcome(ScheduleReceiptExpiryResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setCounterparty(MileageClaimRef.newBuilder()
                                .setNumber(reply0.getReceipt().getCounterparty().getNumber())
                                .build())
                        .addLines(ReceiptLine.newBuilder()
                                .setDescription(reply1.getExpensePolicy().getDescription())
                                .build())
                        .setNotes(request.getReceipt().getNotes())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
