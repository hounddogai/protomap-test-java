package com.acme.finance.domains.expenses;

import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.StreamDepreciationMetricsRequest;
import com.acme.assets.v1.StreamDepreciationMetricsResponse;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.ScheduleForecastReportRequest;
import com.acme.budgeting.v1.ScheduleForecastReportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.ScheduleAttestationReportRequest;
import com.acme.compliance.v1.ScheduleAttestationReportResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentDiscountAction;
import com.acme.customers.v1.EstimateCustomerSegmentRequest;
import com.acme.customers.v1.EstimateCustomerSegmentResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.ScoreHouseholdRequest;
import com.acme.customers.v1.ScoreHouseholdResponse;
import com.acme.expenses.v1.ActivateExpensePolicyRequest;
import com.acme.expenses.v1.ActivateExpensePolicyResponse;
import com.acme.expenses.v1.BrowseReceiptsRequest;
import com.acme.expenses.v1.BrowseReceiptsResponse;
import com.acme.expenses.v1.DeleteExpensePolicyReportRequest;
import com.acme.expenses.v1.DeleteExpensePolicyReportResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyReportingServiceGrpc;
import com.acme.expenses.v1.ExpireExpensePolicyRequest;
import com.acme.expenses.v1.ExpireExpensePolicyResponse;
import com.acme.expenses.v1.ExplainMileageClaimCalculationRequest;
import com.acme.expenses.v1.ExplainMileageClaimCalculationResponse;
import com.acme.expenses.v1.GetExpensePolicyLifecycleRequest;
import com.acme.expenses.v1.GetExpensePolicyLifecycleResponse;
import com.acme.expenses.v1.MileageClaimCalculationServiceGrpc;
import com.acme.expenses.v1.NormalizeReceiptRequest;
import com.acme.expenses.v1.NormalizeReceiptResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptQueryServiceGrpc;
import com.acme.expenses.v1.ReinstateExpensePolicyRequest;
import com.acme.expenses.v1.ReinstateExpensePolicyResponse;
import com.acme.expenses.v1.RenewExpensePolicyRequest;
import com.acme.expenses.v1.RenewExpensePolicyResponse;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryRequest;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryResponse;
import com.acme.expenses.v1.SuspendExpensePolicyRequest;
import com.acme.expenses.v1.SuspendExpensePolicyResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.CommentLoanApprovalRequest;
import com.acme.lending.v1.CommentLoanApprovalResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.SubmitLoanRequest;
import com.acme.lending.v1.SubmitLoanResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.PreviewNotificationRequest;
import com.acme.notifications.v1.PreviewNotificationResponse;
import com.acme.risk.v1.RejectRiskScenarioRequest;
import com.acme.risk.v1.RejectRiskScenarioResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.SubmitRiskScenarioRequest;
import com.acme.risk.v1.SubmitRiskScenarioResponse;
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
 * Moves an expense policy between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class ExpensePolicyLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(ExpensePolicyLifecycleApplication.class);

    private final DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking;

    private final ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final ExpensePolicyReportingServiceGrpc.ExpensePolicyReportingServiceBlockingStub expensesExpensePolicyReportingServiceBlocking;

    private final MileageClaimCalculationServiceGrpc.MileageClaimCalculationServiceBlockingStub expensesMileageClaimCalculationServiceBlocking;

    private final ReceiptQueryServiceGrpc.ReceiptQueryServiceBlockingStub expensesReceiptQueryServiceBlocking;

    private final LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final ExpensesRepository repository;

    private final RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking;

    private final Executor rpcExecutor;

    public ExpensePolicyLifecycleApplication(
            DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking,
            ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking,
            ChannelRegistry channels,
            AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            ExpensePolicyReportingServiceGrpc.ExpensePolicyReportingServiceBlockingStub expensesExpensePolicyReportingServiceBlocking,
            MileageClaimCalculationServiceGrpc.MileageClaimCalculationServiceBlockingStub expensesMileageClaimCalculationServiceBlocking,
            ReceiptQueryServiceGrpc.ReceiptQueryServiceBlockingStub expensesReceiptQueryServiceBlocking,
            LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            ExpensesRepository repository,
            RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsDepreciationReportingServiceBlocking = assetsDepreciationReportingServiceBlocking;
        this.budgetingForecastReportingServiceBlocking = budgetingForecastReportingServiceBlocking;
        this.channels = channels;
        this.complianceAttestationReportingServiceBlocking = complianceAttestationReportingServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.expensesExpensePolicyReportingServiceBlocking = expensesExpensePolicyReportingServiceBlocking;
        this.expensesMileageClaimCalculationServiceBlocking = expensesMileageClaimCalculationServiceBlocking;
        this.expensesReceiptQueryServiceBlocking = expensesReceiptQueryServiceBlocking;
        this.lendingLoanApprovalServiceBlocking = lendingLoanApprovalServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.repository = repository;
        this.riskRiskScenarioApprovalServiceBlocking = riskRiskScenarioApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Activates an expense policy.
     */
    public ActivateExpensePolicyResponse activateExpensePolicy(ActivateExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getExpensePolicyId(), "ActivateExpensePolicy");

        var builder0 = ScheduleAttestationReportRequest.newBuilder();
        builder0.putAllAnnotations(request.getAnnotationsMap());
        builder0.setContext(request.getContext());
        builder0.setDetails(ScheduleAttestationReportRequest.Details.newBuilder()
                .putAllFilters(request.getContext().getBaggageMap())
                .build());
        builder0.setReason(request.getReason());
        var outbound0 = builder0.build();
        var reply0 = complianceAttestationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleAttestationReport(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateExpensePolicy", request.getDetails().getReasonCode());
        return ActivateExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .build())
                .setOutcome(ActivateExpensePolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Suspends an expense policy.
     */
    public SuspendExpensePolicyResponse suspendExpensePolicy(SuspendExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getExpensePolicyId(), "SuspendExpensePolicy");

        var builder0 = BrowseReceiptsRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setFilter(BrowseReceiptsRequest.Filter.newBuilder()
                .setCreatedAfter(request.getDetails().getEffectiveAt())
                .putAllLabels(request.getContext().getBaggageMap())
                .setOwnerId(request.getDetails().getNewOwner().getUserId())
                .build());
        var outbound0 = builder0.build();
        var reply0 = expensesReceiptQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseReceipts(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendExpensePolicy", request.getDetails().getReasonCode());
        return SuspendExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .build())
                .setOutcome(SuspendExpensePolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates an expense policy.
     */
    public ReinstateExpensePolicyResponse reinstateExpensePolicy(ReinstateExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getExpensePolicyId(), "ReinstateExpensePolicy");

        var outbound0 = SubmitLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitLoanRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> lendingLoanApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitLoan(outbound0), rpcExecutor);

        var outbound1 = ScoreHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreHousehold(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = NormalizeReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeReceiptRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getHousehold().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getHousehold().getLocation().getRecipient())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.expenses.v1.ReceiptValidationService");
        var serviceName2 = "acme.expenses.v1" + "." + "ReceiptValidationService";
        var methodName2 = "Nor" + "malizeReceipt";
        var descriptor2 = MethodDescriptor.<NormalizeReceiptRequest, NormalizeReceiptResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(NormalizeReceiptRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(NormalizeReceiptResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateExpensePolicy", request.getDetails().getReasonCode());
        return ReinstateExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getHousehold().getAudit().getEtag())
                                .setUpdatedBy(reply1.getHousehold().getAudit().getUpdatedBy())
                                .build())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .build())
                .setOutcome(ReinstateExpensePolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Renews an expense policy.
     */
    public RenewExpensePolicyResponse renewExpensePolicy(RenewExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getExpensePolicyId(), "RenewExpensePolicy");

        var outbound0 = PreviewNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewNotificationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewNotification(outbound0);

        var outbound1 = CommentLoanApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentLoanApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(reply0.getNotification().getRequester().getEmail())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getNotification().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(reply0.getNotification().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> lendingLoanApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentLoanApproval(value));
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewExpensePolicy", request.getDetails().getReasonCode());
        return RenewExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getLoan().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply1.getLoan().getAudit().getUpdatedBy())
                                .build())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getNotification().getCustomer().getDisplayName())
                                .setEmail(reply0.getNotification().getRequester().getEmail())
                                .build())
                        .build())
                .setOutcome(RenewExpensePolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Expires an expense policy.
     */
    public ExpireExpensePolicyResponse expireExpensePolicy(ExpireExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getExpensePolicyId(), "ExpireExpensePolicy");

        var outbound0 = StreamDepreciationMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNewOwner().getEmail())
                        .build())
                .build();
        var stream0 = assetsDepreciationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamDepreciationMetrics(outbound0);
        var reply0 = StreamDepreciationMetricsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = SubmitRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitRiskScenarioRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = riskRiskScenarioApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitRiskScenario(outbound1);

        var outbound2 = DeleteExpensePolicyReportRequest.newBuilder()
                .setContext(request.getContext())
                .setEtag(reply1.getRiskScenario().getAudit().getEtag())
                .setExpensePolicyId(request.getExpensePolicyId())
                .setReason(request.getReason())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> expensesExpensePolicyReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).deleteExpensePolicyReport(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireExpensePolicy", request.getDetails().getReasonCode());
        return ExpireExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getRiskScenario().getAssignee().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .setOutcome(ExpireExpensePolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply2.getEffectiveAt())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns an expense policy.
     */
    public GetExpensePolicyLifecycleResponse getExpensePolicyLifecycle(GetExpensePolicyLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetExpensePolicyLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getExpensePolicyId(), "GetExpensePolicyLifecycle");

        var builder0 = ExplainMileageClaimCalculationRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(ExplainMileageClaimCalculationRequest.Details.newBuilder()
                .setLocale(request.getContext().getLocale())
                .build());
        var outbound0 = builder0.build();
        var reply0 = expensesMileageClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainMileageClaimCalculation(outbound0);

        var outbound1 = ScheduleForecastReportRequest.newBuilder()
                .putAllAnnotations(reply0.getResult().getDetailsMap())
                .setContext(request.getContext())
                .setDetails(ScheduleForecastReportRequest.Details.newBuilder()
                        .putAllFilters(reply0.getMileageClaim().getMetadataMap())
                        .build())
                .build();
        var reply1 = budgetingForecastReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleForecastReport(outbound1);
        return GetExpensePolicyLifecycleResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Schedules an expense policy.
     */
    public ScheduleExpensePolicyExpiryResponse scheduleExpensePolicyExpiry(ScheduleExpensePolicyExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleExpensePolicyExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = EstimateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setDiscount(CustomerSegmentDiscountAction.newBuilder()
                                .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                .setPercent(request.getExpensePolicy().getDiscount().getPercent())
                                .build())
                        .build())
                .build();
        RpcPort<EstimateCustomerSegmentRequest, EstimateCustomerSegmentResponse> port0 = RpcPort.adapt(customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::estimateCustomerSegment);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = RejectRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getCustomerSegment().getOwner().getUserId())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<RejectRiskScenarioRequest, RejectRiskScenarioResponse>() {
            @Override
            public RejectRiskScenarioResponse apply(RejectRiskScenarioRequest input) {
                return riskRiskScenarioApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectRiskScenario(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleExpensePolicyExpiry", request.getExpensePolicy().getExpensePolicyId());
        return ScheduleExpensePolicyExpiryResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setOwner(request.getExpensePolicy().getOwner())
                        .setPriority(reply1.getRiskScenario().getPriority())
                        .build())
                .setOutcome(ScheduleExpensePolicyExpiryResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }
}
