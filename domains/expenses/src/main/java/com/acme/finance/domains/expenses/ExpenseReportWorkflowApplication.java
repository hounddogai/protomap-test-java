package com.acme.finance.domains.expenses;

import com.acme.accounting.v1.BatchCreateChartOfAccountsesRequest;
import com.acme.accounting.v1.BatchCreateChartOfAccountsesResponse;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.NormalizeRetentionPolicyRequest;
import com.acme.audit.v1.NormalizeRetentionPolicyResponse;
import com.acme.audit.v1.RecordAuditEventVersionRequest;
import com.acme.audit.v1.RecordAuditEventVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.benefits.v1.BenefitPlanQueryServiceGrpc;
import com.acme.benefits.v1.BrowseBenefitPlansRequest;
import com.acme.benefits.v1.BrowseBenefitPlansResponse;
import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.ListBudgetLineReportsRequest;
import com.acme.budgeting.v1.ListBudgetLineReportsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.ListAttestationReportsRequest;
import com.acme.compliance.v1.ListAttestationReportsResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.FindCustomersRequest;
import com.acme.customers.v1.FindCustomersResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.ValidatePreferenceRequest;
import com.acme.customers.v1.ValidatePreferenceResponse;
import com.acme.expenses.v1.AdvanceExpenseReportWorkflowRequest;
import com.acme.expenses.v1.AdvanceExpenseReportWorkflowResponse;
import com.acme.expenses.v1.ApproveReceiptRequest;
import com.acme.expenses.v1.ApproveReceiptResponse;
import com.acme.expenses.v1.CancelExpenseReportWorkflowRequest;
import com.acme.expenses.v1.CancelExpenseReportWorkflowResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.InspectExpenseReportWorkflowRequest;
import com.acme.expenses.v1.InspectExpenseReportWorkflowResponse;
import com.acme.expenses.v1.MileageClaimReportingServiceGrpc;
import com.acme.expenses.v1.PauseExpenseReportWorkflowRequest;
import com.acme.expenses.v1.PauseExpenseReportWorkflowResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptApprovalServiceGrpc;
import com.acme.expenses.v1.ReceiptAttachment;
import com.acme.expenses.v1.ReceiptCardSettlement;
import com.acme.expenses.v1.ReconcileExpensePoliciesRequest;
import com.acme.expenses.v1.ReconcileExpensePoliciesResponse;
import com.acme.expenses.v1.RetryExpenseReportWorkflowRequest;
import com.acme.expenses.v1.RetryExpenseReportWorkflowResponse;
import com.acme.expenses.v1.ShareMileageClaimReportRequest;
import com.acme.expenses.v1.ShareMileageClaimReportResponse;
import com.acme.expenses.v1.SignalExpenseReportWorkflowRequest;
import com.acme.expenses.v1.SignalExpenseReportWorkflowResponse;
import com.acme.expenses.v1.StartExpenseReportWorkflowRequest;
import com.acme.expenses.v1.StartExpenseReportWorkflowResponse;
import com.acme.expenses.v1.StreamExpensePolicyChangesRequest;
import com.acme.expenses.v1.StreamExpensePolicyChangesResponse;
import com.acme.expenses.v1.SubmitReceiptRequest;
import com.acme.expenses.v1.SubmitReceiptResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.GenerateUnderwritingReportRequest;
import com.acme.insurance.v1.GenerateUnderwritingReportResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.ShareUnderwritingReportRequest;
import com.acme.insurance.v1.ShareUnderwritingReportResponse;
import com.acme.insurance.v1.SignalPolicyWorkflowRequest;
import com.acme.insurance.v1.SignalPolicyWorkflowResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationComment;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.PreviewNotificationRequest;
import com.acme.notifications.v1.PreviewNotificationResponse;
import com.acme.notifications.v1.SearchNotificationsRequest;
import com.acme.notifications.v1.SearchNotificationsResponse;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.ScheduleRfqReportRequest;
import com.acme.procurement.v1.ScheduleRfqReportResponse;
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
 * Runs the long-lived workflow that takes an expense report from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ExpenseReportWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(ExpenseReportWorkflowApplication.class);

    private final ChartOfAccountsServiceGrpc.ChartOfAccountsServiceStub accountingChartOfAccountsServiceAsync;

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceFutureStub auditRetentionPolicyValidationServiceFuture;

    private final BenefitPlanQueryServiceGrpc.BenefitPlanQueryServiceBlockingStub benefitsBenefitPlanQueryServiceBlocking;

    private final BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking;

    private final ChannelRegistry channels;

    private final AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceStub expensesExpensePolicySyncServiceAsync;

    private final ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceBlockingStub expensesExpensePolicySyncServiceBlocking;

    private final ReceiptApprovalServiceGrpc.ReceiptApprovalServiceBlockingStub expensesReceiptApprovalServiceBlocking;

    private final PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking;

    private final UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking;

    private final UnderwritingReportingServiceGrpc.UnderwritingReportingServiceFutureStub insuranceUnderwritingReportingServiceFuture;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final RfqReportingServiceGrpc.RfqReportingServiceBlockingStub procurementRfqReportingServiceBlocking;

    private final ExpensesRepository repository;

    private final Executor rpcExecutor;

    public ExpenseReportWorkflowApplication(
            ChartOfAccountsServiceGrpc.ChartOfAccountsServiceStub accountingChartOfAccountsServiceAsync,
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceFutureStub auditRetentionPolicyValidationServiceFuture,
            BenefitPlanQueryServiceGrpc.BenefitPlanQueryServiceBlockingStub benefitsBenefitPlanQueryServiceBlocking,
            BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking,
            ChannelRegistry channels,
            AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceStub expensesExpensePolicySyncServiceAsync,
            ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceBlockingStub expensesExpensePolicySyncServiceBlocking,
            ReceiptApprovalServiceGrpc.ReceiptApprovalServiceBlockingStub expensesReceiptApprovalServiceBlocking,
            PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking,
            UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking,
            UnderwritingReportingServiceGrpc.UnderwritingReportingServiceFutureStub insuranceUnderwritingReportingServiceFuture,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            RfqReportingServiceGrpc.RfqReportingServiceBlockingStub procurementRfqReportingServiceBlocking,
            ExpensesRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.accountingChartOfAccountsServiceAsync = accountingChartOfAccountsServiceAsync;
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.auditRetentionPolicyValidationServiceFuture = auditRetentionPolicyValidationServiceFuture;
        this.benefitsBenefitPlanQueryServiceBlocking = benefitsBenefitPlanQueryServiceBlocking;
        this.budgetingBudgetLineReportingServiceBlocking = budgetingBudgetLineReportingServiceBlocking;
        this.channels = channels;
        this.complianceAttestationReportingServiceBlocking = complianceAttestationReportingServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.expensesExpensePolicySyncServiceAsync = expensesExpensePolicySyncServiceAsync;
        this.expensesExpensePolicySyncServiceBlocking = expensesExpensePolicySyncServiceBlocking;
        this.expensesReceiptApprovalServiceBlocking = expensesReceiptApprovalServiceBlocking;
        this.insurancePolicyWorkflowServiceBlocking = insurancePolicyWorkflowServiceBlocking;
        this.insuranceUnderwritingReportingServiceBlocking = insuranceUnderwritingReportingServiceBlocking;
        this.insuranceUnderwritingReportingServiceFuture = insuranceUnderwritingReportingServiceFuture;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.procurementRfqReportingServiceBlocking = procurementRfqReportingServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Starts an expense report.
     */
    public StartExpenseReportWorkflowResponse startExpenseReportWorkflow(StartExpenseReportWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartExpenseReportWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = (ListBudgetLineReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListBudgetLineReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getExpenseReport().getAudit().getUpdatedAt())
                        .setRegion(request.getExpenseReport().getBillingAddress().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply0 = budgetingBudgetLineReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listBudgetLineReports(outbound0);

        var outbound1 = ShareMileageClaimReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareMileageClaimReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getExpenseReport().getCounterparty().getEmail())
                                .setName(request.getDetails().getNotify().getName())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.expenses.v1.MileageClaimReportingService");
        var selected1 = MileageClaimReportingServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.shareMileageClaimReport(outbound1);

        var outbound2 = NormalizeRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeRetentionPolicyRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getMileageClaim().getAudit().getCreatedBy())
                                .setUpdatedAt(reply1.getMileageClaim().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getMileageClaim().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending2 = auditRetentionPolicyValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeRetentionPolicy(outbound2);
        var reply2 = RpcSupport.await(pending2);

        var outbound3 = PreviewNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getMileageClaim().getAudit().getUpdatedAt())
                                .build())
                        .addComments(NotificationComment.newBuilder()
                                .setCreatedAt(reply1.getMileageClaim().getAudit().getCreatedAt())
                                .build())
                        .setCustomer(NotificationTemplateRef.newBuilder()
                                .setDisplayName(reply2.getRetentionPolicy().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        Function<PreviewNotificationRequest, PreviewNotificationResponse> operation3 = notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewNotification;
        var reply3 = operation3.apply(outbound3);

        var outbound4 = BatchCreateChartOfAccountsesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateChartOfAccountsesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateChartOfAccountsesResponse.getDefaultInstance());
        var input4 = accountingChartOfAccountsServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateChartOfAccountses(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamExpensePolicyChangesRequest.newBuilder()
                .setChange(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getExpenseReport().getAudit().getCreatedAt())
                                .setCreatedBy(request.getExpenseReport().getAudit().getCreatedBy())
                                .setEtag(request.getExpenseReport().getAudit().getEtag())
                                .setUpdatedAt(request.getExpenseReport().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamExpensePolicyChangesResponse.getDefaultInstance());
        var input5 = expensesExpensePolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamExpensePolicyChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartExpenseReportWorkflow", request.getExpenseReport().getExpenseReportId());
        return StartExpenseReportWorkflowResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply3.getNotification().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(reply1.getMileageClaim().getBillingAddress())
                        .setDueAt(request.getExpenseReport().getDueAt())
                        .setExpenseReportId(request.getExpenseReportId())
                        .build())
                .setOutcome(StartExpenseReportWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Advances an expense report.
     */
    public AdvanceExpenseReportWorkflowResponse advanceExpenseReportWorkflow(AdvanceExpenseReportWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceExpenseReportWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = ApproveReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getExpenseReport().getAudit().getCreatedBy())
                                .build())
                        .setCard(ReceiptCardSettlement.newBuilder()
                                .setToken(switch (request.getExpenseReport().getSettlementCase()) {
                                    case CARD -> request.getExpenseReport().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = expensesReceiptApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveReceipt(outbound0);

        List<FindCustomersResponse> results1 = request.getExpenseReport().getLinesList().stream()
                .map(element -> FindCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindCustomersRequest.Filter.newBuilder()
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(reply0.getReceipt().getBillingAddress().getRegion())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findCustomers(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? FindCustomersResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = (ValidatePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidatePreferenceRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getReceipt().getAudit().getCreatedAt())
                                .setUpdatedBy(reply0.getReceipt().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePreference(outbound2);

        var outbound3 = (RecordAuditEventVersionRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(reply0.getReceipt().getCounterparty().getDisplayName())
                                .setNumber(reply0.getReceipt().getCounterparty().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecordAuditEventVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build()).toBuilder().build();
        var reply3 = auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordAuditEventVersion(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceExpenseReportWorkflow", request.getExpenseReport().getExpenseReportId());
        return AdvanceExpenseReportWorkflowResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply2.getPreference().getOwner().getEmail())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .putAllMetadata(request.getExpenseReport().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply3.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Cancels an expense report.
     */
    public CancelExpenseReportWorkflowResponse cancelExpenseReportWorkflow(CancelExpenseReportWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelExpenseReportWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = BrowseBenefitPlansRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseBenefitPlansRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getExpenseReport().getIssuedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getExpenseReport().getExpenseReportId())
                        .setRegion(request.getExpenseReport().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> benefitsBenefitPlanQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseBenefitPlans(outbound0), rpcExecutor);

        var outbound1 = ReconcileExpensePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileExpensePoliciesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getExpenseReport().getAudit().getCreatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getExpenseReport().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> expensesExpensePolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileExpensePolicies(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelExpenseReportWorkflow", request.getExpenseReport().getNumber());
        return CancelExpenseReportWorkflowResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setCounterparty(request.getExpenseReport().getCounterparty())
                        .setExpenseReportId(request.getExpenseReportId())
                        .build())
                .setOutcome(CancelExpenseReportWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Retrys an expense report.
     */
    public RetryExpenseReportWorkflowResponse retryExpenseReportWorkflow(RetryExpenseReportWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryExpenseReportWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = StreamAuditEventExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditEventExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getExpenseReport().getIssuedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getExpenseReportId())
                        .setRegion(request.getExpenseReport().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventExport(outbound0);
        var reply0 = StreamAuditEventExportResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = ScheduleRfqReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> procurementRfqReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleRfqReport(value));

        var outbound2 = ListAttestationReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAttestationReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getExpenseReport().getAudit().getUpdatedAt())
                        .putAllLabels(reply1.getResult().getDetailsMap())
                        .setRegion(request.getExpenseReport().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(complianceAttestationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAttestationReports(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryExpenseReportWorkflow", request.getExpenseReport().getExpenseReportId());
        return RetryExpenseReportWorkflowResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setExpenseReportId(request.getExpenseReportId())
                        .putAllMetadata(request.getExpenseReport().getMetadataMap())
                        .build())
                .setOutcome(RetryExpenseReportWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Pauses an expense report.
     */
    public PauseExpenseReportWorkflowResponse pauseExpenseReportWorkflow(PauseExpenseReportWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseExpenseReportWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = SignalPolicyWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SignalPolicyWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getExpenseReport().getAudit().getCreatedAt())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(request.getExpenseReport().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> insurancePolicyWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalPolicyWorkflow(value));

        var futures1 = request.getExpenseReport().getAttachmentsList().stream()
                .map(element -> SubmitReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .addAttachments(ReceiptAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> expensesReceiptApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitReceipt(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? SubmitReceiptResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseExpenseReportWorkflow", request.getExpenseReport().getExpenseReportId());
        return PauseExpenseReportWorkflowResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPolicy().getAudit().getCreatedBy())
                                .build())
                        .setExpenseReportId(request.getExpenseReportId())
                        .setIssuedAt(request.getExpenseReport().getIssuedAt())
                        .build())
                .setOutcome(PauseExpenseReportWorkflowResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .build();
    }

    /**
     * Returns an expense report.
     */
    public InspectExpenseReportWorkflowResponse inspectExpenseReportWorkflow(InspectExpenseReportWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectExpenseReportWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getExpenseReportId(), "InspectExpenseReportWorkflow");

        var outbound0 = RpcSupport.unpack(Any.pack(SearchNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .setOwnerId(request.getExpenseReportId())
                        .build())
                .build()), SearchNotificationsRequest.class);
        var reply0 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchNotifications(outbound0);

        var outbound1 = GenerateUnderwritingReportRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setDetails(GenerateUnderwritingReportRequest.Details.newBuilder()
                        .putAllFilters(reply0.getResult().getDetailsMap())
                        .build())
                .build();
        Function<GenerateUnderwritingReportRequest, GenerateUnderwritingReportResponse> operation1 = insuranceUnderwritingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::generateUnderwritingReport;
        var reply1 = operation1.apply(outbound1);
        return InspectExpenseReportWorkflowResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setExpenseReportId(request.getExpenseReportId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Signals an expense report.
     */
    public SignalExpenseReportWorkflowResponse signalExpenseReportWorkflow(SignalExpenseReportWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalExpenseReportWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = ShareUnderwritingReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareUnderwritingReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getExpenseReport().getCounterparty().getEmail())
                                .setName(request.getDetails().getNotify().getName())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending0 = insuranceUnderwritingReportingServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).shareUnderwritingReport(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = ReconcileExpensePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileExpensePoliciesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getExpenseReport().getAudit().getEtag())
                                .setUpdatedBy(request.getExpenseReport().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getUnderwriting().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        RpcPort<ReconcileExpensePoliciesRequest, ReconcileExpensePoliciesResponse> port1 = RpcPort.adapt(expensesExpensePolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::reconcileExpensePolicies);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalExpenseReportWorkflow", request.getExpenseReport().getExpenseReportId());
        return SignalExpenseReportWorkflowResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setExpenseReportId(request.getExpenseReportId())
                        .addLines(ExpenseReportLine.newBuilder()
                                .setDescription(reply1.getExpensePolicy().getDescription())
                                .build())
                        .setNotes(request.getExpenseReport().getNotes())
                        .build())
                .setOutcome(SignalExpenseReportWorkflowResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .build();
    }
}
