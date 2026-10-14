package com.acme.finance.domains.expenses;

import com.acme.analytics.v1.AnalyticsEvent;
import com.acme.analytics.v1.UpdateAnalyticsEventRequest;
import com.acme.analytics.v1.UpdateAnalyticsEventResponse;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.BatchGetAccessPoliciesRequest;
import com.acme.authorization.v1.BatchGetAccessPoliciesResponse;
import com.acme.budgeting.v1.BudgetLineRef;
import com.acme.budgeting.v1.RecallVarianceRequest;
import com.acme.budgeting.v1.RecallVarianceResponse;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceApprovalServiceGrpc;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CheckCustomerEligibilityRequest;
import com.acme.customers.v1.CheckCustomerEligibilityResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.LintCustomerSegmentsRequest;
import com.acme.customers.v1.LintCustomerSegmentsResponse;
import com.acme.employees.v1.EmployeeServiceGrpc;
import com.acme.employees.v1.WatchEmployeesRequest;
import com.acme.employees.v1.WatchEmployeesResponse;
import com.acme.expenses.v1.CompareExpenseReportOptionsRequest;
import com.acme.expenses.v1.CompareExpenseReportOptionsResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyRouteAction;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpenseReportQueryServiceGrpc;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.expenses.v1.ExplainExpenseReportCalculationRequest;
import com.acme.expenses.v1.ExplainExpenseReportCalculationResponse;
import com.acme.expenses.v1.ExplainExpenseReportDecisionRequest;
import com.acme.expenses.v1.ExplainExpenseReportDecisionResponse;
import com.acme.expenses.v1.ExportReceiptsRequest;
import com.acme.expenses.v1.ExportReceiptsResponse;
import com.acme.expenses.v1.FindExpenseReportsRequest;
import com.acme.expenses.v1.FindExpenseReportsResponse;
import com.acme.expenses.v1.GetExpensePolicySyncStateRequest;
import com.acme.expenses.v1.GetExpensePolicySyncStateResponse;
import com.acme.expenses.v1.LintExpensePoliciesRequest;
import com.acme.expenses.v1.LintExpensePoliciesResponse;
import com.acme.expenses.v1.ListExpensePolicyConflictsRequest;
import com.acme.expenses.v1.ListExpensePolicyConflictsResponse;
import com.acme.expenses.v1.PullExpensePoliciesRequest;
import com.acme.expenses.v1.PullExpensePoliciesResponse;
import com.acme.expenses.v1.PushExpensePolicyRequest;
import com.acme.expenses.v1.PushExpensePolicyResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptAttachment;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptLineTax;
import com.acme.expenses.v1.ReceiptQueryServiceGrpc;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ReconcileExpensePoliciesRequest;
import com.acme.expenses.v1.ReconcileExpensePoliciesResponse;
import com.acme.expenses.v1.ResolveExpensePolicyConflictRequest;
import com.acme.expenses.v1.ResolveExpensePolicyConflictResponse;
import com.acme.expenses.v1.ScoreReceiptRequest;
import com.acme.expenses.v1.ScoreReceiptResponse;
import com.acme.expenses.v1.StreamExpensePolicyChangesRequest;
import com.acme.expenses.v1.StreamExpensePolicyChangesResponse;
import com.acme.expenses.v1.SyncExpensePoliciesRequest;
import com.acme.expenses.v1.SyncExpensePoliciesResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimRef;
import com.acme.insurance.v1.PullUnderwritingsRequest;
import com.acme.insurance.v1.PullUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.notifications.v1.FindNotificationsRequest;
import com.acme.notifications.v1.FindNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Keeps expense policies in step with the systems of record in other regions. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ExpensePolicySyncApplication {
    private static final Logger log = LoggerFactory.getLogger(ExpensePolicySyncApplication.class);

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final VarianceApprovalServiceGrpc.VarianceApprovalServiceBlockingStub budgetingVarianceApprovalServiceBlocking;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync;

    private final ChannelRegistry channels;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final EmployeeServiceGrpc.EmployeeServiceBlockingStub employeesEmployeeServiceBlocking;

    private final ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceBlockingStub expensesExpensePolicyValidationServiceBlocking;

    private final ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking;

    private final ExpenseReportQueryServiceGrpc.ExpenseReportQueryServiceBlockingStub expensesExpenseReportQueryServiceBlocking;

    private final ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceBlockingStub expensesExpenseReportValidationServiceBlocking;

    private final ReceiptQueryServiceGrpc.ReceiptQueryServiceBlockingStub expensesReceiptQueryServiceBlocking;

    private final ReceiptValidationServiceGrpc.ReceiptValidationServiceBlockingStub expensesReceiptValidationServiceBlocking;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final ExpensesRepository repository;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync;

    private final Executor rpcExecutor;

    public ExpensePolicySyncApplication(
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            VarianceApprovalServiceGrpc.VarianceApprovalServiceBlockingStub budgetingVarianceApprovalServiceBlocking,
            VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync,
            ChannelRegistry channels,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            EmployeeServiceGrpc.EmployeeServiceBlockingStub employeesEmployeeServiceBlocking,
            ExpensePolicyValidationServiceGrpc.ExpensePolicyValidationServiceBlockingStub expensesExpensePolicyValidationServiceBlocking,
            ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking,
            ExpenseReportQueryServiceGrpc.ExpenseReportQueryServiceBlockingStub expensesExpenseReportQueryServiceBlocking,
            ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceBlockingStub expensesExpenseReportValidationServiceBlocking,
            ReceiptQueryServiceGrpc.ReceiptQueryServiceBlockingStub expensesReceiptQueryServiceBlocking,
            ReceiptValidationServiceGrpc.ReceiptValidationServiceBlockingStub expensesReceiptValidationServiceBlocking,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            ExpensesRepository repository,
            RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.budgetingVarianceApprovalServiceBlocking = budgetingVarianceApprovalServiceBlocking;
        this.budgetingVarianceSyncServiceAsync = budgetingVarianceSyncServiceAsync;
        this.channels = channels;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.employeesEmployeeServiceBlocking = employeesEmployeeServiceBlocking;
        this.expensesExpensePolicyValidationServiceBlocking = expensesExpensePolicyValidationServiceBlocking;
        this.expensesExpenseReportCalculationServiceBlocking = expensesExpenseReportCalculationServiceBlocking;
        this.expensesExpenseReportQueryServiceBlocking = expensesExpenseReportQueryServiceBlocking;
        this.expensesExpenseReportValidationServiceBlocking = expensesExpenseReportValidationServiceBlocking;
        this.expensesReceiptQueryServiceBlocking = expensesReceiptQueryServiceBlocking;
        this.expensesReceiptValidationServiceBlocking = expensesReceiptValidationServiceBlocking;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterServiceAsync = riskRiskRegisterServiceAsync;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Syncs an expense policy.
     */
    public SyncExpensePoliciesResponse syncExpensePolicies(SyncExpensePoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SyncExpensePolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = PullUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullUnderwritingsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getExpensePolicy().getAudit().getEtag())
                                .setUpdatedAt(request.getExpensePolicy().getAudit().getUpdatedAt())
                                .build())
                        .setCustomer(ClaimRef.newBuilder()
                                .setDisplayName(request.getExpensePolicy().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.UnderwritingSyncService");
        var serviceName0 = "acme.insurance.v1" + "." + "UnderwritingSyncService";
        var methodName0 = "Pul" + "lUnderwritings";
        var descriptor0 = MethodDescriptor.<PullUnderwritingsRequest, PullUnderwritingsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(PullUnderwritingsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(PullUnderwritingsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = BatchCreateRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateRiskRegistersRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateRiskRegistersResponse.getDefaultInstance());
        var input1 = riskRiskRegisterServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateRiskRegisters(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamVarianceChangesRequest.newBuilder()
                .setChange(Variance.newBuilder()
                        .setAssignee(request.getExpensePolicy().getOwner())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getExpensePolicy().getAudit().getCreatedAt())
                                .setCreatedBy(request.getExpensePolicy().getAudit().getCreatedBy())
                                .setEtag(request.getExpensePolicy().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamVarianceChangesResponse.getDefaultInstance());
        var input2 = budgetingVarianceSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamVarianceChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SyncExpensePolicies", request.getExpensePolicy().getExpensePolicyId());
        return SyncExpensePoliciesResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setName(request.getExpensePolicy().getName())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Pushs an expense policy.
     */
    public PushExpensePolicyResponse pushExpensePolicy(PushExpensePolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PushExpensePolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var builder0 = ScoreReceiptRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setReceipt(Receipt.newBuilder()
                .addAttachments(ReceiptAttachment.newBuilder()
                        .setChecksum(request.getDetails().getChecksum())
                        .build())
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedBy(request.getExpensePolicy().getAudit().getCreatedBy())
                        .setUpdatedAt(request.getExpensePolicy().getAudit().getUpdatedAt())
                        .build())
                .addLines(ReceiptLine.newBuilder()
                        .setTax(ReceiptLineTax.newBuilder()
                                .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                .build())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = expensesReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreReceipt(outbound0);

        var outbound1 = CompareExpenseReportOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getReceipt().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getReceipt().getBillingAddress().getLinesList())
                                .setRecipient(reply0.getReceipt().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareExpenseReportOptions(value)).orElseThrow();

        var outbound2 = ExportReceiptsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportReceiptsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getReceipt().getDueAt())
                        .putAllLabels(reply1.getResult().getDetailsMap())
                        .setOwnerId(reply1.getExpenseReport().getCounterparty().getCustomerId())
                        .setRegion(reply1.getExpenseReport().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream2 = expensesReceiptQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportReceipts(outbound2);
        var reply2 = ExportReceiptsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PushExpensePolicy", request.getExpensePolicy().getExpensePolicyId());
        return PushExpensePolicyResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getReceipt().getAudit().getCreatedAt())
                                .build())
                        .setDescription(request.getExpensePolicy().getDescription())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Pulls an expense policy.
     */
    public PullExpensePoliciesResponse pullExpensePolicies(PullExpensePoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PullExpensePolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = FindExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindExpenseReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getExpensePolicy().getAudit().getUpdatedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getExpensePolicy().getOwner().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> expensesExpenseReportQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findExpenseReports(outbound0), rpcExecutor);

        var outbound1 = FindNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getExpensePolicy().getAudit().getUpdatedAt())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findNotifications(outbound1), rpcExecutor);

        var outbound2 = ExplainExpenseReportDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getExpensePolicy().getOwner().getEmail())
                                .build())
                        .addLines(ExpenseReportLine.newBuilder()
                                .setTax(ExpenseReportLineTax.newBuilder()
                                        .setAmount(request.getExpensePolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> expensesExpenseReportValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainExpenseReportDecision(outbound2), rpcExecutor);

        var outbound3 = BatchGetAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending3 = CompletableFuture.supplyAsync(() -> authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetAccessPolicies(outbound3), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2, pending3).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();
        var reply3 = pending3.join();
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PullExpensePolicies", request.getExpensePolicy().getExpensePolicyId());
        return PullExpensePoliciesResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getExpenseReport().getAudit().getEtag())
                                .build())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setPriority(request.getExpensePolicy().getPriority())
                        .build())
                .setOutcome(PullExpensePoliciesResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Reconciles an expense policy.
     */
    public ReconcileExpensePoliciesResponse reconcileExpensePolicies(ReconcileExpensePoliciesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReconcileExpensePolicies")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = WatchEmployeesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getExpensePolicy().getOwner().getEmail())
                        .build())
                .build();
        var stream0 = employeesEmployeeServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchEmployees(outbound0);
        var reply0 = WatchEmployeesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = LintExpensePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setRoute(ExpensePolicyRouteAction.newBuilder()
                                .setTarget(switch (request.getExpensePolicy().getActionCase()) {
                                    case ROUTE -> request.getExpensePolicy().getRoute().getTarget();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = expensesExpensePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintExpensePolicies(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReconcileExpensePolicies", request.getExpensePolicy().getExpensePolicyId());
        return ReconcileExpensePoliciesResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setDescription(request.getExpensePolicy().getDescription())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setVersion(reply1.getExpensePolicy().getVersion())
                        .build())
                .build();
    }

    /**
     * Exchanges changes of expense policies with a peer.
     */
    public StreamExpensePolicyChangesResponse streamExpensePolicyChanges(StreamExpensePolicyChangesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamExpensePolicyChanges")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasChange()) {
            repository.saveExpensePolicy(request.getContext().getTenantId(), request.getChange());
        }

        var outbound0 = CheckCustomerEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(switch (request.getFrameCase()) {
                                    case CHANGE -> request.getChange().getAudit().getUpdatedBy();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCustomerEligibility(outbound0);

        var outbound1 = JsonCodec.roundTrip(ExplainExpenseReportCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getCustomer().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build(), ExplainExpenseReportCalculationRequest.class);
        var reply1 = expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainExpenseReportCalculation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getChange().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamExpensePolicyChanges", request.getChange().getExpensePolicyId());
        return StreamExpensePolicyChangesResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setOwner(request.getChange().getOwner())
                        .setPriority(request.getChange().getPriority())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Resolves an expense policy.
     */
    public ResolveExpensePolicyConflictResponse resolveExpensePolicyConflict(ResolveExpensePolicyConflictRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveExpensePolicyConflict")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpensePolicy(request.getContext().getTenantId(), request.getExpensePolicy());

        var outbound0 = LintCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getExpensePolicy().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getExpensePolicy().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getExpensePolicy().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.customers.v1.CustomerSegmentValidationService");
        var selected0 = CustomerSegmentValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.lintCustomerSegments(outbound0);

        var outbound1 = BeanCopies.copy(RecallVarianceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallVarianceRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getExpensePolicy().getOwner().getEmail())
                                .build())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getExpensePolicy().getAudit().getCreatedBy())
                                .setEtag(reply0.getCustomerSegment().getAudit().getEtag())
                                .build())
                        .setCustomer(BudgetLineRef.newBuilder()
                                .setDisplayName(reply0.getCustomerSegment().getOwner().getDisplayName())
                                .build())
                        .build())
                .build(), RecallVarianceRequest.class);
        var reply1 = budgetingVarianceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallVariance(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExpensePolicy().getExpensePolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResolveExpensePolicyConflict", request.getExpensePolicy().getExpensePolicyId());
        return ResolveExpensePolicyConflictResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getVariance().getAudit().getCreatedAt())
                                .setEtag(reply0.getCustomerSegment().getAudit().getEtag())
                                .build())
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .setOwner(request.getExpensePolicy().getOwner())
                        .build())
                .build();
    }

    /**
     * Returns an expense policy.
     */
    public GetExpensePolicySyncStateResponse getExpensePolicySyncState(GetExpensePolicySyncStateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetExpensePolicySyncState")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getExpensePolicyId(), "GetExpensePolicySyncState");
        return GetExpensePolicySyncStateResponse.newBuilder()
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setExpensePolicyId(request.getExpensePolicyId())
                        .build())
                .build();
    }

    /**
     * Lists expense policies a page at a time.
     */
    public ListExpensePolicyConflictsResponse listExpensePolicyConflicts(ListExpensePolicyConflictsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListExpensePolicyConflicts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpensePolicy", request.getContext().getTraceId(), "ListExpensePolicyConflicts");

        var outbound0 = UpdateAnalyticsEventRequest.newBuilder()
                .setAnalyticsEvent(AnalyticsEvent.newBuilder()
                        .setCorrelationId(request.getFilter().getOwnerId())
                        .setDetail(Any.pack(request.getContext()))
                        .setOccurredAt(request.getFilter().getCreatedAfter())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.analytics.v1.AnalyticsEventService");
        var serviceName0 = "acme.analytics.v1" + "." + "AnalyticsEventService";
        var methodName0 = "Upd" + "ateAnalyticsEvent";
        var descriptor0 = MethodDescriptor.<UpdateAnalyticsEventRequest, UpdateAnalyticsEventResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(UpdateAnalyticsEventRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(UpdateAnalyticsEventResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListExpensePolicyConflicts", request.getPage().getPageToken());
        return ListExpensePolicyConflictsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
