package com.acme.finance.domains.expenses;

import com.acme.banking.v1.ApproveStandingOrderRequest;
import com.acme.banking.v1.ApproveStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.ListBudgetLineReportsRequest;
import com.acme.budgeting.v1.ListBudgetLineReportsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.ValidateConsentRecordRequest;
import com.acme.customers.v1.ValidateConsentRecordResponse;
import com.acme.expenses.v1.AdvanceMileageClaimWorkflowRequest;
import com.acme.expenses.v1.AdvanceMileageClaimWorkflowResponse;
import com.acme.expenses.v1.AdvanceReceiptWorkflowRequest;
import com.acme.expenses.v1.AdvanceReceiptWorkflowResponse;
import com.acme.expenses.v1.CancelMileageClaimWorkflowRequest;
import com.acme.expenses.v1.CancelMileageClaimWorkflowResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpenseReportWorkflowServiceGrpc;
import com.acme.expenses.v1.InspectMileageClaimWorkflowRequest;
import com.acme.expenses.v1.InspectMileageClaimWorkflowResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.PushExpensePolicyRequest;
import com.acme.expenses.v1.PushExpensePolicyResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptLifecycleServiceGrpc;
import com.acme.expenses.v1.ReceiptRef;
import com.acme.expenses.v1.ReceiptWorkflowServiceGrpc;
import com.acme.expenses.v1.RetryExpenseReportWorkflowRequest;
import com.acme.expenses.v1.RetryExpenseReportWorkflowResponse;
import com.acme.expenses.v1.RetryMileageClaimWorkflowRequest;
import com.acme.expenses.v1.RetryMileageClaimWorkflowResponse;
import com.acme.expenses.v1.ScheduleReceiptExpiryRequest;
import com.acme.expenses.v1.ScheduleReceiptExpiryResponse;
import com.acme.expenses.v1.SignalMileageClaimWorkflowRequest;
import com.acme.expenses.v1.SignalMileageClaimWorkflowResponse;
import com.acme.expenses.v1.StartMileageClaimWorkflowRequest;
import com.acme.expenses.v1.StartMileageClaimWorkflowResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CommentPolicyApprovalRequest;
import com.acme.insurance.v1.CommentPolicyApprovalResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.StreamPremiumChangesRequest;
import com.acme.insurance.v1.StreamPremiumChangesResponse;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesRequest;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesResponse;
import com.acme.invoicing.v1.InvoiceBatchServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a mileage claim from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class MileageClaimWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(MileageClaimWorkflowApplication.class);

    private final StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking;

    private final BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking;

    private final ChannelRegistry channels;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final ExpenseReportWorkflowServiceGrpc.ExpenseReportWorkflowServiceBlockingStub expensesExpenseReportWorkflowServiceBlocking;

    private final ReceiptLifecycleServiceGrpc.ReceiptLifecycleServiceBlockingStub expensesReceiptLifecycleServiceBlocking;

    private final ReceiptWorkflowServiceGrpc.ReceiptWorkflowServiceBlockingStub expensesReceiptWorkflowServiceBlocking;

    private final PolicyApprovalServiceGrpc.PolicyApprovalServiceFutureStub insurancePolicyApprovalServiceFuture;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceStub insurancePremiumSyncServiceAsync;

    private final InvoiceBatchServiceGrpc.InvoiceBatchServiceStub invoicingInvoiceBatchServiceAsync;

    private final ExpensesRepository repository;

    public MileageClaimWorkflowApplication(
            StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking,
            BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking,
            ChannelRegistry channels,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            ExpenseReportWorkflowServiceGrpc.ExpenseReportWorkflowServiceBlockingStub expensesExpenseReportWorkflowServiceBlocking,
            ReceiptLifecycleServiceGrpc.ReceiptLifecycleServiceBlockingStub expensesReceiptLifecycleServiceBlocking,
            ReceiptWorkflowServiceGrpc.ReceiptWorkflowServiceBlockingStub expensesReceiptWorkflowServiceBlocking,
            PolicyApprovalServiceGrpc.PolicyApprovalServiceFutureStub insurancePolicyApprovalServiceFuture,
            PremiumSyncServiceGrpc.PremiumSyncServiceStub insurancePremiumSyncServiceAsync,
            InvoiceBatchServiceGrpc.InvoiceBatchServiceStub invoicingInvoiceBatchServiceAsync,
            ExpensesRepository repository) {
        this.bankingStandingOrderApprovalServiceBlocking = bankingStandingOrderApprovalServiceBlocking;
        this.budgetingBudgetLineReportingServiceBlocking = budgetingBudgetLineReportingServiceBlocking;
        this.channels = channels;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.expensesExpenseReportWorkflowServiceBlocking = expensesExpenseReportWorkflowServiceBlocking;
        this.expensesReceiptLifecycleServiceBlocking = expensesReceiptLifecycleServiceBlocking;
        this.expensesReceiptWorkflowServiceBlocking = expensesReceiptWorkflowServiceBlocking;
        this.insurancePolicyApprovalServiceFuture = insurancePolicyApprovalServiceFuture;
        this.insurancePremiumSyncServiceAsync = insurancePremiumSyncServiceAsync;
        this.invoicingInvoiceBatchServiceAsync = invoicingInvoiceBatchServiceAsync;
        this.repository = repository;
    }

    /**
     * Starts a mileage claim.
     */
    public StartMileageClaimWorkflowResponse startMileageClaimWorkflow(StartMileageClaimWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartMileageClaimWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        List<ValidateConsentRecordResponse> results0 = request.getMileageClaim().getLinesList().stream()
                .map(element -> ValidateConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .putAllAttributes(element.getAttributesMap())
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateConsentRecord(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ValidateConsentRecordResponse.getDefaultInstance()
                : results0.getLast();

        var results1 = new ArrayList<RetryExpenseReportWorkflowResponse>();
        for (var element : request.getMileageClaim().getLinesList()) {
            RetryExpenseReportWorkflowRequest itemRequest = RetryExpenseReportWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .addLines(ExpenseReportLine.newBuilder()
                                .setTax(ExpenseReportLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
            results1.add(expensesExpenseReportWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryExpenseReportWorkflow(itemRequest));
        }
        var reply1 = results1.isEmpty() ? RetryExpenseReportWorkflowResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = BatchCreateInvoiceBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateInvoiceBatchesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateInvoiceBatchesResponse.getDefaultInstance());
        var input2 = invoicingInvoiceBatchServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateInvoiceBatches(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamPremiumChangesRequest.newBuilder()
                .setChange(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getMileageClaim().getAudit().getCreatedAt())
                                .setCreatedBy(request.getMileageClaim().getAudit().getCreatedBy())
                                .setEtag(request.getMileageClaim().getAudit().getEtag())
                                .setUpdatedAt(request.getMileageClaim().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamPremiumChangesResponse.getDefaultInstance());
        var input3 = insurancePremiumSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamPremiumChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartMileageClaimWorkflow", request.getMileageClaim().getMileageClaimId());
        return StartMileageClaimWorkflowResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setMileageClaimId(request.getMileageClaimId())
                        .setNotes(request.getMileageClaim().getNotes())
                        .build())
                .build();
    }

    /**
     * Advances a mileage claim.
     */
    public AdvanceMileageClaimWorkflowResponse advanceMileageClaimWorkflow(AdvanceMileageClaimWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceMileageClaimWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = PushExpensePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushExpensePolicyRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getMileageClaim().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getMileageClaim().getCounterparty().getDisplayName())
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.expenses.v1.ExpensePolicySyncService");
        var selected0 = ExpensePolicySyncServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.pushExpensePolicy(outbound0);

        var outbound1 = BeanCopies.copy(ListBudgetLineReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListBudgetLineReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getMileageClaim().getAudit().getCreatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setOwnerId(reply0.getExpensePolicy().getOwner().getUserId())
                        .setRegion(request.getMileageClaim().getBillingAddress().getRegion())
                        .build())
                .build(), ListBudgetLineReportsRequest.class);
        var reply1 = budgetingBudgetLineReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listBudgetLineReports(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceMileageClaimWorkflow", request.getMileageClaim().getMileageClaimId());
        return AdvanceMileageClaimWorkflowResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setCounterparty(request.getMileageClaim().getCounterparty())
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels a mileage claim.
     */
    public CancelMileageClaimWorkflowResponse cancelMileageClaimWorkflow(CancelMileageClaimWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelMileageClaimWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = AdvanceReceiptWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AdvanceReceiptWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getMileageClaim().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getMileageClaim().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.expenses.v1.ReceiptWorkflowService/AdvanceReceiptWorkflow", outbound0);
        var reply0 = expensesReceiptWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceReceiptWorkflow(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelMileageClaimWorkflow", request.getMileageClaim().getMileageClaimId());
        return CancelMileageClaimWorkflowResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setBillingAddress(reply0.getReceipt().getBillingAddress())
                        .setMileageClaimId(request.getMileageClaimId())
                        .setStatus(request.getMileageClaim().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys a mileage claim.
     */
    public RetryMileageClaimWorkflowResponse retryMileageClaimWorkflow(RetryMileageClaimWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryMileageClaimWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = JsonCodec.roundTrip(ApproveStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getNotify().getName())
                                        .setPhone(request.getDetails().getNotify().getPhone())
                                        .build())
                                .build())
                        .build())
                .build(), ApproveStandingOrderRequest.class);
        var reply0 = bankingStandingOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveStandingOrder(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryMileageClaimWorkflow", request.getMileageClaim().getMileageClaimId());
        return RetryMileageClaimWorkflowResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getStandingOrder().getAudit().getUpdatedBy())
                                .build())
                        .setMileageClaimId(request.getMileageClaimId())
                        .setNumber(request.getMileageClaim().getNumber())
                        .build())
                .build();
    }

    /**
     * Returns a mileage claim.
     */
    public InspectMileageClaimWorkflowResponse inspectMileageClaimWorkflow(InspectMileageClaimWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectMileageClaimWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "MileageClaim", request.getMileageClaimId(), "InspectMileageClaimWorkflow");
        return InspectMileageClaimWorkflowResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .build();
    }

    /**
     * Signals a mileage claim.
     */
    public SignalMileageClaimWorkflowResponse signalMileageClaimWorkflow(SignalMileageClaimWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalMileageClaimWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveMileageClaim(request.getContext().getTenantId(), request.getMileageClaim());

        var outbound0 = RetryExpenseReportWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RetryExpenseReportWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getMileageClaim().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getMileageClaim().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.expenses.v1.ExpenseReportWorkflowService");
        var serviceName0 = "acme.expenses.v1" + "." + "ExpenseReportWorkflowService";
        var methodName0 = "Ret" + "ryExpenseReportWorkflow";
        var descriptor0 = MethodDescriptor.<RetryExpenseReportWorkflowRequest, RetryExpenseReportWorkflowResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(RetryExpenseReportWorkflowRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RetryExpenseReportWorkflowResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ScheduleReceiptExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getExpenseReport().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        RpcPort<ScheduleReceiptExpiryRequest, ScheduleReceiptExpiryResponse> port1 = RpcPort.adapt(expensesReceiptLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scheduleReceiptExpiry);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = CommentPolicyApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getMileageClaim().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var pending2 = insurancePolicyApprovalServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).commentPolicyApproval(outbound2);
        var reply2 = RpcSupport.await(pending2);
        log.atDebug().addKeyValue("auditValue", request.getMileageClaim().getMileageClaimId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalMileageClaimWorkflow", request.getMileageClaim().getMileageClaimId());
        return SignalMileageClaimWorkflowResponse.newBuilder()
                .setMileageClaim(MileageClaim.newBuilder()
                        .setBillingAddress(request.getMileageClaim().getBillingAddress())
                        .setCounterparty(ReceiptRef.newBuilder()
                                .setDisplayName(reply2.getPolicy().getHolder().getDisplayName())
                                .setReceiptId(reply1.getReceipt().getReceiptId())
                                .build())
                        .setIssuedAt(reply0.getExpenseReport().getIssuedAt())
                        .setMileageClaimId(request.getMileageClaimId())
                        .build())
                .setOutcome(SignalMileageClaimWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .build();
    }
}
