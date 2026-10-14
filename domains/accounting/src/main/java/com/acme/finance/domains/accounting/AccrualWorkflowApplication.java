package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualCardSettlement;
import com.acme.accounting.v1.AccrualLine;
import com.acme.accounting.v1.AccrualLineTax;
import com.acme.accounting.v1.AdvanceAccrualWorkflowRequest;
import com.acme.accounting.v1.AdvanceAccrualWorkflowResponse;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationCalculationServiceGrpc;
import com.acme.accounting.v1.AllocationRouteAction;
import com.acme.accounting.v1.CalculateAllocationRequest;
import com.acme.accounting.v1.CalculateAllocationResponse;
import com.acme.accounting.v1.CancelAccrualWorkflowRequest;
import com.acme.accounting.v1.CancelAccrualWorkflowResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsQueryServiceGrpc;
import com.acme.accounting.v1.ChartOfAccountsWorkflowServiceGrpc;
import com.acme.accounting.v1.ExplainAllocationDecisionRequest;
import com.acme.accounting.v1.ExplainAllocationDecisionResponse;
import com.acme.accounting.v1.FindChartOfAccountsesRequest;
import com.acme.accounting.v1.FindChartOfAccountsesResponse;
import com.acme.accounting.v1.GetAllocationRatesRequest;
import com.acme.accounting.v1.GetAllocationRatesResponse;
import com.acme.accounting.v1.InspectAccrualWorkflowRequest;
import com.acme.accounting.v1.InspectAccrualWorkflowResponse;
import com.acme.accounting.v1.ListAccrualWorkflowsRequest;
import com.acme.accounting.v1.ListAccrualWorkflowsResponse;
import com.acme.accounting.v1.PauseAccrualWorkflowRequest;
import com.acme.accounting.v1.PauseAccrualWorkflowResponse;
import com.acme.accounting.v1.RecalculateAllocationsRequest;
import com.acme.accounting.v1.RecalculateAllocationsResponse;
import com.acme.accounting.v1.RecallAccrualRequest;
import com.acme.accounting.v1.RecallAccrualResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.RejectChartOfAccountsRequest;
import com.acme.accounting.v1.RejectChartOfAccountsResponse;
import com.acme.accounting.v1.ResumeAccrualWorkflowRequest;
import com.acme.accounting.v1.ResumeAccrualWorkflowResponse;
import com.acme.accounting.v1.RetryAccrualWorkflowRequest;
import com.acme.accounting.v1.RetryAccrualWorkflowResponse;
import com.acme.accounting.v1.ScheduleReconciliationExpiryRequest;
import com.acme.accounting.v1.ScheduleReconciliationExpiryResponse;
import com.acme.accounting.v1.SignalAccrualWorkflowRequest;
import com.acme.accounting.v1.SignalAccrualWorkflowResponse;
import com.acme.accounting.v1.SignalChartOfAccountsWorkflowRequest;
import com.acme.accounting.v1.SignalChartOfAccountsWorkflowResponse;
import com.acme.accounting.v1.StartAccrualWorkflowRequest;
import com.acme.accounting.v1.StartAccrualWorkflowResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.CreateAuditEventRequest;
import com.acme.audit.v1.CreateAuditEventResponse;
import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.AdvanceExpenseReportWorkflowRequest;
import com.acme.expenses.v1.AdvanceExpenseReportWorkflowResponse;
import com.acme.expenses.v1.CancelExpenseReportWorkflowRequest;
import com.acme.expenses.v1.CancelExpenseReportWorkflowResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportWorkflowServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FindJournalEntriesRequest;
import com.acme.ledger.v1.FindJournalEntriesResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.ResolveJournalEntriesRequest;
import com.acme.ledger.v1.ResolveJournalEntriesResponse;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.orders.v1.EstimateOrderRequest;
import com.acme.orders.v1.EstimateOrderResponse;
import com.acme.orders.v1.Order;
import com.acme.orders.v1.OrderCalculationServiceGrpc;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqApprovalServiceGrpc;
import com.acme.procurement.v1.SubmitRfqRequest;
import com.acme.procurement.v1.SubmitRfqResponse;
import com.acme.suppliers.v1.ApproveSupplierContractRequest;
import com.acme.suppliers.v1.ApproveSupplierContractResponse;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes an accrual from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class AccrualWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(AccrualWorkflowApplication.class);

    private final AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking;

    private final ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceBlockingStub accountingChartOfAccountsApprovalServiceBlocking;

    private final ChartOfAccountsQueryServiceGrpc.ChartOfAccountsQueryServiceBlockingStub accountingChartOfAccountsQueryServiceBlocking;

    private final ChartOfAccountsWorkflowServiceGrpc.ChartOfAccountsWorkflowServiceFutureStub accountingChartOfAccountsWorkflowServiceFuture;

    private final AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking;

    private final ChannelRegistry channels;

    private final ExpenseReportWorkflowServiceGrpc.ExpenseReportWorkflowServiceBlockingStub expensesExpenseReportWorkflowServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking;

    private final OrderCalculationServiceGrpc.OrderCalculationServiceBlockingStub ordersOrderCalculationServiceBlocking;

    private final RfqApprovalServiceGrpc.RfqApprovalServiceBlockingStub procurementRfqApprovalServiceBlocking;

    private final AccountingRepository repository;

    private final Executor rpcExecutor;

    private final SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceFutureStub suppliersSupplierContractApprovalServiceFuture;

    public AccrualWorkflowApplication(
            AllocationCalculationServiceGrpc.AllocationCalculationServiceBlockingStub accountingAllocationCalculationServiceBlocking,
            ChartOfAccountsApprovalServiceGrpc.ChartOfAccountsApprovalServiceBlockingStub accountingChartOfAccountsApprovalServiceBlocking,
            ChartOfAccountsQueryServiceGrpc.ChartOfAccountsQueryServiceBlockingStub accountingChartOfAccountsQueryServiceBlocking,
            ChartOfAccountsWorkflowServiceGrpc.ChartOfAccountsWorkflowServiceFutureStub accountingChartOfAccountsWorkflowServiceFuture,
            AuditEventServiceGrpc.AuditEventServiceBlockingStub auditAuditEventServiceBlocking,
            ChannelRegistry channels,
            ExpenseReportWorkflowServiceGrpc.ExpenseReportWorkflowServiceBlockingStub expensesExpenseReportWorkflowServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking,
            OrderCalculationServiceGrpc.OrderCalculationServiceBlockingStub ordersOrderCalculationServiceBlocking,
            RfqApprovalServiceGrpc.RfqApprovalServiceBlockingStub procurementRfqApprovalServiceBlocking,
            AccountingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceFutureStub suppliersSupplierContractApprovalServiceFuture) {
        this.accountingAllocationCalculationServiceBlocking = accountingAllocationCalculationServiceBlocking;
        this.accountingChartOfAccountsApprovalServiceBlocking = accountingChartOfAccountsApprovalServiceBlocking;
        this.accountingChartOfAccountsQueryServiceBlocking = accountingChartOfAccountsQueryServiceBlocking;
        this.accountingChartOfAccountsWorkflowServiceFuture = accountingChartOfAccountsWorkflowServiceFuture;
        this.auditAuditEventServiceBlocking = auditAuditEventServiceBlocking;
        this.channels = channels;
        this.expensesExpenseReportWorkflowServiceBlocking = expensesExpenseReportWorkflowServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.ledgerJournalEntryQueryServiceBlocking = ledgerJournalEntryQueryServiceBlocking;
        this.ordersOrderCalculationServiceBlocking = ordersOrderCalculationServiceBlocking;
        this.procurementRfqApprovalServiceBlocking = procurementRfqApprovalServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractApprovalServiceFuture = suppliersSupplierContractApprovalServiceFuture;
    }

    /**
     * Starts an accrual.
     */
    public StartAccrualWorkflowResponse startAccrualWorkflow(StartAccrualWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartAccrualWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = ExplainAllocationDecisionRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAccrual().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainAllocationDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.accounting.v1.AllocationValidationService");
        var serviceName0 = "acme.accounting.v1" + "." + "AllocationValidationService";
        var methodName0 = "Exp" + "lainAllocationDecision";
        var descriptor0 = MethodDescriptor.<ExplainAllocationDecisionRequest, ExplainAllocationDecisionResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ExplainAllocationDecisionRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ExplainAllocationDecisionResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartAccrualWorkflow", request.getAccrual().getAccrualId());
        return StartAccrualWorkflowResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAllocation().getAudit().getCreatedAt())
                                .build())
                        .setIssuedAt(request.getAccrual().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Advances an accrual.
     */
    public AdvanceAccrualWorkflowResponse advanceAccrualWorkflow(AdvanceAccrualWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceAccrualWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = CancelExpenseReportWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CancelExpenseReportWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getAccrual().getBillingAddress().getPostalCode())
                                .setRecipient(request.getAccrual().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply0 = expensesExpenseReportWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelExpenseReportWorkflow(outbound0);

        var builder1 = RejectChartOfAccountsRequest.newBuilder();
        builder1.setChartOfAccounts(ChartOfAccounts.newBuilder()
                .addChildren(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setCreatedAt(request.getAccrual().getAudit().getCreatedAt())
                                        .setEtag(request.getAccrual().getAudit().getEtag())
                                        .build())
                                .setLocation(PostalAddress.newBuilder()
                                        .setPostalCode(request.getAccrual().getBillingAddress().getPostalCode())
                                        .build())
                                .build())
                        .build())
                .build());
        builder1.setContext(request.getContext());
        var outbound1 = builder1.build();
        var reply1 = accountingChartOfAccountsApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectChartOfAccounts(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceAccrualWorkflow", request.getAccrual().getAccrualId());
        return AdvanceAccrualWorkflowResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getChartOfAccounts().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getExpenseReport().getBillingAddress().getLinesList())
                                .build())
                        .setDueAt(request.getAccrual().getDueAt())
                        .build())
                .build();
    }

    /**
     * Cancels an accrual.
     */
    public CancelAccrualWorkflowResponse cancelAccrualWorkflow(CancelAccrualWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelAccrualWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = CreateAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setNumber(request.getAccrual().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditAuditEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createAuditEvent(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = GetAllocationRatesRequest.newBuilder()
                .setAllocationId(request.getAccrual().getCounterparty().getAllocationId())
                .setContext(request.getContext())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getAllocationRates(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelAccrualWorkflow", request.getAccrual().getAccrualId());
        return CancelAccrualWorkflowResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getAllocation().getAudit().getUpdatedBy())
                                .build())
                        .addLines(AccrualLine.newBuilder()
                                .setDescription(reply1.getAllocation().getDescription())
                                .build())
                        .setNumber(request.getAccrual().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Retrys an accrual.
     */
    public RetryAccrualWorkflowResponse retryAccrualWorkflow(RetryAccrualWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryAccrualWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = RpcSupport.unpack(Any.pack(ResolveJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .build()), ResolveJournalEntriesRequest.class);
        var reply0 = ledgerJournalEntryQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveJournalEntries(outbound0);

        var outbound1 = ScheduleReconciliationExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAccrual().getAudit().getCreatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.accounting.v1.ReconciliationLifecycleService");
        var serviceName1 = "acme.accounting.v1" + "." + "ReconciliationLifecycleService";
        var methodName1 = "Sch" + "eduleReconciliationExpiry";
        var descriptor1 = MethodDescriptor.<ScheduleReconciliationExpiryRequest, ScheduleReconciliationExpiryResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ScheduleReconciliationExpiryRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ScheduleReconciliationExpiryResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryAccrualWorkflow", request.getAccrual().getAccrualId());
        return RetryAccrualWorkflowResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setNotes(request.getAccrual().getNotes())
                        .build())
                .setOutcome(RetryAccrualWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Pauses an accrual.
     */
    public PauseAccrualWorkflowResponse pauseAccrualWorkflow(PauseAccrualWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseAccrualWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = SignalChartOfAccountsWorkflowRequest.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .addAllLines(request.getAccrual().getBillingAddress().getLinesList())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = accountingChartOfAccountsWorkflowServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).signalChartOfAccountsWorkflow(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = (FindJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindJournalEntriesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getAccrual().getAudit().getCreatedAt())
                        .putAllLabels(request.getAccrual().getMetadataMap())
                        .setOwnerId(reply0.getChartOfAccounts().getManager().getUserId())
                        .setRegion(reply0.getChartOfAccounts().getLocation().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply1 = ledgerJournalEntryQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findJournalEntries(outbound1);

        var outbound2 = BeanCopies.copy(FindChartOfAccountsesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindChartOfAccountsesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getChartOfAccounts().getAudit().getUpdatedAt())
                        .setOwnerId(reply0.getChartOfAccounts().getManager().getUserId())
                        .setRegion(reply0.getChartOfAccounts().getLocation().getRegion())
                        .build())
                .build(), FindChartOfAccountsesRequest.class);
        var reply2 = accountingChartOfAccountsQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findChartOfAccountses(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseAccrualWorkflow", request.getAccrual().getAccrualId());
        return PauseAccrualWorkflowResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getChartOfAccounts().getLocation().getCountryCode())
                                .build())
                        .setStatus(request.getAccrual().getStatus())
                        .build())
                .setOutcome(PauseAccrualWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Resumes an accrual.
     */
    public ResumeAccrualWorkflowResponse resumeAccrualWorkflow(ResumeAccrualWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeAccrualWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = ScheduleFiscalPeriodExportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ScheduleFiscalPeriodExportRequest.Details.newBuilder()
                        .putAllFilters(request.getAccrual().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleFiscalPeriodExport(outbound0), rpcExecutor);

        var outbound1 = SubmitRfqRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitRfqRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getAccrual().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setRfq(Rfq.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getAccrual().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> procurementRfqApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitRfq(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = ResolveRetentionPolicyConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolveRetentionPolicyConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getRfq().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getAccrual().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.audit.v1.RetentionPolicySyncService");
        var serviceName2 = "acme.audit.v1" + "." + "RetentionPolicySyncService";
        var methodName2 = "Res" + "olveRetentionPolicyConflict";
        var descriptor2 = MethodDescriptor.<ResolveRetentionPolicyConflictRequest, ResolveRetentionPolicyConflictResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ResolveRetentionPolicyConflictRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ResolveRetentionPolicyConflictResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = RecallAccrualRequest.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAccrual().getAudit().getCreatedBy())
                                .build())
                        .setCard(AccrualCardSettlement.newBuilder()
                                .setToken(request.getAccrual().getCard().getToken())
                                .build())
                        .addLines(AccrualLine.newBuilder()
                                .setTax(AccrualLineTax.newBuilder()
                                        .setAmount(reply2.getRetentionPolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel3 = channels.lookup("acme.accounting.v1.AccrualApprovalService");
        var serviceName3 = "acme.accounting.v1" + "." + "AccrualApprovalService";
        var methodName3 = "Rec" + "allAccrual";
        var descriptor3 = MethodDescriptor.<RecallAccrualRequest, RecallAccrualResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(RecallAccrualRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(RecallAccrualResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeAccrualWorkflow", request.getAccrual().getAccrualId());
        return ResumeAccrualWorkflowResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getRfq().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(request.getAccrual().getCounterparty())
                        .setIssuedAt(reply3.getAccrual().getIssuedAt())
                        .build())
                .setOutcome(ResumeAccrualWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns an accrual.
     */
    public InspectAccrualWorkflowResponse inspectAccrualWorkflow(InspectAccrualWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectAccrualWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Accrual", request.getAccrualId(), "InspectAccrualWorkflow");
        return InspectAccrualWorkflowResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .build())
                .build();
    }

    /**
     * Lists accruals a page at a time.
     */
    public ListAccrualWorkflowsResponse listAccrualWorkflows(ListAccrualWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListAccrualWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Accrual", request.getContext().getTraceId(), "ListAccrualWorkflows");

        var outbound0 = (EstimateOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateOrderRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setOrder(Order.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = ordersOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateOrder(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListAccrualWorkflows", request.getPage().getPageToken());
        return ListAccrualWorkflowsResponse.newBuilder()
                .addAccruals(Accrual.newBuilder()
                        .setDueAt(reply0.getOrder().getDueAt())
                        .build())
                .build();
    }

    /**
     * Signals an accrual.
     */
    public SignalAccrualWorkflowResponse signalAccrualWorkflow(SignalAccrualWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalAccrualWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAccrual(request.getContext().getTenantId(), request.getAccrual());

        var outbound0 = ApproveSupplierContractRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierContract(SupplierContract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getAccrual().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var pending0 = suppliersSupplierContractApprovalServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).approveSupplierContract(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = AdvanceExpenseReportWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getSupplierContract().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply1 = expensesExpenseReportWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceExpenseReportWorkflow(outbound1);

        var outbound2 = RecalculateAllocationsRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getExpenseReport().getCounterparty().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecalculateAllocationsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getAccrual().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateAllocations(value)).orElseThrow();

        var outbound3 = BeanCopies.copy(CalculateAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getAccrual().getCounterparty().getDisplayName())
                                .build())
                        .setRoute(AllocationRouteAction.newBuilder()
                                .setWeight(reply2.getAllocation().getRoute().getWeight())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CalculateAllocationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(reply1.getExpenseReport().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build(), CalculateAllocationRequest.class);
        var reply3 = accountingAllocationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateAllocation(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getAccrual().getAccrualId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalAccrualWorkflow", request.getAccrual().getAccrualId());
        return SignalAccrualWorkflowResponse.newBuilder()
                .setAccrual(Accrual.newBuilder()
                        .setAccrualId(request.getAccrualId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getExpenseReport().getAudit().getCreatedAt())
                                .setCreatedBy(reply3.getAllocation().getAudit().getCreatedBy())
                                .build())
                        .setNotes(request.getAccrual().getNotes())
                        .setTotal(reply2.getBreakdown().getTotal())
                        .build())
                .setOutcome(SignalAccrualWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }
}
