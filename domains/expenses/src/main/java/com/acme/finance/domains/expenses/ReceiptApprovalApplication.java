package com.acme.finance.domains.expenses;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.CheckAccessPolicyEligibilityRequest;
import com.acme.authorization.v1.CheckAccessPolicyEligibilityResponse;
import com.acme.banking.v1.ResumeStatementWorkflowRequest;
import com.acme.banking.v1.ResumeStatementWorkflowResponse;
import com.acme.banking.v1.StandingOrderRef;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementCardSettlement;
import com.acme.banking.v1.StatementWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CountCustomersRequest;
import com.acme.customers.v1.CountCustomersResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.ExportCustomersRequest;
import com.acme.customers.v1.ExportCustomersResponse;
import com.acme.customers.v1.LintCustomerSegmentsRequest;
import com.acme.customers.v1.LintCustomerSegmentsResponse;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.expenses.v1.ApproveExpenseReportRequest;
import com.acme.expenses.v1.ApproveExpenseReportResponse;
import com.acme.expenses.v1.ApproveReceiptRequest;
import com.acme.expenses.v1.ApproveReceiptResponse;
import com.acme.expenses.v1.CheckReceiptEligibilityRequest;
import com.acme.expenses.v1.CheckReceiptEligibilityResponse;
import com.acme.expenses.v1.CommentReceiptApprovalRequest;
import com.acme.expenses.v1.CommentReceiptApprovalResponse;
import com.acme.expenses.v1.DelegateReceiptApprovalRequest;
import com.acme.expenses.v1.DelegateReceiptApprovalResponse;
import com.acme.expenses.v1.EscalateReceiptRequest;
import com.acme.expenses.v1.EscalateReceiptResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyLifecycleServiceGrpc;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportApprovalServiceGrpc;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpenseReportQueryServiceGrpc;
import com.acme.expenses.v1.GetReceiptApprovalStatusRequest;
import com.acme.expenses.v1.GetReceiptApprovalStatusResponse;
import com.acme.expenses.v1.LintReceiptsRequest;
import com.acme.expenses.v1.LintReceiptsResponse;
import com.acme.expenses.v1.ListExpensePolicyConflictsRequest;
import com.acme.expenses.v1.ListExpensePolicyConflictsResponse;
import com.acme.expenses.v1.ListPendingReceiptsRequest;
import com.acme.expenses.v1.ListPendingReceiptsResponse;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.PreviewReceiptRequest;
import com.acme.expenses.v1.PreviewReceiptResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptAttachment;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ReconcileExpensePoliciesRequest;
import com.acme.expenses.v1.ReconcileExpensePoliciesResponse;
import com.acme.expenses.v1.RejectReceiptRequest;
import com.acme.expenses.v1.RejectReceiptResponse;
import com.acme.expenses.v1.ResolveExpenseReportsRequest;
import com.acme.expenses.v1.ResolveExpenseReportsResponse;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryRequest;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryResponse;
import com.acme.expenses.v1.SubmitReceiptRequest;
import com.acme.expenses.v1.SubmitReceiptResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.keymanagement.v1.BrowseCryptoKeysRequest;
import com.acme.keymanagement.v1.BrowseCryptoKeysResponse;
import com.acme.keymanagement.v1.CryptoKeyQueryServiceGrpc;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanBankFunding;
import com.acme.lending.v1.SubmitLoanRequest;
import com.acme.lending.v1.SubmitLoanResponse;
import com.acme.sponsorships.v1.ListSponsorsRequest;
import com.acme.sponsorships.v1.ListSponsorsResponse;
import com.acme.sponsorships.v1.SponsorServiceGrpc;
import com.google.protobuf.Any;
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
 * Routes receipts through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class ReceiptApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(ReceiptApprovalApplication.class);

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final StatementWorkflowServiceGrpc.StatementWorkflowServiceBlockingStub bankingStatementWorkflowServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final ExpensePolicyLifecycleServiceGrpc.ExpensePolicyLifecycleServiceBlockingStub expensesExpensePolicyLifecycleServiceBlocking;

    private final ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceBlockingStub expensesExpensePolicySyncServiceBlocking;

    private final ExpenseReportApprovalServiceGrpc.ExpenseReportApprovalServiceBlockingStub expensesExpenseReportApprovalServiceBlocking;

    private final ExpenseReportQueryServiceGrpc.ExpenseReportQueryServiceBlockingStub expensesExpenseReportQueryServiceBlocking;

    private final ReceiptValidationServiceGrpc.ReceiptValidationServiceBlockingStub expensesReceiptValidationServiceBlocking;

    private final CryptoKeyQueryServiceGrpc.CryptoKeyQueryServiceBlockingStub keymanagementCryptoKeyQueryServiceBlocking;

    private final LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking;

    private final ReceiptApprovalRequestMapper mapper;

    private final ExpensesRepository repository;

    private final Executor rpcExecutor;

    private final SponsorServiceGrpc.SponsorServiceBlockingStub sponsorshipsSponsorServiceBlocking;

    public ReceiptApprovalApplication(
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            StatementWorkflowServiceGrpc.StatementWorkflowServiceBlockingStub bankingStatementWorkflowServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            ExpensePolicyLifecycleServiceGrpc.ExpensePolicyLifecycleServiceBlockingStub expensesExpensePolicyLifecycleServiceBlocking,
            ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceBlockingStub expensesExpensePolicySyncServiceBlocking,
            ExpenseReportApprovalServiceGrpc.ExpenseReportApprovalServiceBlockingStub expensesExpenseReportApprovalServiceBlocking,
            ExpenseReportQueryServiceGrpc.ExpenseReportQueryServiceBlockingStub expensesExpenseReportQueryServiceBlocking,
            ReceiptValidationServiceGrpc.ReceiptValidationServiceBlockingStub expensesReceiptValidationServiceBlocking,
            CryptoKeyQueryServiceGrpc.CryptoKeyQueryServiceBlockingStub keymanagementCryptoKeyQueryServiceBlocking,
            LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking,
            ReceiptApprovalRequestMapper mapper,
            ExpensesRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SponsorServiceGrpc.SponsorServiceBlockingStub sponsorshipsSponsorServiceBlocking) {
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.bankingStatementWorkflowServiceBlocking = bankingStatementWorkflowServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.expensesExpensePolicyLifecycleServiceBlocking = expensesExpensePolicyLifecycleServiceBlocking;
        this.expensesExpensePolicySyncServiceBlocking = expensesExpensePolicySyncServiceBlocking;
        this.expensesExpenseReportApprovalServiceBlocking = expensesExpenseReportApprovalServiceBlocking;
        this.expensesExpenseReportQueryServiceBlocking = expensesExpenseReportQueryServiceBlocking;
        this.expensesReceiptValidationServiceBlocking = expensesReceiptValidationServiceBlocking;
        this.keymanagementCryptoKeyQueryServiceBlocking = keymanagementCryptoKeyQueryServiceBlocking;
        this.lendingLoanApprovalServiceBlocking = lendingLoanApprovalServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.sponsorshipsSponsorServiceBlocking = sponsorshipsSponsorServiceBlocking;
    }

    /**
     * Submits a receipt.
     */
    public SubmitReceiptResponse submitReceipt(SubmitReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = ResolveExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        RpcPort<ResolveExpenseReportsRequest, ResolveExpenseReportsResponse> port0 = RpcPort.adapt(expensesExpenseReportQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::resolveExpenseReports);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = ListSponsorsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListSponsorsRequest.Filter.newBuilder()
                        .setRegion(request.getReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> sponsorshipsSponsorServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listSponsors(value));

        var outbound2 = RpcSupport.unpack(Any.pack(LintCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getReceipt().getAudit().getEtag())
                                .setUpdatedBy(request.getReceipt().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build()), LintCustomerSegmentsRequest.class);
        var reply2 = customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintCustomerSegments(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitReceipt", request.getReceipt().getReceiptId());
        return SubmitReceiptResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getCustomerSegment().getAudit().getUpdatedBy())
                                .build())
                        .setNotes(request.getReceipt().getNotes())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Approves a receipt.
     */
    public ApproveReceiptResponse approveReceipt(ApproveReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        List<CheckReceiptEligibilityResponse> results0 = request.getReceipt().getAttachmentsList().stream()
                .map(element -> CheckReceiptEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .addAttachments(ReceiptAttachment.newBuilder()
                                .setFileName(element.getFileName())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(expensesReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkReceiptEligibility(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? CheckReceiptEligibilityResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ExportCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportCustomersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getReceipt().getAudit().getUpdatedAt())
                        .setRegion(request.getReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportCustomers(outbound1);
        var reply1 = ExportCustomersResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = PreviewReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getReceipt().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        Function<PreviewReceiptRequest, PreviewReceiptResponse> operation2 = expensesReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewReceipt;
        var reply2 = operation2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveReceipt", request.getReceipt().getReceiptId());
        return ApproveReceiptResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setNotes(request.getReceipt().getNotes())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Rejects a receipt.
     */
    public RejectReceiptResponse rejectReceipt(RejectReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = (ReconcileExpensePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileExpensePoliciesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getReceipt().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = expensesExpensePolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileExpensePolicies(outbound0);

        var futures1 = request.getReceipt().getLinesList().stream()
                .map(element -> CountCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountCustomersRequest.Filter.newBuilder()
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getReceipt().getBillingAddress().getRegion())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countCustomers(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? CountCustomersResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectReceipt", request.getReceipt().getReceiptId());
        return RejectReceiptResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .addLines(ReceiptLine.newBuilder()
                                .setDescription(reply0.getExpensePolicy().getDescription())
                                .build())
                        .setNotes(request.getReceipt().getNotes())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Escalates a receipt.
     */
    public EscalateReceiptResponse escalateReceipt(EscalateReceiptRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateReceipt")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = BrowseCryptoKeysRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseCryptoKeysRequest.Filter.newBuilder()
                        .setRegion(request.getReceipt().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> keymanagementCryptoKeyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseCryptoKeys(value)).orElseThrow();

        var outbound1 = ResumeStatementWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setStatement(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getReceipt().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getReceipt().getBillingAddress().getRecipient())
                                .build())
                        .setCard(StatementCardSettlement.newBuilder()
                                .setToken(switch (request.getReceipt().getSettlementCase()) {
                                    case CARD -> request.getReceipt().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(StandingOrderRef.newBuilder()
                                .setDisplayName(request.getReceipt().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = bankingStatementWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeStatementWorkflow(outbound1);

        var outbound2 = mapper.escalateReceiptStage2(request, reply1);
        var reply2 = lendingLoanApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitLoan(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateReceipt", request.getReceipt().getReceiptId());
        return EscalateReceiptResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setCounterparty(MileageClaimRef.newBuilder()
                                .setDisplayName(reply1.getStatement().getCounterparty().getDisplayName())
                                .build())
                        .putAllMetadata(request.getReceipt().getMetadataMap())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists receipts a page at a time.
     */
    public ListPendingReceiptsResponse listPendingReceipts(ListPendingReceiptsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingReceipts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getContext().getTraceId(), "ListPendingReceipts");

        var outbound0 = ResolveCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveCustomers(outbound0), rpcExecutor);

        var outbound1 = LintReceiptsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintReceiptsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> expensesReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintReceipts(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingReceipts", request.getPage().getPageToken());
        return ListPendingReceiptsResponse.newBuilder()
                .addReceipts(Receipt.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getReceipt().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Delegates a receipt.
     */
    public DelegateReceiptApprovalResponse delegateReceiptApproval(DelegateReceiptApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateReceiptApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var outbound0 = CheckAccessPolicyEligibilityRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getReceipt().getAudit().getCreatedBy())
                                .setUpdatedAt(request.getReceipt().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkAccessPolicyEligibility(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getReceiptId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateReceiptApproval", request.getReceipt().getReceiptId());
        return DelegateReceiptApprovalResponse.newBuilder()
                .setOutcome(DelegateReceiptApprovalResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setCounterparty(MileageClaimRef.newBuilder()
                                .setNumber(request.getReceipt().getNumber())
                                .build())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .build();
    }

    /**
     * Returns a receipt.
     */
    public GetReceiptApprovalStatusResponse getReceiptApprovalStatus(GetReceiptApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetReceiptApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Receipt", request.getReceiptId(), "GetReceiptApprovalStatus");

        var outbound0 = ScheduleExpensePolicyExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScheduleExpensePolicyExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> expensesExpensePolicyLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleExpensePolicyExpiry(value)).orElseThrow();
        return GetReceiptApprovalStatusResponse.newBuilder()
                .setReceipt(Receipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getExpensePolicy().getAudit().getEtag())
                                .build())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Comments a receipt.
     */
    public CommentReceiptApprovalResponse commentReceiptApproval(CommentReceiptApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentReceiptApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveReceipt(request.getContext().getTenantId(), request.getReceipt());

        var futures0 = request.getReceipt().getLinesList().stream()
                .map(element -> ApproveExpenseReportRequest.newBuilder()
                .setContext(request.getContext())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .addLines(ExpenseReportLine.newBuilder()
                                .setTax(ExpenseReportLineTax.newBuilder()
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> expensesExpenseReportApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveExpenseReport(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? ApproveExpenseReportResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = BrowseCryptoKeysRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseCryptoKeysRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getReceipt().getAudit().getCreatedAt())
                        .setRegion(reply0.getExpenseReport().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> keymanagementCryptoKeyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseCryptoKeys(outbound1));

        var outbound2 = JsonCodec.roundTrip(ListExpensePolicyConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListExpensePolicyConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getExpenseReport().getIssuedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(request.getReceipt().getReceiptId())
                        .setRegion(reply0.getExpenseReport().getBillingAddress().getRegion())
                        .build())
                .build(), ListExpensePolicyConflictsRequest.class);
        var reply2 = expensesExpensePolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listExpensePolicyConflicts(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getReceipt().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentReceiptApproval", request.getReceipt().getNumber());
        return CommentReceiptApprovalResponse.newBuilder()
                .setOutcome(CommentReceiptApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setBillingAddress(request.getReceipt().getBillingAddress())
                        .setReceiptId(request.getReceiptId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }
}
