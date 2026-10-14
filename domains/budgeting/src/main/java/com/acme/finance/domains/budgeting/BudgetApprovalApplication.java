package com.acme.finance.domains.budgeting;

import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.BrowseAccessPoliciesRequest;
import com.acme.authorization.v1.BrowseAccessPoliciesResponse;
import com.acme.banking.v1.CommentStatementApprovalRequest;
import com.acme.banking.v1.CommentStatementApprovalResponse;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementApprovalServiceGrpc;
import com.acme.banking.v1.StatementCardSettlement;
import com.acme.budgeting.v1.ApproveBudgetRequest;
import com.acme.budgeting.v1.ApproveBudgetResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.CommentBudgetApprovalRequest;
import com.acme.budgeting.v1.CommentBudgetApprovalResponse;
import com.acme.budgeting.v1.DelegateBudgetApprovalRequest;
import com.acme.budgeting.v1.DelegateBudgetApprovalResponse;
import com.acme.budgeting.v1.EscalateBudgetRequest;
import com.acme.budgeting.v1.EscalateBudgetResponse;
import com.acme.budgeting.v1.ForecastLifecycleServiceGrpc;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.GetBudgetApprovalStatusRequest;
import com.acme.budgeting.v1.GetBudgetApprovalStatusResponse;
import com.acme.budgeting.v1.GetForecastLifecycleRequest;
import com.acme.budgeting.v1.GetForecastLifecycleResponse;
import com.acme.budgeting.v1.ListPendingBudgetsRequest;
import com.acme.budgeting.v1.ListPendingBudgetsResponse;
import com.acme.budgeting.v1.ListVarianceImportsRequest;
import com.acme.budgeting.v1.ListVarianceImportsResponse;
import com.acme.budgeting.v1.PreviewBudgetRequest;
import com.acme.budgeting.v1.PreviewBudgetResponse;
import com.acme.budgeting.v1.QuoteVarianceRequest;
import com.acme.budgeting.v1.QuoteVarianceResponse;
import com.acme.budgeting.v1.RecallBudgetRequest;
import com.acme.budgeting.v1.RecallBudgetResponse;
import com.acme.budgeting.v1.RejectBudgetRequest;
import com.acme.budgeting.v1.RejectBudgetResponse;
import com.acme.budgeting.v1.ScheduleVarianceExpiryRequest;
import com.acme.budgeting.v1.ScheduleVarianceExpiryResponse;
import com.acme.budgeting.v1.SubmitBudgetRequest;
import com.acme.budgeting.v1.SubmitBudgetResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceCalculationServiceGrpc;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.budgeting.v1.VarianceLifecycleServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.compliance.v1.AdvanceAttestationWorkflowRequest;
import com.acme.compliance.v1.AdvanceAttestationWorkflowResponse;
import com.acme.compliance.v1.AttestationWorkflowServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.fulfillment.v1.CreateFulfillmentRequest;
import com.acme.fulfillment.v1.CreateFulfillmentResponse;
import com.acme.fulfillment.v1.Fulfillment;
import com.acme.fulfillment.v1.FulfillmentBankSettlement;
import com.acme.fulfillment.v1.FulfillmentServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CompareJournalEntryVersionsRequest;
import com.acme.ledger.v1.CompareJournalEntryVersionsResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryCardSettlement;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.JournalEntryLine;
import com.acme.ledger.v1.JournalEntryLineTax;
import com.acme.notifications.v1.CreateNotificationRequest;
import com.acme.notifications.v1.CreateNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.acme.search.v1.NormalizeSearchIndexRequest;
import com.acme.search.v1.NormalizeSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.suppliers.v1.CountSupplierContractsRequest;
import com.acme.suppliers.v1.CountSupplierContractsResponse;
import com.acme.suppliers.v1.CountSuppliersRequest;
import com.acme.suppliers.v1.CountSuppliersResponse;
import com.acme.suppliers.v1.LintSupplierRatingsRequest;
import com.acme.suppliers.v1.LintSupplierRatingsResponse;
import com.acme.suppliers.v1.SuggestSuppliersRequest;
import com.acme.suppliers.v1.SuggestSuppliersResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingCardSettlement;
import com.acme.suppliers.v1.SupplierRatingLine;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.venues.v1.SearchVenuesRequest;
import com.acme.venues.v1.SearchVenuesResponse;
import com.acme.venues.v1.VenueServiceGrpc;
import java.util.ArrayList;
import java.util.List;
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
 * Routes budgets through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class BudgetApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(BudgetApprovalApplication.class);

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final StatementApprovalServiceGrpc.StatementApprovalServiceBlockingStub bankingStatementApprovalServiceBlocking;

    private final BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking;

    private final ForecastLifecycleServiceGrpc.ForecastLifecycleServiceBlockingStub budgetingForecastLifecycleServiceBlocking;

    private final VarianceCalculationServiceGrpc.VarianceCalculationServiceBlockingStub budgetingVarianceCalculationServiceBlocking;

    private final VarianceImportServiceGrpc.VarianceImportServiceBlockingStub budgetingVarianceImportServiceBlocking;

    private final VarianceLifecycleServiceGrpc.VarianceLifecycleServiceBlockingStub budgetingVarianceLifecycleServiceBlocking;

    private final AttestationWorkflowServiceGrpc.AttestationWorkflowServiceBlockingStub complianceAttestationWorkflowServiceBlocking;

    private final FulfillmentServiceGrpc.FulfillmentServiceBlockingStub fulfillmentFulfillmentServiceBlocking;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking;

    private final BudgetApprovalRequestMapper mapper;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final BudgetingRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    private final VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking;

    public BudgetApprovalApplication(
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            StatementApprovalServiceGrpc.StatementApprovalServiceBlockingStub bankingStatementApprovalServiceBlocking,
            BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking,
            ForecastLifecycleServiceGrpc.ForecastLifecycleServiceBlockingStub budgetingForecastLifecycleServiceBlocking,
            VarianceCalculationServiceGrpc.VarianceCalculationServiceBlockingStub budgetingVarianceCalculationServiceBlocking,
            VarianceImportServiceGrpc.VarianceImportServiceBlockingStub budgetingVarianceImportServiceBlocking,
            VarianceLifecycleServiceGrpc.VarianceLifecycleServiceBlockingStub budgetingVarianceLifecycleServiceBlocking,
            AttestationWorkflowServiceGrpc.AttestationWorkflowServiceBlockingStub complianceAttestationWorkflowServiceBlocking,
            FulfillmentServiceGrpc.FulfillmentServiceBlockingStub fulfillmentFulfillmentServiceBlocking,
            JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking,
            BudgetApprovalRequestMapper mapper,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            BudgetingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking,
            VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking) {
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.bankingStatementApprovalServiceBlocking = bankingStatementApprovalServiceBlocking;
        this.budgetingBudgetValidationServiceBlocking = budgetingBudgetValidationServiceBlocking;
        this.budgetingForecastLifecycleServiceBlocking = budgetingForecastLifecycleServiceBlocking;
        this.budgetingVarianceCalculationServiceBlocking = budgetingVarianceCalculationServiceBlocking;
        this.budgetingVarianceImportServiceBlocking = budgetingVarianceImportServiceBlocking;
        this.budgetingVarianceLifecycleServiceBlocking = budgetingVarianceLifecycleServiceBlocking;
        this.complianceAttestationWorkflowServiceBlocking = complianceAttestationWorkflowServiceBlocking;
        this.fulfillmentFulfillmentServiceBlocking = fulfillmentFulfillmentServiceBlocking;
        this.ledgerJournalEntryHistoryServiceBlocking = ledgerJournalEntryHistoryServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexValidationServiceBlocking = searchSearchIndexValidationServiceBlocking;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
        this.venuesVenueServiceBlocking = venuesVenueServiceBlocking;
    }

    /**
     * Submits a budget.
     */
    public SubmitBudgetResponse submitBudget(SubmitBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = CountSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBudget().getAudit().getCreatedAt())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .setOwnerId(request.getBudget().getHolder().getForecastId())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countSuppliers(value));

        var futures1 = request.getBudget().getLimitsList().stream()
                .map(element -> CompareJournalEntryVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareJournalEntryVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setCard(JournalEntryCardSettlement.newBuilder()
                                .setHolderName(request.getBudget().getCard().getHolderName())
                                .build())
                        .addLines(JournalEntryLine.newBuilder()
                                .setTax(JournalEntryLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareJournalEntryVersions(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? CompareJournalEntryVersionsResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = mapper.submitBudgetStage2(request);
        var reply2 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createNotification(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitBudget", request.getBudget().getBudgetId());
        return SubmitBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAvailable(request.getBudget().getAvailable())
                        .setBudgetId(request.getBudgetId())
                        .build())
                .setOutcome(SubmitBudgetResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Approves a budget.
     */
    public ApproveBudgetResponse approveBudget(ApproveBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = (GetForecastLifecycleRequest.newBuilder()
                .setContext(request.getContext())
                .setForecastId(request.getBudget().getHolder().getForecastId())
                .build()).toBuilder().build();
        var reply0 = budgetingForecastLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getForecastLifecycle(outbound0);

        var outbound1 = StreamAuditEventExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditEventExportRequest.Filter.newBuilder()
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .setRegion(reply0.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventExport(outbound1);
        var reply1 = StreamAuditEventExportResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveBudget", request.getBudget().getBudgetId());
        return ApproveBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getForecast().getAudit().getUpdatedAt())
                                .build())
                        .setBudgetId(request.getBudgetId())
                        .setHolder(ForecastRef.newBuilder()
                                .setNumber(reply0.getForecast().getNumber())
                                .build())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .build())
                .build();
    }

    /**
     * Rejects a budget.
     */
    public RejectBudgetResponse rejectBudget(RejectBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = CreateFulfillmentRequest.newBuilder()
                .setContext(request.getContext())
                .setFulfillment(Fulfillment.newBuilder()
                        .setBank(FulfillmentBankSettlement.newBuilder()
                                .setAccountHolder(switch (request.getBudget().getFundingCase()) {
                                    case BANK -> request.getBudget().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = fulfillmentFulfillmentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createFulfillment(outbound0);

        var outbound1 = PreviewBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getFulfillment().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewBudgetRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getBudget().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewBudget(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectBudget", request.getBudget().getBudgetId());
        return RejectBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setHolder(request.getBudget().getHolder())
                        .build())
                .setOutcome(RejectBudgetResponse.Outcome.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Escalates a budget.
     */
    public EscalateBudgetResponse escalateBudget(EscalateBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var results0 = new ArrayList<LintSupplierRatingsResponse>();
        for (var element : request.getBudget().getLimitsList()) {
            LintSupplierRatingsRequest itemRequest = LintSupplierRatingsRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getBudget().getAudit().getEtag())
                                .setUpdatedBy(request.getBudget().getAudit().getUpdatedBy())
                                .build())
                        .setCard(SupplierRatingCardSettlement.newBuilder()
                                .setHolderName(request.getBudget().getCard().getHolderName())
                                .build())
                        .addLines(SupplierRatingLine.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .build();
            results0.add(suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintSupplierRatings(itemRequest));
        }
        var reply0 = results0.isEmpty() ? LintSupplierRatingsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ListVarianceImportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListVarianceImportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBudget().getAudit().getCreatedAt())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .setOwnerId(reply0.getSupplierRating().getCounterparty().getCustomerId())
                        .setRegion(reply0.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> budgetingVarianceImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listVarianceImports(value)).orElseThrow();

        var futures2 = request.getBudget().getLimitsList().stream()
                .map(element -> NormalizeSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeSearchIndexRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getBudget().getContact().getEmail())
                                .build())
                        .build())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getBudget().getAudit().getUpdatedAt())
                                .build())
                        .setName(element.getName())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> searchSearchIndexValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeSearchIndex(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? NormalizeSearchIndexResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateBudget", request.getBudget().getBudgetId());
        return EscalateBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .build())
                .setOutcome(EscalateBudgetResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists budgets a page at a time.
     */
    public ListPendingBudgetsResponse listPendingBudgets(ListPendingBudgetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingBudgets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getContext().getTraceId(), "ListPendingBudgets");

        var outbound0 = BrowseAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseAccessPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageToken(request.getPage().getPageToken())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseAccessPolicies(outbound0), rpcExecutor);

        var outbound1 = ScheduleVarianceExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> budgetingVarianceLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleVarianceExpiry(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingBudgets", request.getFilter().getQuery());
        return ListPendingBudgetsResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply0.getPageInfo().getNextPageToken())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Delegates a budget.
     */
    public DelegateBudgetApprovalResponse delegateBudgetApproval(DelegateBudgetApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateBudgetApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = CommentStatementApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentStatementApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .setStatement(Statement.newBuilder()
                        .setCard(StatementCardSettlement.newBuilder()
                                .setHolderName(switch (request.getBudget().getFundingCase()) {
                                    case CARD -> request.getBudget().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = bankingStatementApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentStatementApproval(outbound0);

        var outbound1 = QuoteVarianceRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getBudget().getContact().getName())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> budgetingVarianceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteVariance(value));
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateBudgetApproval", request.getBudget().getBudgetId());
        return DelegateBudgetApprovalResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getVariance().getAudit().getUpdatedBy())
                                .build())
                        .setBudgetId(request.getBudgetId())
                        .setHolder(request.getBudget().getHolder())
                        .build())
                .setOutcome(DelegateBudgetApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Recalls a budget.
     */
    public RecallBudgetResponse recallBudget(RecallBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = SearchVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchVenuesRequest.Filter.newBuilder()
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchVenues(outbound0), rpcExecutor);

        var outbound1 = AdvanceAttestationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AdvanceAttestationWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setPhone(request.getBudget().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> complianceAttestationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceAttestationWorkflow(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallBudget", request.getBudget().getBudgetId());
        return RecallBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(reply1.getAttestation().getCounterparty().getDisplayName())
                                .build())
                        .setOpenedAt(request.getBudget().getOpenedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns a budget.
     */
    public GetBudgetApprovalStatusResponse getBudgetApprovalStatus(GetBudgetApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetBudgetApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "GetBudgetApprovalStatus");
        return GetBudgetApprovalStatusResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .build())
                .build();
    }

    /**
     * Comments a budget.
     */
    public CommentBudgetApprovalResponse commentBudgetApproval(CommentBudgetApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentBudgetApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = CountSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSupplierContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBudget().getAudit().getCreatedAt())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .setOwnerId(request.getDetails().getApprover().getUserId())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countSupplierContracts(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = BeanCopies.copy(SuggestSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBudget().getOpenedAt())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .setOwnerId(request.getBudget().getBudgetId())
                        .build())
                .build(), SuggestSuppliersRequest.class);
        var reply1 = suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestSuppliers(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getHolder().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentBudgetApproval", request.getBudget().getHolder().getForecastId());
        return CommentBudgetApprovalResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setOpenedAt(request.getBudget().getOpenedAt())
                        .build())
                .setOutcome(CommentBudgetApprovalResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }
}
