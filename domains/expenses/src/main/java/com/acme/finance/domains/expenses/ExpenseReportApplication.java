package com.acme.finance.domains.expenses;

import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.BatchCreateExpenseReportsRequest;
import com.acme.expenses.v1.BatchCreateExpenseReportsResponse;
import com.acme.expenses.v1.BatchGetExpenseReportsRequest;
import com.acme.expenses.v1.BatchGetExpenseReportsResponse;
import com.acme.expenses.v1.CalculateExpensePolicyRequest;
import com.acme.expenses.v1.CalculateExpensePolicyResponse;
import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.CheckExpenseReportEligibilityRequest;
import com.acme.expenses.v1.CheckExpenseReportEligibilityResponse;
import com.acme.expenses.v1.CloneExpenseReportRequest;
import com.acme.expenses.v1.CloneExpenseReportResponse;
import com.acme.expenses.v1.CompareMileageClaimOptionsRequest;
import com.acme.expenses.v1.CompareMileageClaimOptionsResponse;
import com.acme.expenses.v1.CountExpenseReportsRequest;
import com.acme.expenses.v1.CountExpenseReportsResponse;
import com.acme.expenses.v1.CreateExpenseReportRequest;
import com.acme.expenses.v1.CreateExpenseReportResponse;
import com.acme.expenses.v1.DeleteExpenseReportRequest;
import com.acme.expenses.v1.DeleteExpenseReportResponse;
import com.acme.expenses.v1.EstimateExpensePolicyRequest;
import com.acme.expenses.v1.EstimateExpensePolicyResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportQueryServiceGrpc;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.expenses.v1.GetExpenseReportRequest;
import com.acme.expenses.v1.GetExpenseReportResponse;
import com.acme.expenses.v1.ListExpenseReportsRequest;
import com.acme.expenses.v1.ListExpenseReportsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimCalculationServiceGrpc;
import com.acme.expenses.v1.MileageClaimLine;
import com.acme.expenses.v1.MileageClaimLineTax;
import com.acme.expenses.v1.SearchExpenseReportsRequest;
import com.acme.expenses.v1.SearchExpenseReportsResponse;
import com.acme.expenses.v1.UpdateExpenseReportRequest;
import com.acme.expenses.v1.UpdateExpenseReportResponse;
import com.acme.expenses.v1.WatchExpenseReportsRequest;
import com.acme.expenses.v1.WatchExpenseReportsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ScoreUnderwritingRequest;
import com.acme.insurance.v1.ScoreUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.lending.v1.EstimateRepaymentRequest;
import com.acme.lending.v1.EstimateRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.procurement.v1.CompareRfqOptionsRequest;
import com.acme.procurement.v1.CompareRfqOptionsResponse;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.suppliers.v1.CountSupplierContractsRequest;
import com.acme.suppliers.v1.CountSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Manages expense reports: creates, reads, changes, and lists them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class ExpenseReportApplication {
    private static final Logger log = LoggerFactory.getLogger(ExpenseReportApplication.class);

    private final FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking;

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final ExpensePolicyCalculationServiceGrpc.ExpensePolicyCalculationServiceBlockingStub expensesExpensePolicyCalculationServiceBlocking;

    private final ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking;

    private final ExpenseReportQueryServiceGrpc.ExpenseReportQueryServiceBlockingStub expensesExpenseReportQueryServiceBlocking;

    private final ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceBlockingStub expensesExpenseReportValidationServiceBlocking;

    private final MileageClaimCalculationServiceGrpc.MileageClaimCalculationServiceBlockingStub expensesMileageClaimCalculationServiceBlocking;

    private final UnderwritingValidationServiceGrpc.UnderwritingValidationServiceFutureStub insuranceUnderwritingValidationServiceFuture;

    private final RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking;

    private final ExpenseReportRequestMapper mapper;

    private final RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking;

    private final ExpensesRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    public ExpenseReportApplication(
            FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking,
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            ExpensePolicyCalculationServiceGrpc.ExpensePolicyCalculationServiceBlockingStub expensesExpensePolicyCalculationServiceBlocking,
            ExpenseReportCalculationServiceGrpc.ExpenseReportCalculationServiceBlockingStub expensesExpenseReportCalculationServiceBlocking,
            ExpenseReportQueryServiceGrpc.ExpenseReportQueryServiceBlockingStub expensesExpenseReportQueryServiceBlocking,
            ExpenseReportValidationServiceGrpc.ExpenseReportValidationServiceBlockingStub expensesExpenseReportValidationServiceBlocking,
            MileageClaimCalculationServiceGrpc.MileageClaimCalculationServiceBlockingStub expensesMileageClaimCalculationServiceBlocking,
            UnderwritingValidationServiceGrpc.UnderwritingValidationServiceFutureStub insuranceUnderwritingValidationServiceFuture,
            RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking,
            ExpenseReportRequestMapper mapper,
            RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking,
            ExpensesRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking) {
        this.assetsFixedAssetCalculationServiceBlocking = assetsFixedAssetCalculationServiceBlocking;
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.expensesExpensePolicyCalculationServiceBlocking = expensesExpensePolicyCalculationServiceBlocking;
        this.expensesExpenseReportCalculationServiceBlocking = expensesExpenseReportCalculationServiceBlocking;
        this.expensesExpenseReportQueryServiceBlocking = expensesExpenseReportQueryServiceBlocking;
        this.expensesExpenseReportValidationServiceBlocking = expensesExpenseReportValidationServiceBlocking;
        this.expensesMileageClaimCalculationServiceBlocking = expensesMileageClaimCalculationServiceBlocking;
        this.insuranceUnderwritingValidationServiceFuture = insuranceUnderwritingValidationServiceFuture;
        this.lendingRepaymentCalculationServiceBlocking = lendingRepaymentCalculationServiceBlocking;
        this.mapper = mapper;
        this.procurementRfqCalculationServiceBlocking = procurementRfqCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
    }

    /**
     * Creates an expense report.
     */
    public CreateExpenseReportResponse createExpenseReport(CreateExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = CountExpenseReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountExpenseReportsRequest.Filter.newBuilder()
                        .setRegion(request.getExpenseReport().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> expensesExpenseReportQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countExpenseReports(outbound0), rpcExecutor);

        var outbound1 = CalculateExpensePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getExpenseReport().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> expensesExpensePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateExpensePolicy(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = mapper.createExpenseReportStage2(request, reply1);
        var reply2 = expensesMileageClaimCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareMileageClaimOptions(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateExpenseReport", request.getExpenseReport().getExpenseReportId());
        return CreateExpenseReportResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getExpensePolicy().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply2.getMileageClaim().getCounterparty().getDisplayName())
                                .build())
                        .setDueAt(request.getExpenseReport().getDueAt())
                        .setStatus(request.getExpenseReport().getStatus())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns an expense report.
     */
    public GetExpenseReportResponse getExpenseReport(GetExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getExpenseReportId(), "GetExpenseReport");
        return GetExpenseReportResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setExpenseReportId(request.getExpenseReportId())
                        .build())
                .build();
    }

    /**
     * Changes an expense report.
     */
    public UpdateExpenseReportResponse updateExpenseReport(UpdateExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdateExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = ScoreUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getExpenseReport().getCounterparty().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getExpenseReport().getAudit().getCreatedAt())
                                .setCreatedBy(request.getExpenseReport().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getExpenseReport().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var pending0 = insuranceUnderwritingValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).scoreUnderwriting(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = mapper.updateExpenseReportStage1(request, reply0);
        var reply1 = searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSearchIndexes(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getCounterparty().getCustomerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdateExpenseReport", request.getExpenseReport().getCounterparty().getCustomerId());
        return UpdateExpenseReportResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setDueAt(request.getExpenseReport().getDueAt())
                        .setNumber(request.getExpenseReport().getNumber())
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Deletes an expense report.
     */
    public DeleteExpenseReportResponse deleteExpenseReport(DeleteExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getExpenseReportId(), "DeleteExpenseReport");

        var outbound0 = StreamAuditEventExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditEventExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getExpenseReportId())
                        .build())
                .build();
        var stream0 = auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditEventExport(outbound0);
        var reply0 = StreamAuditEventExportResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = (CountSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSupplierContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getExpenseReportId())
                        .build())
                .build()).toBuilder().build();
        var reply1 = suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countSupplierContracts(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getReason()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteExpenseReport", request.getReason());
        return DeleteExpenseReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists expense reports a page at a time.
     */
    public ListExpenseReportsResponse listExpenseReports(ListExpenseReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListExpenseReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getContext().getTraceId(), "ListExpenseReports");

        var outbound0 = BeanCopies.copy(CheckExpenseReportEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckExpenseReportEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build(), CheckExpenseReportEligibilityRequest.class);
        var reply0 = expensesExpenseReportValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkExpenseReportEligibility(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListExpenseReports", request.getPage().getPageToken());
        return ListExpenseReportsResponse.newBuilder()
                .addExpenseReports(ExpenseReport.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getExpenseReport().getAudit().getUpdatedBy())
                                .build())
                        .setStatus(reply0.getExpenseReport().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns several expense reports at once.
     */
    public BatchGetExpenseReportsResponse batchGetExpenseReports(BatchGetExpenseReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetExpenseReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getContext().getTraceId(), "BatchGetExpenseReports");

        var outbound0 = EstimateRepaymentRequest.newBuilder()
                .putAllAnnotations(request.getContext().getBaggageMap())
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getContext().getActor().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = lendingRepaymentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateRepayment(outbound0);
        return BatchGetExpenseReportsResponse.newBuilder()
                .addExpenseReports(ExpenseReport.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getRepayment().getLocation().getLocality())
                                .build())
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .build();
    }

    /**
     * Lists expense reports a page at a time.
     */
    public SearchExpenseReportsResponse searchExpenseReports(SearchExpenseReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchExpenseReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getContext().getTraceId(), "SearchExpenseReports");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchExpenseReports", request.getPage().getPageToken());
        return SearchExpenseReportsResponse.getDefaultInstance();
    }

    /**
     * Streams the changes of expense reports as they happen.
     */
    public WatchExpenseReportsResponse watchExpenseReports(WatchExpenseReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchExpenseReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ExpenseReport", request.getContext().getTraceId(), "WatchExpenseReports");

        var builder0 = CalculateExpenseReportRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(CalculateExpenseReportRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build());
        builder0.setExpenseReport(ExpenseReport.newBuilder()
                .setCounterparty(CustomerRef.newBuilder()
                        .setEmail(request.getSubscriber().getEmail())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = expensesExpenseReportCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateExpenseReport(outbound0);

        var outbound1 = mapper.watchExpenseReportsStage1(request, reply0);
        var reply1 = assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFixedAsset(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchExpenseReports", request.getFilter().getQuery());
        return WatchExpenseReportsResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply1.getFixedAsset().getAddress().getPostalCode())
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Takes a stream of expense reports and answers once at the end.
     */
    public BatchCreateExpenseReportsResponse batchCreateExpenseReports(BatchCreateExpenseReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreateExpenseReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveExpenseReport(request.getContext().getTenantId(), request.getRecord());
        }

        var outbound0 = CompareRfqOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareRfqOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(switch (request.getChunkCase()) {
                                    case RECORD -> request.getRecord().getBillingAddress().getCountryCode();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = procurementRfqCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareRfqOptions(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreateExpenseReports", request.getHeader().getSource());
        return BatchCreateExpenseReportsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Creates an expense report.
     */
    public CloneExpenseReportResponse cloneExpenseReport(CloneExpenseReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CloneExpenseReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveExpenseReport(request.getContext().getTenantId(), request.getExpenseReport());

        var outbound0 = RpcSupport.unpack(Any.pack(EstimateExpensePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getExpenseReport().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build()), EstimateExpensePolicyRequest.class);
        var reply0 = expensesExpensePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateExpensePolicy(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getExpenseReport().getExpenseReportId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CloneExpenseReport", request.getExpenseReport().getExpenseReportId());
        return CloneExpenseReportResponse.newBuilder()
                .setExpenseReport(ExpenseReport.newBuilder()
                        .setDueAt(request.getExpenseReport().getDueAt())
                        .setNotes(request.getExpenseReport().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
