package com.acme.finance.domains.invoicing;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferAttachment;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.ReconcileAssetTransfersRequest;
import com.acme.assets.v1.ReconcileAssetTransfersResponse;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.ListForecastReportsRequest;
import com.acme.budgeting.v1.ListForecastReportsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.QuoteConsentRecordRequest;
import com.acme.customers.v1.QuoteConsentRecordResponse;
import com.acme.customers.v1.QuoteHouseholdRequest;
import com.acme.customers.v1.QuoteHouseholdResponse;
import com.acme.customers.v1.RecalculateCustomersRequest;
import com.acme.customers.v1.RecalculateCustomersResponse;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.invoicing.v1.AdvanceEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.AdvanceEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.CalculateEInvoiceRequest;
import com.acme.invoicing.v1.CalculateEInvoiceResponse;
import com.acme.invoicing.v1.CancelEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.CancelEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.CommentEInvoiceApprovalRequest;
import com.acme.invoicing.v1.CommentEInvoiceApprovalResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceApprovalServiceGrpc;
import com.acme.invoicing.v1.EInvoiceAttachment;
import com.acme.invoicing.v1.EInvoiceCalculationServiceGrpc;
import com.acme.invoicing.v1.EInvoiceLifecycleServiceGrpc;
import com.acme.invoicing.v1.EInvoiceLine;
import com.acme.invoicing.v1.EInvoiceLineTax;
import com.acme.invoicing.v1.EInvoiceReportingServiceGrpc;
import com.acme.invoicing.v1.InspectEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.InspectEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.InvoiceBatchRef;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateApprovalServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateCardSettlement;
import com.acme.invoicing.v1.InvoiceTemplateWorkflowServiceGrpc;
import com.acme.invoicing.v1.ListEInvoiceWorkflowsRequest;
import com.acme.invoicing.v1.ListEInvoiceWorkflowsResponse;
import com.acme.invoicing.v1.ListPendingInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ListPendingInvoiceTemplatesResponse;
import com.acme.invoicing.v1.PauseEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.PauseEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.QuoteEInvoiceRequest;
import com.acme.invoicing.v1.QuoteEInvoiceResponse;
import com.acme.invoicing.v1.RejectInvoiceTemplateRequest;
import com.acme.invoicing.v1.RejectInvoiceTemplateResponse;
import com.acme.invoicing.v1.ResumeEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.ResumeEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.RetryEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.RetryEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.RetryInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.RetryInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.ShareEInvoiceReportRequest;
import com.acme.invoicing.v1.ShareEInvoiceReportResponse;
import com.acme.invoicing.v1.SignalEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.SignalEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.StartEInvoiceWorkflowRequest;
import com.acme.invoicing.v1.StartEInvoiceWorkflowResponse;
import com.acme.invoicing.v1.TerminateEInvoiceRequest;
import com.acme.invoicing.v1.TerminateEInvoiceResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.RetryLoanApplicationWorkflowRequest;
import com.acme.lending.v1.RetryLoanApplicationWorkflowResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.metrics.v1.SummarizeMetricSeriesesRequest;
import com.acme.metrics.v1.SummarizeMetricSeriesesResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexComponent;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.UpdateSearchIndexRequest;
import com.acme.search.v1.UpdateSearchIndexResponse;
import com.acme.suppliers.v1.RejectSupplierRiskRequest;
import com.acme.suppliers.v1.RejectSupplierRiskResponse;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskApprovalServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
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
 * Runs the long-lived workflow that takes an e invoice from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class EInvoiceWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(EInvoiceWorkflowApplication.class);

    private final AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking;

    private final RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking;

    private final BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountServiceAsync;

    private final ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking;

    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync;

    private final EInvoiceApprovalServiceGrpc.EInvoiceApprovalServiceBlockingStub invoicingEInvoiceApprovalServiceBlocking;

    private final EInvoiceCalculationServiceGrpc.EInvoiceCalculationServiceBlockingStub invoicingEInvoiceCalculationServiceBlocking;

    private final EInvoiceLifecycleServiceGrpc.EInvoiceLifecycleServiceBlockingStub invoicingEInvoiceLifecycleServiceBlocking;

    private final EInvoiceReportingServiceGrpc.EInvoiceReportingServiceBlockingStub invoicingEInvoiceReportingServiceBlocking;

    private final InvoiceTemplateApprovalServiceGrpc.InvoiceTemplateApprovalServiceBlockingStub invoicingInvoiceTemplateApprovalServiceBlocking;

    private final InvoiceTemplateWorkflowServiceGrpc.InvoiceTemplateWorkflowServiceBlockingStub invoicingInvoiceTemplateWorkflowServiceBlocking;

    private final LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking;

    private final EInvoiceWorkflowRequestMapper mapper;

    private final MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking;

    private final InvoicingRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    private final SupplierRiskApprovalServiceGrpc.SupplierRiskApprovalServiceBlockingStub suppliersSupplierRiskApprovalServiceBlocking;

    public EInvoiceWorkflowApplication(
            AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking,
            RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking,
            BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountServiceAsync,
            ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking,
            ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync,
            EInvoiceApprovalServiceGrpc.EInvoiceApprovalServiceBlockingStub invoicingEInvoiceApprovalServiceBlocking,
            EInvoiceCalculationServiceGrpc.EInvoiceCalculationServiceBlockingStub invoicingEInvoiceCalculationServiceBlocking,
            EInvoiceLifecycleServiceGrpc.EInvoiceLifecycleServiceBlockingStub invoicingEInvoiceLifecycleServiceBlocking,
            EInvoiceReportingServiceGrpc.EInvoiceReportingServiceBlockingStub invoicingEInvoiceReportingServiceBlocking,
            InvoiceTemplateApprovalServiceGrpc.InvoiceTemplateApprovalServiceBlockingStub invoicingInvoiceTemplateApprovalServiceBlocking,
            InvoiceTemplateWorkflowServiceGrpc.InvoiceTemplateWorkflowServiceBlockingStub invoicingInvoiceTemplateWorkflowServiceBlocking,
            LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking,
            EInvoiceWorkflowRequestMapper mapper,
            MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking,
            InvoicingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking,
            SupplierRiskApprovalServiceGrpc.SupplierRiskApprovalServiceBlockingStub suppliersSupplierRiskApprovalServiceBlocking) {
        this.assetsAssetTransferSyncServiceBlocking = assetsAssetTransferSyncServiceBlocking;
        this.auditRetentionPolicyExportServiceBlocking = auditRetentionPolicyExportServiceBlocking;
        this.bankingBankAccountServiceAsync = bankingBankAccountServiceAsync;
        this.budgetingForecastReportingServiceBlocking = budgetingForecastReportingServiceBlocking;
        this.customersConsentRecordCalculationServiceBlocking = customersConsentRecordCalculationServiceBlocking;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.insuranceUnderwritingSyncServiceAsync = insuranceUnderwritingSyncServiceAsync;
        this.invoicingEInvoiceApprovalServiceBlocking = invoicingEInvoiceApprovalServiceBlocking;
        this.invoicingEInvoiceCalculationServiceBlocking = invoicingEInvoiceCalculationServiceBlocking;
        this.invoicingEInvoiceLifecycleServiceBlocking = invoicingEInvoiceLifecycleServiceBlocking;
        this.invoicingEInvoiceReportingServiceBlocking = invoicingEInvoiceReportingServiceBlocking;
        this.invoicingInvoiceTemplateApprovalServiceBlocking = invoicingInvoiceTemplateApprovalServiceBlocking;
        this.invoicingInvoiceTemplateWorkflowServiceBlocking = invoicingInvoiceTemplateWorkflowServiceBlocking;
        this.lendingLoanApplicationWorkflowServiceBlocking = lendingLoanApplicationWorkflowServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesQueryServiceBlocking = metricsMetricSeriesQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
        this.suppliersSupplierRiskApprovalServiceBlocking = suppliersSupplierRiskApprovalServiceBlocking;
    }

    /**
     * Starts an e invoice.
     */
    public StartEInvoiceWorkflowResponse startEInvoiceWorkflow(StartEInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartEInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = mapper.startEInvoiceWorkflowStage0(request);
        var reply0 = budgetingForecastReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listForecastReports(outbound0);

        var outbound1 = BatchCreateBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBankAccountsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotify())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateBankAccountsResponse.getDefaultInstance());
        var input1 = bankingBankAccountServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBankAccounts(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamUnderwritingChangesRequest.newBuilder()
                .setChange(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getEInvoice().getCounterparty().getDisplayName())
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getEInvoice().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamUnderwritingChangesResponse.getDefaultInstance());
        var input2 = insuranceUnderwritingSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamUnderwritingChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartEInvoiceWorkflow", request.getEInvoice().getEInvoiceId());
        return StartEInvoiceWorkflowResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(request.getEInvoice().getBillingAddress())
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Advances an e invoice.
     */
    public AdvanceEInvoiceWorkflowResponse advanceEInvoiceWorkflow(AdvanceEInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceEInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = ShareEInvoiceReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareEInvoiceReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setName(request.getDetails().getNotify().getName())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = invoicingEInvoiceReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareEInvoiceReport(outbound0);

        var outbound1 = QuoteEInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getEInvoice().getBillingAddress().getCountryCode())
                                .addAllLines(reply0.getEInvoice().getBillingAddress().getLinesList())
                                .setLocality(reply0.getEInvoice().getBillingAddress().getLocality())
                                .build())
                        .setCounterparty(InvoiceBatchRef.newBuilder()
                                .setNumber(request.getEInvoice().getCounterparty().getNumber())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> invoicingEInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteEInvoice(value));

        var outbound2 = mapper.advanceEInvoiceWorkflowStage2(request, reply0);
        var reply2 = invoicingEInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateEInvoice(outbound2);

        var futures3 = request.getEInvoice().getAttachmentsList().stream()
                .map(element -> CommentEInvoiceApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setEInvoice(EInvoice.newBuilder()
                        .addAttachments(EInvoiceAttachment.newBuilder()
                                .setContentType(element.getContentType())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> invoicingEInvoiceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentEInvoiceApproval(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? CommentEInvoiceApprovalResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceEInvoiceWorkflow", request.getEInvoice().getEInvoiceId());
        return AdvanceEInvoiceWorkflowResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getEInvoice().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getEInvoice().getBillingAddress().getRecipient())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setStatus(request.getEInvoice().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels an e invoice.
     */
    public CancelEInvoiceWorkflowResponse cancelEInvoiceWorkflow(CancelEInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelEInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = ListPendingInvoiceTemplatesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingInvoiceTemplatesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getEInvoice().getAudit().getCreatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setOwnerId(request.getEInvoice().getCounterparty().getInvoiceBatchId())
                        .setRegion(request.getEInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> invoicingInvoiceTemplateApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingInvoiceTemplates(value)).orElseThrow();

        var outbound1 = ResolveCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var adapter1 = new Function<ResolveCustomersRequest, ResolveCustomersResponse>() {
            @Override
            public ResolveCustomersResponse apply(ResolveCustomersRequest input) {
                return customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveCustomers(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelEInvoiceWorkflow", request.getEInvoice().getEInvoiceId());
        return CancelEInvoiceWorkflowResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setDueAt(request.getEInvoice().getDueAt())
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .setOutcome(CancelEInvoiceWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys an e invoice.
     */
    public RetryEInvoiceWorkflowResponse retryEInvoiceWorkflow(RetryEInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryEInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = RejectInvoiceTemplateRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEInvoice().getAudit().getEtag())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getEInvoice().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> invoicingInvoiceTemplateApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectInvoiceTemplate(value));

        var outbound1 = QuoteConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(QuoteConsentRecordRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getEInvoice().getBillingAddress().getLinesList())
                                .setPostalCode(reply0.getInvoiceTemplate().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        Function<QuoteConsentRecordRequest, QuoteConsentRecordResponse> operation1 = customersConsentRecordCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::quoteConsentRecord;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryEInvoiceWorkflow", request.getEInvoice().getEInvoiceId());
        return RetryEInvoiceWorkflowResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setCounterparty(InvoiceBatchRef.newBuilder()
                                .setDisplayName(reply0.getInvoiceTemplate().getCounterparty().getDisplayName())
                                .setNumber(request.getEInvoice().getNumber())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .addLines(EInvoiceLine.newBuilder()
                                .putAllAttributes(reply1.getConsentRecord().getAttributesMap())
                                .build())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Pauses an e invoice.
     */
    public PauseEInvoiceWorkflowResponse pauseEInvoiceWorkflow(PauseEInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseEInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = RetryInvoiceTemplateWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getEInvoice().getBillingAddress().getCountryCode())
                                .setLocality(request.getEInvoice().getBillingAddress().getLocality())
                                .build())
                        .setCard(InvoiceTemplateCardSettlement.newBuilder()
                                .setToken(request.getEInvoice().getCard().getToken())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> invoicingInvoiceTemplateWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryInvoiceTemplateWorkflow(outbound0), rpcExecutor);

        var outbound1 = TerminateEInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(TerminateEInvoiceRequest.Details.newBuilder()
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .setEInvoiceId(request.getEInvoice().getEInvoiceId())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> invoicingEInvoiceLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).terminateEInvoice(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseEInvoiceWorkflow", request.getEInvoice().getNumber());
        return PauseEInvoiceWorkflowResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setDueAt(request.getEInvoice().getDueAt())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setNotes(reply0.getInvoiceTemplate().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Resumes an e invoice.
     */
    public ResumeEInvoiceWorkflowResponse resumeEInvoiceWorkflow(ResumeEInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeEInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        List<UpdateSearchIndexResponse> results0 = request.getEInvoice().getLinesList().stream()
                .map(element -> UpdateSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .putComponents("primary", SearchIndexComponent.newBuilder()
                                .setDescription(element.getDescription())
                                .setQuantity(element.getQuantity())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateSearchIndex(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? UpdateSearchIndexResponse.getDefaultInstance()
                : results0.getLast();

        var results1 = new ArrayList<ReconcileAssetTransfersResponse>();
        for (var element : request.getEInvoice().getAttachmentsList()) {
            ReconcileAssetTransfersRequest itemRequest = ReconcileAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .addAttachments(AssetTransferAttachment.newBuilder()
                                .setContentType(element.getContentType())
                                .setSizeBytes(element.getSizeBytes())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ReconcileAssetTransfersRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
            results1.add(assetsAssetTransferSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileAssetTransfers(itemRequest));
        }
        var reply1 = results1.isEmpty() ? ReconcileAssetTransfersResponse.getDefaultInstance()
                : results1.getLast();

        var futures2 = request.getEInvoice().getLinesList().stream()
                .map(element -> QuoteHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .putAllAttributes(element.getAttributesMap())
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply0.getSearchIndex().getAudit().getCreatedAt())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteHousehold(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? QuoteHouseholdResponse.getDefaultInstance()
                : results2.getLast();

        var builder3 = RetryLoanApplicationWorkflowRequest.newBuilder();
        builder3.setContext(request.getContext());
        builder3.setLoanApplication(LoanApplication.newBuilder()
                .setBillingAddress(PostalAddress.newBuilder()
                        .setRegion(reply0.getSearchIndex().getAddress().getRegion())
                        .build())
                .setCard(LoanApplicationCardSettlement.newBuilder()
                        .setLast4(reply1.getAssetTransfer().getCard().getLast4())
                        .build())
                .build());
        var outbound3 = builder3.build();
        var reply3 = lendingLoanApplicationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryLoanApplicationWorkflow(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeEInvoiceWorkflow", request.getEInvoice().getEInvoiceId());
        return ResumeEInvoiceWorkflowResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply3.getLoanApplication().getBillingAddress().getPostalCode())
                                .build())
                        .setCounterparty(request.getEInvoice().getCounterparty())
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .build();
    }

    /**
     * Returns an e invoice.
     */
    public InspectEInvoiceWorkflowResponse inspectEInvoiceWorkflow(InspectEInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectEInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getEInvoiceId(), "InspectEInvoiceWorkflow");

        var outbound0 = ShareRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareRetentionPolicyExportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getContext().getBaggageMap())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareRetentionPolicyExport(value)).orElseThrow();
        return InspectEInvoiceWorkflowResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setEInvoiceId(request.getEInvoiceId())
                        .addLines(EInvoiceLine.newBuilder()
                                .setTax(EInvoiceLineTax.newBuilder()
                                        .setAmount(reply0.getRetentionPolicy().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists e invoices a page at a time.
     */
    public ListEInvoiceWorkflowsResponse listEInvoiceWorkflows(ListEInvoiceWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListEInvoiceWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getContext().getTraceId(), "ListEInvoiceWorkflows");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListEInvoiceWorkflows", request.getPage().getPageToken());
        return ListEInvoiceWorkflowsResponse.getDefaultInstance();
    }

    /**
     * Signals an e invoice.
     */
    public SignalEInvoiceWorkflowResponse signalEInvoiceWorkflow(SignalEInvoiceWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalEInvoiceWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = SummarizeMetricSeriesesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> metricsMetricSeriesQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeMetricSerieses(outbound0), rpcExecutor);

        var outbound1 = RecalculateCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getEInvoice().getAudit().getUpdatedAt())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getEInvoice().getBillingAddress().getLinesList())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(request.getEInvoice().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setDetails(RecalculateCustomersRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getEInvoice().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCustomers(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        List<UpdateSearchIndexResponse> results2 = request.getEInvoice().getLinesList().stream()
                .map(element -> UpdateSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getEInvoice().getAudit().getUpdatedBy())
                                .build())
                        .putComponents("primary", SearchIndexComponent.newBuilder()
                                .setDescription(element.getDescription())
                                .setQuantity(element.getQuantity())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateSearchIndex(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? UpdateSearchIndexResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = RpcSupport.unpack(Any.pack(RejectSupplierRiskRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getCustomer().getRelationshipManager().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getCustomer().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(reply1.getCustomer().getContact().getName())
                                .build())
                        .build())
                .build()), RejectSupplierRiskRequest.class);
        var reply3 = suppliersSupplierRiskApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectSupplierRisk(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalEInvoiceWorkflow", request.getEInvoice().getEInvoiceId());
        return SignalEInvoiceWorkflowResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply1.getCustomer().getPrimaryAddress().getRegion())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setNotes(request.getEInvoice().getNotes())
                        .build())
                .setOutcome(SignalEInvoiceWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply3.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
