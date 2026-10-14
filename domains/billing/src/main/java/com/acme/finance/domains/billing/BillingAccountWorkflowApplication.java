package com.acme.finance.domains.billing;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalApprovalServiceGrpc;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.RecallDisposalRequest;
import com.acme.assets.v1.RecallDisposalResponse;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.StreamAuditTrailExportRequest;
import com.acme.audit.v1.StreamAuditTrailExportResponse;
import com.acme.billing.v1.AdvanceBillingAccountWorkflowRequest;
import com.acme.billing.v1.AdvanceBillingAccountWorkflowResponse;
import com.acme.billing.v1.ApproveInvoiceRequest;
import com.acme.billing.v1.ApproveInvoiceResponse;
import com.acme.billing.v1.BatchGetBillingAccountsRequest;
import com.acme.billing.v1.BatchGetBillingAccountsResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.billing.v1.BillingAccountBankFunding;
import com.acme.billing.v1.BillingAccountLimit;
import com.acme.billing.v1.BillingAccountServiceGrpc;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CancelBillingAccountWorkflowRequest;
import com.acme.billing.v1.CancelBillingAccountWorkflowResponse;
import com.acme.billing.v1.CommentInvoiceApprovalRequest;
import com.acme.billing.v1.CommentInvoiceApprovalResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteCardSettlement;
import com.acme.billing.v1.DunningReportingServiceGrpc;
import com.acme.billing.v1.GenerateDunningReportRequest;
import com.acme.billing.v1.GenerateDunningReportResponse;
import com.acme.billing.v1.InspectBillingAccountWorkflowRequest;
import com.acme.billing.v1.InspectBillingAccountWorkflowResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceApprovalServiceGrpc;
import com.acme.billing.v1.InvoiceBankSettlement;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.LintBillingAccountsRequest;
import com.acme.billing.v1.LintBillingAccountsResponse;
import com.acme.billing.v1.ListBillingAccountWorkflowsRequest;
import com.acme.billing.v1.ListBillingAccountWorkflowsResponse;
import com.acme.billing.v1.PauseBillingAccountWorkflowRequest;
import com.acme.billing.v1.PauseBillingAccountWorkflowResponse;
import com.acme.billing.v1.RecalculateCreditNotesRequest;
import com.acme.billing.v1.RecalculateCreditNotesResponse;
import com.acme.billing.v1.ResumeBillingAccountWorkflowRequest;
import com.acme.billing.v1.ResumeBillingAccountWorkflowResponse;
import com.acme.billing.v1.RetryBillingAccountWorkflowRequest;
import com.acme.billing.v1.RetryBillingAccountWorkflowResponse;
import com.acme.billing.v1.SignalBillingAccountWorkflowRequest;
import com.acme.billing.v1.SignalBillingAccountWorkflowResponse;
import com.acme.billing.v1.StartBillingAccountWorkflowRequest;
import com.acme.billing.v1.StartBillingAccountWorkflowResponse;
import com.acme.billing.v1.SubmitBillingAccountRequest;
import com.acme.billing.v1.SubmitBillingAccountResponse;
import com.acme.billing.v1.UpdateBillingAccountRequest;
import com.acme.billing.v1.UpdateBillingAccountResponse;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptWorkflowServiceGrpc;
import com.acme.expenses.v1.StartReceiptWorkflowRequest;
import com.acme.expenses.v1.StartReceiptWorkflowResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.AdvanceCoverageWorkflowRequest;
import com.acme.insurance.v1.AdvanceCoverageWorkflowResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.SignalCoverageWorkflowRequest;
import com.acme.insurance.v1.SignalCoverageWorkflowResponse;
import com.acme.pricing.v1.BatchGetPriceListsRequest;
import com.acme.pricing.v1.BatchGetPriceListsResponse;
import com.acme.pricing.v1.PriceListServiceGrpc;
import com.acme.recruiting.v1.ExportJobOpeningsRequest;
import com.acme.recruiting.v1.ExportJobOpeningsResponse;
import com.acme.recruiting.v1.JobOpeningQueryServiceGrpc;
import com.acme.search.v1.BatchGetSearchIndexesRequest;
import com.acme.search.v1.BatchGetSearchIndexesResponse;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
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
 * Runs the long-lived workflow that takes a billing account from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class BillingAccountWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(BillingAccountWorkflowApplication.class);

    private final DisposalApprovalServiceGrpc.DisposalApprovalServiceBlockingStub assetsDisposalApprovalServiceBlocking;

    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync;

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceBlockingStub billingBillingAccountApprovalServiceBlocking;

    private final BillingAccountServiceGrpc.BillingAccountServiceBlockingStub billingBillingAccountServiceBlocking;

    private final BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking;

    private final CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking;

    private final DunningReportingServiceGrpc.DunningReportingServiceBlockingStub billingDunningReportingServiceBlocking;

    private final InvoiceApprovalServiceGrpc.InvoiceApprovalServiceBlockingStub billingInvoiceApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final ReceiptWorkflowServiceGrpc.ReceiptWorkflowServiceBlockingStub expensesReceiptWorkflowServiceBlocking;

    private final CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking;

    private final PriceListServiceGrpc.PriceListServiceBlockingStub pricingPriceListServiceBlocking;

    private final JobOpeningQueryServiceGrpc.JobOpeningQueryServiceBlockingStub recruitingJobOpeningQueryServiceBlocking;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    public BillingAccountWorkflowApplication(
            DisposalApprovalServiceGrpc.DisposalApprovalServiceBlockingStub assetsDisposalApprovalServiceBlocking,
            DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync,
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceBlockingStub billingBillingAccountApprovalServiceBlocking,
            BillingAccountServiceGrpc.BillingAccountServiceBlockingStub billingBillingAccountServiceBlocking,
            BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking,
            CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking,
            DunningReportingServiceGrpc.DunningReportingServiceBlockingStub billingDunningReportingServiceBlocking,
            InvoiceApprovalServiceGrpc.InvoiceApprovalServiceBlockingStub billingInvoiceApprovalServiceBlocking,
            ChannelRegistry channels,
            CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            ReceiptWorkflowServiceGrpc.ReceiptWorkflowServiceBlockingStub expensesReceiptWorkflowServiceBlocking,
            CoverageWorkflowServiceGrpc.CoverageWorkflowServiceBlockingStub insuranceCoverageWorkflowServiceBlocking,
            PriceListServiceGrpc.PriceListServiceBlockingStub pricingPriceListServiceBlocking,
            JobOpeningQueryServiceGrpc.JobOpeningQueryServiceBlockingStub recruitingJobOpeningQueryServiceBlocking,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking) {
        this.assetsDisposalApprovalServiceBlocking = assetsDisposalApprovalServiceBlocking;
        this.assetsDisposalSyncServiceAsync = assetsDisposalSyncServiceAsync;
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.billingBillingAccountApprovalServiceBlocking = billingBillingAccountApprovalServiceBlocking;
        this.billingBillingAccountServiceBlocking = billingBillingAccountServiceBlocking;
        this.billingBillingAccountValidationServiceBlocking = billingBillingAccountValidationServiceBlocking;
        this.billingCreditNoteCalculationServiceBlocking = billingCreditNoteCalculationServiceBlocking;
        this.billingDunningReportingServiceBlocking = billingDunningReportingServiceBlocking;
        this.billingInvoiceApprovalServiceBlocking = billingInvoiceApprovalServiceBlocking;
        this.channels = channels;
        this.collectionsCollectionCaseServiceAsync = collectionsCollectionCaseServiceAsync;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.expensesReceiptWorkflowServiceBlocking = expensesReceiptWorkflowServiceBlocking;
        this.insuranceCoverageWorkflowServiceBlocking = insuranceCoverageWorkflowServiceBlocking;
        this.pricingPriceListServiceBlocking = pricingPriceListServiceBlocking;
        this.recruitingJobOpeningQueryServiceBlocking = recruitingJobOpeningQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
    }

    /**
     * Starts a billing account.
     */
    public StartBillingAccountWorkflowResponse startBillingAccountWorkflow(StartBillingAccountWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartBillingAccountWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = ExportJobOpeningsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportJobOpeningsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBillingAccount().getAudit().getCreatedAt())
                        .putAllLabels(request.getBillingAccount().getLabelsMap())
                        .setOwnerId(request.getBillingAccount().getBillingAccountId())
                        .build())
                .build();
        var stream0 = recruitingJobOpeningQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportJobOpenings(outbound0);
        var reply0 = ExportJobOpeningsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = SubmitBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getBillingAccount().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> billingBillingAccountApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitBillingAccount(value));

        var outbound2 = BatchCreateCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateCollectionCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getBillingAccount().getBillingAccountId())
                        .setSubmittedBy(request.getBillingAccount().getContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateCollectionCasesResponse.getDefaultInstance());
        var input2 = collectionsCollectionCaseServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateCollectionCases(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamDisposalChangesRequest.newBuilder()
                .setChange(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getBillingAccount().getHolder().getDisplayName())
                                .setEmail(request.getBillingAccount().getContact().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBillingAccount().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamDisposalChangesResponse.getDefaultInstance());
        var input3 = assetsDisposalSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getHolder().getInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartBillingAccountWorkflow", request.getBillingAccount().getHolder().getInvoiceId());
        return StartBillingAccountWorkflowResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAvailable(reply1.getBillingAccount().getAvailable())
                        .setBillingAccountId(request.getBillingAccountId())
                        .setStatus(request.getBillingAccount().getStatus())
                        .build())
                .build();
    }

    /**
     * Advances a billing account.
     */
    public AdvanceBillingAccountWorkflowResponse advanceBillingAccountWorkflow(AdvanceBillingAccountWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceBillingAccountWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = AdvanceCoverageWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getBillingAccount().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getBillingAccount().getHolder().getDisplayName())
                                .build())
                        .build())
                .setDetails(AdvanceCoverageWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setPhone(request.getBillingAccount().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> insuranceCoverageWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceCoverageWorkflow(value));

        var outbound1 = BatchGetSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply1 = RpcSupport.retryRead(() -> searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetSearchIndexes(outbound1));
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceBillingAccountWorkflow", request.getBillingAccount().getBillingAccountId());
        return AdvanceBillingAccountWorkflowResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAvailable(request.getBillingAccount().getAvailable())
                        .setBillingAccountId(request.getBillingAccountId())
                        .build())
                .setOutcome(AdvanceBillingAccountWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels a billing account.
     */
    public CancelBillingAccountWorkflowResponse cancelBillingAccountWorkflow(CancelBillingAccountWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelBillingAccountWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = StreamAuditTrailExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditTrailExportRequest.Filter.newBuilder()
                        .putAllLabels(request.getBillingAccount().getLabelsMap())
                        .build())
                .build();
        var stream0 = auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditTrailExport(outbound0);
        var reply0 = StreamAuditTrailExportResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = CommentInvoiceApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentInvoiceApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getBillingAccount().getHolder().getDisplayName())
                                .build())
                        .build())
                .setInvoice(Invoice.newBuilder()
                        .setBank(InvoiceBankSettlement.newBuilder()
                                .setIban(switch (request.getBillingAccount().getFundingCase()) {
                                    case BANK -> request.getBillingAccount().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = billingInvoiceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentInvoiceApproval(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelBillingAccountWorkflow", request.getBillingAccount().getBillingAccountId());
        return CancelBillingAccountWorkflowResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setBillingAccountId(request.getBillingAccountId())
                        .setContact(request.getBillingAccount().getContact())
                        .build())
                .setOutcome(CancelBillingAccountWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Retrys a billing account.
     */
    public RetryBillingAccountWorkflowResponse retryBillingAccountWorkflow(RetryBillingAccountWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryBillingAccountWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        List<UpdateBillingAccountResponse> results0 = request.getBillingAccount().getLimitsList().stream()
                .map(element -> UpdateBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .addLimits(BillingAccountLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .setName(element.getName())
                                .setPeriod(element.getPeriod())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(billingBillingAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateBillingAccount(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? UpdateBillingAccountResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = RecallDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getBillingAccount().getContact().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBillingAccount().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotify().getName())
                                .build())
                        .build())
                .build();
        var reply1 = assetsDisposalApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallDisposal(outbound1);

        var outbound2 = JsonCodec.roundTrip(QuotePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getBillingAccount().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build(), QuotePreferenceRequest.class);
        var reply2 = customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePreference(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryBillingAccountWorkflow", request.getBillingAccount().getBillingAccountId());
        return RetryBillingAccountWorkflowResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getPreference().getAudit().getEtag())
                                .build())
                        .setBillingAccountId(request.getBillingAccountId())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getDisposal().getRequester().getEmail())
                                .build())
                        .setCurrencyCode(request.getBillingAccount().getCurrencyCode())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Pauses a billing account.
     */
    public PauseBillingAccountWorkflowResponse pauseBillingAccountWorkflow(PauseBillingAccountWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseBillingAccountWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = BatchGetPriceListsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> pricingPriceListServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetPriceLists(outbound0), rpcExecutor);

        var outbound1 = GenerateDunningReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(GenerateDunningReportRequest.Details.newBuilder()
                        .putAllFilters(request.getBillingAccount().getLabelsMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> billingDunningReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateDunningReport(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseBillingAccountWorkflow", request.getBillingAccount().getBillingAccountId());
        return PauseBillingAccountWorkflowResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAvailable(request.getBillingAccount().getAvailable())
                        .setBillingAccountId(request.getBillingAccountId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Resumes a billing account.
     */
    public ResumeBillingAccountWorkflowResponse resumeBillingAccountWorkflow(ResumeBillingAccountWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeBillingAccountWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = BatchGetBillingAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.retryRead(() -> billingBillingAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetBillingAccounts(outbound0));
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeBillingAccountWorkflow", request.getBillingAccount().getBillingAccountId());
        return ResumeBillingAccountWorkflowResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setBillingAccountId(request.getBillingAccountId())
                        .putAllLabels(request.getBillingAccount().getLabelsMap())
                        .build())
                .setOutcome(ResumeBillingAccountWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a billing account.
     */
    public InspectBillingAccountWorkflowResponse inspectBillingAccountWorkflow(InspectBillingAccountWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectBillingAccountWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BillingAccount", request.getBillingAccountId(), "InspectBillingAccountWorkflow");

        var outbound0 = (StartReceiptWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(StartReceiptWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReceipt(Receipt.newBuilder()
                        .setDueAt(request.getContext().getDeadline())
                        .addLines(ReceiptLine.newBuilder()
                                .putAllAttributes(request.getContext().getBaggageMap())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = expensesReceiptWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startReceiptWorkflow(outbound0);
        return InspectBillingAccountWorkflowResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getReceipt().getAudit().getEtag())
                                .build())
                        .setBillingAccountId(request.getBillingAccountId())
                        .setHolder(InvoiceRef.newBuilder()
                                .setDisplayName(reply0.getReceipt().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists billing accounts a page at a time.
     */
    public ListBillingAccountWorkflowsResponse listBillingAccountWorkflows(ListBillingAccountWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListBillingAccountWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BillingAccount", request.getContext().getTraceId(), "ListBillingAccountWorkflows");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListBillingAccountWorkflows", request.getPage().getPageToken());
        return ListBillingAccountWorkflowsResponse.getDefaultInstance();
    }

    /**
     * Signals a billing account.
     */
    public SignalBillingAccountWorkflowResponse signalBillingAccountWorkflow(SignalBillingAccountWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalBillingAccountWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBillingAccount(request.getContext().getTenantId(), request.getBillingAccount());

        var outbound0 = ApproveInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBillingAccount().getAudit().getCreatedAt())
                                .setEtag(request.getBillingAccount().getAudit().getEtag())
                                .setUpdatedAt(request.getBillingAccount().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.billing.v1.InvoiceApprovalService");
        var serviceName0 = "acme.billing.v1" + "." + "InvoiceApprovalService";
        var methodName0 = "App" + "roveInvoice";
        var descriptor0 = MethodDescriptor.<ApproveInvoiceRequest, ApproveInvoiceResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ApproveInvoiceRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ApproveInvoiceResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = LintBillingAccountsRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setBank(BillingAccountBankFunding.newBuilder()
                                .setAccountHolder(switch (request.getBillingAccount().getFundingCase()) {
                                    case BANK -> request.getBillingAccount().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getInvoice().getCounterparty().getEmail())
                                .build())
                        .setHolder(InvoiceRef.newBuilder()
                                .setNumber(request.getBillingAccount().getHolder().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintBillingAccounts(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(SignalCoverageWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SignalCoverageWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(reply1.getBillingAccount().getContact().getName())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                .build())
                        .build())
                .build()), SignalCoverageWorkflowRequest.class);
        var reply2 = insuranceCoverageWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalCoverageWorkflow(outbound2);

        var outbound3 = RecalculateCreditNotesRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getInvoice().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getBillingAccount().getAudit().getUpdatedBy())
                                .build())
                        .setCard(CreditNoteCardSettlement.newBuilder()
                                .setHolderName(switch (request.getBillingAccount().getFundingCase()) {
                                    case CARD -> request.getBillingAccount().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setDetails(RecalculateCreditNotesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply0.getInvoice().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var reply3 = billingCreditNoteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCreditNotes(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getBillingAccount().getBillingAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalBillingAccountWorkflow", request.getBillingAccount().getBillingAccountId());
        return SignalBillingAccountWorkflowResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getInvoice().getAudit().getCreatedBy())
                                .build())
                        .setBalance(request.getBillingAccount().getBalance())
                        .setBillingAccountId(request.getBillingAccountId())
                        .setHolder(InvoiceRef.newBuilder()
                                .setNumber(reply3.getCreditNote().getCounterparty().getNumber())
                                .build())
                        .setStatus(reply1.getBillingAccount().getStatus())
                        .build())
                .setOutcome(SignalBillingAccountWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .addAllNextSteps(reply2.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
