package com.acme.finance.domains.billing;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.assets.v1.StreamDisposalMetricsRequest;
import com.acme.assets.v1.StreamDisposalMetricsResponse;
import com.acme.assets.v1.SubmitFixedAssetRequest;
import com.acme.assets.v1.SubmitFixedAssetResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.RevertAuditEventRequest;
import com.acme.audit.v1.RevertAuditEventResponse;
import com.acme.billing.v1.BillingAccountQueryServiceGrpc;
import com.acme.billing.v1.CompareInvoiceOptionsRequest;
import com.acme.billing.v1.CompareInvoiceOptionsResponse;
import com.acme.billing.v1.CountBillingAccountsRequest;
import com.acme.billing.v1.CountBillingAccountsResponse;
import com.acme.billing.v1.CountDunningTotalsRequest;
import com.acme.billing.v1.CountDunningTotalsResponse;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningComment;
import com.acme.billing.v1.DunningValidationServiceGrpc;
import com.acme.billing.v1.EstimateInvoiceRequest;
import com.acme.billing.v1.EstimateInvoiceResponse;
import com.acme.billing.v1.FetchDunningReportRequest;
import com.acme.billing.v1.FetchDunningReportResponse;
import com.acme.billing.v1.GenerateDunningReportRequest;
import com.acme.billing.v1.GenerateDunningReportResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceLineTax;
import com.acme.billing.v1.ListDunningReportsRequest;
import com.acme.billing.v1.ListDunningReportsResponse;
import com.acme.billing.v1.NormalizeDunningRequest;
import com.acme.billing.v1.NormalizeDunningResponse;
import com.acme.billing.v1.ScheduleDunningReportRequest;
import com.acme.billing.v1.ScheduleDunningReportResponse;
import com.acme.billing.v1.ShareDunningReportRequest;
import com.acme.billing.v1.ShareDunningReportResponse;
import com.acme.billing.v1.StreamDunningMetricsRequest;
import com.acme.billing.v1.StreamDunningMetricsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.contracts.v1.AdvanceContractClauseWorkflowRequest;
import com.acme.contracts.v1.AdvanceContractClauseWorkflowResponse;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseBlockAction;
import com.acme.contracts.v1.ContractClauseWorkflowServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentBlockAction;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.PreviewCustomerSegmentRequest;
import com.acme.customers.v1.PreviewCustomerSegmentResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.learning.v1.CourseQueryServiceGrpc;
import com.acme.learning.v1.SummarizeCoursesRequest;
import com.acme.learning.v1.SummarizeCoursesResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeRequest;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeResponse;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesRequest;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesResponse;
import com.acme.notifications.v1.CountNotificationsRequest;
import com.acme.notifications.v1.CountNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.suppliers.v1.BatchCreateSuppliersRequest;
import com.acme.suppliers.v1.BatchCreateSuppliersResponse;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about dunnings. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class DunningReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(DunningReportingApplication.class);

    private final DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking;

    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync;

    private final FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking;

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final BillingAccountQueryServiceGrpc.BillingAccountQueryServiceBlockingStub billingBillingAccountQueryServiceBlocking;

    private final DunningValidationServiceGrpc.DunningValidationServiceBlockingStub billingDunningValidationServiceBlocking;

    private final InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking;

    private final ContractClauseWorkflowServiceGrpc.ContractClauseWorkflowServiceBlockingStub contractsContractClauseWorkflowServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final CourseQueryServiceGrpc.CourseQueryServiceBlockingStub learningCourseQueryServiceBlocking;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final DunningReportingRequestMapper mapper;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final BillingRepository repository;

    private final SupplierServiceGrpc.SupplierServiceStub suppliersSupplierServiceAsync;

    public DunningReportingApplication(
            DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking,
            DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync,
            FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking,
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            BillingAccountQueryServiceGrpc.BillingAccountQueryServiceBlockingStub billingBillingAccountQueryServiceBlocking,
            DunningValidationServiceGrpc.DunningValidationServiceBlockingStub billingDunningValidationServiceBlocking,
            InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking,
            ContractClauseWorkflowServiceGrpc.ContractClauseWorkflowServiceBlockingStub contractsContractClauseWorkflowServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            CourseQueryServiceGrpc.CourseQueryServiceBlockingStub learningCourseQueryServiceBlocking,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            DunningReportingRequestMapper mapper,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            BillingRepository repository,
            SupplierServiceGrpc.SupplierServiceStub suppliersSupplierServiceAsync) {
        this.assetsDisposalReportingServiceBlocking = assetsDisposalReportingServiceBlocking;
        this.assetsDisposalSyncServiceAsync = assetsDisposalSyncServiceAsync;
        this.assetsFixedAssetApprovalServiceBlocking = assetsFixedAssetApprovalServiceBlocking;
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.billingBillingAccountQueryServiceBlocking = billingBillingAccountQueryServiceBlocking;
        this.billingDunningValidationServiceBlocking = billingDunningValidationServiceBlocking;
        this.billingInvoiceCalculationServiceBlocking = billingInvoiceCalculationServiceBlocking;
        this.contractsContractClauseWorkflowServiceBlocking = contractsContractClauseWorkflowServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.learningCourseQueryServiceBlocking = learningCourseQueryServiceBlocking;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.repository = repository;
        this.suppliersSupplierServiceAsync = suppliersSupplierServiceAsync;
    }

    /**
     * Generates a dunning.
     */
    public GenerateDunningReportResponse generateDunningReport(GenerateDunningReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateDunningReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getDunningId(), "GenerateDunningReport");

        var outbound0 = NormalizeDunningRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDunning(Dunning.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setDunningId(request.getDunningId())
                        .build())
                .build();
        final class Stage0Adapter {
            NormalizeDunningResponse fetch(NormalizeDunningRequest input) {
                return billingDunningValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeDunning(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = BatchCreateSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateSuppliersRequest.StreamHeader.newBuilder()
                        .setFormat(request.getDetails().getFormat())
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateSuppliersResponse.getDefaultInstance());
        var input1 = suppliersSupplierServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateSuppliers(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamDisposalChangesRequest.newBuilder()
                .setChange(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamDisposalChangesResponse.getDefaultInstance());
        var input2 = assetsDisposalSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getCron()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateDunningReport", request.getDetails().getCron());
        return GenerateDunningReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a dunning.
     */
    public FetchDunningReportResponse fetchDunningReport(FetchDunningReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchDunningReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getDunningId(), "FetchDunningReport");

        var outbound0 = mapper.fetchDunningReportStage0(request);
        var reply0 = assetsFixedAssetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitFixedAsset(outbound0);
        return FetchDunningReportResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setDunningId(request.getDunningId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Schedules a dunning.
     */
    public ScheduleDunningReportResponse scheduleDunningReport(ScheduleDunningReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleDunningReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getDunningId(), "ScheduleDunningReport");

        var outbound0 = PreviewCustomerSegmentRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setBlock(CustomerSegmentBlockAction.newBuilder()
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setDetails(PreviewCustomerSegmentRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<PreviewCustomerSegmentRequest, PreviewCustomerSegmentResponse> port0 = RpcPort.adapt(customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewCustomerSegment);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = StreamDisposalMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply0.getCustomerSegment().getBlock().getNotifyEmail())
                        .build())
                .build();
        var stream1 = assetsDisposalReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalMetrics(outbound1);
        var reply1 = StreamDisposalMetricsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = mapper.scheduleDunningReportStage2(request, reply0);
        var reply2 = ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendFiscalPeriodTestNotice(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDunningId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleDunningReport", request.getDunningId());
        return ScheduleDunningReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Streams the changes of dunnings as they happen.
     */
    public StreamDunningMetricsResponse streamDunningMetrics(StreamDunningMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamDunningMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getContext().getTraceId(), "StreamDunningMetrics");

        var outbound0 = mapper.streamDunningMetricsStage0(request);
        var reply0 = notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countNotifications(outbound0);

        var outbound1 = RevertAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(RevertAuditEventRequest.Details.newBuilder()
                        .setAuthorEmail(request.getSubscriber().getEmail())
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var adapter1 = new Function<RevertAuditEventRequest, RevertAuditEventResponse>() {
            @Override
            public RevertAuditEventResponse apply(RevertAuditEventRequest input) {
                return auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertAuditEvent(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = EstimateInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply1.getAuditEvent().getSubject().getDisplayName())
                                .build())
                        .setNumber(reply1.getAuditEvent().getSubject().getNumber())
                        .build())
                .build();
        var reply2 = billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateInvoice(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getSubscriber().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamDunningMetrics", request.getSubscriber().getPhone());
        return StreamDunningMetricsResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .addComments(DunningComment.newBuilder()
                                .setCreatedAt(reply2.getInvoice().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .setName(request.getSubscriber().getName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists dunnings a page at a time.
     */
    public ListDunningReportsResponse listDunningReports(ListDunningReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListDunningReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getContext().getTraceId(), "ListDunningReports");

        var outbound0 = (CountBillingAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountBillingAccountsRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageSize(request.getPage().getPageSize())
                        .build())
                .build()).toBuilder().build();
        var reply0 = billingBillingAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countBillingAccounts(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListDunningReports", request.getPage().getPageToken());
        return ListDunningReportsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Shares a dunning.
     */
    public ShareDunningReportResponse shareDunningReport(ShareDunningReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareDunningReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getDunningId(), "ShareDunningReport");

        var outbound0 = mapper.shareDunningReportStage0(request);
        var reply0 = contractsContractClauseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceContractClauseWorkflow(outbound0);

        var outbound1 = CompareInvoiceOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .addLines(InvoiceLine.newBuilder()
                                .setTax(InvoiceLineTax.newBuilder()
                                        .setAmount(reply0.getContractClause().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareInvoiceOptions(outbound1);

        var outbound2 = StreamFiscalPeriodNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply0.getContractClause().getOwner().getEmail())
                        .setPhone(request.getDetails().getRecipient().getPhone())
                        .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                        .build())
                .build();
        var stream2 = ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamFiscalPeriodNotices(outbound2);
        var reply2 = StreamFiscalPeriodNoticesResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDetails().getTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareDunningReport", request.getDetails().getTemplateId());
        return ShareDunningReportResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getContractClause().getAudit().getUpdatedAt())
                                .build())
                        .setDunningId(request.getDunningId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Counts dunnings by status.
     */
    public CountDunningTotalsResponse countDunningTotals(CountDunningTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountDunningTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getContext().getTraceId(), "CountDunningTotals");

        var outbound0 = SummarizeCoursesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SummarizeCoursesRequest.Details.newBuilder()
                        .putAllFilters(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var reply0 = learningCourseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeCourses(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountDunningTotals", request.getPage().getPageToken());
        return CountDunningTotalsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
