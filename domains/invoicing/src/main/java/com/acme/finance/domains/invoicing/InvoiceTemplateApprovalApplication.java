package com.acme.finance.domains.invoicing;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.RecallDisposalRequest;
import com.acme.assets.v1.RecallDisposalResponse;
import com.acme.assets.v1.RejectDisposalRequest;
import com.acme.assets.v1.RejectDisposalResponse;
import com.acme.assets.v1.SubmitDisposalRequest;
import com.acme.assets.v1.SubmitDisposalResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.AuditTrailLineTax;
import com.acme.audit.v1.RetryAuditTrailExportRequest;
import com.acme.audit.v1.RetryAuditTrailExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.BrowseHouseholdsRequest;
import com.acme.customers.v1.BrowseHouseholdsResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.ExplainHouseholdCalculationRequest;
import com.acme.customers.v1.ExplainHouseholdCalculationResponse;
import com.acme.customers.v1.FindHouseholdsRequest;
import com.acme.customers.v1.FindHouseholdsResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.LintCustomerSegmentsRequest;
import com.acme.customers.v1.LintCustomerSegmentsResponse;
import com.acme.customers.v1.RecalculateCustomersRequest;
import com.acme.customers.v1.RecalculateCustomersResponse;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.SuggestDocumentsRequest;
import com.acme.documents.v1.SuggestDocumentsResponse;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.ApproveInvoiceTemplateRequest;
import com.acme.invoicing.v1.ApproveInvoiceTemplateResponse;
import com.acme.invoicing.v1.CalculateInvoiceBatchRequest;
import com.acme.invoicing.v1.CalculateInvoiceBatchResponse;
import com.acme.invoicing.v1.CommentInvoiceTemplateApprovalRequest;
import com.acme.invoicing.v1.CommentInvoiceTemplateApprovalResponse;
import com.acme.invoicing.v1.DelegateInvoiceTemplateApprovalRequest;
import com.acme.invoicing.v1.DelegateInvoiceTemplateApprovalResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceValidationServiceGrpc;
import com.acme.invoicing.v1.EscalateInvoiceTemplateRequest;
import com.acme.invoicing.v1.EscalateInvoiceTemplateResponse;
import com.acme.invoicing.v1.GetInvoiceTemplateApprovalStatusRequest;
import com.acme.invoicing.v1.GetInvoiceTemplateApprovalStatusResponse;
import com.acme.invoicing.v1.GetInvoiceTemplateRulesRequest;
import com.acme.invoicing.v1.GetInvoiceTemplateRulesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchApprovalServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchAttachment;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchCardSettlement;
import com.acme.invoicing.v1.InvoiceBatchLine;
import com.acme.invoicing.v1.InvoiceBatchLineTax;
import com.acme.invoicing.v1.InvoiceBatchRef;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateLifecycleServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.ListPendingInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ListPendingInvoiceTemplatesResponse;
import com.acme.invoicing.v1.NormalizeEInvoiceRequest;
import com.acme.invoicing.v1.NormalizeEInvoiceResponse;
import com.acme.invoicing.v1.RecalculateInvoiceBatchesRequest;
import com.acme.invoicing.v1.RecalculateInvoiceBatchesResponse;
import com.acme.invoicing.v1.RecallInvoiceBatchRequest;
import com.acme.invoicing.v1.RecallInvoiceBatchResponse;
import com.acme.invoicing.v1.RecallInvoiceTemplateRequest;
import com.acme.invoicing.v1.RecallInvoiceTemplateResponse;
import com.acme.invoicing.v1.RejectInvoiceBatchRequest;
import com.acme.invoicing.v1.RejectInvoiceBatchResponse;
import com.acme.invoicing.v1.RejectInvoiceTemplateRequest;
import com.acme.invoicing.v1.RejectInvoiceTemplateResponse;
import com.acme.invoicing.v1.RenewInvoiceTemplateRequest;
import com.acme.invoicing.v1.RenewInvoiceTemplateResponse;
import com.acme.invoicing.v1.SubmitInvoiceTemplateRequest;
import com.acme.invoicing.v1.SubmitInvoiceTemplateResponse;
import com.acme.notifications.v1.ListNotificationsRequest;
import com.acme.notifications.v1.ListNotificationsResponse;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.risk.v1.ApproveStressTestRequest;
import com.acme.risk.v1.ApproveStressTestResponse;
import com.acme.risk.v1.RetryRiskRegisterWorkflowRequest;
import com.acme.risk.v1.RetryRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestApprovalServiceGrpc;
import com.acme.risk.v1.StressTestCardSettlement;
import com.acme.suppliers.v1.ListPendingSupplierRatingsRequest;
import com.acme.suppliers.v1.ListPendingSupplierRatingsResponse;
import com.acme.suppliers.v1.SupplierRatingApprovalServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes invoice templates through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class InvoiceTemplateApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceTemplateApprovalApplication.class);

    private final DisposalApprovalServiceGrpc.DisposalApprovalServiceBlockingStub assetsDisposalApprovalServiceBlocking;

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking;

    private final DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final EInvoiceValidationServiceGrpc.EInvoiceValidationServiceBlockingStub invoicingEInvoiceValidationServiceBlocking;

    private final InvoiceBatchApprovalServiceGrpc.InvoiceBatchApprovalServiceBlockingStub invoicingInvoiceBatchApprovalServiceBlocking;

    private final InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking;

    private final InvoiceTemplateLifecycleServiceGrpc.InvoiceTemplateLifecycleServiceBlockingStub invoicingInvoiceTemplateLifecycleServiceBlocking;

    private final InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceBlockingStub invoicingInvoiceTemplateValidationServiceBlocking;

    private final InvoiceTemplateApprovalRequestMapper mapper;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final InvoicingRepository repository;

    private final RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking;

    private final StressTestApprovalServiceGrpc.StressTestApprovalServiceBlockingStub riskStressTestApprovalServiceBlocking;

    private final SupplierRatingApprovalServiceGrpc.SupplierRatingApprovalServiceBlockingStub suppliersSupplierRatingApprovalServiceBlocking;

    public InvoiceTemplateApprovalApplication(
            DisposalApprovalServiceGrpc.DisposalApprovalServiceBlockingStub assetsDisposalApprovalServiceBlocking,
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking,
            DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking,
            ApplicationEventPublisher events,
            EInvoiceValidationServiceGrpc.EInvoiceValidationServiceBlockingStub invoicingEInvoiceValidationServiceBlocking,
            InvoiceBatchApprovalServiceGrpc.InvoiceBatchApprovalServiceBlockingStub invoicingInvoiceBatchApprovalServiceBlocking,
            InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking,
            InvoiceTemplateLifecycleServiceGrpc.InvoiceTemplateLifecycleServiceBlockingStub invoicingInvoiceTemplateLifecycleServiceBlocking,
            InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceBlockingStub invoicingInvoiceTemplateValidationServiceBlocking,
            InvoiceTemplateApprovalRequestMapper mapper,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            InvoicingRepository repository,
            RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking,
            StressTestApprovalServiceGrpc.StressTestApprovalServiceBlockingStub riskStressTestApprovalServiceBlocking,
            SupplierRatingApprovalServiceGrpc.SupplierRatingApprovalServiceBlockingStub suppliersSupplierRatingApprovalServiceBlocking) {
        this.assetsDisposalApprovalServiceBlocking = assetsDisposalApprovalServiceBlocking;
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.customersHouseholdQueryServiceBlocking = customersHouseholdQueryServiceBlocking;
        this.documentsDocumentQueryServiceBlocking = documentsDocumentQueryServiceBlocking;
        this.events = events;
        this.invoicingEInvoiceValidationServiceBlocking = invoicingEInvoiceValidationServiceBlocking;
        this.invoicingInvoiceBatchApprovalServiceBlocking = invoicingInvoiceBatchApprovalServiceBlocking;
        this.invoicingInvoiceBatchCalculationServiceBlocking = invoicingInvoiceBatchCalculationServiceBlocking;
        this.invoicingInvoiceTemplateLifecycleServiceBlocking = invoicingInvoiceTemplateLifecycleServiceBlocking;
        this.invoicingInvoiceTemplateValidationServiceBlocking = invoicingInvoiceTemplateValidationServiceBlocking;
        this.mapper = mapper;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterWorkflowServiceBlocking = riskRiskRegisterWorkflowServiceBlocking;
        this.riskStressTestApprovalServiceBlocking = riskStressTestApprovalServiceBlocking;
        this.suppliersSupplierRatingApprovalServiceBlocking = suppliersSupplierRatingApprovalServiceBlocking;
    }

    public record RejectInvoiceTemplateStage1Requested(ExplainHouseholdCalculationRequest request, CompletableFuture<ExplainHouseholdCalculationResponse> completion, io.grpc.Context context) {}

    /**
     * Submits an invoice template.
     */
    public SubmitInvoiceTemplateResponse submitInvoiceTemplate(SubmitInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = ListPendingSupplierRatingsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingSupplierRatingsRequest.Filter.newBuilder()
                        .setRegion(request.getInvoiceTemplate().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(suppliersSupplierRatingApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingSupplierRatings(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitInvoiceTemplate", request.getInvoiceTemplate().getInvoiceTemplateId());
        return SubmitInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setBillingAddress(request.getInvoiceTemplate().getBillingAddress())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Approves an invoice template.
     */
    public ApproveInvoiceTemplateResponse approveInvoiceTemplate(ApproveInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        List<RetryAuditTrailExportResponse> results0 = request.getInvoiceTemplate().getLinesList().stream()
                .map(element -> RetryAuditTrailExportRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .addLines(AuditTrailLine.newBuilder()
                                .setTax(AuditTrailLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryAuditTrailExport(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? RetryAuditTrailExportResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = RetryRiskRegisterWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setCountryCode(request.getInvoiceTemplate().getBillingAddress().getCountryCode())
                                                .setRecipient(reply0.getAuditTrail().getBillingAddress().getRecipient())
                                                .setRegion(reply0.getAuditTrail().getBillingAddress().getRegion())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.risk.v1.RiskRegisterWorkflowService/RetryRiskRegisterWorkflow", outbound1);
        var reply1 = riskRiskRegisterWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryRiskRegisterWorkflow(command1.payload());

        var outbound2 = RejectDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getAuditTrail().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> assetsDisposalApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectDisposal(value));
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveInvoiceTemplate", request.getInvoiceTemplate().getInvoiceTemplateId());
        return ApproveInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getDisposal().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getRiskRegister().getLocation().getLinesList())
                                .build())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .setIssuedAt(request.getInvoiceTemplate().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Rejects an invoice template.
     */
    public RejectInvoiceTemplateResponse rejectInvoiceTemplate(RejectInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = SubmitDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitDisposalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .setCustomer(FixedAssetRef.newBuilder()
                                .setDisplayName(request.getInvoiceTemplate().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<SubmitDisposalRequest, SubmitDisposalResponse>() {
            @Override
            public SubmitDisposalResponse apply(SubmitDisposalRequest input) {
                return assetsDisposalApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitDisposal(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ExplainHouseholdCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(request.getInvoiceTemplate().getAudit().getCreatedAt())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<ExplainHouseholdCalculationResponse>();
        events.publishEvent(new RejectInvoiceTemplateStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectInvoiceTemplate", request.getInvoiceTemplate().getInvoiceTemplateId());
        return RejectInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getDisposal().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(request.getInvoiceTemplate().getBillingAddress())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setOutcome(RejectInvoiceTemplateResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Escalates an invoice template.
     */
    public EscalateInvoiceTemplateResponse escalateInvoiceTemplate(EscalateInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = RpcSupport.unpack(Any.pack(GetInvoiceTemplateRulesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplateId(request.getInvoiceTemplate().getInvoiceTemplateId())
                .build()), GetInvoiceTemplateRulesRequest.class);
        var reply0 = invoicingInvoiceTemplateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getInvoiceTemplateRules(outbound0);

        var outbound1 = ApproveStressTestRequest.newBuilder()
                .setContext(request.getContext())
                .setStressTest(StressTest.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getInvoiceTemplate().getBillingAddress().getPostalCode())
                                .build())
                        .setCard(StressTestCardSettlement.newBuilder()
                                .setNetwork(reply0.getInvoiceTemplate().getCard().getNetwork())
                                .build())
                        .build())
                .build();
        Function<ApproveStressTestRequest, ApproveStressTestResponse> operation1 = riskStressTestApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::approveStressTest;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(BrowseHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseHouseholdsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoiceTemplate().getAudit().getCreatedAt())
                        .setOwnerId(reply1.getStressTest().getCounterparty().getCustomerId())
                        .setRegion(reply0.getInvoiceTemplate().getBillingAddress().getRegion())
                        .build())
                .build()), BrowseHouseholdsRequest.class);
        var reply2 = customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseHouseholds(outbound2);

        var outbound3 = (LintCustomerSegmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getStressTest().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply1.getStressTest().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setDetails(LintCustomerSegmentsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getStressTest().getCounterparty().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply3 = customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintCustomerSegments(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateInvoiceTemplate", request.getInvoiceTemplate().getNumber());
        return EscalateInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .setNotes(reply1.getStressTest().getNotes())
                        .setTotal(reply0.getInvoiceTemplate().getTotal())
                        .build())
                .setOutcome(EscalateInvoiceTemplateResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply3.getOutcome().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists invoice templates a page at a time.
     */
    public ListPendingInvoiceTemplatesResponse listPendingInvoiceTemplates(ListPendingInvoiceTemplatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingInvoiceTemplates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getContext().getTraceId(), "ListPendingInvoiceTemplates");

        var outbound0 = mapper.listPendingInvoiceTemplatesStage0(request);
        var reply0 = customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findHouseholds(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingInvoiceTemplates", request.getPage().getPageToken());
        return ListPendingInvoiceTemplatesResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply0.getPageInfo().getNextPageToken())
                        .build())
                .build();
    }

    /**
     * Delegates an invoice template.
     */
    public DelegateInvoiceTemplateApprovalResponse delegateInvoiceTemplateApproval(DelegateInvoiceTemplateApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateInvoiceTemplateApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = SuggestDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestDocumentsRequest.Filter.newBuilder()
                        .setRegion(request.getInvoiceTemplate().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(documentsDocumentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestDocuments(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = NormalizeEInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getInvoiceTemplate().getBillingAddress().getLinesList())
                                .build())
                        .setCounterparty(InvoiceBatchRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        RpcPort<NormalizeEInvoiceRequest, NormalizeEInvoiceResponse> port1 = RpcPort.adapt(invoicingEInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeEInvoice);
        var reply1 = port1.exchange(outbound1);

        List<RecalculateInvoiceBatchesResponse> results2 = request.getInvoiceTemplate().getAttachmentsList().stream()
                .map(element -> RecalculateInvoiceBatchesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .addAttachments(InvoiceBatchAttachment.newBuilder()
                                .setFileName(element.getFileName())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateInvoiceBatches(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? RecalculateInvoiceBatchesResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateInvoiceTemplateApproval", request.getInvoiceTemplate().getInvoiceTemplateId());
        return DelegateInvoiceTemplateApprovalResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setDueAt(request.getInvoiceTemplate().getDueAt())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .setIssuedAt(reply1.getEInvoice().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalls an invoice template.
     */
    public RecallInvoiceTemplateResponse recallInvoiceTemplate(RecallInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var builder0 = ResolveCustomersRequest.newBuilder();
        builder0.setContext(request.getContext());
        var outbound0 = builder0.build();
        var reply0 = customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveCustomers(outbound0);

        var outbound1 = RejectInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getInvoiceTemplate().getBillingAddress().getRegion())
                                .build())
                        .setCard(InvoiceBatchCardSettlement.newBuilder()
                                .setHolderName(switch (request.getInvoiceTemplate().getSettlementCase()) {
                                    case CARD -> request.getInvoiceTemplate().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = invoicingInvoiceBatchApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectInvoiceBatch(outbound1);

        List<CalculateInvoiceBatchResponse> results2 = request.getInvoiceTemplate().getLinesList().stream()
                .map(element -> CalculateInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .addLines(InvoiceBatchLine.newBuilder()
                                .setSku(element.getSku())
                                .setTax(InvoiceBatchLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .setUnitPrice(element.getUnitPrice())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateInvoiceBatch(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? CalculateInvoiceBatchResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallInvoiceTemplate", request.getInvoiceTemplate().getInvoiceTemplateId());
        return RecallInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getInvoiceBatch().getAudit().getUpdatedBy())
                                .build())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .putAllMetadata(request.getInvoiceTemplate().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an invoice template.
     */
    public GetInvoiceTemplateApprovalStatusResponse getInvoiceTemplateApprovalStatus(GetInvoiceTemplateApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetInvoiceTemplateApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getInvoiceTemplateId(), "GetInvoiceTemplateApprovalStatus");

        var builder0 = RenewInvoiceTemplateRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(RenewInvoiceTemplateRequest.Details.newBuilder()
                .setNewOwner(UserRef.newBuilder()
                        .setUserId(request.getContext().getActor().getUserId())
                        .build())
                .build());
        builder0.setInvoiceTemplateId(request.getInvoiceTemplateId());
        var outbound0 = builder0.build();
        var reply0 = invoicingInvoiceTemplateLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).renewInvoiceTemplate(outbound0);

        var outbound1 = RecallInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setCard(InvoiceBatchCardSettlement.newBuilder()
                                .setNetwork(switch (reply0.getInvoiceTemplate().getSettlementCase()) {
                                    case CARD -> reply0.getInvoiceTemplate().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = invoicingInvoiceBatchApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallInvoiceBatch(outbound1);
        return GetInvoiceTemplateApprovalStatusResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getInvoiceBatch().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(reply0.getInvoiceTemplate().getCounterparty().getCustomerId())
                                .build())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .build();
    }

    /**
     * Comments an invoice template.
     */
    public CommentInvoiceTemplateApprovalResponse commentInvoiceTemplateApproval(CommentInvoiceTemplateApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentInvoiceTemplateApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = RecallDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallDisposalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getInvoiceTemplate().getCounterparty().getDisplayName())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .setDisposal(Disposal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getInvoiceTemplate().getAudit().getUpdatedBy())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getInvoiceTemplate().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = assetsDisposalApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallDisposal(outbound0);

        var outbound1 = RecalculateCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply0.getDisposal().getRequester().getPhone())
                                .build())
                        .build())
                .setDetails(RecalculateCustomersRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getInvoiceTemplate().getBillingAddress().getPostalCode())
                                .setRecipient(request.getInvoiceTemplate().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCustomers(value)))
                .collect(Collectors.toList()).getFirst();

        List<ListNotificationsResponse> results2 = request.getInvoiceTemplate().getLinesList().stream()
                .map(element -> ListNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListNotificationsRequest.Filter.newBuilder()
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getInvoiceTemplate().getBillingAddress().getRegion())
                        .build())
                .build())
                .flatMap(value -> Stream.of(notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listNotifications(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? ListNotificationsResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentInvoiceTemplateApproval", request.getInvoiceTemplate().getInvoiceTemplateId());
        return CommentInvoiceTemplateApprovalResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getCustomer().getPrimaryAddress().getCountryCode())
                                .setRecipient(reply1.getCustomer().getPrimaryAddress().getRecipient())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getDisposal().getAssignee().getDisplayName())
                                .build())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .setNumber(request.getInvoiceTemplate().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
