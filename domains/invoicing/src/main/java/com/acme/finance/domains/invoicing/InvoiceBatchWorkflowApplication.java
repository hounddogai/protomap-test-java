package com.acme.finance.domains.invoicing;

import com.acme.admissions.v1.Admission;
import com.acme.admissions.v1.AdmissionCalculationServiceGrpc;
import com.acme.admissions.v1.EstimateAdmissionRequest;
import com.acme.admissions.v1.EstimateAdmissionResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.PreviewAuditTrailRequest;
import com.acme.audit.v1.PreviewAuditTrailResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.BrowseCustomersRequest;
import com.acme.customers.v1.BrowseCustomersResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationRequest;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.AdvanceInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.AdvanceInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.CancelInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.CancelInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.EscalateInvoiceBatchRequest;
import com.acme.invoicing.v1.EscalateInvoiceBatchResponse;
import com.acme.invoicing.v1.EscalateInvoiceTemplateRequest;
import com.acme.invoicing.v1.EscalateInvoiceTemplateResponse;
import com.acme.invoicing.v1.FindInvoiceTemplatesRequest;
import com.acme.invoicing.v1.FindInvoiceTemplatesResponse;
import com.acme.invoicing.v1.InspectInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.InspectInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchApprovalServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchCardSettlement;
import com.acme.invoicing.v1.InvoiceBatchLine;
import com.acme.invoicing.v1.InvoiceBatchLineTax;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateApprovalServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateCardSettlement;
import com.acme.invoicing.v1.InvoiceTemplateLine;
import com.acme.invoicing.v1.InvoiceTemplateLineTax;
import com.acme.invoicing.v1.InvoiceTemplateQueryServiceGrpc;
import com.acme.invoicing.v1.ListInvoiceBatchWorkflowsRequest;
import com.acme.invoicing.v1.ListInvoiceBatchWorkflowsResponse;
import com.acme.invoicing.v1.PauseInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.PauseInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.RejectInvoiceBatchRequest;
import com.acme.invoicing.v1.RejectInvoiceBatchResponse;
import com.acme.invoicing.v1.ResumeInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.ResumeInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.RetryInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.RetryInvoiceBatchWorkflowResponse;
import com.acme.invoicing.v1.StartInvoiceBatchWorkflowRequest;
import com.acme.invoicing.v1.StartInvoiceBatchWorkflowResponse;
import com.acme.procurement.v1.CommentGoodsReceiptApprovalRequest;
import com.acme.procurement.v1.CommentGoodsReceiptApprovalResponse;
import com.acme.procurement.v1.GoodsReceiptApprovalServiceGrpc;
import com.acme.procurement.v1.ListPurchaseRequisitionWorkflowsRequest;
import com.acme.procurement.v1.ListPurchaseRequisitionWorkflowsResponse;
import com.acme.procurement.v1.PurchaseRequisitionWorkflowServiceGrpc;
import com.acme.search.v1.CreateSearchIndexRequest;
import com.acme.search.v1.CreateSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes an invoice batch from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class InvoiceBatchWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceBatchWorkflowApplication.class);

    private final AdmissionCalculationServiceGrpc.AdmissionCalculationServiceBlockingStub admissionsAdmissionCalculationServiceBlocking;

    private final AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceFutureStub customersCustomerQueryServiceFuture;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final InvoiceBatchApprovalServiceGrpc.InvoiceBatchApprovalServiceBlockingStub invoicingInvoiceBatchApprovalServiceBlocking;

    private final InvoiceTemplateApprovalServiceGrpc.InvoiceTemplateApprovalServiceBlockingStub invoicingInvoiceTemplateApprovalServiceBlocking;

    private final InvoiceTemplateQueryServiceGrpc.InvoiceTemplateQueryServiceBlockingStub invoicingInvoiceTemplateQueryServiceBlocking;

    private final GoodsReceiptApprovalServiceGrpc.GoodsReceiptApprovalServiceFutureStub procurementGoodsReceiptApprovalServiceFuture;

    private final PurchaseRequisitionWorkflowServiceGrpc.PurchaseRequisitionWorkflowServiceBlockingStub procurementPurchaseRequisitionWorkflowServiceBlocking;

    private final InvoicingRepository repository;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    public InvoiceBatchWorkflowApplication(
            AdmissionCalculationServiceGrpc.AdmissionCalculationServiceBlockingStub admissionsAdmissionCalculationServiceBlocking,
            AuditTrailValidationServiceGrpc.AuditTrailValidationServiceBlockingStub auditAuditTrailValidationServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceFutureStub customersCustomerQueryServiceFuture,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            InvoiceBatchApprovalServiceGrpc.InvoiceBatchApprovalServiceBlockingStub invoicingInvoiceBatchApprovalServiceBlocking,
            InvoiceTemplateApprovalServiceGrpc.InvoiceTemplateApprovalServiceBlockingStub invoicingInvoiceTemplateApprovalServiceBlocking,
            InvoiceTemplateQueryServiceGrpc.InvoiceTemplateQueryServiceBlockingStub invoicingInvoiceTemplateQueryServiceBlocking,
            GoodsReceiptApprovalServiceGrpc.GoodsReceiptApprovalServiceFutureStub procurementGoodsReceiptApprovalServiceFuture,
            PurchaseRequisitionWorkflowServiceGrpc.PurchaseRequisitionWorkflowServiceBlockingStub procurementPurchaseRequisitionWorkflowServiceBlocking,
            InvoicingRepository repository,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking) {
        this.admissionsAdmissionCalculationServiceBlocking = admissionsAdmissionCalculationServiceBlocking;
        this.auditAuditTrailValidationServiceBlocking = auditAuditTrailValidationServiceBlocking;
        this.customersCustomerQueryServiceFuture = customersCustomerQueryServiceFuture;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.invoicingInvoiceBatchApprovalServiceBlocking = invoicingInvoiceBatchApprovalServiceBlocking;
        this.invoicingInvoiceTemplateApprovalServiceBlocking = invoicingInvoiceTemplateApprovalServiceBlocking;
        this.invoicingInvoiceTemplateQueryServiceBlocking = invoicingInvoiceTemplateQueryServiceBlocking;
        this.procurementGoodsReceiptApprovalServiceFuture = procurementGoodsReceiptApprovalServiceFuture;
        this.procurementPurchaseRequisitionWorkflowServiceBlocking = procurementPurchaseRequisitionWorkflowServiceBlocking;
        this.repository = repository;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
    }

    /**
     * Starts an invoice batch.
     */
    public StartInvoiceBatchWorkflowResponse startInvoiceBatchWorkflow(StartInvoiceBatchWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartInvoiceBatchWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = FindInvoiceTemplatesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindInvoiceTemplatesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoiceBatch().getAudit().getCreatedAt())
                        .putAllLabels(request.getDetails().getParametersMap())
                        .setOwnerId(request.getInvoiceBatch().getInvoiceBatchId())
                        .setRegion(request.getInvoiceBatch().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> invoicingInvoiceTemplateQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findInvoiceTemplates(value)).orElseThrow();

        var builder1 = ListPurchaseRequisitionWorkflowsRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setFilter(ListPurchaseRequisitionWorkflowsRequest.Filter.newBuilder()
                .setCreatedAfter(request.getInvoiceBatch().getAudit().getUpdatedAt())
                .setRegion(request.getInvoiceBatch().getBillingAddress().getRegion())
                .build());
        var outbound1 = builder1.build();
        var reply1 = procurementPurchaseRequisitionWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPurchaseRequisitionWorkflows(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartInvoiceBatchWorkflow", request.getInvoiceBatch().getNumber());
        return StartInvoiceBatchWorkflowResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .setNotes(request.getInvoiceBatch().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Advances an invoice batch.
     */
    public AdvanceInvoiceBatchWorkflowResponse advanceInvoiceBatchWorkflow(AdvanceInvoiceBatchWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceInvoiceBatchWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        List<RejectInvoiceBatchResponse> results0 = request.getInvoiceBatch().getLinesList().stream()
                .map(element -> RejectInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .addLines(InvoiceBatchLine.newBuilder()
                                .setTax(InvoiceBatchLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(invoicingInvoiceBatchApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectInvoiceBatch(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? RejectInvoiceBatchResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceInvoiceBatchWorkflow", request.getInvoiceBatch().getInvoiceBatchId());
        return AdvanceInvoiceBatchWorkflowResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .setNumber(request.getInvoiceBatch().getNumber())
                        .build())
                .build();
    }

    /**
     * Cancels an invoice batch.
     */
    public CancelInvoiceBatchWorkflowResponse cancelInvoiceBatchWorkflow(CancelInvoiceBatchWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelInvoiceBatchWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = EscalateInvoiceTemplateRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getInvoiceBatch().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getInvoiceBatch().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getInvoiceBatch().getBillingAddress().getCountryCode())
                                .build())
                        .setCard(InvoiceTemplateCardSettlement.newBuilder()
                                .setHolderName(switch (request.getInvoiceBatch().getSettlementCase()) {
                                    case CARD -> request.getInvoiceBatch().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = invoicingInvoiceTemplateApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateInvoiceTemplate(outbound0);

        var outbound1 = BrowseCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseCustomersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getInvoiceTemplate().getDueAt())
                        .setOwnerId(reply0.getOutcome().getReferenceId())
                        .setRegion(request.getInvoiceBatch().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending1 = customersCustomerQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).browseCustomers(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelInvoiceBatchWorkflow", request.getInvoiceBatch().getInvoiceBatchId());
        return CancelInvoiceBatchWorkflowResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setDueAt(request.getInvoiceBatch().getDueAt())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .build())
                .setOutcome(CancelInvoiceBatchWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Retrys an invoice batch.
     */
    public RetryInvoiceBatchWorkflowResponse retryInvoiceBatchWorkflow(RetryInvoiceBatchWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryInvoiceBatchWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = EstimateAdmissionRequest.newBuilder()
                .setAdmission(Admission.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(EstimateAdmissionRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getInvoiceBatch().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> admissionsAdmissionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateAdmission(value));

        var outbound1 = JsonCodec.roundTrip(PreviewAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getInvoiceBatch().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getInvoiceBatch().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), PreviewAuditTrailRequest.class);
        var reply1 = auditAuditTrailValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAuditTrail(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryInvoiceBatchWorkflow", request.getInvoiceBatch().getInvoiceBatchId());
        return RetryInvoiceBatchWorkflowResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .setNumber(request.getInvoiceBatch().getNumber())
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .setOutcome(RetryInvoiceBatchWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Pauses an invoice batch.
     */
    public PauseInvoiceBatchWorkflowResponse pauseInvoiceBatchWorkflow(PauseInvoiceBatchWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseInvoiceBatchWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = EscalateInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getInvoiceBatch().getBillingAddress().getCountryCode())
                                .build())
                        .setCard(InvoiceBatchCardSettlement.newBuilder()
                                .setToken(switch (request.getInvoiceBatch().getSettlementCase()) {
                                    case CARD -> request.getInvoiceBatch().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(request.getInvoiceBatch().getCounterparty().getCustomerId())
                                .setDisplayName(request.getInvoiceBatch().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = invoicingInvoiceBatchApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateInvoiceBatch(outbound0);

        List<EscalateInvoiceTemplateResponse> results1 = request.getInvoiceBatch().getLinesList().stream()
                .map(element -> EscalateInvoiceTemplateRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .addLines(InvoiceTemplateLine.newBuilder()
                                .setTax(InvoiceTemplateLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(invoicingInvoiceTemplateApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateInvoiceTemplate(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? EscalateInvoiceTemplateResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseInvoiceBatchWorkflow", request.getInvoiceBatch().getInvoiceBatchId());
        return PauseInvoiceBatchWorkflowResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .setNotes(reply0.getInvoiceBatch().getNotes())
                        .setTotal(request.getInvoiceBatch().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Resumes an invoice batch.
     */
    public ResumeInvoiceBatchWorkflowResponse resumeInvoiceBatchWorkflow(ResumeInvoiceBatchWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeInvoiceBatchWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceBatch(request.getContext().getTenantId(), request.getInvoiceBatch());

        var outbound0 = ExplainCustomerSegmentCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getInvoiceBatch().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getInvoiceBatch().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCustomerSegmentCalculation(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceBatch().getInvoiceBatchId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeInvoiceBatchWorkflow", request.getInvoiceBatch().getInvoiceBatchId());
        return ResumeInvoiceBatchWorkflowResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setDueAt(request.getInvoiceBatch().getDueAt())
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .build())
                .setOutcome(ResumeInvoiceBatchWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns an invoice batch.
     */
    public InspectInvoiceBatchWorkflowResponse inspectInvoiceBatchWorkflow(InspectInvoiceBatchWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectInvoiceBatchWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceBatch", request.getInvoiceBatchId(), "InspectInvoiceBatchWorkflow");

        var outbound0 = CommentGoodsReceiptApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentGoodsReceiptApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var pending0 = procurementGoodsReceiptApprovalServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).commentGoodsReceiptApproval(outbound0);
        var reply0 = RpcSupport.await(pending0);
        return InspectInvoiceBatchWorkflowResponse.newBuilder()
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setInvoiceBatchId(request.getInvoiceBatchId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists invoice batches a page at a time.
     */
    public ListInvoiceBatchWorkflowsResponse listInvoiceBatchWorkflows(ListInvoiceBatchWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListInvoiceBatchWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceBatch", request.getContext().getTraceId(), "ListInvoiceBatchWorkflows");

        var outbound0 = CreateSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        RpcPort<CreateSearchIndexRequest, CreateSearchIndexResponse> port0 = RpcPort.adapt(searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::createSearchIndex);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListInvoiceBatchWorkflows", request.getPage().getPageToken());
        return ListInvoiceBatchWorkflowsResponse.newBuilder()
                .addInvoiceBatches(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getSearchIndex().getAddress().getPostalCode())
                                .setRecipient(reply0.getSearchIndex().getAddress().getRecipient())
                                .build())
                        .build())
                .build();
    }
}
