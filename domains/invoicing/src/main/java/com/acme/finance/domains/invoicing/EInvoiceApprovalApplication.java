package com.acme.finance.domains.invoicing;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.audit.v1.AnnotateAuditEventVersionRequest;
import com.acme.audit.v1.AnnotateAuditEventVersionResponse;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionRequest;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.PreviewAuditEventRequest;
import com.acme.audit.v1.PreviewAuditEventResponse;
import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.ImportVariancesRequest;
import com.acme.budgeting.v1.ImportVariancesResponse;
import com.acme.budgeting.v1.SyncForecastsRequest;
import com.acme.budgeting.v1.SyncForecastsResponse;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CompareCustomerSegmentOptionsRequest;
import com.acme.customers.v1.CompareCustomerSegmentOptionsResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerNationalId;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.EstimateCustomerSegmentRequest;
import com.acme.customers.v1.EstimateCustomerSegmentResponse;
import com.acme.customers.v1.PreviewCustomerRequest;
import com.acme.customers.v1.PreviewCustomerResponse;
import com.acme.customers.v1.ScoreCustomerRequest;
import com.acme.customers.v1.ScoreCustomerResponse;
import com.acme.documents.v1.BatchGetDocumentsRequest;
import com.acme.documents.v1.BatchGetDocumentsResponse;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.giftcards.v1.ExportGiftCardsRequest;
import com.acme.giftcards.v1.ExportGiftCardsResponse;
import com.acme.giftcards.v1.GiftCardQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.StreamPremiumMetricsRequest;
import com.acme.insurance.v1.StreamPremiumMetricsResponse;
import com.acme.invoicing.v1.ApproveEInvoiceRequest;
import com.acme.invoicing.v1.ApproveEInvoiceResponse;
import com.acme.invoicing.v1.CommentEInvoiceApprovalRequest;
import com.acme.invoicing.v1.CommentEInvoiceApprovalResponse;
import com.acme.invoicing.v1.CommentInvoiceBatchApprovalRequest;
import com.acme.invoicing.v1.CommentInvoiceBatchApprovalResponse;
import com.acme.invoicing.v1.DelegateEInvoiceApprovalRequest;
import com.acme.invoicing.v1.DelegateEInvoiceApprovalResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EscalateEInvoiceRequest;
import com.acme.invoicing.v1.EscalateEInvoiceResponse;
import com.acme.invoicing.v1.ExportInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ExportInvoiceTemplatesResponse;
import com.acme.invoicing.v1.GetEInvoiceApprovalStatusRequest;
import com.acme.invoicing.v1.GetEInvoiceApprovalStatusResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchApprovalServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchRef;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateCardSettlement;
import com.acme.invoicing.v1.InvoiceTemplateQueryServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.LintInvoiceTemplatesRequest;
import com.acme.invoicing.v1.LintInvoiceTemplatesResponse;
import com.acme.invoicing.v1.ListPendingEInvoicesRequest;
import com.acme.invoicing.v1.ListPendingEInvoicesResponse;
import com.acme.invoicing.v1.RecallEInvoiceRequest;
import com.acme.invoicing.v1.RecallEInvoiceResponse;
import com.acme.invoicing.v1.RecallInvoiceBatchRequest;
import com.acme.invoicing.v1.RecallInvoiceBatchResponse;
import com.acme.invoicing.v1.RejectEInvoiceRequest;
import com.acme.invoicing.v1.RejectEInvoiceResponse;
import com.acme.invoicing.v1.SubmitEInvoiceRequest;
import com.acme.invoicing.v1.SubmitEInvoiceResponse;
import com.acme.lending.v1.CancelLoanApplicationWorkflowRequest;
import com.acme.lending.v1.CancelLoanApplicationWorkflowResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationLineTax;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.patients.v1.EstimatePatientRequest;
import com.acme.patients.v1.EstimatePatientResponse;
import com.acme.patients.v1.Patient;
import com.acme.patients.v1.PatientCalculationServiceGrpc;
import com.acme.patients.v1.PatientPassport;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Routes e invoices through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class EInvoiceApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(EInvoiceApprovalApplication.class);

    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync;

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking;

    private final VarianceImportServiceGrpc.VarianceImportServiceStub budgetingVarianceImportServiceAsync;

    private final ChannelRegistry channels;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final GiftCardQueryServiceGrpc.GiftCardQueryServiceBlockingStub giftcardsGiftCardQueryServiceBlocking;

    private final PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking;

    private final InvoiceBatchApprovalServiceGrpc.InvoiceBatchApprovalServiceBlockingStub invoicingInvoiceBatchApprovalServiceBlocking;

    private final InvoiceTemplateQueryServiceGrpc.InvoiceTemplateQueryServiceBlockingStub invoicingInvoiceTemplateQueryServiceBlocking;

    private final InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceBlockingStub invoicingInvoiceTemplateValidationServiceBlocking;

    private final LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking;

    private final EInvoiceApprovalRequestMapper mapper;

    private final InvoicingRepository repository;

    public EInvoiceApprovalApplication(
            DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync,
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            AuditEventValidationServiceGrpc.AuditEventValidationServiceBlockingStub auditAuditEventValidationServiceBlocking,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking,
            VarianceImportServiceGrpc.VarianceImportServiceStub budgetingVarianceImportServiceAsync,
            ChannelRegistry channels,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            GiftCardQueryServiceGrpc.GiftCardQueryServiceBlockingStub giftcardsGiftCardQueryServiceBlocking,
            PremiumReportingServiceGrpc.PremiumReportingServiceBlockingStub insurancePremiumReportingServiceBlocking,
            InvoiceBatchApprovalServiceGrpc.InvoiceBatchApprovalServiceBlockingStub invoicingInvoiceBatchApprovalServiceBlocking,
            InvoiceTemplateQueryServiceGrpc.InvoiceTemplateQueryServiceBlockingStub invoicingInvoiceTemplateQueryServiceBlocking,
            InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceBlockingStub invoicingInvoiceTemplateValidationServiceBlocking,
            LoanApplicationWorkflowServiceGrpc.LoanApplicationWorkflowServiceBlockingStub lendingLoanApplicationWorkflowServiceBlocking,
            EInvoiceApprovalRequestMapper mapper,
            InvoicingRepository repository) {
        this.assetsDisposalSyncServiceAsync = assetsDisposalSyncServiceAsync;
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.auditAuditEventValidationServiceBlocking = auditAuditEventValidationServiceBlocking;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.budgetingForecastSyncServiceBlocking = budgetingForecastSyncServiceBlocking;
        this.budgetingVarianceImportServiceAsync = budgetingVarianceImportServiceAsync;
        this.channels = channels;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.giftcardsGiftCardQueryServiceBlocking = giftcardsGiftCardQueryServiceBlocking;
        this.insurancePremiumReportingServiceBlocking = insurancePremiumReportingServiceBlocking;
        this.invoicingInvoiceBatchApprovalServiceBlocking = invoicingInvoiceBatchApprovalServiceBlocking;
        this.invoicingInvoiceTemplateQueryServiceBlocking = invoicingInvoiceTemplateQueryServiceBlocking;
        this.invoicingInvoiceTemplateValidationServiceBlocking = invoicingInvoiceTemplateValidationServiceBlocking;
        this.lendingLoanApplicationWorkflowServiceBlocking = lendingLoanApplicationWorkflowServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
    }

    /**
     * Submits an e invoice.
     */
    public SubmitEInvoiceResponse submitEInvoice(SubmitEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = AnnotateAuditEventVersionRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getEInvoice().getCounterparty().getDisplayName())
                                .setNumber(request.getEInvoice().getCounterparty().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(AnnotateAuditEventVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setReason(request.getReason())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).annotateAuditEventVersion(value)).orElseThrow();

        var outbound1 = mapper.submitEInvoiceStage1(request, reply0);
        var reply1 = auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordRetentionPolicyVersion(outbound1);

        var outbound2 = EstimatePatientRequest.newBuilder()
                .setContext(request.getContext())
                .setPatient(Patient.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getEInvoice().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getRetentionPolicy().getOwner().getEmail())
                                .build())
                        .setPassport(PatientPassport.newBuilder()
                                .setNumber(request.getEInvoice().getCounterparty().getNumber())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.patients.v1.PatientCalculationService");
        var selected2 = PatientCalculationServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.estimatePatient(outbound2);

        var outbound3 = ImportVariancesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(ImportVariancesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(ImportVariancesResponse.getDefaultInstance());
        var input3 = budgetingVarianceImportServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).importVariances(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamDisposalChangesRequest.newBuilder()
                .setChange(Disposal.newBuilder()
                        .setAssignee(request.getDetails().getApprover())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getEInvoice().getAudit().getCreatedAt())
                                .setCreatedBy(request.getEInvoice().getAudit().getCreatedBy())
                                .setEtag(request.getEInvoice().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamDisposalChangesResponse.getDefaultInstance());
        var input4 = assetsDisposalSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitEInvoice", request.getEInvoice().getEInvoiceId());
        return SubmitEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getPatient().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(InvoiceBatchRef.newBuilder()
                                .setNumber(reply2.getPatient().getNationalId().getNumber())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .setOutcome(SubmitEInvoiceResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Approves an e invoice.
     */
    public ApproveEInvoiceResponse approveEInvoice(ApproveEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = ExportInvoiceTemplatesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportInvoiceTemplatesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getEInvoice().getAudit().getCreatedAt())
                        .setRegion(request.getEInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = invoicingInvoiceTemplateQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportInvoiceTemplates(outbound0);
        var reply0 = ExportInvoiceTemplatesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = EstimateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getEInvoice().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setDetails(EstimateCustomerSegmentRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getEInvoice().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<EstimateCustomerSegmentRequest, EstimateCustomerSegmentResponse>() {
            @Override
            public EstimateCustomerSegmentResponse apply(EstimateCustomerSegmentRequest input) {
                return customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCustomerSegment(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveEInvoice", request.getEInvoice().getEInvoiceId());
        return ApproveEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getCustomerSegment().getAudit().getEtag())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setNumber(request.getEInvoice().getNumber())
                        .build())
                .build();
    }

    /**
     * Rejects an e invoice.
     */
    public RejectEInvoiceResponse rejectEInvoice(RejectEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = JsonCodec.roundTrip(CompareCustomerSegmentOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getEInvoice().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getEInvoice().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .setDetails(CompareCustomerSegmentOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getEInvoice().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build(), CompareCustomerSegmentOptionsRequest.class);
        var reply0 = customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCustomerSegmentOptions(outbound0);

        var outbound1 = ExportGiftCardsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportGiftCardsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getEInvoice().getAudit().getCreatedAt())
                        .setRegion(request.getEInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = giftcardsGiftCardQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportGiftCards(outbound1);
        var reply1 = ExportGiftCardsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var results2 = new ArrayList<CancelLoanApplicationWorkflowResponse>();
        for (var element : request.getEInvoice().getLinesList()) {
            CancelLoanApplicationWorkflowRequest itemRequest = CancelLoanApplicationWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .addLines(LoanApplicationLine.newBuilder()
                                .setTax(LoanApplicationLineTax.newBuilder()
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build();
            results2.add(lendingLoanApplicationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelLoanApplicationWorkflow(itemRequest));
        }
        var reply2 = results2.isEmpty() ? CancelLoanApplicationWorkflowResponse.getDefaultInstance()
                : results2.getLast();

        var outbound3 = PreviewCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getLoanApplication().getAudit().getCreatedAt())
                                .setEtag(reply2.getLoanApplication().getAudit().getEtag())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(request.getEInvoice().getCounterparty().getDisplayName())
                                .setEmail(reply2.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage3Adapter {
            PreviewCustomerResponse fetch(PreviewCustomerRequest input) {
                return customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewCustomer(input);
            }
        }
        var reply3 = new Stage3Adapter().fetch(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectEInvoice", request.getEInvoice().getEInvoiceId());
        return RejectEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCustomerSegment().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply3.getCustomer().getPrimaryAddress().getLinesList())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setNotes(request.getEInvoice().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Escalates an e invoice.
     */
    public EscalateEInvoiceResponse escalateEInvoice(EscalateEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = CommentInvoiceBatchApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentInvoiceBatchApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.invoicing.v1.InvoiceBatchApprovalService/CommentInvoiceBatchApproval", outbound0);
        var reply0 = invoicingInvoiceBatchApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentInvoiceBatchApproval(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateEInvoice", request.getEInvoice().getEInvoiceId());
        return EscalateEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setCounterparty(InvoiceBatchRef.newBuilder()
                                .setInvoiceBatchId(reply0.getInvoiceBatch().getInvoiceBatchId())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setTotal(request.getEInvoice().getTotal())
                        .build())
                .build();
    }

    /**
     * Lists e invoices a page at a time.
     */
    public ListPendingEInvoicesResponse listPendingEInvoices(ListPendingEInvoicesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingEInvoices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getContext().getTraceId(), "ListPendingEInvoices");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingEInvoices", request.getPage().getPageToken());
        return ListPendingEInvoicesResponse.getDefaultInstance();
    }

    /**
     * Delegates an e invoice.
     */
    public DelegateEInvoiceApprovalResponse delegateEInvoiceApproval(DelegateEInvoiceApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateEInvoiceApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = BeanCopies.copy(BatchGetDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .build(), BatchGetDocumentsRequest.class);
        var reply0 = documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetDocuments(outbound0);

        var outbound1 = StreamPremiumMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamPremiumMetricsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getEInvoice().getAudit().getUpdatedAt())
                        .setRegion(request.getEInvoice().getBillingAddress().getRegion())
                        .build())
                .setSince(request.getEInvoice().getIssuedAt())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getApprover().getEmail())
                        .build())
                .build();
        var stream1 = insurancePremiumReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamPremiumMetrics(outbound1);
        var reply1 = StreamPremiumMetricsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = AnnotateRetentionPolicyVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AnnotateRetentionPolicyVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getEInvoice().getAudit().getCreatedBy())
                                .setEtag(request.getEInvoice().getAudit().getEtag())
                                .setUpdatedBy(request.getEInvoice().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        Function<AnnotateRetentionPolicyVersionRequest, AnnotateRetentionPolicyVersionResponse> operation2 = auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::annotateRetentionPolicyVersion;
        var reply2 = operation2.apply(outbound2);

        var outbound3 = PreviewAuditEventRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(reply2.getRetentionPolicy().getOwner().getDisplayName())
                                .setNumber(request.getEInvoice().getCounterparty().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewAuditEventRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply2.getRetentionPolicy().getOwner().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        RpcPort<PreviewAuditEventRequest, PreviewAuditEventResponse> port3 = RpcPort.adapt(auditAuditEventValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewAuditEvent);
        var reply3 = port3.exchange(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateEInvoiceApproval", request.getEInvoice().getEInvoiceId());
        return DelegateEInvoiceApprovalResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getRetentionPolicy().getAudit().getCreatedAt())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setNotes(request.getEInvoice().getNotes())
                        .build())
                .setOutcome(DelegateEInvoiceApprovalResponse.Outcome.newBuilder()
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalls an e invoice.
     */
    public RecallEInvoiceResponse recallEInvoice(RecallEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = RecallInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallInvoiceBatchRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getEInvoice().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> invoicingInvoiceBatchApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallInvoiceBatch(value));

        var outbound1 = LintInvoiceTemplatesRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getEInvoice().getAudit().getUpdatedAt())
                                .build())
                        .setCard(InvoiceTemplateCardSettlement.newBuilder()
                                .setHolderName(switch (reply0.getInvoiceBatch().getSettlementCase()) {
                                    case CARD -> reply0.getInvoiceBatch().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getInvoiceBatch().getCounterparty().getDisplayName())
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = invoicingInvoiceTemplateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintInvoiceTemplates(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallEInvoice", request.getEInvoice().getEInvoiceId());
        return RecallEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setEInvoiceId(request.getEInvoiceId())
                        .setNumber(request.getEInvoice().getNumber())
                        .setTotal(reply1.getInvoiceTemplate().getTotal())
                        .build())
                .setOutcome(RecallEInvoiceResponse.Outcome.newBuilder()
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns an e invoice.
     */
    public GetEInvoiceApprovalStatusResponse getEInvoiceApprovalStatus(GetEInvoiceApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetEInvoiceApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getEInvoiceId(), "GetEInvoiceApprovalStatus");

        var outbound0 = SyncForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SyncForecastsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.budgeting.v1.ForecastSyncService/SyncForecasts", outbound0);
        var reply0 = budgetingForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).syncForecasts(command0.payload());
        return GetEInvoiceApprovalStatusResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setEInvoiceId(request.getEInvoiceId())
                        .setNumber(reply0.getForecast().getNumber())
                        .build())
                .build();
    }

    /**
     * Comments an e invoice.
     */
    public CommentEInvoiceApprovalResponse commentEInvoiceApproval(CommentEInvoiceApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentEInvoiceApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        List<ScoreCustomerResponse> results0 = request.getEInvoice().getLinesList().stream()
                .map(element -> ScoreCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .putAllAttributes(element.getAttributesMap())
                        .setNationalId(CustomerNationalId.newBuilder()
                                .setNumber(request.getEInvoice().getCounterparty().getNumber())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getEInvoice().getBillingAddress().getRecipient())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreCustomer(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ScoreCustomerResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentEInvoiceApproval", request.getEInvoice().getEInvoiceId());
        return CommentEInvoiceApprovalResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setEInvoiceId(request.getEInvoiceId())
                        .setNotes(request.getEInvoice().getNotes())
                        .build())
                .build();
    }
}
