package com.acme.finance.domains.billing;

import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.BatchGetAccessPoliciesRequest;
import com.acme.authorization.v1.BatchGetAccessPoliciesResponse;
import com.acme.billing.v1.ApproveCreditNoteRequest;
import com.acme.billing.v1.ApproveCreditNoteResponse;
import com.acme.billing.v1.BatchCreateBillingAccountsRequest;
import com.acme.billing.v1.BatchCreateBillingAccountsResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.billing.v1.BillingAccountCardFunding;
import com.acme.billing.v1.BillingAccountLimit;
import com.acme.billing.v1.BillingAccountServiceGrpc;
import com.acme.billing.v1.BillingAccountWorkflowServiceGrpc;
import com.acme.billing.v1.CommentCreditNoteApprovalRequest;
import com.acme.billing.v1.CommentCreditNoteApprovalResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteLine;
import com.acme.billing.v1.CreditNoteLineTax;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.GetCreditNoteApprovalStatusRequest;
import com.acme.billing.v1.GetCreditNoteApprovalStatusResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceApprovalServiceGrpc;
import com.acme.billing.v1.InvoiceCardSettlement;
import com.acme.billing.v1.InvoiceLine;
import com.acme.billing.v1.InvoiceLineTax;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.ListPendingCreditNotesRequest;
import com.acme.billing.v1.ListPendingCreditNotesResponse;
import com.acme.billing.v1.NormalizeCreditNoteRequest;
import com.acme.billing.v1.NormalizeCreditNoteResponse;
import com.acme.billing.v1.PauseBillingAccountWorkflowRequest;
import com.acme.billing.v1.PauseBillingAccountWorkflowResponse;
import com.acme.billing.v1.RecallInvoiceRequest;
import com.acme.billing.v1.RecallInvoiceResponse;
import com.acme.billing.v1.RejectBillingAccountRequest;
import com.acme.billing.v1.RejectBillingAccountResponse;
import com.acme.billing.v1.RejectCreditNoteRequest;
import com.acme.billing.v1.RejectCreditNoteResponse;
import com.acme.billing.v1.SubmitCreditNoteRequest;
import com.acme.billing.v1.SubmitCreditNoteResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.ExplainHouseholdDecisionRequest;
import com.acme.customers.v1.ExplainHouseholdDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.QuoteCustomerRequest;
import com.acme.customers.v1.QuoteCustomerResponse;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.FindDocumentsRequest;
import com.acme.documents.v1.FindDocumentsResponse;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryCardSettlement;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.ValidateJournalEntryRequest;
import com.acme.ledger.v1.ValidateJournalEntryResponse;
import java.util.ArrayList;
import java.util.List;
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
 * Routes credit notes through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class CreditNoteApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(CreditNoteApprovalApplication.class);

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceBlockingStub billingBillingAccountApprovalServiceBlocking;

    private final BillingAccountServiceGrpc.BillingAccountServiceStub billingBillingAccountServiceAsync;

    private final BillingAccountWorkflowServiceGrpc.BillingAccountWorkflowServiceBlockingStub billingBillingAccountWorkflowServiceBlocking;

    private final CreditNoteValidationServiceGrpc.CreditNoteValidationServiceBlockingStub billingCreditNoteValidationServiceBlocking;

    private final InvoiceApprovalServiceGrpc.InvoiceApprovalServiceBlockingStub billingInvoiceApprovalServiceBlocking;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync;

    private final JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking;

    private final CreditNoteApprovalRequestMapper mapper;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    public CreditNoteApprovalApplication(
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            BillingAccountApprovalServiceGrpc.BillingAccountApprovalServiceBlockingStub billingBillingAccountApprovalServiceBlocking,
            BillingAccountServiceGrpc.BillingAccountServiceStub billingBillingAccountServiceAsync,
            BillingAccountWorkflowServiceGrpc.BillingAccountWorkflowServiceBlockingStub billingBillingAccountWorkflowServiceBlocking,
            CreditNoteValidationServiceGrpc.CreditNoteValidationServiceBlockingStub billingCreditNoteValidationServiceBlocking,
            InvoiceApprovalServiceGrpc.InvoiceApprovalServiceBlockingStub billingInvoiceApprovalServiceBlocking,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            DocumentQueryServiceGrpc.DocumentQueryServiceBlockingStub documentsDocumentQueryServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync,
            JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking,
            CreditNoteApprovalRequestMapper mapper,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.billingBillingAccountApprovalServiceBlocking = billingBillingAccountApprovalServiceBlocking;
        this.billingBillingAccountServiceAsync = billingBillingAccountServiceAsync;
        this.billingBillingAccountWorkflowServiceBlocking = billingBillingAccountWorkflowServiceBlocking;
        this.billingCreditNoteValidationServiceBlocking = billingCreditNoteValidationServiceBlocking;
        this.billingInvoiceApprovalServiceBlocking = billingInvoiceApprovalServiceBlocking;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.documentsDocumentQueryServiceBlocking = documentsDocumentQueryServiceBlocking;
        this.insuranceUnderwritingSyncServiceAsync = insuranceUnderwritingSyncServiceAsync;
        this.ledgerJournalEntryValidationServiceBlocking = ledgerJournalEntryValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Submits a credit note.
     */
    public SubmitCreditNoteResponse submitCreditNote(SubmitCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        List<NormalizeCreditNoteResponse> results0 = request.getCreditNote().getLinesList().stream()
                .map(element -> NormalizeCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .addLines(CreditNoteLine.newBuilder()
                                .putAllAttributes(element.getAttributesMap())
                                .setTax(CreditNoteLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .build())
                                .setUnitPrice(element.getUnitPrice())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(billingCreditNoteValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeCreditNote(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? NormalizeCreditNoteResponse.getDefaultInstance()
                : results0.getLast();

        var results1 = new ArrayList<PauseBillingAccountWorkflowResponse>();
        for (var element : request.getCreditNote().getLinesList()) {
            PauseBillingAccountWorkflowRequest itemRequest = PauseBillingAccountWorkflowRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setCard(BillingAccountCardFunding.newBuilder()
                                .setToken(request.getCreditNote().getCard().getToken())
                                .build())
                        .setHolder(InvoiceRef.newBuilder()
                                .setDisplayName(reply0.getCreditNote().getCounterparty().getDisplayName())
                                .build())
                        .addLimits(BillingAccountLimit.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results1.add(billingBillingAccountWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseBillingAccountWorkflow(itemRequest));
        }
        var reply1 = results1.isEmpty() ? PauseBillingAccountWorkflowResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = ExplainHouseholdDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(reply0.getCreditNote().getAudit().getCreatedBy())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply0.getCreditNote().getCounterparty().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainHouseholdDecision(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = BatchCreateBillingAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBillingAccountsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateBillingAccountsResponse.getDefaultInstance());
        var input3 = billingBillingAccountServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBillingAccounts(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamUnderwritingChangesRequest.newBuilder()
                .setChange(Underwriting.newBuilder()
                        .setAssignee(request.getDetails().getApprover())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCreditNote().getAudit().getCreatedAt())
                                .setCreatedBy(request.getCreditNote().getAudit().getCreatedBy())
                                .setEtag(request.getCreditNote().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamUnderwritingChangesResponse.getDefaultInstance());
        var input4 = insuranceUnderwritingSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamUnderwritingChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitCreditNote", request.getCreditNote().getCreditNoteId());
        return SubmitCreditNoteResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setBillingAddress(request.getCreditNote().getBillingAddress())
                        .setCreditNoteId(request.getCreditNoteId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Approves a credit note.
     */
    public ApproveCreditNoteResponse approveCreditNote(ApproveCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = ValidateJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateJournalEntryRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCreditNote().getAudit().getUpdatedAt())
                                .build())
                        .setCard(JournalEntryCardSettlement.newBuilder()
                                .setNetwork(request.getCreditNote().getCard().getNetwork())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateJournalEntry(outbound0), rpcExecutor);

        var outbound1 = RecordRetentionPolicyVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordRetentionPolicyVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCreditNote().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getCreditNote().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordRetentionPolicyVersion(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = RejectBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setCard(BillingAccountCardFunding.newBuilder()
                                .setHolderName(switch (reply0.getJournalEntry().getSettlementCase()) {
                                    case CARD -> reply0.getJournalEntry().getCard().getHolderName();
                                    default -> "";
                                })
                                .setLast4(switch (reply0.getJournalEntry().getSettlementCase()) {
                                    case CARD -> reply0.getJournalEntry().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setHolder(InvoiceRef.newBuilder()
                                .setNumber(request.getCreditNote().getCounterparty().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = billingBillingAccountApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectBillingAccount(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveCreditNote", request.getCreditNote().getCreditNoteId());
        return ApproveCreditNoteResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getBillingAccount().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getJournalEntry().getBillingAddress().getPostalCode())
                                .build())
                        .setCounterparty(InvoiceRef.newBuilder()
                                .setNumber(request.getCreditNote().getNumber())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .addLines(CreditNoteLine.newBuilder()
                                .setDescription(reply1.getRetentionPolicy().getDescription())
                                .build())
                        .build())
                .setOutcome(ApproveCreditNoteResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply2.getOutcome().getEffectiveAt())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Rejects a credit note.
     */
    public RejectCreditNoteResponse rejectCreditNote(RejectCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        List<RecallInvoiceResponse> results0 = request.getCreditNote().getLinesList().stream()
                .map(element -> RecallInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .addLines(InvoiceLine.newBuilder()
                                .setAmount(element.getAmount())
                                .setTax(InvoiceLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(billingInvoiceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallInvoice(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? RecallInvoiceResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectCreditNote", request.getCreditNote().getCreditNoteId());
        return RejectCreditNoteResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCounterparty(InvoiceRef.newBuilder()
                                .setNumber(request.getCreditNote().getNumber())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .build())
                .build();
    }

    /**
     * Lists credit notes a page at a time.
     */
    public ListPendingCreditNotesResponse listPendingCreditNotes(ListPendingCreditNotesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingCreditNotes")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CreditNote", request.getContext().getTraceId(), "ListPendingCreditNotes");

        var outbound0 = mapper.listPendingCreditNotesStage0(request);
        var reply0 = authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetAccessPolicies(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingCreditNotes", request.getPage().getPageToken());
        return ListPendingCreditNotesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns a credit note.
     */
    public GetCreditNoteApprovalStatusResponse getCreditNoteApprovalStatus(GetCreditNoteApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCreditNoteApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CreditNote", request.getCreditNoteId(), "GetCreditNoteApprovalStatus");

        var outbound0 = FindDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindDocumentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .setOwnerId(request.getCreditNoteId())
                        .build())
                .build();
        Function<FindDocumentsRequest, FindDocumentsResponse> operation0 = documentsDocumentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::findDocuments;
        var reply0 = operation0.apply(outbound0);
        return GetCreditNoteApprovalStatusResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCreditNoteId(request.getCreditNoteId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Comments a credit note.
     */
    public CommentCreditNoteApprovalResponse commentCreditNoteApproval(CommentCreditNoteApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentCreditNoteApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = RecallInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setCard(InvoiceCardSettlement.newBuilder()
                                .setNetwork(switch (request.getCreditNote().getSettlementCase()) {
                                    case CARD -> request.getCreditNote().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = billingInvoiceApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallInvoice(outbound0);

        var outbound1 = (QuoteCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getCreditNote().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .setDetails(QuoteCustomerRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getInvoice().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCustomer(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentCreditNoteApproval", request.getCreditNote().getCreditNoteId());
        return CommentCreditNoteApprovalResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCreditNoteId(request.getCreditNoteId())
                        .putAllMetadata(request.getCreditNote().getMetadataMap())
                        .setNumber(reply0.getInvoice().getNumber())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
