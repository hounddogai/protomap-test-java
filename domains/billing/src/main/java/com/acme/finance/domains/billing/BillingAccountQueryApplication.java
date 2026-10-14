package com.acme.finance.domains.billing;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.NormalizeAccessPolicyRequest;
import com.acme.authorization.v1.NormalizeAccessPolicyResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BrowseBillingAccountsRequest;
import com.acme.billing.v1.BrowseBillingAccountsResponse;
import com.acme.billing.v1.CheckInvoiceEligibilityRequest;
import com.acme.billing.v1.CheckInvoiceEligibilityResponse;
import com.acme.billing.v1.CountBillingAccountsRequest;
import com.acme.billing.v1.CountBillingAccountsResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteLine;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.ExportBillingAccountsRequest;
import com.acme.billing.v1.ExportBillingAccountsResponse;
import com.acme.billing.v1.FindBillingAccountsRequest;
import com.acme.billing.v1.FindBillingAccountsResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceBankSettlement;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.LookupBillingAccountRequest;
import com.acme.billing.v1.LookupBillingAccountResponse;
import com.acme.billing.v1.PeekBillingAccountRequest;
import com.acme.billing.v1.PeekBillingAccountResponse;
import com.acme.billing.v1.PreviewInvoiceRequest;
import com.acme.billing.v1.PreviewInvoiceResponse;
import com.acme.billing.v1.ResolveBillingAccountsRequest;
import com.acme.billing.v1.ResolveBillingAccountsResponse;
import com.acme.billing.v1.ScoreCreditNoteRequest;
import com.acme.billing.v1.ScoreCreditNoteResponse;
import com.acme.billing.v1.SuggestBillingAccountsRequest;
import com.acme.billing.v1.SuggestBillingAccountsResponse;
import com.acme.billing.v1.ValidateCreditNoteRequest;
import com.acme.billing.v1.ValidateCreditNoteResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.CheckDocumentEligibilityRequest;
import com.acme.documents.v1.CheckDocumentEligibilityResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentBankSettlement;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.acme.notifications.v1.CheckNotificationEligibilityRequest;
import com.acme.notifications.v1.CheckNotificationEligibilityResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.ValidateNotificationRequest;
import com.acme.notifications.v1.ValidateNotificationResponse;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about billing accounts, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class BillingAccountQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(BillingAccountQueryApplication.class);

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final CreditNoteValidationServiceGrpc.CreditNoteValidationServiceBlockingStub billingCreditNoteValidationServiceBlocking;

    private final InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceFutureStub notificationsNotificationValidationServiceFuture;

    private final BillingRepository repository;

    public BillingAccountQueryApplication(
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            CreditNoteValidationServiceGrpc.CreditNoteValidationServiceBlockingStub billingCreditNoteValidationServiceBlocking,
            InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking,
            ChannelRegistry channels,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceFutureStub notificationsNotificationValidationServiceFuture,
            BillingRepository repository) {
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.billingCreditNoteValidationServiceBlocking = billingCreditNoteValidationServiceBlocking;
        this.billingInvoiceValidationServiceBlocking = billingInvoiceValidationServiceBlocking;
        this.channels = channels;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.notificationsNotificationValidationServiceFuture = notificationsNotificationValidationServiceFuture;
        this.repository = repository;
    }

    /**
     * Returns a billing account.
     */
    public LookupBillingAccountResponse lookupBillingAccount(LookupBillingAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupBillingAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BillingAccount", request.getBillingAccountId(), "LookupBillingAccount");

        var outbound0 = ValidateNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateNotificationRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = notificationsNotificationValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).validateNotification(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = SendLedgerAccountTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendLedgerAccountTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(reply0.getNotification().getRequester().getEmail())
                                .setName(reply0.getNotification().getRequester().getName())
                                .setPhone(reply0.getNotification().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.ledger.v1.LedgerAccountNotificationService");
        var selected1 = LedgerAccountNotificationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.sendLedgerAccountTestNotice(outbound1);
        return LookupBillingAccountResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setBillingAccountId(request.getBillingAccountId())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply1.getLedgerAccount().getContact().getPhone())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists billing accounts a page at a time.
     */
    public BrowseBillingAccountsResponse browseBillingAccounts(BrowseBillingAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseBillingAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BillingAccount", request.getContext().getTraceId(), "BrowseBillingAccounts");

        var outbound0 = ScoreCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .addLines(CreditNoteLine.newBuilder()
                                .putAllAttributes(request.getFilter().getLabelsMap())
                                .build())
                        .build())
                .setDetails(ScoreCreditNoteRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        RpcPort<ScoreCreditNoteRequest, ScoreCreditNoteResponse> port0 = RpcPort.adapt(billingCreditNoteValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scoreCreditNote);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseBillingAccounts", request.getPage().getPageToken());
        return BrowseBillingAccountsResponse.newBuilder()
                .addBillingAccounts(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getCreditNote().getAudit().getEtag())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists billing accounts a page at a time.
     */
    public FindBillingAccountsResponse findBillingAccounts(FindBillingAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindBillingAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BillingAccount", request.getContext().getTraceId(), "FindBillingAccounts");

        var outbound0 = ValidateCreditNoteRequest.newBuilder()
                .setContext(request.getContext())
                .setCreditNote(CreditNote.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<ValidateCreditNoteRequest, ValidateCreditNoteResponse>() {
            @Override
            public ValidateCreditNoteResponse apply(ValidateCreditNoteRequest input) {
                return billingCreditNoteValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateCreditNote(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = CheckInvoiceEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getCreditNote().getAudit().getUpdatedBy())
                                .build())
                        .setBank(InvoiceBankSettlement.newBuilder()
                                .setBic(switch (reply0.getCreditNote().getSettlementCase()) {
                                    case BANK -> reply0.getCreditNote().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkInvoiceEligibility(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindBillingAccounts", request.getPage().getPageToken());
        return FindBillingAccountsResponse.newBuilder()
                .addBillingAccounts(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getInvoice().getAudit().getCreatedBy())
                                .setEtag(reply0.getCreditNote().getAudit().getEtag())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Counts billing accounts by status.
     */
    public CountBillingAccountsResponse countBillingAccounts(CountBillingAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountBillingAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BillingAccount", request.getContext().getTraceId(), "CountBillingAccounts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountBillingAccounts", request.getPage().getPageToken());
        return CountBillingAccountsResponse.getDefaultInstance();
    }

    /**
     * Returns several billing accounts at once.
     */
    public ResolveBillingAccountsResponse resolveBillingAccounts(ResolveBillingAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveBillingAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BillingAccount", request.getContext().getTraceId(), "ResolveBillingAccounts");
        return ResolveBillingAccountsResponse.getDefaultInstance();
    }

    /**
     * Streams every matching billing account.
     */
    public ExportBillingAccountsResponse exportBillingAccounts(ExportBillingAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportBillingAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BillingAccount", request.getContext().getTraceId(), "ExportBillingAccounts");

        var outbound0 = PreviewInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewInvoiceRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setInvoice(Invoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setIssuedAt(request.getFilter().getCreatedAfter())
                        .build())
                .build();
        var reply0 = billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewInvoice(outbound0);

        var outbound1 = CheckDocumentEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckDocumentEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getInvoice().getCounterparty().getEmail())
                                .build())
                        .build())
                .setDocument(Document.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getInvoice().getAudit().getCreatedAt())
                                .build())
                        .setBank(DocumentBankSettlement.newBuilder()
                                .setBic(switch (reply0.getInvoice().getSettlementCase()) {
                                    case BANK -> reply0.getInvoice().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkDocumentEligibility(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportBillingAccounts", request.getFilter().getQuery());
        return ExportBillingAccountsResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getDocument().getAudit().getCreatedBy())
                                .build())
                        .setHolder(InvoiceRef.newBuilder()
                                .setDisplayName(reply1.getDocument().getCounterparty().getDisplayName())
                                .build())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Lists billing accounts a page at a time.
     */
    public SuggestBillingAccountsResponse suggestBillingAccounts(SuggestBillingAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestBillingAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BillingAccount", request.getContext().getTraceId(), "SuggestBillingAccounts");

        var outbound0 = JsonCodec.roundTrip(NormalizeAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeAccessPolicyRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build(), NormalizeAccessPolicyRequest.class);
        var reply0 = authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAccessPolicy(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestBillingAccounts", request.getPage().getPageToken());
        return SuggestBillingAccountsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a billing account.
     */
    public PeekBillingAccountResponse peekBillingAccount(PeekBillingAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekBillingAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BillingAccount", request.getBillingAccountId(), "PeekBillingAccount");

        var outbound0 = RpcSupport.unpack(Any.pack(CheckNotificationEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckNotificationEligibilityRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build()), CheckNotificationEligibilityRequest.class);
        var reply0 = notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkNotificationEligibility(outbound0);
        return PeekBillingAccountResponse.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setBillingAccountId(request.getBillingAccountId())
                        .setHolder(InvoiceRef.newBuilder()
                                .setDisplayName(reply0.getNotification().getCustomer().getDisplayName())
                                .build())
                        .build())
                .build();
    }
}
