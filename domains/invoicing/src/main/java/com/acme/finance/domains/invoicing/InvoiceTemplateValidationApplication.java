package com.acme.finance.domains.invoicing;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferAttachment;
import com.acme.assets.v1.AssetTransferCardSettlement;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.NormalizeAssetTransferRequest;
import com.acme.assets.v1.NormalizeAssetTransferResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.ResolveBankAccountsRequest;
import com.acme.banking.v1.ResolveBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.identity.v1.ValidateUserRequest;
import com.acme.identity.v1.ValidateUserResponse;
import com.acme.invoicing.v1.CalculateEInvoiceRequest;
import com.acme.invoicing.v1.CalculateEInvoiceResponse;
import com.acme.invoicing.v1.CalculateInvoiceBatchRequest;
import com.acme.invoicing.v1.CalculateInvoiceBatchResponse;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceBankSettlement;
import com.acme.invoicing.v1.EInvoiceCalculationServiceGrpc;
import com.acme.invoicing.v1.EstimateEInvoiceRequest;
import com.acme.invoicing.v1.EstimateEInvoiceResponse;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationRequest;
import com.acme.invoicing.v1.ExplainInvoiceBatchCalculationResponse;
import com.acme.invoicing.v1.ExplainInvoiceTemplateDecisionRequest;
import com.acme.invoicing.v1.ExplainInvoiceTemplateDecisionResponse;
import com.acme.invoicing.v1.GetInvoiceTemplateRulesRequest;
import com.acme.invoicing.v1.GetInvoiceTemplateRulesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchBankSettlement;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.LintInvoiceTemplatesRequest;
import com.acme.invoicing.v1.LintInvoiceTemplatesResponse;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateRequest;
import com.acme.invoicing.v1.NormalizeInvoiceTemplateResponse;
import com.acme.invoicing.v1.PreviewInvoiceTemplateRequest;
import com.acme.invoicing.v1.PreviewInvoiceTemplateResponse;
import com.acme.invoicing.v1.ScoreInvoiceTemplateRequest;
import com.acme.invoicing.v1.ScoreInvoiceTemplateResponse;
import com.acme.invoicing.v1.ValidateInvoiceTemplateRequest;
import com.acme.invoicing.v1.ValidateInvoiceTemplateResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.ValidateJournalEntryRequest;
import com.acme.ledger.v1.ValidateJournalEntryResponse;
import com.acme.lending.v1.BatchGetLoanApplicationsRequest;
import com.acme.lending.v1.BatchGetLoanApplicationsResponse;
import com.acme.lending.v1.CloneLoanApplicationRequest;
import com.acme.lending.v1.CloneLoanApplicationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationLineTax;
import com.acme.lending.v1.LoanApplicationServiceGrpc;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.SearchNotificationsRequest;
import com.acme.notifications.v1.SearchNotificationsResponse;
import com.acme.travel.v1.BrowseTripsRequest;
import com.acme.travel.v1.BrowseTripsResponse;
import com.acme.travel.v1.TripQueryServiceGrpc;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Checks invoice templates against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class InvoiceTemplateValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceTemplateValidationApplication.class);

    private final AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking;

    private final BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final EInvoiceCalculationServiceGrpc.EInvoiceCalculationServiceBlockingStub invoicingEInvoiceCalculationServiceBlocking;

    private final InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking;

    private final LoanApplicationServiceGrpc.LoanApplicationServiceBlockingStub lendingLoanApplicationServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final InvoicingRepository repository;

    private final Executor rpcExecutor;

    private final TripQueryServiceGrpc.TripQueryServiceBlockingStub travelTripQueryServiceBlocking;

    public InvoiceTemplateValidationApplication(
            AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking,
            BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking,
            ChannelRegistry channels,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            EInvoiceCalculationServiceGrpc.EInvoiceCalculationServiceBlockingStub invoicingEInvoiceCalculationServiceBlocking,
            InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking,
            LoanApplicationServiceGrpc.LoanApplicationServiceBlockingStub lendingLoanApplicationServiceBlocking,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            InvoicingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            TripQueryServiceGrpc.TripQueryServiceBlockingStub travelTripQueryServiceBlocking) {
        this.assetsAssetTransferValidationServiceBlocking = assetsAssetTransferValidationServiceBlocking;
        this.bankingBankAccountQueryServiceBlocking = bankingBankAccountQueryServiceBlocking;
        this.channels = channels;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.invoicingEInvoiceCalculationServiceBlocking = invoicingEInvoiceCalculationServiceBlocking;
        this.invoicingInvoiceBatchCalculationServiceBlocking = invoicingInvoiceBatchCalculationServiceBlocking;
        this.lendingLoanApplicationServiceBlocking = lendingLoanApplicationServiceBlocking;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.travelTripQueryServiceBlocking = travelTripQueryServiceBlocking;
    }

    /**
     * Validates an invoice template.
     */
    public ValidateInvoiceTemplateResponse validateInvoiceTemplate(ValidateInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var futures0 = request.getInvoiceTemplate().getLinesList().stream()
                .map(element -> CloneLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .addLines(LoanApplicationLine.newBuilder()
                                .setLineId(element.getLineId())
                                .setTax(LoanApplicationLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> lendingLoanApplicationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneLoanApplication(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? CloneLoanApplicationResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = JsonCodec.roundTrip(ExplainInvoiceBatchCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainInvoiceBatchCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getLoanApplication().getBillingAddress().getLinesList())
                                .setPostalCode(request.getInvoiceTemplate().getBillingAddress().getPostalCode())
                                .setRecipient(request.getInvoiceTemplate().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build(), ExplainInvoiceBatchCalculationRequest.class);
        var reply1 = invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainInvoiceBatchCalculation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateInvoiceTemplate", request.getInvoiceTemplate().getInvoiceTemplateId());
        return ValidateInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getInvoiceBatch().getBillingAddress().getLinesList())
                                .build())
                        .setDueAt(request.getInvoiceTemplate().getDueAt())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .build();
    }

    /**
     * Previews an invoice template.
     */
    public PreviewInvoiceTemplateResponse previewInvoiceTemplate(PreviewInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = BeanCopies.copy(ResolveBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .build(), ResolveBankAccountsRequest.class);
        var reply0 = bankingBankAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveBankAccounts(outbound0);

        var outbound1 = (CalculateEInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getInvoiceTemplate().getBillingAddress().getCountryCode())
                                .setLocality(request.getInvoiceTemplate().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = invoicingEInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateEInvoice(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewInvoiceTemplate", request.getInvoiceTemplate().getInvoiceTemplateId());
        return PreviewInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getEInvoice().getBillingAddress().getCountryCode())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply1.getEInvoice().getCounterparty().getDisplayName())
                                .build())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .setNotes(request.getInvoiceTemplate().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Checks an invoice template.
     */
    public CheckInvoiceTemplateEligibilityResponse checkInvoiceTemplateEligibility(CheckInvoiceTemplateEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckInvoiceTemplateEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = CalculateInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getInvoiceTemplate().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getInvoiceTemplate().getAudit().getUpdatedBy())
                                .build())
                        .setBank(InvoiceBatchBankSettlement.newBuilder()
                                .setBic(switch (request.getInvoiceTemplate().getSettlementCase()) {
                                    case BANK -> request.getInvoiceTemplate().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateInvoiceBatch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckInvoiceTemplateEligibility", request.getInvoiceTemplate().getInvoiceTemplateId());
        return CheckInvoiceTemplateEligibilityResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getInvoiceBatch().getBillingAddress().getPostalCode())
                                .build())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .setStatus(request.getInvoiceTemplate().getStatus())
                        .build())
                .build();
    }

    /**
     * Normalizes an invoice template.
     */
    public NormalizeInvoiceTemplateResponse normalizeInvoiceTemplate(NormalizeInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var futures0 = request.getInvoiceTemplate().getAttachmentsList().stream()
                .map(element -> NormalizeAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .addAttachments(AssetTransferAttachment.newBuilder()
                                .setSizeBytes(element.getSizeBytes())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAssetTransfer(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? NormalizeAssetTransferResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeInvoiceTemplate", request.getInvoiceTemplate().getInvoiceTemplateId());
        return NormalizeInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .putAllMetadata(request.getInvoiceTemplate().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Explains an invoice template.
     */
    public ExplainInvoiceTemplateDecisionResponse explainInvoiceTemplateDecision(ExplainInvoiceTemplateDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainInvoiceTemplateDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = ValidateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getInvoiceTemplate().getAudit().getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getInvoiceTemplate().getBillingAddress().getCountryCode())
                                .setRecipient(request.getInvoiceTemplate().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            ValidateUserResponse fetch(ValidateUserRequest input) {
                return identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateUser(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = SearchNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchNotificationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoiceTemplate().getAudit().getCreatedAt())
                        .setOwnerId(reply0.getUser().getRelationshipManager().getUserId())
                        .setRegion(reply0.getUser().getPrimaryAddress().getRegion())
                        .build())
                .build();
        Function<SearchNotificationsRequest, SearchNotificationsResponse> operation1 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::searchNotifications;
        var reply1 = operation1.apply(outbound1);

        var outbound2 = ValidateJournalEntryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateJournalEntryRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getUser().getContact().getEmail())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .setJournalEntry(JournalEntry.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getUser().getPrimaryAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.ledger.v1.JournalEntryValidationService");
        var selected2 = JournalEntryValidationServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.validateJournalEntry(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainInvoiceTemplateDecision", request.getInvoiceTemplate().getInvoiceTemplateId());
        return ExplainInvoiceTemplateDecisionResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setDueAt(reply2.getJournalEntry().getDueAt())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .setStatus(request.getInvoiceTemplate().getStatus())
                        .build())
                .setOutcome(ExplainInvoiceTemplateDecisionResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Scores an invoice template.
     */
    public ScoreInvoiceTemplateResponse scoreInvoiceTemplate(ScoreInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = NormalizeAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getInvoiceTemplate().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getInvoiceTemplate().getAudit().getUpdatedBy())
                                .build())
                        .setCard(AssetTransferCardSettlement.newBuilder()
                                .setToken(switch (request.getInvoiceTemplate().getSettlementCase()) {
                                    case CARD -> request.getInvoiceTemplate().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeAssetTransfer(outbound0);

        var outbound1 = (BrowseTripsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseTripsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getInvoiceTemplate().getAudit().getCreatedAt())
                        .setOwnerId(reply0.getAssetTransfer().getCounterparty().getFixedAssetId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply1 = travelTripQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseTrips(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreInvoiceTemplate", request.getInvoiceTemplate().getInvoiceTemplateId());
        return ScoreInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAssetTransfer().getAudit().getCreatedAt())
                                .build())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .setNumber(request.getInvoiceTemplate().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lints an invoice template.
     */
    public LintInvoiceTemplatesResponse lintInvoiceTemplates(LintInvoiceTemplatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintInvoiceTemplates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveInvoiceTemplate(request.getContext().getTenantId(), request.getInvoiceTemplate());

        var outbound0 = CalculateEInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateEInvoiceRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getInvoiceTemplate().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setEInvoice(EInvoice.newBuilder()
                        .setBank(EInvoiceBankSettlement.newBuilder()
                                .setIban(switch (request.getInvoiceTemplate().getSettlementCase()) {
                                    case BANK -> request.getInvoiceTemplate().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = invoicingEInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateEInvoice(outbound0);

        var outbound1 = EstimateEInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getInvoiceTemplate().getBillingAddress().getCountryCode())
                                .addAllLines(request.getInvoiceTemplate().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        Function<EstimateEInvoiceRequest, EstimateEInvoiceResponse> operation1 = invoicingEInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::estimateEInvoice;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplate().getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintInvoiceTemplates", request.getInvoiceTemplate().getInvoiceTemplateId());
        return LintInvoiceTemplatesResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getEInvoice().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getEInvoice().getBillingAddress().getRecipient())
                                .build())
                        .setDueAt(request.getInvoiceTemplate().getDueAt())
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an invoice template.
     */
    public GetInvoiceTemplateRulesResponse getInvoiceTemplateRules(GetInvoiceTemplateRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetInvoiceTemplateRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getInvoiceTemplateId(), "GetInvoiceTemplateRules");

        var outbound0 = (BatchGetLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply0 = lendingLoanApplicationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetLoanApplications(outbound0);
        return GetInvoiceTemplateRulesResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
