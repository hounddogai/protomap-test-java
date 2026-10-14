package com.acme.finance.domains.billing;

import com.acme.accounting.v1.BatchCreateChartOfAccountsesRequest;
import com.acme.accounting.v1.BatchCreateChartOfAccountsesResponse;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeRequest;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.CountAccessPoliciesRequest;
import com.acme.authorization.v1.CountAccessPoliciesResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.SubmitMandateRequest;
import com.acme.banking.v1.SubmitMandateResponse;
import com.acme.billing.v1.ActivateCreditNoteRequest;
import com.acme.billing.v1.ActivateCreditNoteResponse;
import com.acme.billing.v1.CalculateInvoiceRequest;
import com.acme.billing.v1.CalculateInvoiceResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.CreditNoteLine;
import com.acme.billing.v1.ExpireCreditNoteRequest;
import com.acme.billing.v1.ExpireCreditNoteResponse;
import com.acme.billing.v1.ExplainPaymentTermCalculationRequest;
import com.acme.billing.v1.ExplainPaymentTermCalculationResponse;
import com.acme.billing.v1.GetCreditNoteLifecycleRequest;
import com.acme.billing.v1.GetCreditNoteLifecycleResponse;
import com.acme.billing.v1.GetCreditNoteRatesRequest;
import com.acme.billing.v1.GetCreditNoteRatesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceBankSettlement;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.NormalizeInvoiceRequest;
import com.acme.billing.v1.NormalizeInvoiceResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.ReinstateCreditNoteRequest;
import com.acme.billing.v1.ReinstateCreditNoteResponse;
import com.acme.billing.v1.RenewCreditNoteRequest;
import com.acme.billing.v1.RenewCreditNoteResponse;
import com.acme.billing.v1.ScheduleCreditNoteExpiryRequest;
import com.acme.billing.v1.ScheduleCreditNoteExpiryResponse;
import com.acme.billing.v1.ScoreInvoiceRequest;
import com.acme.billing.v1.ScoreInvoiceResponse;
import com.acme.billing.v1.SimulateInvoiceRequest;
import com.acme.billing.v1.SimulateInvoiceResponse;
import com.acme.billing.v1.SuspendCreditNoteRequest;
import com.acme.billing.v1.SuspendCreditNoteResponse;
import com.acme.billing.v1.TerminateCreditNoteRequest;
import com.acme.billing.v1.TerminateCreditNoteResponse;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.PreviewHouseholdRequest;
import com.acme.customers.v1.PreviewHouseholdResponse;
import com.acme.customers.v1.QuoteCustomerRequest;
import com.acme.customers.v1.QuoteCustomerResponse;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UpdateUserRequest;
import com.acme.identity.v1.UpdateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountBankFunding;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.RevertLedgerAccountRequest;
import com.acme.ledger.v1.RevertLedgerAccountResponse;
import com.acme.ledger.v1.ScoreLedgerAccountRequest;
import com.acme.ledger.v1.ScoreLedgerAccountResponse;
import com.acme.notifications.v1.CreateNotificationRequest;
import com.acme.notifications.v1.CreateNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Moves a credit note between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class CreditNoteLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(CreditNoteLifecycleApplication.class);

    private final ChartOfAccountsServiceGrpc.ChartOfAccountsServiceStub accountingChartOfAccountsServiceAsync;

    private final RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking;

    private final RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking;

    private final InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking;

    private final InvoiceCalculationServiceGrpc.InvoiceCalculationServiceFutureStub billingInvoiceCalculationServiceFuture;

    private final InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking;

    private final InvoiceValidationServiceGrpc.InvoiceValidationServiceFutureStub billingInvoiceValidationServiceFuture;

    private final PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync;

    private final ChannelRegistry channels;

    private final CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final BillingRepository repository;

    public CreditNoteLifecycleApplication(
            ChartOfAccountsServiceGrpc.ChartOfAccountsServiceStub accountingChartOfAccountsServiceAsync,
            RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceBlockingStub auditRetentionPolicyHistoryServiceBlocking,
            RetentionPolicyNotificationServiceGrpc.RetentionPolicyNotificationServiceBlockingStub auditRetentionPolicyNotificationServiceBlocking,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceBlockingStub billingCreditNoteCalculationServiceBlocking,
            InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking,
            InvoiceCalculationServiceGrpc.InvoiceCalculationServiceFutureStub billingInvoiceCalculationServiceFuture,
            InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking,
            InvoiceValidationServiceGrpc.InvoiceValidationServiceFutureStub billingInvoiceValidationServiceFuture,
            PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceBlockingStub billingPaymentTermCalculationServiceBlocking,
            VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync,
            ChannelRegistry channels,
            CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            BillingRepository repository) {
        this.accountingChartOfAccountsServiceAsync = accountingChartOfAccountsServiceAsync;
        this.auditRetentionPolicyHistoryServiceBlocking = auditRetentionPolicyHistoryServiceBlocking;
        this.auditRetentionPolicyNotificationServiceBlocking = auditRetentionPolicyNotificationServiceBlocking;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.billingCreditNoteCalculationServiceBlocking = billingCreditNoteCalculationServiceBlocking;
        this.billingInvoiceCalculationServiceBlocking = billingInvoiceCalculationServiceBlocking;
        this.billingInvoiceCalculationServiceFuture = billingInvoiceCalculationServiceFuture;
        this.billingInvoiceValidationServiceBlocking = billingInvoiceValidationServiceBlocking;
        this.billingInvoiceValidationServiceFuture = billingInvoiceValidationServiceFuture;
        this.billingPaymentTermCalculationServiceBlocking = billingPaymentTermCalculationServiceBlocking;
        this.budgetingVarianceSyncServiceAsync = budgetingVarianceSyncServiceAsync;
        this.channels = channels;
        this.customersCustomerCalculationServiceBlocking = customersCustomerCalculationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.ledgerLedgerAccountValidationServiceBlocking = ledgerLedgerAccountValidationServiceBlocking;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Activates a credit note.
     */
    public ActivateCreditNoteResponse activateCreditNote(ActivateCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CreditNote", request.getCreditNoteId(), "ActivateCreditNote");

        var outbound0 = BeanCopies.copy(PreviewHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                        .setUserId(request.getDetails().getNewOwner().getUserId())
                                        .build())
                                .build())
                        .build())
                .build(), PreviewHouseholdRequest.class);
        var reply0 = customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewHousehold(outbound0);

        var outbound1 = QuotePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuotePreferenceRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(reply0.getHousehold().getLocation().getRecipient())
                                .build())
                        .build())
                .build();
        RpcPort<QuotePreferenceRequest, QuotePreferenceResponse> port1 = RpcPort.adapt(customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::quotePreference);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = JsonCodec.roundTrip(ScoreInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getPreference().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply0.getHousehold().getManager().getEmail())
                                .build())
                        .build())
                .build(), ScoreInvoiceRequest.class);
        var reply2 = billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreInvoice(outbound2);

        var outbound3 = SendRetentionPolicyTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendRetentionPolicyTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply3 = Optional.of(outbound3).map(value -> value.toBuilder().build())
                .map(value -> auditRetentionPolicyNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendRetentionPolicyTestNotice(value)).orElseThrow();

        var outbound4 = BatchCreateChartOfAccountsesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateChartOfAccountsesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotifyContact())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateChartOfAccountsesResponse.getDefaultInstance());
        var input4 = accountingChartOfAccountsServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateChartOfAccountses(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamVarianceChangesRequest.newBuilder()
                .setChange(Variance.newBuilder()
                        .setAssignee(request.getDetails().getNewOwner())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .addComments(VarianceComment.newBuilder()
                                .setAuthorEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamVarianceChangesResponse.getDefaultInstance());
        var input5 = budgetingVarianceSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamVarianceChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateCreditNote", request.getDetails().getReasonCode());
        return ActivateCreditNoteResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply3.getRetentionPolicy().getAudit().getCreatedAt())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .addLines(CreditNoteLine.newBuilder()
                                .setAmount(reply1.getPreference().getDiscount().getAmount())
                                .build())
                        .setTotal(reply2.getBreakdown().getTotal())
                        .build())
                .setOutcome(ActivateCreditNoteResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Suspends a credit note.
     */
    public SuspendCreditNoteResponse suspendCreditNote(SuspendCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CreditNote", request.getCreditNoteId(), "SuspendCreditNote");

        var outbound0 = CountAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAccessPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getCreditNoteId())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAccessPolicies(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = ScoreLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .setHolder(PostingBatchRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<ScoreLedgerAccountRequest, ScoreLedgerAccountResponse>() {
            @Override
            public ScoreLedgerAccountResponse apply(ScoreLedgerAccountRequest input) {
                return ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreLedgerAccount(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendCreditNote", request.getDetails().getReasonCode());
        return SuspendCreditNoteResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCreditNoteId(request.getCreditNoteId())
                        .build())
                .setOutcome(SuspendCreditNoteResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates a credit note.
     */
    public ReinstateCreditNoteResponse reinstateCreditNote(ReinstateCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CreditNote", request.getCreditNoteId(), "ReinstateCreditNote");

        var outbound0 = BeanCopies.copy(QuoteCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build(), QuoteCustomerRequest.class);
        var reply0 = customersCustomerCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCustomer(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateCreditNote", request.getDetails().getReasonCode());
        return ReinstateCreditNoteResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCounterparty(InvoiceRef.newBuilder()
                                .setDisplayName(reply0.getCustomer().getRelationshipManager().getDisplayName())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .build())
                .setOutcome(ReinstateCreditNoteResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Terminates a credit note.
     */
    public TerminateCreditNoteResponse terminateCreditNote(TerminateCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CreditNote", request.getCreditNoteId(), "TerminateCreditNote");

        var outbound0 = ListRetentionPolicyVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListRetentionPolicyVersionsRequest.Filter.newBuilder()
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        Function<ListRetentionPolicyVersionsRequest, ListRetentionPolicyVersionsResponse> operation0 = auditRetentionPolicyHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listRetentionPolicyVersions;
        var reply0 = operation0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateCreditNote", request.getDetails().getReasonCode());
        return TerminateCreditNoteResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setCreditNoteId(request.getCreditNoteId())
                        .build())
                .setOutcome(TerminateCreditNoteResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Renews a credit note.
     */
    public RenewCreditNoteResponse renewCreditNote(RenewCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CreditNote", request.getCreditNoteId(), "RenewCreditNote");

        var outbound0 = NormalizeInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeInvoiceRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .setInvoice(Invoice.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = billingInvoiceValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeInvoice(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewCreditNote", request.getDetails().getReasonCode());
        return RenewCreditNoteResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getInvoice().getAudit().getEtag())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .build())
                .setOutcome(RenewCreditNoteResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Expires a credit note.
     */
    public ExpireCreditNoteResponse expireCreditNote(ExpireCreditNoteRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireCreditNote")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CreditNote", request.getCreditNoteId(), "ExpireCreditNote");

        var builder0 = GetCreditNoteRatesRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setCreditNoteId(request.getCreditNoteId());
        var outbound0 = builder0.build();
        var reply0 = billingCreditNoteCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getCreditNoteRates(outbound0);

        var outbound1 = SimulateInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getCreditNote().getAudit().getEtag())
                                .build())
                        .setBank(InvoiceBankSettlement.newBuilder()
                                .setBic(reply0.getCreditNote().getBank().getBic())
                                .setIban(reply0.getCreditNote().getBank().getIban())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getCreditNote().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var pending1 = billingInvoiceCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).simulateInvoice(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = BeanCopies.copy(CreateNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getInvoice().getAudit().getEtag())
                                .build())
                        .setCustomer(NotificationTemplateRef.newBuilder()
                                .setDisplayName(reply1.getInvoice().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build(), CreateNotificationRequest.class);
        var reply2 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createNotification(outbound2);

        var builder3 = ExplainPaymentTermCalculationRequest.newBuilder();
        builder3.setContext(request.getContext());
        builder3.setPaymentTerm(PaymentTerm.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedAt(reply2.getNotification().getAudit().getUpdatedAt())
                        .build())
                .build());
        var outbound3 = builder3.build();
        var reply3 = billingPaymentTermCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPaymentTermCalculation(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireCreditNote", request.getDetails().getReasonCode());
        return ExpireCreditNoteResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getCreditNote().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply1.getInvoice().getBillingAddress().getLocality())
                                .setRegion(reply0.getCreditNote().getBillingAddress().getRegion())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .build())
                .setOutcome(ExpireCreditNoteResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a credit note.
     */
    public GetCreditNoteLifecycleResponse getCreditNoteLifecycle(GetCreditNoteLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCreditNoteLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CreditNote", request.getCreditNoteId(), "GetCreditNoteLifecycle");

        var outbound0 = SubmitMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitMandateRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.banking.v1.MandateApprovalService");
        var serviceName0 = "acme.banking.v1" + "." + "MandateApprovalService";
        var methodName0 = "Sub" + "mitMandate";
        var descriptor0 = MethodDescriptor.<SubmitMandateRequest, SubmitMandateResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(SubmitMandateRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SubmitMandateResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = CalculateInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getMandate().getAudit().getCreatedAt())
                                .build())
                        .setBank(InvoiceBankSettlement.newBuilder()
                                .setBic(reply0.getMandate().getBank().getBic())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<CalculateInvoiceRequest, CalculateInvoiceResponse>() {
            @Override
            public CalculateInvoiceResponse apply(CalculateInvoiceRequest input) {
                return billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateInvoice(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        return GetCreditNoteLifecycleResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getMandate().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getInvoice().getBillingAddress().getCountryCode())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .setNumber(reply1.getInvoice().getNumber())
                        .build())
                .build();
    }

    /**
     * Schedules a credit note.
     */
    public ScheduleCreditNoteExpiryResponse scheduleCreditNoteExpiry(ScheduleCreditNoteExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleCreditNoteExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCreditNote(request.getContext().getTenantId(), request.getCreditNote());

        var outbound0 = RevertLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertLedgerAccountRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getCreditNote().getAudit().getUpdatedBy())
                                .build())
                        .setBank(LedgerAccountBankFunding.newBuilder()
                                .setAccountHolder(switch (request.getCreditNote().getSettlementCase()) {
                                    case BANK -> request.getCreditNote().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .setBic(switch (request.getCreditNote().getSettlementCase()) {
                                    case BANK -> request.getCreditNote().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .setHolder(PostingBatchRef.newBuilder()
                                .setNumber(request.getCreditNote().getCounterparty().getNumber())
                                .build())
                        .build())
                .build();
        var reply0 = ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertLedgerAccount(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(UpdateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getLedgerAccount().getContact().getEmail())
                                .setPhone(reply0.getLedgerAccount().getContact().getPhone())
                                .build())
                        .build())
                .build()), UpdateUserRequest.class);
        var reply1 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateUser(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCreditNote().getCreditNoteId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleCreditNoteExpiry", request.getCreditNote().getCreditNoteId());
        return ScheduleCreditNoteExpiryResponse.newBuilder()
                .setCreditNote(CreditNote.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply1.getUser().getPrimaryAddress().getRecipient())
                                .build())
                        .setCreditNoteId(request.getCreditNoteId())
                        .setIssuedAt(request.getCreditNote().getIssuedAt())
                        .addLines(CreditNoteLine.newBuilder()
                                .putAllAttributes(reply1.getUser().getAttributesMap())
                                .build())
                        .build())
                .setOutcome(ScheduleCreditNoteExpiryResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .build();
    }
}
