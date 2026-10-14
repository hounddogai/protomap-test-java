package com.acme.finance.domains.banking;

import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.WatchAccessPoliciesRequest;
import com.acme.authorization.v1.WatchAccessPoliciesResponse;
import com.acme.banking.v1.AdvanceBankAccountWorkflowRequest;
import com.acme.banking.v1.AdvanceBankAccountWorkflowResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountLimit;
import com.acme.banking.v1.CancelBankAccountWorkflowRequest;
import com.acme.banking.v1.CancelBankAccountWorkflowResponse;
import com.acme.banking.v1.ExplainMandateDecisionRequest;
import com.acme.banking.v1.ExplainMandateDecisionResponse;
import com.acme.banking.v1.InspectBankAccountWorkflowRequest;
import com.acme.banking.v1.InspectBankAccountWorkflowResponse;
import com.acme.banking.v1.ListBankAccountWorkflowsRequest;
import com.acme.banking.v1.ListBankAccountWorkflowsResponse;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.ResumeBankAccountWorkflowRequest;
import com.acme.banking.v1.ResumeBankAccountWorkflowResponse;
import com.acme.banking.v1.SignalBankAccountWorkflowRequest;
import com.acme.banking.v1.SignalBankAccountWorkflowResponse;
import com.acme.banking.v1.StartBankAccountWorkflowRequest;
import com.acme.banking.v1.StartBankAccountWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CalculateHouseholdRequest;
import com.acme.customers.v1.CalculateHouseholdResponse;
import com.acme.customers.v1.CalculatePreferenceRequest;
import com.acme.customers.v1.CalculatePreferenceResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ExplainConsentRecordDecisionRequest;
import com.acme.customers.v1.ExplainConsentRecordDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.customers.v1.UpdateCustomerRequest;
import com.acme.customers.v1.UpdateCustomerResponse;
import com.acme.customers.v1.ValidatePreferenceRequest;
import com.acme.customers.v1.ValidatePreferenceResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.NormalizeUserRequest;
import com.acme.identity.v1.NormalizeUserResponse;
import com.acme.identity.v1.PreviewUserRequest;
import com.acme.identity.v1.PreviewUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.insurance.v1.ApprovePolicyRequest;
import com.acme.insurance.v1.ApprovePolicyResponse;
import com.acme.insurance.v1.EscalatePolicyRequest;
import com.acme.insurance.v1.EscalatePolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountVersionsRequest;
import com.acme.ledger.v1.ListLedgerAccountVersionsResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.ResolveNotificationsRequest;
import com.acme.notifications.v1.ResolveNotificationsResponse;
import com.acme.notifications.v1.ScoreNotificationRequest;
import com.acme.notifications.v1.ScoreNotificationResponse;
import com.acme.search.v1.NormalizeSearchIndexRequest;
import com.acme.search.v1.NormalizeSearchIndexResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.venues.v1.SummarizeVenuesRequest;
import com.acme.venues.v1.SummarizeVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a bank account from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class BankAccountWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(BankAccountWorkflowApplication.class);

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking;

    private final BankingRepository repository;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public BankAccountWorkflowApplication(
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            ChannelRegistry channels,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            ApplicationEventPublisher events,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            PolicyApprovalServiceGrpc.PolicyApprovalServiceBlockingStub insurancePolicyApprovalServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            NotificationValidationServiceGrpc.NotificationValidationServiceBlockingStub notificationsNotificationValidationServiceBlocking,
            BankingRepository repository,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.channels = channels;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.events = events;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.insurancePolicyApprovalServiceBlocking = insurancePolicyApprovalServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.notificationsNotificationValidationServiceBlocking = notificationsNotificationValidationServiceBlocking;
        this.repository = repository;
        this.searchSearchIndexValidationServiceBlocking = searchSearchIndexValidationServiceBlocking;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    public record ResumeBankAccountWorkflowStage1Requested(UpdateCustomerRequest request, CompletableFuture<UpdateCustomerResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a bank account.
     */
    public StartBankAccountWorkflowResponse startBankAccountWorkflow(StartBankAccountWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartBankAccountWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankAccount(request.getContext().getTenantId(), request.getBankAccount());

        var outbound0 = ResolveNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var command0 = new DispatchCommand<>("acme.notifications.v1.NotificationQueryService/ResolveNotifications", outbound0);
        var reply0 = notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveNotifications(command0.payload());

        var outbound1 = BeanCopies.copy(EscalatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EscalatePolicyRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getBankAccount().getContact().getPhone())
                                .build())
                        .build())
                .build(), EscalatePolicyRequest.class);
        var reply1 = insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalatePolicy(outbound1);

        var outbound2 = QuotePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getBankAccount().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<QuotePreferenceRequest, QuotePreferenceResponse>() {
            @Override
            public QuotePreferenceResponse apply(QuotePreferenceRequest input) {
                return customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePreference(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBankAccount().getBankAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartBankAccountWorkflow", request.getBankAccount().getBankAccountId());
        return StartBankAccountWorkflowResponse.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getPolicy().getAudit().getCreatedAt())
                                .setUpdatedAt(reply2.getPreference().getAudit().getUpdatedAt())
                                .build())
                        .setBalance(request.getBankAccount().getBalance())
                        .setBankAccountId(request.getBankAccountId())
                        .build())
                .setOutcome(StartBankAccountWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Advances a bank account.
     */
    public AdvanceBankAccountWorkflowResponse advanceBankAccountWorkflow(AdvanceBankAccountWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceBankAccountWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankAccount(request.getContext().getTenantId(), request.getBankAccount());

        var outbound0 = ApprovePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setBank(PolicyBankFunding.newBuilder()
                                .setBic(switch (request.getBankAccount().getFundingCase()) {
                                    case BANK -> request.getBankAccount().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getBankAccount().getHolder().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = insurancePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approvePolicy(outbound0);

        var outbound1 = CalculateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(request.getBankAccount().getAudit().getUpdatedAt())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        Function<CalculateHouseholdRequest, CalculateHouseholdResponse> operation1 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::calculateHousehold;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBankAccount().getBankAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceBankAccountWorkflow", request.getBankAccount().getBankAccountId());
        return AdvanceBankAccountWorkflowResponse.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPolicy().getAudit().getCreatedAt())
                                .build())
                        .setBankAccountId(request.getBankAccountId())
                        .addLimits(BankAccountLimit.newBuilder()
                                .setName(reply1.getHousehold().getName())
                                .build())
                        .setOpenedAt(request.getBankAccount().getOpenedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Cancels a bank account.
     */
    public CancelBankAccountWorkflowResponse cancelBankAccountWorkflow(CancelBankAccountWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelBankAccountWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankAccount(request.getContext().getTenantId(), request.getBankAccount());

        var outbound0 = CalculatePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getBankAccount().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getBankAccount().getHolder().getDisplayName())
                                .setEmail(request.getBankAccount().getHolder().getEmail())
                                .build())
                        .putAllParameters(request.getDetails().getParametersMap())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.customers.v1.PreferenceCalculationService");
        var selected0 = PreferenceCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.calculatePreference(outbound0);

        var outbound1 = ValidatePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidatePreferenceRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getBankAccount().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getBankAccount().getAudit().getCreatedBy())
                                .setEtag(reply0.getPreference().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getPreference().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.customers.v1.PreferenceValidationService");
        var serviceName1 = "acme.customers.v1" + "." + "PreferenceValidationService";
        var methodName1 = "Val" + "idatePreference";
        var descriptor1 = MethodDescriptor.<ValidatePreferenceRequest, ValidatePreferenceResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ValidatePreferenceRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ValidatePreferenceResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = ScoreNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPreference().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> notificationsNotificationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreNotification(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getBankAccount().getBankAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelBankAccountWorkflow", request.getBankAccount().getBankAccountId());
        return CancelBankAccountWorkflowResponse.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getPreference().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getPreference().getAudit().getUpdatedAt())
                                .build())
                        .setBankAccountId(request.getBankAccountId())
                        .setContact(request.getBankAccount().getContact())
                        .setCurrencyCode(reply0.getBreakdown().getCurrencyCode())
                        .setHolder(CustomerRef.newBuilder()
                                .setDisplayName(reply1.getPreference().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Resumes a bank account.
     */
    public ResumeBankAccountWorkflowResponse resumeBankAccountWorkflow(ResumeBankAccountWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeBankAccountWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankAccount(request.getContext().getTenantId(), request.getBankAccount());

        var outbound0 = SummarizeVenuesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<SummarizeVenuesRequest, SummarizeVenuesResponse>() {
            @Override
            public SummarizeVenuesResponse apply(SummarizeVenuesRequest input) {
                return venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeVenues(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = UpdateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getBankAccount().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<UpdateCustomerResponse>();
        events.publishEvent(new ResumeBankAccountWorkflowStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = PreviewUserRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewUserRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getBankAccount().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getCustomer().getAudit().getUpdatedBy())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getCustomer().getPrimaryAddress().getLinesList())
                                .setLocality(reply1.getCustomer().getPrimaryAddress().getLocality())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewUser(value));
        log.atDebug().addKeyValue("auditValue", request.getBankAccount().getBankAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeBankAccountWorkflow", request.getBankAccount().getBankAccountId());
        return ResumeBankAccountWorkflowResponse.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getCustomer().getAudit().getUpdatedBy())
                                .build())
                        .setBalance(request.getBankAccount().getBalance())
                        .setBankAccountId(request.getBankAccountId())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply1.getCustomer().getContact().getPhone())
                                .build())
                        .build())
                .setOutcome(ResumeBankAccountWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a bank account.
     */
    public InspectBankAccountWorkflowResponse inspectBankAccountWorkflow(InspectBankAccountWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectBankAccountWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getBankAccountId(), "InspectBankAccountWorkflow");
        return InspectBankAccountWorkflowResponse.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setBankAccountId(request.getBankAccountId())
                        .build())
                .build();
    }

    /**
     * Lists bank accounts a page at a time.
     */
    public ListBankAccountWorkflowsResponse listBankAccountWorkflows(ListBankAccountWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListBankAccountWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getContext().getTraceId(), "ListBankAccountWorkflows");

        var outbound0 = RpcSupport.unpack(Any.pack(NormalizeUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build()), NormalizeUserRequest.class);
        var reply0 = identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeUser(outbound0);

        var outbound1 = NormalizeSearchIndexRequest.newBuilder()
                .setContext(request.getContext())
                .setSearchIndex(SearchIndex.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getUser().getPrimaryAddress().getPostalCode())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getUser().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            NormalizeSearchIndexResponse fetch(NormalizeSearchIndexRequest input) {
                return searchSearchIndexValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeSearchIndex(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListBankAccountWorkflows", request.getPage().getPageToken());
        return ListBankAccountWorkflowsResponse.newBuilder()
                .addBankAccounts(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getUser().getAudit().getCreatedAt())
                                .build())
                        .setHolder(CustomerRef.newBuilder()
                                .setDisplayName(reply1.getSearchIndex().getOwner().getDisplayName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Signals a bank account.
     */
    public SignalBankAccountWorkflowResponse signalBankAccountWorkflow(SignalBankAccountWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalBankAccountWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankAccount(request.getContext().getTenantId(), request.getBankAccount());

        var outbound0 = ExplainMandateDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainMandateDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getBankAccount().getHolder().getEmail())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.banking.v1.MandateValidationService/ExplainMandateDecision", outbound0);
        var reply0 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainMandateDecision(command0.payload());

        var outbound1 = RpcSupport.unpack(Any.pack(ListLedgerAccountVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLedgerAccountVersionsRequest.Filter.newBuilder()
                        .putAllLabels(request.getBankAccount().getLabelsMap())
                        .setRegion(reply0.getMandate().getBillingAddress().getRegion())
                        .build())
                .build()), ListLedgerAccountVersionsRequest.class);
        var reply1 = ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listLedgerAccountVersions(outbound1);

        var outbound2 = WatchAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchAccessPoliciesRequest.Filter.newBuilder()
                        .setRegion(reply0.getMandate().getBillingAddress().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply0.getMandate().getCounterparty().getEmail())
                        .setPhone(request.getDetails().getNotify().getPhone())
                        .setPreferredChannel(request.getBankAccount().getContact().getPreferredChannel())
                        .build())
                .build();
        var stream2 = authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchAccessPolicies(outbound2);
        var reply2 = WatchAccessPoliciesResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = ExplainConsentRecordDecisionRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainConsentRecordDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getBankAccount().getHolder().getEmail())
                                .setName(request.getBankAccount().getContact().getName())
                                .build())
                        .build())
                .build();
        var reply3 = Optional.of(outbound3).map(value -> value.toBuilder().build())
                .map(value -> customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainConsentRecordDecision(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getBankAccount().getBankAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalBankAccountWorkflow", request.getBankAccount().getBankAccountId());
        return SignalBankAccountWorkflowResponse.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setBankAccountId(request.getBankAccountId())
                        .setStatus(request.getBankAccount().getStatus())
                        .build())
                .setOutcome(SignalBankAccountWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply3.getResult().getCorrelationId())
                        .setMessage(reply3.getOutcome().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
