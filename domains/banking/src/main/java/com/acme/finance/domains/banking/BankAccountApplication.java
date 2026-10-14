package com.acme.finance.domains.banking;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.banking.v1.BatchGetBankAccountsRequest;
import com.acme.banking.v1.BatchGetBankAccountsResponse;
import com.acme.banking.v1.CloneBankAccountRequest;
import com.acme.banking.v1.CloneBankAccountResponse;
import com.acme.banking.v1.CreateBankAccountRequest;
import com.acme.banking.v1.CreateBankAccountResponse;
import com.acme.banking.v1.DeleteBankAccountRequest;
import com.acme.banking.v1.DeleteBankAccountResponse;
import com.acme.banking.v1.ExplainMandateDecisionRequest;
import com.acme.banking.v1.ExplainMandateDecisionResponse;
import com.acme.banking.v1.GetBankAccountRequest;
import com.acme.banking.v1.GetBankAccountResponse;
import com.acme.banking.v1.ListBankAccountsRequest;
import com.acme.banking.v1.ListBankAccountsResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateBankSettlement;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.NormalizeMandateRequest;
import com.acme.banking.v1.NormalizeMandateResponse;
import com.acme.banking.v1.SearchBankAccountsRequest;
import com.acme.banking.v1.SearchBankAccountsResponse;
import com.acme.banking.v1.UpdateBankAccountRequest;
import com.acme.banking.v1.UpdateBankAccountResponse;
import com.acme.banking.v1.ValidateMandateRequest;
import com.acme.banking.v1.ValidateMandateResponse;
import com.acme.banking.v1.WatchBankAccountsRequest;
import com.acme.banking.v1.WatchBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.corporateevents.v1.CorporateEventServiceGrpc;
import com.acme.corporateevents.v1.SearchCorporateEventsRequest;
import com.acme.corporateevents.v1.SearchCorporateEventsResponse;
import com.acme.corporateevents.v1.WatchCorporateEventsRequest;
import com.acme.corporateevents.v1.WatchCorporateEventsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.SearchUsersRequest;
import com.acme.identity.v1.SearchUsersResponse;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.identity.v1.WatchUsersRequest;
import com.acme.identity.v1.WatchUsersResponse;
import com.acme.insurance.v1.CheckPremiumEligibilityRequest;
import com.acme.insurance.v1.CheckPremiumEligibilityResponse;
import com.acme.insurance.v1.EstimateUnderwritingRequest;
import com.acme.insurance.v1.EstimateUnderwritingResponse;
import com.acme.insurance.v1.ExplainPolicyDecisionRequest;
import com.acme.insurance.v1.ExplainPolicyDecisionResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.insurance.v1.ValidatePremiumRequest;
import com.acme.insurance.v1.ValidatePremiumResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeRequest;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeResponse;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesRequest;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesResponse;
import com.acme.lineage.v1.CalculateLineageNodeRequest;
import com.acme.lineage.v1.CalculateLineageNodeResponse;
import com.acme.lineage.v1.LineageNode;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Manages bank accounts: creates, reads, changes, and lists them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class BankAccountApplication {
    private static final Logger log = LoggerFactory.getLogger(BankAccountApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final CorporateEventServiceGrpc.CorporateEventServiceBlockingStub corporateeventsCorporateEventServiceBlocking;

    private final ApplicationEventPublisher events;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking;

    private final UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final BankAccountRequestMapper mapper;

    private final BankingRepository repository;

    public BankAccountApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            RetentionPolicyValidationServiceGrpc.RetentionPolicyValidationServiceBlockingStub auditRetentionPolicyValidationServiceBlocking,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            ChannelRegistry channels,
            CorporateEventServiceGrpc.CorporateEventServiceBlockingStub corporateeventsCorporateEventServiceBlocking,
            ApplicationEventPublisher events,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            PremiumValidationServiceGrpc.PremiumValidationServiceBlockingStub insurancePremiumValidationServiceBlocking,
            UnderwritingCalculationServiceGrpc.UnderwritingCalculationServiceBlockingStub insuranceUnderwritingCalculationServiceBlocking,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            BankAccountRequestMapper mapper,
            BankingRepository repository) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.auditRetentionPolicyValidationServiceBlocking = auditRetentionPolicyValidationServiceBlocking;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.channels = channels;
        this.corporateeventsCorporateEventServiceBlocking = corporateeventsCorporateEventServiceBlocking;
        this.events = events;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.insurancePremiumValidationServiceBlocking = insurancePremiumValidationServiceBlocking;
        this.insuranceUnderwritingCalculationServiceBlocking = insuranceUnderwritingCalculationServiceBlocking;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
    }

    public record GetBankAccountStage1Requested(ExplainMandateDecisionRequest request, CompletableFuture<ExplainMandateDecisionResponse> completion, io.grpc.Context context) {}

    /**
     * Creates a bank account.
     */
    public CreateBankAccountResponse createBankAccount(CreateBankAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CreateBankAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankAccount(request.getContext().getTenantId(), request.getBankAccount());

        var outbound0 = WatchUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchUsersRequest.Filter.newBuilder()
                        .putAllLabels(request.getBankAccount().getLabelsMap())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getBankAccount().getHolder().getEmail())
                        .setPhone(request.getBankAccount().getContact().getPhone())
                        .setPreferredChannel(request.getBankAccount().getContact().getPreferredChannel())
                        .build())
                .build();
        var stream0 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchUsers(outbound0);
        var reply0 = WatchUsersResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var results1 = new ArrayList<ValidateMandateResponse>();
        for (var element : request.getBankAccount().getLimitsList()) {
            ValidateMandateRequest itemRequest = ValidateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateMandateRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getBankAccount().getContact().getName())
                                .setPreferredChannel(request.getBankAccount().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .addLines(MandateLine.newBuilder()
                                .setTax(MandateLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
            results1.add(bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateMandate(itemRequest));
        }
        var reply1 = results1.isEmpty() ? ValidateMandateResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getBankAccount().getBankAccountId())
                        .setSubmittedBy(request.getBankAccount().getContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input2 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBankAccount().getAudit().getCreatedAt())
                                .setCreatedBy(request.getBankAccount().getAudit().getCreatedBy())
                                .setEtag(request.getBankAccount().getAudit().getEtag())
                                .setUpdatedAt(request.getBankAccount().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getBankAccount().getHolder().getCustomerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CreateBankAccount", request.getBankAccount().getHolder().getCustomerId());
        return CreateBankAccountResponse.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAvailable(request.getBankAccount().getAvailable())
                        .putAllLabels(request.getBankAccount().getLabelsMap())
                        .build())
                .build();
    }

    /**
     * Returns a bank account.
     */
    public GetBankAccountResponse getBankAccount(GetBankAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetBankAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getBankAccountId(), "GetBankAccount");

        var outbound0 = ValidatePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.PremiumValidationService");
        var serviceName0 = "acme.insurance.v1" + "." + "PremiumValidationService";
        var methodName0 = "Val" + "idatePremium";
        var descriptor0 = MethodDescriptor.<ValidatePremiumRequest, ValidatePremiumResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ValidatePremiumRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ValidatePremiumResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ExplainMandateDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setBank(MandateBankSettlement.newBuilder()
                                .setAccountHolder(reply0.getPremium().getBank().getAccountHolder())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPremium().getBillingAddress().getCountryCode())
                                .setLocality(reply0.getPremium().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<ExplainMandateDecisionResponse>();
        events.publishEvent(new GetBankAccountStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        return GetBankAccountResponse.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getMandate().getAudit().getUpdatedBy())
                                .build())
                        .setBankAccountId(request.getBankAccountId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Changes a bank account.
     */
    public UpdateBankAccountResponse updateBankAccount(UpdateBankAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UpdateBankAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankAccount(request.getContext().getTenantId(), request.getBankAccount());

        var outbound0 = StreamFiscalPeriodNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getBankAccount().getHolder().getEmail())
                        .setPreferredChannel(request.getBankAccount().getContact().getPreferredChannel())
                        .build())
                .build();
        var stream0 = ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamFiscalPeriodNotices(outbound0);
        var reply0 = StreamFiscalPeriodNoticesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = BeanCopies.copy(SearchUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchUsersRequest.Filter.newBuilder()
                        .putAllLabels(request.getBankAccount().getLabelsMap())
                        .build())
                .build(), SearchUsersRequest.class);
        var reply1 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchUsers(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBankAccount().getBankAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UpdateBankAccount", request.getBankAccount().getBankAccountId());
        return UpdateBankAccountResponse.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAvailable(request.getBankAccount().getAvailable())
                        .setStatus(request.getBankAccount().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Deletes a bank account.
     */
    public DeleteBankAccountResponse deleteBankAccount(DeleteBankAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteBankAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getBankAccountId(), "DeleteBankAccount");

        var outbound0 = SearchCorporateEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchCorporateEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getBankAccountId())
                        .build())
                .build();
        Function<SearchCorporateEventsRequest, SearchCorporateEventsResponse> operation0 = corporateeventsCorporateEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::searchCorporateEvents;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = mapper.deleteBankAccountStage1(request);
        var reply1 = insurancePremiumValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPremiumEligibility(outbound1);

        var outbound2 = ScoreRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreRetentionPolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getPremium().getBillingAddress().getCountryCode())
                                .addAllLines(reply1.getPremium().getBillingAddress().getLinesList())
                                .setLocality(reply1.getPremium().getBillingAddress().getLocality())
                                .setPostalCode(reply1.getPremium().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply2 = auditRetentionPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreRetentionPolicy(outbound2);

        var outbound3 = NormalizeMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getRetentionPolicy().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply3 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeMandate(outbound3);
        return DeleteBankAccountResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply3.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists bank accounts a page at a time.
     */
    public ListBankAccountsResponse listBankAccounts(ListBankAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListBankAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getContext().getTraceId(), "ListBankAccounts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListBankAccounts", request.getPage().getPageToken());
        return ListBankAccountsResponse.getDefaultInstance();
    }

    /**
     * Returns several bank accounts at once.
     */
    public BatchGetBankAccountsResponse batchGetBankAccounts(BatchGetBankAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchGetBankAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getContext().getTraceId(), "BatchGetBankAccounts");

        var outbound0 = ExplainPolicyDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainPolicyDecisionRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setExternalAccountId(request.getContext().getActor().getUserId())
                        .setOpenedAt(request.getContext().getDeadline())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.insurance.v1.PolicyValidationService");
        var selected0 = PolicyValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.explainPolicyDecision(outbound0);
        return BatchGetBankAccountsResponse.newBuilder()
                .addBankAccounts(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getPolicy().getAudit().getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply0.getPolicy().getContact().getPhone())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists bank accounts a page at a time.
     */
    public SearchBankAccountsResponse searchBankAccounts(SearchBankAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SearchBankAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getContext().getTraceId(), "SearchBankAccounts");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SearchBankAccounts", request.getPage().getPageToken());
        return SearchBankAccountsResponse.getDefaultInstance();
    }

    /**
     * Streams the changes of bank accounts as they happen.
     */
    public WatchBankAccountsResponse watchBankAccounts(WatchBankAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "WatchBankAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankAccount", request.getContext().getTraceId(), "WatchBankAccounts");

        var outbound0 = CalculateLineageNodeRequest.newBuilder()
                .setContext(request.getContext())
                .setLineageNode(LineageNode.newBuilder()
                        .addChildren(LineageNode.newBuilder()
                                .addChildren(LineageNode.newBuilder()
                                        .setLocation(PostalAddress.newBuilder()
                                                .setRegion(request.getFilter().getRegion())
                                                .build())
                                        .build())
                                .setManager(UserRef.newBuilder()
                                        .setEmail(request.getSubscriber().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.lineage.v1.LineageNodeCalculationService");
        var serviceName0 = "acme.lineage.v1" + "." + "LineageNodeCalculationService";
        var methodName0 = "Cal" + "culateLineageNode";
        var descriptor0 = MethodDescriptor.<CalculateLineageNodeRequest, CalculateLineageNodeResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(CalculateLineageNodeRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CalculateLineageNodeResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "WatchBankAccounts", request.getFilter().getQuery());
        return WatchBankAccountsResponse.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getLineageNode().getAudit().getCreatedAt())
                                .setUpdatedBy(reply0.getLineageNode().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                                .build())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .build();
    }

    /**
     * Takes a stream of bank accounts and answers once at the end.
     */
    public BatchCreateBankAccountsResponse batchCreateBankAccounts(BatchCreateBankAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BatchCreateBankAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasRecord()) {
            repository.saveBankAccount(request.getContext().getTenantId(), request.getRecord());
        }

        var results0 = new ArrayList<ValidateMandateResponse>();
        for (var element : request.getRecord().getLimitsList()) {
            ValidateMandateRequest itemRequest = ValidateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getRecord().getAudit().getEtag())
                                .build())
                        .addLines(MandateLine.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .build();
            results0.add(bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateMandate(itemRequest));
        }
        var reply0 = results0.isEmpty() ? ValidateMandateResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = WatchCorporateEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchCorporateEventsRequest.Filter.newBuilder()
                        .putAllLabels(request.getRecord().getLabelsMap())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setPhone(request.getRecord().getContact().getPhone())
                        .build())
                .build();
        var stream1 = corporateeventsCorporateEventServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchCorporateEvents(outbound1);
        var reply1 = WatchCorporateEventsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getHeader().getSource()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BatchCreateBankAccounts", request.getHeader().getSource());
        return BatchCreateBankAccountsResponse.getDefaultInstance();
    }

    /**
     * Creates a bank account.
     */
    public CloneBankAccountResponse cloneBankAccount(CloneBankAccountRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CloneBankAccount")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBankAccount(request.getContext().getTenantId(), request.getBankAccount());

        var outbound0 = RpcSupport.unpack(Any.pack(SendFiscalPeriodTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendFiscalPeriodTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getBankAccount().getHolder().getEmail())
                                .setName(request.getBankAccount().getContact().getName())
                                .setPhone(request.getBankAccount().getContact().getPhone())
                                .setPreferredChannel(request.getBankAccount().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build()), SendFiscalPeriodTestNoticeRequest.class);
        var reply0 = ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendFiscalPeriodTestNotice(outbound0);

        var outbound1 = EstimateUnderwritingRequest.newBuilder()
                .setContext(request.getContext())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBankAccount().getAudit().getCreatedAt())
                                .setCreatedBy(request.getBankAccount().getAudit().getCreatedBy())
                                .setEtag(reply0.getFiscalPeriod().getAudit().getEtag())
                                .setUpdatedAt(request.getBankAccount().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply1 = insuranceUnderwritingCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateUnderwriting(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBankAccount().getBankAccountId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CloneBankAccount", request.getBankAccount().getBankAccountId());
        return CloneBankAccountResponse.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply1.getUnderwriting().getRequester().getName())
                                .build())
                        .setHolder(request.getBankAccount().getHolder())
                        .putAllLabels(request.getBankAccount().getLabelsMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
