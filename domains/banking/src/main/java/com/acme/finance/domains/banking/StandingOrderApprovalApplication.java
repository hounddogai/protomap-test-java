package com.acme.finance.domains.banking;

import com.acme.accounting.v1.CompareReconciliationOptionsRequest;
import com.acme.accounting.v1.CompareReconciliationOptionsResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyBlockAction;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.PreviewAccessPolicyRequest;
import com.acme.authorization.v1.PreviewAccessPolicyResponse;
import com.acme.banking.v1.ApproveStandingOrderRequest;
import com.acme.banking.v1.ApproveStandingOrderResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.CommentStatementApprovalRequest;
import com.acme.banking.v1.CommentStatementApprovalResponse;
import com.acme.banking.v1.DelegateStandingOrderApprovalRequest;
import com.acme.banking.v1.DelegateStandingOrderApprovalResponse;
import com.acme.banking.v1.EscalateStandingOrderRequest;
import com.acme.banking.v1.EscalateStandingOrderResponse;
import com.acme.banking.v1.ListBankAccountsRequest;
import com.acme.banking.v1.ListBankAccountsResponse;
import com.acme.banking.v1.ListPendingStandingOrdersRequest;
import com.acme.banking.v1.ListPendingStandingOrdersResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.PreviewStandingOrderRequest;
import com.acme.banking.v1.PreviewStandingOrderResponse;
import com.acme.banking.v1.RecalculateMandatesRequest;
import com.acme.banking.v1.RecalculateMandatesResponse;
import com.acme.banking.v1.RecallStandingOrderRequest;
import com.acme.banking.v1.RecallStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.StatementApprovalServiceGrpc;
import com.acme.banking.v1.SubmitStandingOrderRequest;
import com.acme.banking.v1.SubmitStandingOrderResponse;
import com.acme.banking.v1.SummarizeBankAccountsRequest;
import com.acme.banking.v1.SummarizeBankAccountsResponse;
import com.acme.banking.v1.UpdateBankAccountRequest;
import com.acme.banking.v1.UpdateBankAccountResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ExportCustomersRequest;
import com.acme.customers.v1.ExportCustomersResponse;
import com.acme.customers.v1.ExportHouseholdsRequest;
import com.acme.customers.v1.ExportHouseholdsResponse;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.SuggestHouseholdsRequest;
import com.acme.customers.v1.SuggestHouseholdsResponse;
import com.acme.customers.v1.ValidatePreferenceRequest;
import com.acme.customers.v1.ValidatePreferenceResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApprovePremiumRequest;
import com.acme.insurance.v1.ApprovePremiumResponse;
import com.acme.insurance.v1.ImportUnderwritingsRequest;
import com.acme.insurance.v1.ImportUnderwritingsResponse;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import java.util.ArrayList;
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
 * Routes standing orders through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class StandingOrderApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(StandingOrderApprovalApplication.class);

    private final ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking;

    private final BankAccountServiceGrpc.BankAccountServiceBlockingStub bankingBankAccountServiceBlocking;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking;

    private final StatementApprovalServiceGrpc.StatementApprovalServiceFutureStub bankingStatementApprovalServiceFuture;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking;

    private final UnderwritingImportServiceGrpc.UnderwritingImportServiceStub insuranceUnderwritingImportServiceAsync;

    private final BankingRepository repository;

    public StandingOrderApprovalApplication(
            ReconciliationCalculationServiceGrpc.ReconciliationCalculationServiceBlockingStub accountingReconciliationCalculationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking,
            BankAccountServiceGrpc.BankAccountServiceBlockingStub bankingBankAccountServiceBlocking,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking,
            StatementApprovalServiceGrpc.StatementApprovalServiceFutureStub bankingStatementApprovalServiceFuture,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            ApplicationEventPublisher events,
            PremiumApprovalServiceGrpc.PremiumApprovalServiceBlockingStub insurancePremiumApprovalServiceBlocking,
            UnderwritingImportServiceGrpc.UnderwritingImportServiceStub insuranceUnderwritingImportServiceAsync,
            BankingRepository repository) {
        this.accountingReconciliationCalculationServiceBlocking = accountingReconciliationCalculationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.bankingBankAccountQueryServiceBlocking = bankingBankAccountQueryServiceBlocking;
        this.bankingBankAccountServiceBlocking = bankingBankAccountServiceBlocking;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.bankingStandingOrderValidationServiceBlocking = bankingStandingOrderValidationServiceBlocking;
        this.bankingStatementApprovalServiceFuture = bankingStatementApprovalServiceFuture;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersHouseholdQueryServiceBlocking = customersHouseholdQueryServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.events = events;
        this.insurancePremiumApprovalServiceBlocking = insurancePremiumApprovalServiceBlocking;
        this.insuranceUnderwritingImportServiceAsync = insuranceUnderwritingImportServiceAsync;
        this.repository = repository;
    }

    public record EscalateStandingOrderStage2Requested(ListBankAccountsRequest request, CompletableFuture<ListBankAccountsResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a standing order.
     */
    public SubmitStandingOrderResponse submitStandingOrder(SubmitStandingOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitStandingOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var outbound0 = ApprovePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ApprovePremiumRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getStandingOrder().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        Function<ApprovePremiumRequest, ApprovePremiumResponse> operation0 = insurancePremiumApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::approvePremium;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = ExportCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportCustomersRequest.Filter.newBuilder()
                        .setRegion(request.getStandingOrder().getLocation().getRegion())
                        .build())
                .build();
        var stream1 = customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportCustomers(outbound1);
        var reply1 = ExportCustomersResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = ImportUnderwritingsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(ImportUnderwritingsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(ImportUnderwritingsResponse.getDefaultInstance());
        var input2 = insuranceUnderwritingImportServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).importUnderwritings(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getStandingOrder().getAudit().getCreatedAt())
                                .setCreatedBy(request.getStandingOrder().getAudit().getCreatedBy())
                                .setEtag(request.getStandingOrder().getAudit().getEtag())
                                .setUpdatedAt(request.getStandingOrder().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitStandingOrder", request.getStandingOrder().getStandingOrderId());
        return SubmitStandingOrderResponse.newBuilder()
                .setStandingOrder(StandingOrder.newBuilder()
                        .setOrganizer(request.getStandingOrder().getOrganizer())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getPremium().getCounterparty().getEmail())
                                        .build())
                                .build())
                        .setStandingOrderId(request.getStandingOrderId())
                        .build())
                .build();
    }

    /**
     * Approves a standing order.
     */
    public ApproveStandingOrderResponse approveStandingOrder(ApproveStandingOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveStandingOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        List<CompareReconciliationOptionsResponse> results0 = request.getStandingOrder().getParticipantsList().stream()
                .map(element -> CompareReconciliationOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setReconciliation(Reconciliation.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .setName(element.getContact().getName())
                                .setPhone(element.getContact().getPhone())
                                .setPreferredChannel(element.getContact().getPreferredChannel())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(accountingReconciliationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareReconciliationOptions(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? CompareReconciliationOptionsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = SuggestHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestHouseholdsRequest.Filter.newBuilder()
                        .setRegion(request.getStandingOrder().getLocation().getRegion())
                        .build())
                .build();
        var adapter1 = new Function<SuggestHouseholdsRequest, SuggestHouseholdsResponse>() {
            @Override
            public SuggestHouseholdsResponse apply(SuggestHouseholdsRequest input) {
                return customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestHouseholds(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);

        var outbound2 = (SummarizeBankAccountsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeBankAccountsRequest.Details.newBuilder()
                        .putAllFilters(reply0.getReconciliation().getCustomFieldsMap())
                        .setPeriod(request.getStandingOrder().getWindow())
                        .build())
                .setReason(request.getReason())
                .build()).toBuilder().build();
        var reply2 = bankingBankAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeBankAccounts(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveStandingOrder", request.getStandingOrder().getStandingOrderId());
        return ApproveStandingOrderResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setStandingOrderId(request.getStandingOrderId())
                        .setWindow(request.getStandingOrder().getWindow())
                        .build())
                .build();
    }

    /**
     * Escalates a standing order.
     */
    public EscalateStandingOrderResponse escalateStandingOrder(EscalateStandingOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateStandingOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var outbound0 = ValidatePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidatePreferenceRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getStandingOrder().getOrganizer().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validatePreference(outbound0);

        var outbound1 = PreviewStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getApprover().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply1 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewStandingOrder(outbound1);

        var outbound2 = ListBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListBankAccountsRequest.Filter.newBuilder()
                        .setRegion(reply1.getStandingOrder().getLocation().getRegion())
                        .build())
                .build();
        var completion2 = new CompletableFuture<ListBankAccountsResponse>();
        events.publishEvent(new EscalateStandingOrderStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateStandingOrder", request.getStandingOrder().getStandingOrderId());
        return EscalateStandingOrderResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPreference().getAudit().getCreatedBy())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRecipient(reply1.getStandingOrder().getLocation().getRecipient())
                                .build())
                        .setStandingOrderId(request.getStandingOrderId())
                        .setStatus(request.getStandingOrder().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists standing orders a page at a time.
     */
    public ListPendingStandingOrdersResponse listPendingStandingOrders(ListPendingStandingOrdersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingStandingOrders")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "StandingOrder", request.getContext().getTraceId(), "ListPendingStandingOrders");

        var outbound0 = PreviewAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setBlock(AccessPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewAccessPolicyRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAccessPolicy(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = UpdateBankAccountRequest.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAccessPolicy().getAudit().getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getAccessPolicy().getOwner().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage1Adapter {
            UpdateBankAccountResponse fetch(UpdateBankAccountRequest input) {
                return bankingBankAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateBankAccount(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingStandingOrders", request.getPage().getPageToken());
        return ListPendingStandingOrdersResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .addStandingOrders(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getAccessPolicy().getAudit().getEtag())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getAccessPolicy().getOwner().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    /**
     * Delegates a standing order.
     */
    public DelegateStandingOrderApprovalResponse delegateStandingOrderApproval(DelegateStandingOrderApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegateStandingOrderApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var outbound0 = BeanCopies.copy(RecalculateMandatesRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getStandingOrder().getAudit().getCreatedBy())
                                .setEtag(request.getStandingOrder().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getStandingOrder().getLocation().getLocality())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build(), RecalculateMandatesRequest.class);
        var reply0 = bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateMandates(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegateStandingOrderApproval", request.getStandingOrder().getStandingOrderId());
        return DelegateStandingOrderApprovalResponse.newBuilder()
                .setStandingOrder(StandingOrder.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getMandate().getBillingAddress().getPostalCode())
                                .build())
                        .setOrganizer(request.getStandingOrder().getOrganizer())
                        .setStandingOrderId(request.getStandingOrderId())
                        .build())
                .build();
    }

    /**
     * Recalls a standing order.
     */
    public RecallStandingOrderResponse recallStandingOrder(RecallStandingOrderRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallStandingOrder")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStandingOrder(request.getContext().getTenantId(), request.getStandingOrder());

        var results0 = new ArrayList<UpdateBankAccountResponse>();
        for (var element : request.getStandingOrder().getParticipantsList()) {
            UpdateBankAccountRequest itemRequest = UpdateBankAccountRequest.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(element.getContact().getName())
                                .setPreferredChannel(element.getContact().getPreferredChannel())
                                .build())
                        .setHolder(CustomerRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results0.add(bankingBankAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateBankAccount(itemRequest));
        }
        var reply0 = results0.isEmpty() ? UpdateBankAccountResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ExportHouseholdsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportHouseholdsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getBankAccount().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getBankAccount().getLabelsMap())
                        .setRegion(request.getStandingOrder().getLocation().getRegion())
                        .build())
                .build();
        var stream1 = customersHouseholdQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportHouseholds(outbound1);
        var reply1 = ExportHouseholdsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = CommentStatementApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setStatement(Statement.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getBankAccount().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getStandingOrder().getLocation().getPostalCode())
                                .build())
                        .build())
                .build();
        var pending2 = bankingStatementApprovalServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).commentStatementApproval(outbound2);
        var reply2 = RpcSupport.await(pending2);
        log.atDebug().addKeyValue("auditValue", request.getStandingOrder().getStandingOrderId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallStandingOrder", request.getStandingOrder().getStandingOrderId());
        return RecallStandingOrderResponse.newBuilder()
                .setOutcome(RecallStandingOrderResponse.Outcome.newBuilder()
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setStandingOrderId(request.getStandingOrderId())
                        .setWindow(request.getStandingOrder().getWindow())
                        .build())
                .build();
    }
}
