package com.acme.finance.domains.lending;

import com.acme.banking.v1.ApproveMandateRequest;
import com.acme.banking.v1.ApproveMandateResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.SubmitMandateRequest;
import com.acme.banking.v1.SubmitMandateResponse;
import com.acme.collections.v1.CommentPaymentPromiseApprovalRequest;
import com.acme.collections.v1.CommentPaymentPromiseApprovalResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.BrowseCustomersRequest;
import com.acme.customers.v1.BrowseCustomersResponse;
import com.acme.customers.v1.CalculateHouseholdRequest;
import com.acme.customers.v1.CalculateHouseholdResponse;
import com.acme.customers.v1.CheckHouseholdEligibilityRequest;
import com.acme.customers.v1.CheckHouseholdEligibilityResponse;
import com.acme.customers.v1.CreateCustomerRequest;
import com.acme.customers.v1.CreateCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentDiscountAction;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.NormalizeHouseholdRequest;
import com.acme.customers.v1.NormalizeHouseholdResponse;
import com.acme.customers.v1.RecalculateHouseholdsRequest;
import com.acme.customers.v1.RecalculateHouseholdsResponse;
import com.acme.customers.v1.SimulateCustomerSegmentRequest;
import com.acme.customers.v1.SimulateCustomerSegmentResponse;
import com.acme.customers.v1.SimulateHouseholdRequest;
import com.acme.customers.v1.SimulateHouseholdResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.CommentClaimApprovalRequest;
import com.acme.insurance.v1.CommentClaimApprovalResponse;
import com.acme.lending.v1.ApproveLoanRequest;
import com.acme.lending.v1.ApproveLoanResponse;
import com.acme.lending.v1.CommentLoanApprovalRequest;
import com.acme.lending.v1.CommentLoanApprovalResponse;
import com.acme.lending.v1.CountLoansRequest;
import com.acme.lending.v1.CountLoansResponse;
import com.acme.lending.v1.DelegateLoanApplicationApprovalRequest;
import com.acme.lending.v1.DelegateLoanApplicationApprovalResponse;
import com.acme.lending.v1.EscalateLoanRequest;
import com.acme.lending.v1.EscalateLoanResponse;
import com.acme.lending.v1.GetLoanApprovalStatusRequest;
import com.acme.lending.v1.GetLoanApprovalStatusResponse;
import com.acme.lending.v1.LintRepaymentsRequest;
import com.acme.lending.v1.LintRepaymentsResponse;
import com.acme.lending.v1.ListPendingLoanApplicationsRequest;
import com.acme.lending.v1.ListPendingLoanApplicationsResponse;
import com.acme.lending.v1.ListPendingLoansRequest;
import com.acme.lending.v1.ListPendingLoansResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanLifecycleServiceGrpc;
import com.acme.lending.v1.LoanQueryServiceGrpc;
import com.acme.lending.v1.RecallLoanRequest;
import com.acme.lending.v1.RecallLoanResponse;
import com.acme.lending.v1.ReinstateLoanRequest;
import com.acme.lending.v1.ReinstateLoanResponse;
import com.acme.lending.v1.RejectLoanRequest;
import com.acme.lending.v1.RejectLoanResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.SubmitLoanRequest;
import com.acme.lending.v1.SubmitLoanResponse;
import com.acme.messaging.v1.Conversation;
import com.acme.messaging.v1.ConversationCalculationServiceGrpc;
import com.acme.messaging.v1.SimulateConversationRequest;
import com.acme.messaging.v1.SimulateConversationResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Routes loans through approval, with escalation and delegation. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class LoanApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(LoanApprovalApplication.class);

    private final MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking;

    private final ChannelRegistry channels;

    private final PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking;

    private final CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking;

    private final LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking;

    private final LoanLifecycleServiceGrpc.LoanLifecycleServiceBlockingStub lendingLoanLifecycleServiceBlocking;

    private final LoanQueryServiceGrpc.LoanQueryServiceBlockingStub lendingLoanQueryServiceBlocking;

    private final RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking;

    private final LoanApprovalRequestMapper mapper;

    private final ConversationCalculationServiceGrpc.ConversationCalculationServiceBlockingStub messagingConversationCalculationServiceBlocking;

    private final LendingRepository repository;

    public LoanApprovalApplication(
            MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking,
            ChannelRegistry channels,
            PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking,
            CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking,
            LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking,
            LoanLifecycleServiceGrpc.LoanLifecycleServiceBlockingStub lendingLoanLifecycleServiceBlocking,
            LoanQueryServiceGrpc.LoanQueryServiceBlockingStub lendingLoanQueryServiceBlocking,
            RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking,
            LoanApprovalRequestMapper mapper,
            ConversationCalculationServiceGrpc.ConversationCalculationServiceBlockingStub messagingConversationCalculationServiceBlocking,
            LendingRepository repository) {
        this.bankingMandateApprovalServiceBlocking = bankingMandateApprovalServiceBlocking;
        this.channels = channels;
        this.collectionsPaymentPromiseApprovalServiceBlocking = collectionsPaymentPromiseApprovalServiceBlocking;
        this.customersCustomerQueryServiceBlocking = customersCustomerQueryServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersHouseholdCalculationServiceBlocking = customersHouseholdCalculationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.insuranceClaimApprovalServiceBlocking = insuranceClaimApprovalServiceBlocking;
        this.lendingLoanApplicationApprovalServiceBlocking = lendingLoanApplicationApprovalServiceBlocking;
        this.lendingLoanLifecycleServiceBlocking = lendingLoanLifecycleServiceBlocking;
        this.lendingLoanQueryServiceBlocking = lendingLoanQueryServiceBlocking;
        this.lendingRepaymentValidationServiceBlocking = lendingRepaymentValidationServiceBlocking;
        this.mapper = mapper;
        this.messagingConversationCalculationServiceBlocking = messagingConversationCalculationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Submits a loan.
     */
    public SubmitLoanResponse submitLoan(SubmitLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = DelegateLoanApplicationApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(DelegateLoanApplicationApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setCard(LoanApplicationCardSettlement.newBuilder()
                                .setNetwork(switch (request.getLoan().getFundingCase()) {
                                    case CARD -> request.getLoan().getCard().getNetwork();
                                    default -> "";
                                })
                                .setToken(switch (request.getLoan().getFundingCase()) {
                                    case CARD -> request.getLoan().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = lendingLoanApplicationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateLoanApplicationApproval(outbound0);

        var outbound1 = ApproveMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getLoan().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.banking.v1.MandateApprovalService/ApproveMandate", outbound1);
        var reply1 = bankingMandateApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveMandate(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitLoan", request.getLoan().getLoanId());
        return SubmitLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .setCurrencyCode(request.getLoan().getCurrencyCode())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setNumber(reply0.getLoanApplication().getNumber())
                                .build())
                        .setLoanId(request.getLoanId())
                        .build())
                .build();
    }

    /**
     * Approves a loan.
     */
    public ApproveLoanResponse approveLoan(ApproveLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = CommentClaimApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CommentClaimApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getLoan().getHolder().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(insuranceClaimApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentClaimApproval(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = RpcSupport.unpack(Any.pack(CommentPaymentPromiseApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getLoan().getContact().getEmail())
                                        .setName(reply0.getClaim().getRequester().getName())
                                        .setPhone(reply0.getClaim().getRequester().getPhone())
                                        .setPreferredChannel(reply0.getClaim().getRequester().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build()), CommentPaymentPromiseApprovalRequest.class);
        var reply1 = collectionsPaymentPromiseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentPaymentPromiseApproval(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveLoan", request.getLoan().getLoanId());
        return ApproveLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getClaim().getAudit().getUpdatedBy())
                                .build())
                        .setLoanId(request.getLoanId())
                        .setOpenedAt(request.getLoan().getOpenedAt())
                        .build())
                .setOutcome(ApproveLoanResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Rejects a loan.
     */
    public RejectLoanResponse rejectLoan(RejectLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = mapper.rejectLoanStage0(request);
        var reply0 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateHouseholds(outbound0);

        var outbound1 = CreateCustomerRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getLoan().getAudit().getCreatedBy())
                                .setEtag(request.getLoan().getAudit().getEtag())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getHousehold().getLocation().getLocality())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getLoan().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.customers.v1.CustomerService");
        var selected1 = CustomerServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.createCustomer(outbound1);

        List<SimulateHouseholdResponse> results2 = request.getLoan().getLimitsList().stream()
                .map(element -> SimulateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(request.getLoan().getAudit().getUpdatedAt())
                                                .build())
                                        .setLocation(PostalAddress.newBuilder()
                                                .setCountryCode(reply1.getCustomer().getPrimaryAddress().getCountryCode())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getDetails().getApprover().getEmail())
                                                .build())
                                        .build())
                                .setName(element.getName())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateHousehold(value)))
                .collect(Collectors.toList());
        var reply2 = results2.isEmpty() ? SimulateHouseholdResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectLoan", request.getLoan().getLoanId());
        return RejectLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getHousehold().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply1.getCustomer().getContact().getName())
                                .build())
                        .setLoanId(request.getLoanId())
                        .setOpenedAt(request.getLoan().getOpenedAt())
                        .build())
                .build();
    }

    /**
     * Escalates a loan.
     */
    public EscalateLoanResponse escalateLoan(EscalateLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        List<SimulateCustomerSegmentResponse> results0 = request.getLoan().getLimitsList().stream()
                .map(element -> SimulateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setDiscount(CustomerSegmentDiscountAction.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCustomerSegment(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? SimulateCustomerSegmentResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ListPendingLoanApplicationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingLoanApplicationsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getCustomerSegment().getAudit().getUpdatedAt())
                        .putAllLabels(request.getLoan().getLabelsMap())
                        .setOwnerId(request.getDetails().getApprover().getUserId())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.lending.v1.LoanApplicationApprovalService/ListPendingLoanApplications", outbound1);
        var reply1 = lendingLoanApplicationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingLoanApplications(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateLoan", request.getLoan().getLoanId());
        return EscalateLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setLoanId(request.getLoanId())
                        .setStatus(request.getLoan().getStatus())
                        .build())
                .setOutcome(EscalateLoanResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists loans a page at a time.
     */
    public ListPendingLoansResponse listPendingLoans(ListPendingLoansRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingLoans")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getContext().getTraceId(), "ListPendingLoans");

        var outbound0 = mapper.listPendingLoansStage0(request);
        var reply0 = lendingRepaymentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintRepayments(outbound0);

        var outbound1 = BrowseCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseCustomersRequest.Filter.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageSize(request.getPage().getPageSize())
                        .build())
                .build();
        var reply1 = RpcSupport.retryRead(() -> customersCustomerQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseCustomers(outbound1));
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingLoans", request.getPage().getPageToken());
        return ListPendingLoansResponse.newBuilder()
                .addLoans(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getRepayment().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setPageInfo(PageInfo.newBuilder()
                        .setTotalSize(reply1.getPageInfo().getTotalSize())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Recalls a loan.
     */
    public RecallLoanResponse recallLoan(RecallLoanRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallLoan")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = CountLoansRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountLoansRequest.Filter.newBuilder()
                        .putAllLabels(request.getLoan().getLabelsMap())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> lendingLoanQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countLoans(value));

        var outbound1 = ReinstateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReinstateLoanRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(request.getLoan().getHolder().getDisplayName())
                                .setEmail(request.getLoan().getContact().getEmail())
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setLoanId(request.getLoanId())
                .build();
        var reply1 = lendingLoanLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reinstateLoan(outbound1);

        var outbound2 = CheckHouseholdEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply1.getLoan().getAudit().getCreatedAt())
                                                .setUpdatedBy(request.getLoan().getAudit().getUpdatedBy())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply2 = customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkHouseholdEligibility(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallLoan", request.getLoan().getLoanId());
        return RecallLoanResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getHousehold().getAudit().getEtag())
                                .build())
                        .setContact(request.getLoan().getContact())
                        .setLoanId(request.getLoanId())
                        .build())
                .setOutcome(RecallLoanResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllNextSteps(reply1.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a loan.
     */
    public GetLoanApprovalStatusResponse getLoanApprovalStatus(GetLoanApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLoanApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Loan", request.getLoanId(), "GetLoanApprovalStatus");
        return GetLoanApprovalStatusResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setLoanId(request.getLoanId())
                        .build())
                .build();
    }

    /**
     * Comments a loan.
     */
    public CommentLoanApprovalResponse commentLoanApproval(CommentLoanApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentLoanApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLoan(request.getContext().getTenantId(), request.getLoan());

        var outbound0 = SimulateConversationRequest.newBuilder()
                .setContext(request.getContext())
                .setConversation(Conversation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLoan().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<SimulateConversationRequest, SimulateConversationResponse>() {
            @Override
            public SimulateConversationResponse apply(SimulateConversationRequest input) {
                return messagingConversationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateConversation(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = JsonCodec.roundTrip(NormalizeHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedAt(reply0.getConversation().getAudit().getCreatedAt())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getLoan().getHolder().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build(), NormalizeHouseholdRequest.class);
        var reply1 = customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeHousehold(outbound1);

        var outbound2 = JsonCodec.roundTrip(CalculateHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setCreatedBy(reply0.getConversation().getAudit().getCreatedBy())
                                                .setEtag(request.getLoan().getAudit().getEtag())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply0.getConversation().getAssignee().getDisplayName())
                                                .setEmail(reply1.getHousehold().getManager().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build(), CalculateHouseholdRequest.class);
        var reply2 = customersHouseholdCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateHousehold(outbound2);

        var outbound3 = SubmitMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitMandateRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(reply2.getHousehold().getManager().getUserId())
                                .build())
                        .build())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getConversation().getAudit().getCreatedBy())
                                .build())
                        .setCard(MandateCardSettlement.newBuilder()
                                .setLast4(switch (request.getLoan().getFundingCase()) {
                                    case CARD -> request.getLoan().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply2.getHousehold().getManager().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply3 = bankingMandateApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitMandate(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getLoan().getLoanId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentLoanApproval", request.getLoan().getLoanId());
        return CommentLoanApprovalResponse.newBuilder()
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply3.getMandate().getAudit().getCreatedAt())
                                .setEtag(reply2.getHousehold().getAudit().getEtag())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getConversation().getRequester().getPreferredChannel())
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(reply0.getConversation().getAssignee().getDisplayName())
                                .build())
                        .putAllLabels(request.getLoan().getLabelsMap())
                        .setLoanId(request.getLoanId())
                        .build())
                .setOutcome(CommentLoanApprovalResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllNextSteps(reply3.getOutcome().getNextStepsList())
                        .build())
                .build();
    }
}
