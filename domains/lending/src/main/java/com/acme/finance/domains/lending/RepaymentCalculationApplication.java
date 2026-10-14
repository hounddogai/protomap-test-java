package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.CheckAccessPolicyEligibilityRequest;
import com.acme.authorization.v1.CheckAccessPolicyEligibilityResponse;
import com.acme.authorization.v1.ScoreAccessPolicyRequest;
import com.acme.authorization.v1.ScoreAccessPolicyResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.BrowseBankAccountsRequest;
import com.acme.banking.v1.BrowseBankAccountsResponse;
import com.acme.banking.v1.CreateBankAccountRequest;
import com.acme.banking.v1.CreateBankAccountResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.ValidateMandateRequest;
import com.acme.banking.v1.ValidateMandateResponse;
import com.acme.chat.v1.ChatRoom;
import com.acme.chat.v1.ChatRoomServiceGrpc;
import com.acme.chat.v1.CreateChatRoomRequest;
import com.acme.chat.v1.CreateChatRoomResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.lending.v1.CalculateCollateralRequest;
import com.acme.lending.v1.CalculateCollateralResponse;
import com.acme.lending.v1.CalculateRepaymentRequest;
import com.acme.lending.v1.CalculateRepaymentResponse;
import com.acme.lending.v1.CheckLoanEligibilityRequest;
import com.acme.lending.v1.CheckLoanEligibilityResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CollateralComponent;
import com.acme.lending.v1.CompareCollateralOptionsRequest;
import com.acme.lending.v1.CompareCollateralOptionsResponse;
import com.acme.lending.v1.CompareRepaymentOptionsRequest;
import com.acme.lending.v1.CompareRepaymentOptionsResponse;
import com.acme.lending.v1.EstimateCollateralRequest;
import com.acme.lending.v1.EstimateCollateralResponse;
import com.acme.lending.v1.EstimateRepaymentRequest;
import com.acme.lending.v1.EstimateRepaymentResponse;
import com.acme.lending.v1.ExplainLoanDecisionRequest;
import com.acme.lending.v1.ExplainLoanDecisionResponse;
import com.acme.lending.v1.ExplainRepaymentCalculationRequest;
import com.acme.lending.v1.ExplainRepaymentCalculationResponse;
import com.acme.lending.v1.GetRepaymentRatesRequest;
import com.acme.lending.v1.GetRepaymentRatesResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.QuoteCollateralRequest;
import com.acme.lending.v1.QuoteCollateralResponse;
import com.acme.lending.v1.QuoteRepaymentRequest;
import com.acme.lending.v1.QuoteRepaymentResponse;
import com.acme.lending.v1.RecalculateRepaymentsRequest;
import com.acme.lending.v1.RecalculateRepaymentsResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import com.acme.lending.v1.SimulateRepaymentRequest;
import com.acme.lending.v1.SimulateRepaymentResponse;
import com.acme.lending.v1.ValidateLoanApplicationRequest;
import com.acme.lending.v1.ValidateLoanApplicationResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.notifications.v1.ResolveNotificationsRequest;
import com.acme.notifications.v1.ResolveNotificationsResponse;
import com.acme.notifications.v1.SummarizeNotificationsRequest;
import com.acme.notifications.v1.SummarizeNotificationsResponse;
import com.acme.personalization.v1.CreateProfileRequest;
import com.acme.personalization.v1.CreateProfileResponse;
import com.acme.personalization.v1.Profile;
import com.acme.personalization.v1.ProfileServiceGrpc;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a repayment, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class RepaymentCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(RepaymentCalculationApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ChatRoomServiceGrpc.ChatRoomServiceBlockingStub chatChatRoomServiceBlocking;

    private final ApplicationEventPublisher events;

    private final CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking;

    private final LoanValidationServiceGrpc.LoanValidationServiceBlockingStub lendingLoanValidationServiceBlocking;

    private final NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking;

    private final ProfileServiceGrpc.ProfileServiceBlockingStub personalizationProfileServiceBlocking;

    private final LendingRepository repository;

    private final Executor rpcExecutor;

    public RepaymentCalculationApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            BankAccountQueryServiceGrpc.BankAccountQueryServiceBlockingStub bankingBankAccountQueryServiceBlocking,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            ChannelRegistry channels,
            ChatRoomServiceGrpc.ChatRoomServiceBlockingStub chatChatRoomServiceBlocking,
            ApplicationEventPublisher events,
            CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking,
            LoanValidationServiceGrpc.LoanValidationServiceBlockingStub lendingLoanValidationServiceBlocking,
            NotificationQueryServiceGrpc.NotificationQueryServiceBlockingStub notificationsNotificationQueryServiceBlocking,
            ProfileServiceGrpc.ProfileServiceBlockingStub personalizationProfileServiceBlocking,
            LendingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.bankingBankAccountQueryServiceBlocking = bankingBankAccountQueryServiceBlocking;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.channels = channels;
        this.chatChatRoomServiceBlocking = chatChatRoomServiceBlocking;
        this.events = events;
        this.lendingCollateralCalculationServiceBlocking = lendingCollateralCalculationServiceBlocking;
        this.lendingLoanValidationServiceBlocking = lendingLoanValidationServiceBlocking;
        this.notificationsNotificationQueryServiceBlocking = notificationsNotificationQueryServiceBlocking;
        this.personalizationProfileServiceBlocking = personalizationProfileServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record ExplainRepaymentCalculationStage2Requested(CreateBankAccountRequest request, CompletableFuture<CreateBankAccountResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates a repayment.
     */
    public CalculateRepaymentResponse calculateRepayment(CalculateRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = BrowseBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseBankAccountsRequest.Filter.newBuilder()
                        .setRegion(request.getRepayment().getLocation().getRegion())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> bankingBankAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseBankAccounts(value)).orElseThrow();

        var outbound1 = CompareCollateralOptionsRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setLocality(request.getRepayment().getLocation().getLocality())
                                .setRecipient(request.getRepayment().getLocation().getRecipient())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getRepayment().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareCollateralOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .build();
        var reply1 = lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCollateralOptions(outbound1);

        var outbound2 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
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
                                .setCreatedAt(request.getRepayment().getAudit().getCreatedAt())
                                .setCreatedBy(request.getRepayment().getAudit().getCreatedBy())
                                .setEtag(request.getRepayment().getAudit().getEtag())
                                .setUpdatedAt(request.getRepayment().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateRepayment", request.getRepayment().getRepaymentId());
        return CalculateRepaymentResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .putAllMetadata(request.getRepayment().getMetadataMap())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply1.getCollateral().getOwner().getEmail())
                                        .build())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Estimates a repayment.
     */
    public EstimateRepaymentResponse estimateRepayment(EstimateRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = ValidateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getRepayment().getLocation().getCountryCode())
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateMandate(value));
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateRepayment", request.getRepayment().getRepaymentId());
        return EstimateRepaymentResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply0.getMandate().getBillingAddress().getLinesList())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .setWindow(request.getRepayment().getWindow())
                        .build())
                .build();
    }

    /**
     * Quotes a repayment.
     */
    public QuoteRepaymentResponse quoteRepayment(QuoteRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = SimulateCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getRepayment().getOrganizer().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCollateral(value));
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteRepayment", request.getRepayment().getRepaymentId());
        return QuoteRepaymentResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply0.getCollateral().getAddress().getLinesList())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .setStatus(request.getRepayment().getStatus())
                        .build())
                .build();
    }

    /**
     * Compares a repayment.
     */
    public CompareRepaymentOptionsResponse compareRepaymentOptions(CompareRepaymentOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareRepaymentOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        List<CheckAccessPolicyEligibilityResponse> results0 = request.getRepayment().getParticipantsList().stream()
                .map(element -> CheckAccessPolicyEligibilityRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CheckAccessPolicyEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(element.getContact().getName())
                                .setPhone(element.getContact().getPhone())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkAccessPolicyEligibility(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? CheckAccessPolicyEligibilityResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = JsonCodec.roundTrip(EstimateCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getRepayment().getLocation().getPostalCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), EstimateCollateralRequest.class);
        var reply1 = lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateCollateral(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareRepaymentOptions", request.getRepayment().getRepaymentId());
        return CompareRepaymentOptionsResponse.newBuilder()
                .setBreakdown(CompareRepaymentOptionsResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Recalculates a repayment.
     */
    public RecalculateRepaymentsResponse recalculateRepayments(RecalculateRepaymentsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateRepayments")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = CalculateCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getRepayment().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CalculateCollateralRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.lending.v1.CollateralCalculationService/CalculateCollateral", outbound0);
        var reply0 = lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCollateral(command0.payload());

        var outbound1 = ValidateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.lending.v1.LoanApplicationValidationService");
        var selected1 = LoanApplicationValidationServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.validateLoanApplication(outbound1);

        var outbound2 = SummarizeNotificationsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeNotificationsRequest.Details.newBuilder()
                        .setPeriod(request.getRepayment().getWindow())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply2 = notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeNotifications(outbound2);

        var outbound3 = ExplainCoverageCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getCollateral().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getLoanApplication().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply1.getLoanApplication().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.insurance.v1.CoverageCalculationService");
        var selected3 = CoverageCalculationServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.explainCoverageCalculation(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateRepayments", request.getRepayment().getRepaymentId());
        return RecalculateRepaymentsResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getLoanApplication().getAudit().getUpdatedBy())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply0.getCollateral().getOwner().getEmail())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply3.getCoverage().getOwner().getEmail())
                                        .build())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .setTitle(request.getRepayment().getTitle())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Simulates a repayment.
     */
    public SimulateRepaymentResponse simulateRepayment(SimulateRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = CheckLoanEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getRepayment().getAudit().getUpdatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getRepayment().getOrganizer().getEmail())
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(request.getRepayment().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> lendingLoanValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkLoanEligibility(value));

        var outbound1 = CompareCollateralOptionsRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getRepayment().getOrganizer().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareCollateralOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            CompareCollateralOptionsResponse fetch(CompareCollateralOptionsRequest input) {
                return lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCollateralOptions(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var outbound2 = JsonCodec.roundTrip(ResolveNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .build(), ResolveNotificationsRequest.class);
        var reply2 = notificationsNotificationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveNotifications(outbound2);

        var results3 = new ArrayList<CreateProfileResponse>();
        for (var element : request.getRepayment().getParticipantsList()) {
            CreateProfileRequest itemRequest = CreateProfileRequest.newBuilder()
                .setContext(request.getContext())
                .setProfile(Profile.newBuilder()
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .build();
            results3.add(personalizationProfileServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createProfile(itemRequest));
        }
        var reply3 = results3.isEmpty() ? CreateProfileResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateRepayment", request.getRepayment().getRepaymentId());
        return SimulateRepaymentResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRecipient(reply1.getCollateral().getAddress().getRecipient())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPreferredChannel(reply0.getLoan().getContact().getPreferredChannel())
                                        .build())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .setTitle(request.getRepayment().getTitle())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a repayment.
     */
    public ExplainRepaymentCalculationResponse explainRepaymentCalculation(ExplainRepaymentCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainRepaymentCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = ScoreAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getRepayment().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.authorization.v1.AccessPolicyValidationService");
        var selected0 = AccessPolicyValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.scoreAccessPolicy(outbound0);

        var futures1 = request.getRepayment().getParticipantsList().stream()
                .map(element -> ExplainLoanDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainLoanDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(element.getContact().getName())
                                .setPhone(element.getContact().getPhone())
                                .setPreferredChannel(element.getContact().getPreferredChannel())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> lendingLoanValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainLoanDecision(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? ExplainLoanDecisionResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = CreateBankAccountRequest.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRepayment().getAudit().getCreatedAt())
                                .setCreatedBy(reply1.getLoan().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var completion2 = new CompletableFuture<CreateBankAccountResponse>();
        events.publishEvent(new ExplainRepaymentCalculationStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainRepaymentCalculation", request.getRepayment().getRepaymentId());
        return ExplainRepaymentCalculationResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getBankAccount().getAudit().getUpdatedAt())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply2.getBankAccount().getHolder().getEmail())
                                        .build())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .setTitle(request.getRepayment().getTitle())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a repayment.
     */
    public GetRepaymentRatesResponse getRepaymentRates(GetRepaymentRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRepaymentRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getRepaymentId(), "GetRepaymentRates");

        var builder0 = QuoteCollateralRequest.newBuilder();
        builder0.setCollateral(Collateral.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedAt(request.getContext().getDeadline())
                        .build())
                .putComponents("primary", CollateralComponent.newBuilder()
                        .setComponentId(request.getRepaymentId())
                        .build())
                .setOwner(UserRef.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .setUserId(request.getContext().getActor().getUserId())
                        .build())
                .build());
        builder0.setContext(request.getContext());
        var outbound0 = builder0.build();
        var reply0 = lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCollateral(outbound0);

        var outbound1 = JsonCodec.roundTrip(CreateChatRoomRequest.newBuilder()
                .setChatRoom(ChatRoom.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getCollateral().getAddress().getPostalCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), CreateChatRoomRequest.class);
        var reply1 = chatChatRoomServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createChatRoom(outbound1);
        return GetRepaymentRatesResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getChatRoom().getAddress().getCountryCode())
                                .setPostalCode(reply0.getCollateral().getAddress().getPostalCode())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }
}
