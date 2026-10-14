package com.acme.finance.domains.lending;

import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CheckPreferenceEligibilityRequest;
import com.acme.customers.v1.CheckPreferenceEligibilityResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.ActivateRepaymentRequest;
import com.acme.lending.v1.ActivateRepaymentResponse;
import com.acme.lending.v1.EstimateRepaymentRequest;
import com.acme.lending.v1.EstimateRepaymentResponse;
import com.acme.lending.v1.ExpireRepaymentRequest;
import com.acme.lending.v1.ExpireRepaymentResponse;
import com.acme.lending.v1.ExplainLoanCalculationRequest;
import com.acme.lending.v1.ExplainLoanCalculationResponse;
import com.acme.lending.v1.GetRepaymentLifecycleRequest;
import com.acme.lending.v1.GetRepaymentLifecycleResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.LoanLifecycleServiceGrpc;
import com.acme.lending.v1.PreviewLoanApplicationRequest;
import com.acme.lending.v1.PreviewLoanApplicationResponse;
import com.acme.lending.v1.RenewRepaymentRequest;
import com.acme.lending.v1.RenewRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.ScheduleLoanExpiryRequest;
import com.acme.lending.v1.ScheduleLoanExpiryResponse;
import com.acme.lending.v1.ScheduleRepaymentExpiryRequest;
import com.acme.lending.v1.ScheduleRepaymentExpiryResponse;
import com.acme.lending.v1.SimulateRepaymentRequest;
import com.acme.lending.v1.SimulateRepaymentResponse;
import com.acme.lending.v1.SuspendRepaymentRequest;
import com.acme.lending.v1.SuspendRepaymentResponse;
import com.acme.lending.v1.TerminateRepaymentRequest;
import com.acme.lending.v1.TerminateRepaymentResponse;
import com.acme.search.v1.ResolveSearchIndexesRequest;
import com.acme.search.v1.ResolveSearchIndexesResponse;
import com.acme.venues.v1.PreviewVenueRequest;
import com.acme.venues.v1.PreviewVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Moves a repayment between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class RepaymentLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(RepaymentLifecycleApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceFutureStub customersPreferenceValidationServiceFuture;

    private final ApplicationEventPublisher events;

    private final LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking;

    private final LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking;

    private final LoanLifecycleServiceGrpc.LoanLifecycleServiceBlockingStub lendingLoanLifecycleServiceBlocking;

    private final RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking;

    private final LendingRepository repository;

    private final VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking;

    public RepaymentLifecycleApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceFutureStub customersPreferenceValidationServiceFuture,
            ApplicationEventPublisher events,
            LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking,
            LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking,
            LoanLifecycleServiceGrpc.LoanLifecycleServiceBlockingStub lendingLoanLifecycleServiceBlocking,
            RepaymentCalculationServiceGrpc.RepaymentCalculationServiceBlockingStub lendingRepaymentCalculationServiceBlocking,
            LendingRepository repository,
            VenueValidationServiceGrpc.VenueValidationServiceBlockingStub venuesVenueValidationServiceBlocking) {
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.customersCustomerValidationServiceBlocking = customersCustomerValidationServiceBlocking;
        this.customersPreferenceValidationServiceFuture = customersPreferenceValidationServiceFuture;
        this.events = events;
        this.lendingLoanApplicationValidationServiceBlocking = lendingLoanApplicationValidationServiceBlocking;
        this.lendingLoanCalculationServiceBlocking = lendingLoanCalculationServiceBlocking;
        this.lendingLoanLifecycleServiceBlocking = lendingLoanLifecycleServiceBlocking;
        this.lendingRepaymentCalculationServiceBlocking = lendingRepaymentCalculationServiceBlocking;
        this.repository = repository;
        this.venuesVenueValidationServiceBlocking = venuesVenueValidationServiceBlocking;
    }

    public record RenewRepaymentStage1Requested(ResolveSearchIndexesRequest request, CompletableFuture<ResolveSearchIndexesResponse> completion, io.grpc.Context context) {}

    /**
     * Activates a repayment.
     */
    public ActivateRepaymentResponse activateRepayment(ActivateRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getRepaymentId(), "ActivateRepayment");

        var outbound0 = ScheduleLoanExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScheduleLoanExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> lendingLoanLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleLoanExpiry(value));

        var outbound1 = PreviewVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewVenueRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getLoan().getContact().getName())
                                .build())
                        .build())
                .setVenue(Venue.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var command1 = new DispatchCommand<>("acme.venues.v1.VenueValidationService/PreviewVenue", outbound1);
        var reply1 = venuesVenueValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewVenue(command1.payload());
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateRepayment", request.getDetails().getReasonCode());
        return ActivateRepaymentResponse.newBuilder()
                .setOutcome(ActivateRepaymentResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply0.getLoan().getContact().getEmail())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Suspends a repayment.
     */
    public SuspendRepaymentResponse suspendRepayment(SuspendRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getRepaymentId(), "SuspendRepayment");

        var outbound0 = RpcSupport.unpack(Any.pack(ExplainLoanCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build()), ExplainLoanCalculationRequest.class);
        var reply0 = lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainLoanCalculation(outbound0);

        var outbound1 = JsonCodec.roundTrip(EstimateRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(reply0.getLoan().getContact().getPhone())
                                        .setPreferredChannel(reply0.getLoan().getContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build(), EstimateRepaymentRequest.class);
        var reply1 = lendingRepaymentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateRepayment(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendRepayment", request.getDetails().getReasonCode());
        return SuspendRepaymentResponse.newBuilder()
                .setOutcome(SuspendRepaymentResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(reply1.getRepayment().getLocation().getLocality())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .setStatus(reply1.getRepayment().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Terminates a repayment.
     */
    public TerminateRepaymentResponse terminateRepayment(TerminateRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getRepaymentId(), "TerminateRepayment");

        var outbound0 = ResolveRetentionPolicyConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolveRetentionPolicyConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getDetails().getNewOwner().getEmail())
                                .setReason(request.getReason())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            ResolveRetentionPolicyConflictResponse fetch(ResolveRetentionPolicyConflictRequest input) {
                return auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveRetentionPolicyConflict(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateRepayment", request.getDetails().getReasonCode());
        return TerminateRepaymentResponse.newBuilder()
                .setOutcome(TerminateRepaymentResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getRetentionPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .build();
    }

    /**
     * Renews a repayment.
     */
    public RenewRepaymentResponse renewRepayment(RenewRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getRepaymentId(), "RenewRepayment");

        var outbound0 = PreviewLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewLoanApplication(outbound0);

        var outbound1 = ResolveSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var completion1 = new CompletableFuture<ResolveSearchIndexesResponse>();
        events.publishEvent(new RenewRepaymentStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewRepayment", request.getDetails().getReasonCode());
        return RenewRepaymentResponse.newBuilder()
                .setOutcome(RenewRepaymentResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getLoanApplication().getBillingAddress().getPostalCode())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Expires a repayment.
     */
    public ExpireRepaymentResponse expireRepayment(ExpireRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getRepaymentId(), "ExpireRepayment");

        var outbound0 = RpcSupport.unpack(Any.pack(LintCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomer(Customer.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .setDetails(LintCustomersRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build()), LintCustomersRequest.class);
        var reply0 = customersCustomerValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintCustomers(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireRepayment", request.getDetails().getReasonCode());
        return ExpireRepaymentResponse.newBuilder()
                .setOutcome(ExpireRepaymentResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply0.getCustomer().getRelationshipManager().getEmail())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .build();
    }

    /**
     * Returns a repayment.
     */
    public GetRepaymentLifecycleResponse getRepaymentLifecycle(GetRepaymentLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRepaymentLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getRepaymentId(), "GetRepaymentLifecycle");

        var outbound0 = CheckPreferenceEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var pending0 = customersPreferenceValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).checkPreferenceEligibility(outbound0);
        var reply0 = RpcSupport.await(pending0);
        return GetRepaymentLifecycleResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Schedules a repayment.
     */
    public ScheduleRepaymentExpiryResponse scheduleRepaymentExpiry(ScheduleRepaymentExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleRepaymentExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = JsonCodec.roundTrip(SimulateRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getRepayment().getAudit().getUpdatedBy())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(request.getRepayment().getLocation().getCountryCode())
                                .build())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(request.getDetails().getNotify().getPhone())
                                        .setPreferredChannel(request.getDetails().getNotify().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build(), SimulateRepaymentRequest.class);
        var reply0 = lendingRepaymentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateRepayment(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleRepaymentExpiry", request.getRepayment().getRepaymentId());
        return ScheduleRepaymentExpiryResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setLocation(request.getRepayment().getLocation())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getRepayment().getOrganizer().getEmail())
                                        .build())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .build();
    }
}
