package com.acme.finance.domains.lending;

import com.acme.audit.v1.PullRetentionPoliciesRequest;
import com.acme.audit.v1.PullRetentionPoliciesResponse;
import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeRequest;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.ExportAccessPoliciesRequest;
import com.acme.authorization.v1.ExportAccessPoliciesResponse;
import com.acme.banking.v1.ExplainStandingOrderDecisionRequest;
import com.acme.banking.v1.ExplainStandingOrderDecisionResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.employees.v1.EmployeeRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.FindUsersRequest;
import com.acme.identity.v1.FindUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.itsm.v1.Incident;
import com.acme.itsm.v1.IncidentCalculationServiceGrpc;
import com.acme.itsm.v1.RecalculateIncidentsRequest;
import com.acme.itsm.v1.RecalculateIncidentsResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodWeeklyRule;
import com.acme.ledger.v1.RecordFiscalPeriodVersionRequest;
import com.acme.ledger.v1.RecordFiscalPeriodVersionResponse;
import com.acme.lending.v1.CheckRepaymentEligibilityRequest;
import com.acme.lending.v1.CheckRepaymentEligibilityResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.CompareCollateralOptionsRequest;
import com.acme.lending.v1.CompareCollateralOptionsResponse;
import com.acme.lending.v1.ExplainRepaymentDecisionRequest;
import com.acme.lending.v1.ExplainRepaymentDecisionResponse;
import com.acme.lending.v1.GetRepaymentRulesRequest;
import com.acme.lending.v1.GetRepaymentRulesResponse;
import com.acme.lending.v1.LintRepaymentsRequest;
import com.acme.lending.v1.LintRepaymentsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.NormalizeRepaymentRequest;
import com.acme.lending.v1.NormalizeRepaymentResponse;
import com.acme.lending.v1.PreviewLoanApplicationRequest;
import com.acme.lending.v1.PreviewLoanApplicationResponse;
import com.acme.lending.v1.PreviewLoanRequest;
import com.acme.lending.v1.PreviewLoanResponse;
import com.acme.lending.v1.PreviewRepaymentRequest;
import com.acme.lending.v1.PreviewRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.ScoreCollateralRequest;
import com.acme.lending.v1.ScoreCollateralResponse;
import com.acme.lending.v1.ScoreRepaymentRequest;
import com.acme.lending.v1.ScoreRepaymentResponse;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import com.acme.lending.v1.SimulateLoanApplicationRequest;
import com.acme.lending.v1.SimulateLoanApplicationResponse;
import com.acme.lending.v1.SimulateLoanRequest;
import com.acme.lending.v1.SimulateLoanResponse;
import com.acme.lending.v1.ValidateRepaymentRequest;
import com.acme.lending.v1.ValidateRepaymentResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
 * Checks repayments against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class RepaymentValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(RepaymentValidationApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final IncidentCalculationServiceGrpc.IncidentCalculationServiceBlockingStub itsmIncidentCalculationServiceBlocking;

    private final FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking;

    private final CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking;

    private final CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking;

    private final LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking;

    private final LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking;

    private final LoanValidationServiceGrpc.LoanValidationServiceBlockingStub lendingLoanValidationServiceBlocking;

    private final LendingRepository repository;

    public RepaymentValidationApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            IncidentCalculationServiceGrpc.IncidentCalculationServiceBlockingStub itsmIncidentCalculationServiceBlocking,
            FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking,
            CollateralCalculationServiceGrpc.CollateralCalculationServiceBlockingStub lendingCollateralCalculationServiceBlocking,
            CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking,
            LoanApplicationCalculationServiceGrpc.LoanApplicationCalculationServiceBlockingStub lendingLoanApplicationCalculationServiceBlocking,
            LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking,
            LoanValidationServiceGrpc.LoanValidationServiceBlockingStub lendingLoanValidationServiceBlocking,
            LendingRepository repository) {
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.bankingStandingOrderValidationServiceBlocking = bankingStandingOrderValidationServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.identityUserQueryServiceBlocking = identityUserQueryServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.itsmIncidentCalculationServiceBlocking = itsmIncidentCalculationServiceBlocking;
        this.ledgerFiscalPeriodHistoryServiceBlocking = ledgerFiscalPeriodHistoryServiceBlocking;
        this.lendingCollateralCalculationServiceBlocking = lendingCollateralCalculationServiceBlocking;
        this.lendingCollateralValidationServiceBlocking = lendingCollateralValidationServiceBlocking;
        this.lendingLoanApplicationCalculationServiceBlocking = lendingLoanApplicationCalculationServiceBlocking;
        this.lendingLoanApplicationValidationServiceBlocking = lendingLoanApplicationValidationServiceBlocking;
        this.lendingLoanValidationServiceBlocking = lendingLoanValidationServiceBlocking;
        this.repository = repository;
    }

    public record GetRepaymentRulesStage0Requested(SimulateCollateralRequest request, CompletableFuture<SimulateCollateralResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a repayment.
     */
    public ValidateRepaymentResponse validateRepayment(ValidateRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = RecordFiscalPeriodVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordFiscalPeriodVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(request.getRepayment().getLocation().getCountryCode())
                                .build())
                        .setWeekly(FiscalPeriodWeeklyRule.newBuilder()
                                .addAllDays(request.getRepayment().getWeekly().getDaysList())
                                .setInterval(request.getRepayment().getWeekly().getInterval())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<RecordFiscalPeriodVersionRequest, RecordFiscalPeriodVersionResponse>() {
            @Override
            public RecordFiscalPeriodVersionResponse apply(RecordFiscalPeriodVersionRequest input) {
                return ledgerFiscalPeriodHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordFiscalPeriodVersion(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = SimulateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getRepayment().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getRepayment().getLocation().getRecipient())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .build();
        Function<SimulateLoanApplicationRequest, SimulateLoanApplicationResponse> operation1 = lendingLoanApplicationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::simulateLoanApplication;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateRepayment", request.getRepayment().getRepaymentId());
        return ValidateRepaymentResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(reply0.getFiscalPeriod().getLocation().getLocality())
                                .build())
                        .putAllMetadata(request.getRepayment().getMetadataMap())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply1.getLoanApplication().getCounterparty().getDisplayName())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .build();
    }

    /**
     * Previews a repayment.
     */
    public PreviewRepaymentResponse previewRepayment(PreviewRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = ExportAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportAccessPoliciesRequest.Filter.newBuilder()
                        .setRegion(request.getRepayment().getLocation().getRegion())
                        .build())
                .build();
        var stream0 = authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportAccessPolicies(outbound0);
        var reply0 = ExportAccessPoliciesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = RpcSupport.unpack(Any.pack(PreviewLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewLoanRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getRepayment().getOrganizer().getEmail())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build()), PreviewLoanRequest.class);
        var reply1 = lendingLoanValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewLoan(outbound1);

        var results2 = new ArrayList<RecalculateIncidentsResponse>();
        for (var element : request.getRepayment().getParticipantsList()) {
            RecalculateIncidentsRequest itemRequest = RecalculateIncidentsRequest.newBuilder()
                .setContext(request.getContext())
                .setIncident(Incident.newBuilder()
                        .setCustomer(EmployeeRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(element.getContact().getName())
                                .setPhone(element.getContact().getPhone())
                                .setPreferredChannel(element.getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
            results2.add(itsmIncidentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateIncidents(itemRequest));
        }
        var reply2 = results2.isEmpty() ? RecalculateIncidentsResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewRepayment", request.getRepayment().getRepaymentId());
        return PreviewRepaymentResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setRepaymentId(request.getRepaymentId())
                        .setWindow(request.getRepayment().getWindow())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks a repayment.
     */
    public CheckRepaymentEligibilityResponse checkRepaymentEligibility(CheckRepaymentEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckRepaymentEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        List<CalculatePolicyResponse> results0 = request.getRepayment().getParticipantsList().stream()
                .map(element -> CalculatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .setPhone(element.getContact().getPhone())
                                .setPreferredChannel(element.getContact().getPreferredChannel())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePolicy(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? CalculatePolicyResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CompareCollateralOptionsRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getRepayment().getLocation().getPostalCode())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPolicy().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CompareCollateralOptionsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getRepayment().getLocation().getCountryCode())
                                .addAllLines(request.getRepayment().getLocation().getLinesList())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(lendingCollateralCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCollateralOptions(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = FindUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPolicy().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getPolicy().getLabelsMap())
                        .setOwnerId(reply0.getPolicy().getHolder().getCoverageId())
                        .setRegion(switch (reply1.getCollateral().getLocatorCase()) {
                            case ADDRESS -> reply1.getCollateral().getAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply2 = identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findUsers(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckRepaymentEligibility", request.getRepayment().getRepaymentId());
        return CheckRepaymentEligibilityResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getCollateral().getAudit().getUpdatedBy())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .setStatus(request.getRepayment().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Normalizes a repayment.
     */
    public NormalizeRepaymentResponse normalizeRepayment(NormalizeRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = SendRetentionPolicyTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendRetentionPolicyTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.audit.v1.RetentionPolicyNotificationService");
        var selected0 = RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.sendRetentionPolicyTestNotice(outbound0);

        var builder1 = ExplainStandingOrderDecisionRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setStandingOrder(StandingOrder.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(reply0.getRetentionPolicy().getAudit().getCreatedAt())
                        .build())
                .setLocation(PostalAddress.newBuilder()
                        .setRecipient(request.getRepayment().getLocation().getRecipient())
                        .build())
                .addParticipants(StandingOrderParticipant.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getRepayment().getOrganizer().getEmail())
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainStandingOrderDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeRepayment", request.getRepayment().getRepaymentId());
        return NormalizeRepaymentResponse.newBuilder()
                .setOutcome(NormalizeRepaymentResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getRetentionPolicy().getAudit().getUpdatedBy())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .setStatus(request.getRepayment().getStatus())
                        .setTitle(reply1.getStandingOrder().getTitle())
                        .build())
                .build();
    }

    /**
     * Explains a repayment.
     */
    public ExplainRepaymentDecisionResponse explainRepaymentDecision(ExplainRepaymentDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainRepaymentDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = PullRetentionPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PullRetentionPoliciesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRepayment().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullRetentionPolicies(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainRepaymentDecision", request.getRepayment().getRepaymentId());
        return ExplainRepaymentDecisionResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRetentionPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .setStatus(request.getRepayment().getStatus())
                        .build())
                .build();
    }

    /**
     * Scores a repayment.
     */
    public ScoreRepaymentResponse scoreRepayment(ScoreRepaymentRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreRepayment")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = PreviewLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRepayment().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .setPostalCode(request.getRepayment().getLocation().getPostalCode())
                                .build())
                        .build())
                .build();
        RpcPort<PreviewLoanApplicationRequest, PreviewLoanApplicationResponse> port0 = RpcPort.adapt(lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewLoanApplication);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = SimulateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateLoanRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getLoanApplication().getBillingAddress().getCountryCode())
                                .setPostalCode(reply0.getLoanApplication().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.lending.v1.LoanCalculationService");
        var serviceName1 = "acme.lending.v1" + "." + "LoanCalculationService";
        var methodName1 = "Sim" + "ulateLoan";
        var descriptor1 = MethodDescriptor.<SimulateLoanRequest, SimulateLoanResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(SimulateLoanRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(SimulateLoanResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreRepayment", request.getRepayment().getRepaymentId());
        return ScoreRepaymentResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getLoan().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getLoanApplication().getAudit().getUpdatedAt())
                                .build())
                        .putAllMetadata(request.getRepayment().getMetadataMap())
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(reply1.getLoan().getContact())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lints a repayment.
     */
    public LintRepaymentsResponse lintRepayments(LintRepaymentsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintRepayments")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRepayment(request.getContext().getTenantId(), request.getRepayment());

        var outbound0 = ResolveRetentionPolicyConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolveRetentionPolicyConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getRepayment().getAudit().getEtag())
                                .setUpdatedAt(request.getRepayment().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .setUserId(request.getRepayment().getOrganizer().getUserId())
                                .build())
                        .build())
                .build();
        var reply0 = auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveRetentionPolicyConflict(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(ScoreCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setLocality(request.getRepayment().getLocation().getLocality())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRetentionPolicy().getAudit().getEtag())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getRetentionPolicy().getOwner().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreCollateralRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getRepayment().getLocation().getLinesList())
                                .build())
                        .build())
                .build()), ScoreCollateralRequest.class);
        var reply1 = lendingCollateralValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreCollateral(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRepayment().getRepaymentId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintRepayments", request.getRepayment().getRepaymentId());
        return LintRepaymentsResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .addAllLines(reply1.getCollateral().getAddress().getLinesList())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply1.getCollateral().getOwner().getEmail())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .setTitle(request.getRepayment().getTitle())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a repayment.
     */
    public GetRepaymentRulesResponse getRepaymentRules(GetRepaymentRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRepaymentRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Repayment", request.getRepaymentId(), "GetRepaymentRules");

        var outbound0 = SimulateCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<SimulateCollateralResponse>();
        events.publishEvent(new GetRepaymentRulesStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        return GetRepaymentRulesResponse.newBuilder()
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getCollateral().getAddress().getPostalCode())
                                .build())
                        .setRepaymentId(request.getRepaymentId())
                        .build())
                .build();
    }
}
