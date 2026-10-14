package com.acme.finance.domains.contracts;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CloneAccessPolicyRequest;
import com.acme.authorization.v1.CloneAccessPolicyResponse;
import com.acme.budgeting.v1.QuoteVarianceRequest;
import com.acme.budgeting.v1.QuoteVarianceResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.CheckRenewalEligibilityRequest;
import com.acme.contracts.v1.CheckRenewalEligibilityResponse;
import com.acme.contracts.v1.CompareObligationOptionsRequest;
import com.acme.contracts.v1.CompareObligationOptionsResponse;
import com.acme.contracts.v1.EstimateObligationRequest;
import com.acme.contracts.v1.EstimateObligationResponse;
import com.acme.contracts.v1.ExplainObligationCalculationRequest;
import com.acme.contracts.v1.ExplainObligationCalculationResponse;
import com.acme.contracts.v1.ExplainRenewalDecisionRequest;
import com.acme.contracts.v1.ExplainRenewalDecisionResponse;
import com.acme.contracts.v1.GetRenewalRulesRequest;
import com.acme.contracts.v1.GetRenewalRulesResponse;
import com.acme.contracts.v1.NormalizeRenewalRequest;
import com.acme.contracts.v1.NormalizeRenewalResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationCalculationServiceGrpc;
import com.acme.contracts.v1.ObligationComment;
import com.acme.contracts.v1.ObligationSla;
import com.acme.contracts.v1.PreviewRenewalRequest;
import com.acme.contracts.v1.PreviewRenewalResponse;
import com.acme.contracts.v1.QuoteObligationRequest;
import com.acme.contracts.v1.QuoteObligationResponse;
import com.acme.contracts.v1.RecalculateObligationsRequest;
import com.acme.contracts.v1.RecalculateObligationsResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.ScoreRenewalRequest;
import com.acme.contracts.v1.ScoreRenewalResponse;
import com.acme.contracts.v1.SimulateObligationRequest;
import com.acme.contracts.v1.SimulateObligationResponse;
import com.acme.contracts.v1.ValidateRenewalRequest;
import com.acme.contracts.v1.ValidateRenewalResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.featurestore.v1.CountFeatureViewsRequest;
import com.acme.featurestore.v1.CountFeatureViewsResponse;
import com.acme.featurestore.v1.FeatureViewQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.ListUsersRequest;
import com.acme.identity.v1.ListUsersResponse;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.SimulateLoanRequest;
import com.acme.lending.v1.SimulateLoanResponse;
import com.acme.notifications.v1.CloneNotificationRequest;
import com.acme.notifications.v1.CloneNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.risk.v1.CountRiskScenariosRequest;
import com.acme.risk.v1.CountRiskScenariosResponse;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Checks renewals against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class RenewalValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(RenewalValidationApplication.class);

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final ChannelRegistry channels;

    private final ObligationCalculationServiceGrpc.ObligationCalculationServiceBlockingStub contractsObligationCalculationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final FeatureViewQueryServiceGrpc.FeatureViewQueryServiceBlockingStub featurestoreFeatureViewQueryServiceBlocking;

    private final LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final ContractsRepository repository;

    private final RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking;

    private final Executor rpcExecutor;

    public RenewalValidationApplication(
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            ChannelRegistry channels,
            ObligationCalculationServiceGrpc.ObligationCalculationServiceBlockingStub contractsObligationCalculationServiceBlocking,
            ApplicationEventPublisher events,
            FeatureViewQueryServiceGrpc.FeatureViewQueryServiceBlockingStub featurestoreFeatureViewQueryServiceBlocking,
            LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            ContractsRepository repository,
            RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceBlockingStub riskRiskScenarioQueryServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.channels = channels;
        this.contractsObligationCalculationServiceBlocking = contractsObligationCalculationServiceBlocking;
        this.events = events;
        this.featurestoreFeatureViewQueryServiceBlocking = featurestoreFeatureViewQueryServiceBlocking;
        this.lendingLoanCalculationServiceBlocking = lendingLoanCalculationServiceBlocking;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.repository = repository;
        this.riskRiskScenarioQueryServiceBlocking = riskRiskScenarioQueryServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    public record ValidateRenewalStage0Requested(QuoteVarianceRequest request, CompletableFuture<QuoteVarianceResponse> completion, io.grpc.Context context) {}

    public record PreviewRenewalStage3Requested(ListUsersRequest request, CompletableFuture<ListUsersResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a renewal.
     */
    public ValidateRenewalResponse validateRenewal(ValidateRenewalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateRenewal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        var outbound0 = QuoteVarianceRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getContact().getPhone())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<QuoteVarianceResponse>();
        events.publishEvent(new ValidateRenewalStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = RecalculateObligationsRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(reply0.getVariance().getRequester().getEmail())
                                .build())
                        .setSla(ObligationSla.newBuilder()
                                .setPolicyId(reply0.getVariance().getSla().getPolicyId())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            RecalculateObligationsResponse fetch(RecalculateObligationsRequest input) {
                return contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateObligations(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateRenewal", request.getRenewal().getRenewalId());
        return ValidateRenewalResponse.newBuilder()
                .setRenewal(Renewal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getVariance().getAudit().getUpdatedBy())
                                .build())
                        .setOrganizer(request.getRenewal().getOrganizer())
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(reply1.getObligation().getRequester().getPhone())
                                        .build())
                                .build())
                        .setRenewalId(request.getRenewalId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Previews a renewal.
     */
    public PreviewRenewalResponse previewRenewal(PreviewRenewalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewRenewal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        var outbound0 = EstimateObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getRenewal().getOrganizer().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getRenewal().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<EstimateObligationRequest, EstimateObligationResponse>() {
            @Override
            public EstimateObligationResponse apply(EstimateObligationRequest input) {
                return contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateObligation(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = CloneNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getObligation().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getObligation().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneNotification(outbound1);

        var outbound2 = ExplainObligationCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainObligationCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getObligation().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainObligationCalculation(value));

        var outbound3 = ListUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getObligation().getAudit().getCreatedAt())
                        .setOwnerId(reply1.getNotification().getSla().getPolicyId())
                        .setRegion(request.getRenewal().getLocation().getRegion())
                        .build())
                .build();
        var completion3 = new CompletableFuture<ListUsersResponse>();
        events.publishEvent(new PreviewRenewalStage3Requested(outbound3, completion3, io.grpc.Context.current()));
        var reply3 = completion3.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewRenewal", request.getRenewal().getRenewalId());
        return PreviewRenewalResponse.newBuilder()
                .setOutcome(PreviewRenewalResponse.Outcome.newBuilder()
                        .setReferenceId(reply2.getOutcome().getReferenceId())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setRenewal(Renewal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getNotification().getAudit().getCreatedBy())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply0.getObligation().getAssignee().getDisplayName())
                                .build())
                        .setRenewalId(request.getRenewalId())
                        .setWindow(request.getRenewal().getWindow())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks a renewal.
     */
    public CheckRenewalEligibilityResponse checkRenewalEligibility(CheckRenewalEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckRenewalEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        var outbound0 = CloneAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getRenewal().getAudit().getCreatedAt())
                                .setEtag(request.getRenewal().getAudit().getEtag())
                                .setUpdatedBy(request.getRenewal().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getRenewal().getOrganizer().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<CloneAccessPolicyRequest, CloneAccessPolicyResponse> port0 = RpcPort.adapt(authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::cloneAccessPolicy);
        var reply0 = port0.exchange(outbound0);

        List<SimulateLoanResponse> results1 = request.getRenewal().getParticipantsList().stream()
                .map(element -> SimulateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(element.getContact().getPhone())
                                .setPreferredChannel(element.getContact().getPreferredChannel())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateLoan(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? SimulateLoanResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = CompareObligationOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getAccessPolicy().getOwner().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAccessPolicy().getAudit().getCreatedAt())
                                .setUpdatedBy(reply1.getLoan().getAudit().getUpdatedBy())
                                .build())
                        .addComments(ObligationComment.newBuilder()
                                .setCreatedAt(reply1.getLoan().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.contracts.v1.ObligationCalculationService");
        var selected2 = ObligationCalculationServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.compareObligationOptions(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckRenewalEligibility", request.getRenewal().getRenewalId());
        return CheckRenewalEligibilityResponse.newBuilder()
                .setRenewal(Renewal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getObligation().getAudit().getUpdatedBy())
                                .build())
                        .setLocation(request.getRenewal().getLocation())
                        .addParticipants(RenewalParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply2.getObligation().getCustomer().getEmail())
                                        .build())
                                .build())
                        .setRenewalId(request.getRenewalId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Normalizes a renewal.
     */
    public NormalizeRenewalResponse normalizeRenewal(NormalizeRenewalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeRenewal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        var outbound0 = ExplainObligationCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getRenewal().getOrganizer().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getRenewal().getAudit().getEtag())
                                .build())
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getRenewal().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainObligationCalculation(outbound0);

        var outbound1 = CountFeatureViewsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountFeatureViewsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getObligation().getSla().getDueAt())
                        .setOwnerId(reply0.getObligation().getSla().getPolicyId())
                        .setRegion(request.getRenewal().getLocation().getRegion())
                        .build())
                .build();
        final class Stage1Adapter {
            CountFeatureViewsResponse fetch(CountFeatureViewsRequest input) {
                return featurestoreFeatureViewQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countFeatureViews(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);

        var builder2 = RecalculateObligationsRequest.newBuilder();
        builder2.setContext(request.getContext());
        builder2.setDetails(RecalculateObligationsRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .setPostalCode(request.getRenewal().getLocation().getPostalCode())
                        .build())
                .build());
        var outbound2 = builder2.build();
        var reply2 = contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateObligations(outbound2);

        var futures3 = request.getRenewal().getParticipantsList().stream()
                .map(element -> CountRiskScenariosRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountRiskScenariosRequest.Filter.newBuilder()
                        .setCreatedAfter(reply2.getObligation().getSla().getDueAt())
                        .setOwnerId(element.getParticipantId())
                        .setRegion(request.getRenewal().getLocation().getRegion())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> riskRiskScenarioQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countRiskScenarios(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures3.toArray(CompletableFuture[]::new)).join();
        var results3 = futures3.stream().map(CompletableFuture::join).toList();
        var reply3 = results3.isEmpty() ? CountRiskScenariosResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeRenewal", request.getRenewal().getRenewalId());
        return NormalizeRenewalResponse.newBuilder()
                .setRenewal(Renewal.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply2.getObligation().getAudit().getCreatedBy())
                                .setEtag(reply0.getObligation().getAudit().getEtag())
                                .build())
                        .setLocation(request.getRenewal().getLocation())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply2.getObligation().getCustomer().getDisplayName())
                                .build())
                        .setRenewalId(request.getRenewalId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a renewal.
     */
    public ExplainRenewalDecisionResponse explainRenewalDecision(ExplainRenewalDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainRenewalDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        List<QuoteObligationResponse> results0 = request.getRenewal().getParticipantsList().stream()
                .map(element -> QuoteObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setEmail(element.getContact().getEmail())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(element.getContact().getName())
                                .setPreferredChannel(element.getContact().getPreferredChannel())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteObligation(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? QuoteObligationResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainRenewalDecision", request.getRenewal().getRenewalId());
        return ExplainRenewalDecisionResponse.newBuilder()
                .setRenewal(Renewal.newBuilder()
                        .setRenewalId(request.getRenewalId())
                        .setWindow(request.getRenewal().getWindow())
                        .build())
                .build();
    }

    /**
     * Scores a renewal.
     */
    public ScoreRenewalResponse scoreRenewal(ScoreRenewalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreRenewal")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRenewal(request.getContext().getTenantId(), request.getRenewal());

        var outbound0 = SimulateObligationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateObligationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getRenewal().getLocation().getRecipient())
                                .build())
                        .build())
                .setObligation(Obligation.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setDisplayName(request.getRenewal().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            SimulateObligationResponse fetch(SimulateObligationRequest input) {
                return contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateObligation(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = RecalculateObligationsRequest.newBuilder()
                .setContext(request.getContext())
                .setObligation(Obligation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getObligation().getAssignee().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getObligation().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        final class Stage1Adapter {
            RecalculateObligationsResponse fetch(RecalculateObligationsRequest input) {
                return contractsObligationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateObligations(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getRenewal().getRenewalId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreRenewal", request.getRenewal().getRenewalId());
        return ScoreRenewalResponse.newBuilder()
                .setBreakdown(ScoreRenewalResponse.Breakdown.newBuilder()
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .setRenewal(Renewal.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply1.getObligation().getCustomer().getEmail())
                                .build())
                        .setRenewalId(request.getRenewalId())
                        .setStatus(request.getRenewal().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a renewal.
     */
    public GetRenewalRulesResponse getRenewalRules(GetRenewalRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRenewalRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Renewal", request.getRenewalId(), "GetRenewalRules");
        return GetRenewalRulesResponse.newBuilder()
                .setRenewal(Renewal.newBuilder()
                        .setRenewalId(request.getRenewalId())
                        .build())
                .build();
    }
}
