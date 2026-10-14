package com.acme.finance.domains.risk;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CreateAccessPolicyRequest;
import com.acme.authorization.v1.CreateAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CountPoliciesRequest;
import com.acme.insurance.v1.CountPoliciesResponse;
import com.acme.insurance.v1.ExportPoliciesRequest;
import com.acme.insurance.v1.ExportPoliciesResponse;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumAttachment;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.QuotePremiumRequest;
import com.acme.insurance.v1.QuotePremiumResponse;
import com.acme.notifications.v1.BatchGetNotificationsRequest;
import com.acme.notifications.v1.BatchGetNotificationsResponse;
import com.acme.notifications.v1.CloneNotificationRequest;
import com.acme.notifications.v1.CloneNotificationResponse;
import com.acme.notifications.v1.CreateNotificationRequest;
import com.acme.notifications.v1.CreateNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.risk.v1.CalculateStressTestRequest;
import com.acme.risk.v1.CalculateStressTestResponse;
import com.acme.risk.v1.CheckRiskScenarioEligibilityRequest;
import com.acme.risk.v1.CheckRiskScenarioEligibilityResponse;
import com.acme.risk.v1.EstimateStressTestRequest;
import com.acme.risk.v1.EstimateStressTestResponse;
import com.acme.risk.v1.ExplainRiskScenarioDecisionRequest;
import com.acme.risk.v1.ExplainRiskScenarioDecisionResponse;
import com.acme.risk.v1.ExplainStressTestCalculationRequest;
import com.acme.risk.v1.ExplainStressTestCalculationResponse;
import com.acme.risk.v1.GetStressTestRatesRequest;
import com.acme.risk.v1.GetStressTestRatesResponse;
import com.acme.risk.v1.RecalculateStressTestsRequest;
import com.acme.risk.v1.RecalculateStressTestsResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.SimulateStressTestRequest;
import com.acme.risk.v1.SimulateStressTestResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.ValidateRiskScenarioRequest;
import com.acme.risk.v1.ValidateRiskScenarioResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a stress test, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class StressTestCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(StressTestCalculationApplication.class);

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final ApplicationEventPublisher events;

    private final PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking;

    private final PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceFutureStub notificationsNotificationServiceFuture;

    private final RiskRepository repository;

    private final RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking;

    public StressTestCalculationApplication(
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            ApplicationEventPublisher events,
            PolicyQueryServiceGrpc.PolicyQueryServiceBlockingStub insurancePolicyQueryServiceBlocking,
            PremiumCalculationServiceGrpc.PremiumCalculationServiceBlockingStub insurancePremiumCalculationServiceBlocking,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            NotificationServiceGrpc.NotificationServiceFutureStub notificationsNotificationServiceFuture,
            RiskRepository repository,
            RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking) {
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.events = events;
        this.insurancePolicyQueryServiceBlocking = insurancePolicyQueryServiceBlocking;
        this.insurancePremiumCalculationServiceBlocking = insurancePremiumCalculationServiceBlocking;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.notificationsNotificationServiceFuture = notificationsNotificationServiceFuture;
        this.repository = repository;
        this.riskRiskScenarioValidationServiceBlocking = riskRiskScenarioValidationServiceBlocking;
    }

    public record CalculateStressTestStage2Requested(CheckRiskScenarioEligibilityRequest request, CompletableFuture<CheckRiskScenarioEligibilityResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates a stress test.
     */
    public CalculateStressTestResponse calculateStressTest(CalculateStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var results0 = new ArrayList<QuotePremiumResponse>();
        for (var element : request.getStressTest().getAttachmentsList()) {
            QuotePremiumRequest itemRequest = QuotePremiumRequest.newBuilder()
                .setContext(request.getContext())
                .setPremium(Premium.newBuilder()
                        .addAttachments(PremiumAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .setContentType(element.getContentType())
                                .setSizeBytes(element.getSizeBytes())
                                .setUri(element.getUri())
                                .build())
                        .build())
                .build();
            results0.add(insurancePremiumCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePremium(itemRequest));
        }
        var reply0 = results0.isEmpty() ? QuotePremiumResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CreateAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getStressTest().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getPremium().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createAccessPolicy(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = CheckRiskScenarioEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getStressTest().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var completion2 = new CompletableFuture<CheckRiskScenarioEligibilityResponse>();
        events.publishEvent(new CalculateStressTestStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateStressTest", request.getStressTest().getStressTestId());
        return CalculateStressTestResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getAccessPolicy().getAudit().getCreatedAt())
                                .setUpdatedAt(reply1.getAccessPolicy().getAudit().getUpdatedAt())
                                .build())
                        .setStressTestId(request.getStressTestId())
                        .setTotal(request.getStressTest().getTotal())
                        .build())
                .build();
    }

    /**
     * Estimates a stress test.
     */
    public EstimateStressTestResponse estimateStressTest(EstimateStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var outbound0 = ExportPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getStressTest().getAudit().getUpdatedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var stream0 = insurancePolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportPolicies(outbound0);
        var reply0 = ExportPoliciesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = ExplainRiskScenarioDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setCustomer(CustomerRef.newBuilder()
                                .setCustomerId(request.getStressTest().getCounterparty().getCustomerId())
                                .build())
                        .build())
                .build();
        var reply1 = riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainRiskScenarioDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateStressTest", request.getStressTest().getStressTestId());
        return EstimateStressTestResponse.newBuilder()
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getRiskScenario().getAudit().getEtag())
                                .setUpdatedBy(reply1.getRiskScenario().getAudit().getUpdatedBy())
                                .build())
                        .setStatus(request.getStressTest().getStatus())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Recalculates a stress test.
     */
    public RecalculateStressTestsResponse recalculateStressTests(RecalculateStressTestsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateStressTests")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var outbound0 = BatchGetNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetNotifications(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateStressTests", request.getStressTest().getStressTestId());
        return RecalculateStressTestsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setNotes(request.getStressTest().getNotes())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Simulates a stress test.
     */
    public SimulateStressTestResponse simulateStressTest(SimulateStressTestRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateStressTest")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var builder0 = ValidateRiskScenarioRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setRiskScenario(RiskScenario.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedBy(request.getStressTest().getAudit().getCreatedBy())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateRiskScenario(outbound0);

        var outbound1 = CloneNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getStressTest().getCounterparty().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getRiskScenario().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var pending1 = notificationsNotificationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).cloneNotification(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateStressTest", request.getStressTest().getStressTestId());
        return SimulateStressTestResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(reply1.getNotification().getRequester().getEmail())
                                .build())
                        .setNumber(request.getStressTest().getNumber())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Explains a stress test.
     */
    public ExplainStressTestCalculationResponse explainStressTestCalculation(ExplainStressTestCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainStressTestCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveStressTest(request.getContext().getTenantId(), request.getStressTest());

        var outbound0 = BeanCopies.copy(CountPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getStressTest().getAudit().getUpdatedAt())
                        .putAllLabels(request.getStressTest().getMetadataMap())
                        .setOwnerId(request.getStressTest().getCounterparty().getCustomerId())
                        .setRegion(request.getStressTest().getBillingAddress().getRegion())
                        .build())
                .build(), CountPoliciesRequest.class);
        var reply0 = insurancePolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countPolicies(outbound0);

        var builder1 = CreateNotificationRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setNotification(Notification.newBuilder()
                .setAssignee(UserRef.newBuilder()
                        .setEmail(request.getDetails().getContact().getEmail())
                        .build())
                .setAudit(AuditInfo.newBuilder()
                        .setUpdatedBy(request.getStressTest().getAudit().getUpdatedBy())
                        .build())
                .setRequester(ContactInfo.newBuilder()
                        .setName(request.getDetails().getContact().getName())
                        .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createNotification(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getStressTest().getStressTestId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainStressTestCalculation", request.getStressTest().getStressTestId());
        return ExplainStressTestCalculationResponse.newBuilder()
                .setOutcome(ExplainStressTestCalculationResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setStressTest(StressTest.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply1.getNotification().getAssignee().getDisplayName())
                                .build())
                        .setDueAt(request.getStressTest().getDueAt())
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }

    /**
     * Returns a stress test.
     */
    public GetStressTestRatesResponse getStressTestRates(GetStressTestRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetStressTestRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "StressTest", request.getStressTestId(), "GetStressTestRates");
        return GetStressTestRatesResponse.newBuilder()
                .setStressTest(StressTest.newBuilder()
                        .setStressTestId(request.getStressTestId())
                        .build())
                .build();
    }
}
