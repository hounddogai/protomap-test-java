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
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
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
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.GetStressTestRatesRequest;
import com.acme.risk.v1.GetStressTestRatesResponse;
import com.acme.risk.v1.RecalculateStressTestsRequest;
import com.acme.risk.v1.RecalculateStressTestsResponse;
import com.acme.risk.v1.RiskRegister;
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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class StressTestCalculationApplicationTest {
    @Test
    void calculateStressTestPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var listener = new StressTestCalculationApplicationEvents(RiskScenarioValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof StressTestCalculationApplication.CalculateStressTestStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new StressTestCalculationApplication(
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                events,
                PolicyQueryServiceGrpc.newBlockingStub(channel),
                PremiumCalculationServiceGrpc.newBlockingStub(channel),
                NotificationServiceGrpc.newBlockingStub(channel),
                NotificationServiceGrpc.newFutureStub(channel),
                repository,
                RiskScenarioValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CalculateStressTestRequest.getDefaultInstance());
        var response = application.calculateStressTest(request);
        var sent0 = channel.request("acme.insurance.v1.PremiumCalculationService/QuotePremium");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "attachments", "content_type"),
                ProbeSupport.value(sent0, "premium", "attachments", "content_type"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "attachments", "checksum"),
                ProbeSupport.value(sent0, "premium", "attachments", "checksum"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "attachments", "size_bytes"),
                ProbeSupport.value(sent0, "premium", "attachments", "size_bytes"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "attachments", "uri"),
                ProbeSupport.value(sent0, "premium", "attachments", "uri"));
        var sent1 = channel.request("acme.authorization.v1.AccessPolicyService/CreateAccessPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "audit", "created_at"),
                ProbeSupport.value(sent1, "access_policy", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumCalculationService/QuotePremium"), "premium", "audit", "created_by"),
                ProbeSupport.value(sent1, "access_policy", "audit", "created_by"));
        var sent2 = channel.request("acme.risk.v1.RiskScenarioValidationService/CheckRiskScenarioEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "audit", "created_by"),
                ProbeSupport.value(sent2, "risk_scenario", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyService/CreateAccessPolicy"), "access_policy", "audit", "created_at"),
                ProbeSupport.value(response, "stress_test", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyService/CreateAccessPolicy"), "access_policy", "audit", "updated_at"),
                ProbeSupport.value(response, "stress_test", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioValidationService/CheckRiskScenarioEligibility"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test_id"),
                ProbeSupport.value(response, "stress_test", "stress_test_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "total"),
                ProbeSupport.value(response, "stress_test", "total"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new StressTestCalculationApplicationEvents(RiskScenarioValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CheckRiskScenarioEligibilityResponse>();
        listener.on(new StressTestCalculationApplication.CalculateStressTestStage2Requested(ProbeSupport.sample(CheckRiskScenarioEligibilityRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
