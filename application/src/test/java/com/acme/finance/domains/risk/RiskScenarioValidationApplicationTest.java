package com.acme.finance.domains.risk;

import com.acme.audiences.v1.AudienceServiceGrpc;
import com.acme.audiences.v1.SearchAudiencesRequest;
import com.acme.audiences.v1.SearchAudiencesResponse;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CloneAccessPolicyRequest;
import com.acme.authorization.v1.CloneAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.PreviewPolicyRequest;
import com.acme.insurance.v1.PreviewPolicyResponse;
import com.acme.insurance.v1.ScoreUnderwritingRequest;
import com.acme.insurance.v1.ScoreUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.labeling.v1.LabelingTaskQueryServiceGrpc;
import com.acme.labeling.v1.ResolveLabelingTasksRequest;
import com.acme.labeling.v1.ResolveLabelingTasksResponse;
import com.acme.risk.v1.CheckRiskRegisterEligibilityRequest;
import com.acme.risk.v1.CheckRiskRegisterEligibilityResponse;
import com.acme.risk.v1.CheckRiskScenarioEligibilityRequest;
import com.acme.risk.v1.CheckRiskScenarioEligibilityResponse;
import com.acme.risk.v1.ExplainRiskRegisterDecisionRequest;
import com.acme.risk.v1.ExplainRiskRegisterDecisionResponse;
import com.acme.risk.v1.ExplainRiskScenarioDecisionRequest;
import com.acme.risk.v1.ExplainRiskScenarioDecisionResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.GetRiskScenarioRulesRequest;
import com.acme.risk.v1.GetRiskScenarioRulesResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.LintRiskScenariosRequest;
import com.acme.risk.v1.LintRiskScenariosResponse;
import com.acme.risk.v1.NormalizeRiskRegisterRequest;
import com.acme.risk.v1.NormalizeRiskRegisterResponse;
import com.acme.risk.v1.NormalizeRiskScenarioRequest;
import com.acme.risk.v1.NormalizeRiskScenarioResponse;
import com.acme.risk.v1.PreviewRiskRegisterRequest;
import com.acme.risk.v1.PreviewRiskRegisterResponse;
import com.acme.risk.v1.PreviewRiskScenarioRequest;
import com.acme.risk.v1.PreviewRiskScenarioResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.ScoreRiskScenarioRequest;
import com.acme.risk.v1.ScoreRiskScenarioResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.ValidateRiskRegisterRequest;
import com.acme.risk.v1.ValidateRiskRegisterResponse;
import com.acme.risk.v1.ValidateRiskScenarioRequest;
import com.acme.risk.v1.ValidateRiskScenarioResponse;
import com.acme.venues.v1.ScoreVenueRequest;
import com.acme.venues.v1.ScoreVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class RiskScenarioValidationApplicationTest {
    @Test
    void previewRiskScenarioPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var listener = new RiskScenarioValidationApplicationEvents(RiskRegisterValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof RiskScenarioValidationApplication.NormalizeRiskScenarioStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new RiskScenarioValidationApplication(
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                PolicyValidationServiceGrpc.newBlockingStub(channel),
                UnderwritingValidationServiceGrpc.newBlockingStub(channel),
                LabelingTaskQueryServiceGrpc.newBlockingStub(channel),
                new RiskScenarioValidationRequestMapperImpl(),
                repository,
                RiskRegisterValidationServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(PreviewRiskScenarioRequest.getDefaultInstance());
        var response = application.previewRiskScenario(request);
        var sent0 = channel.request("acme.risk.v1.RiskRegisterValidationService/CheckRiskRegisterEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "audit", "updated_at"),
                ProbeSupport.value(sent0, "risk_register", "children", "children", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "customer", "email"),
                ProbeSupport.value(sent0, "risk_register", "children", "children", "manager", "email"));
        var sent1 = channel.request("acme.audiences.v1.AudienceService/SearchAudiences");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterValidationService/CheckRiskRegisterEligibility"), "risk_register", "location", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "sla", "due_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.risk.v1.RiskRegisterValidationService/ExplainRiskRegisterDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "comments", "created_at"),
                ProbeSupport.value(sent2, "risk_register", "children", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "comments", "author_email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterValidationService/CheckRiskRegisterEligibility"), "risk_register", "location", "locality"),
                ProbeSupport.value(sent2, "risk_register", "children", "children", "location", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterValidationService/CheckRiskRegisterEligibility"), "risk_register", "location", "postal_code"),
                ProbeSupport.value(sent2, "risk_register", "children", "children", "location", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterValidationService/CheckRiskRegisterEligibility"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audiences.v1.AudienceService/SearchAudiences"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario_id"),
                ProbeSupport.value(response, "risk_scenario", "risk_scenario_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_scenario", "requester"),
                ProbeSupport.value(response, "risk_scenario", "requester"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new RiskScenarioValidationApplicationEvents(RiskRegisterValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ValidateRiskRegisterResponse>();
        listener.on(new RiskScenarioValidationApplication.NormalizeRiskScenarioStage1Requested(ProbeSupport.sample(ValidateRiskRegisterRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
