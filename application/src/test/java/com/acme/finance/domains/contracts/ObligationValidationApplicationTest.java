package com.acme.finance.domains.contracts;

import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.DisposalSla;
import com.acme.assets.v1.EstimateDisposalRequest;
import com.acme.assets.v1.EstimateDisposalResponse;
import com.acme.assets.v1.RecalculateDepreciationsRequest;
import com.acme.assets.v1.RecalculateDepreciationsResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.NormalizeAuditEventRequest;
import com.acme.audit.v1.NormalizeAuditEventResponse;
import com.acme.audit.v1.StreamAuditEventNoticesRequest;
import com.acme.audit.v1.StreamAuditEventNoticesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.CalculateObligationRequest;
import com.acme.contracts.v1.CalculateObligationResponse;
import com.acme.contracts.v1.CheckObligationEligibilityRequest;
import com.acme.contracts.v1.CheckObligationEligibilityResponse;
import com.acme.contracts.v1.CompareObligationOptionsRequest;
import com.acme.contracts.v1.CompareObligationOptionsResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ExplainObligationCalculationRequest;
import com.acme.contracts.v1.ExplainObligationCalculationResponse;
import com.acme.contracts.v1.ExplainObligationDecisionRequest;
import com.acme.contracts.v1.ExplainObligationDecisionResponse;
import com.acme.contracts.v1.GetObligationRatesRequest;
import com.acme.contracts.v1.GetObligationRatesResponse;
import com.acme.contracts.v1.GetObligationRulesRequest;
import com.acme.contracts.v1.GetObligationRulesResponse;
import com.acme.contracts.v1.LintObligationsRequest;
import com.acme.contracts.v1.LintObligationsResponse;
import com.acme.contracts.v1.NormalizeObligationRequest;
import com.acme.contracts.v1.NormalizeObligationResponse;
import com.acme.contracts.v1.NormalizeRenewalRequest;
import com.acme.contracts.v1.NormalizeRenewalResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationCalculationServiceGrpc;
import com.acme.contracts.v1.ObligationComment;
import com.acme.contracts.v1.ObligationSla;
import com.acme.contracts.v1.ObligationStatusChange;
import com.acme.contracts.v1.PreviewObligationRequest;
import com.acme.contracts.v1.PreviewObligationResponse;
import com.acme.contracts.v1.QuoteObligationRequest;
import com.acme.contracts.v1.QuoteObligationResponse;
import com.acme.contracts.v1.RecalculateObligationsRequest;
import com.acme.contracts.v1.RecalculateObligationsResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ScoreObligationRequest;
import com.acme.contracts.v1.ScoreObligationResponse;
import com.acme.contracts.v1.ValidateObligationRequest;
import com.acme.contracts.v1.ValidateObligationResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.featureflags.v1.SummarizeFeatureFlagsRequest;
import com.acme.featureflags.v1.SummarizeFeatureFlagsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.QuoteCollateralRequest;
import com.acme.lending.v1.QuoteCollateralResponse;
import com.acme.onboarding.v1.EquipmentRef;
import com.acme.onboarding.v1.OnboardingPlan;
import com.acme.onboarding.v1.OnboardingPlanCalculationServiceGrpc;
import com.acme.onboarding.v1.SimulateOnboardingPlanRequest;
import com.acme.onboarding.v1.SimulateOnboardingPlanResponse;
import com.acme.risk.v1.ExplainStressTestDecisionRequest;
import com.acme.risk.v1.ExplainStressTestDecisionResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCardSettlement;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.acme.risk.v1.ValidateStressTestRequest;
import com.acme.risk.v1.ValidateStressTestResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class ObligationValidationApplicationTest {
    @Test
    void normalizeObligationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ContractsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveContract(String tenant, Contract value) {}
            public void saveContractClause(String tenant, ContractClause value) {}
            public void saveObligation(String tenant, Obligation value) {}
            public void saveRenewal(String tenant, Renewal value) {}
        };
        var application = new ObligationValidationApplication(
                DepreciationCalculationServiceGrpc.newBlockingStub(channel),
                DisposalCalculationServiceGrpc.newBlockingStub(channel),
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                ObligationCalculationServiceGrpc.newBlockingStub(channel),
                RenewalValidationServiceGrpc.newBlockingStub(channel),
                FeatureFlagQueryServiceGrpc.newBlockingStub(channel),
                CollateralCalculationServiceGrpc.newBlockingStub(channel),
                new ObligationValidationRequestMapperImpl(),
                OnboardingPlanCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                StressTestValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(NormalizeObligationRequest.getDefaultInstance());
        var response = application.normalizeObligation(request);
        var sent0 = channel.request("acme.onboarding.v1.OnboardingPlanCalculationService/SimulateOnboardingPlan");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "assignee", "display_name"),
                ProbeSupport.value(sent0, "onboarding_plan", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "sla", "due_at"),
                ProbeSupport.value(sent0, "onboarding_plan", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "audit", "updated_at"),
                ProbeSupport.value(sent0, "onboarding_plan", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "audit", "created_by"),
                ProbeSupport.value(sent0, "onboarding_plan", "audit", "created_by"));
        var sent1 = channel.request("acme.contracts.v1.ObligationCalculationService/CalculateObligation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "customer", "email"),
                ProbeSupport.value(sent1, "obligation", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "email"),
                ProbeSupport.value(sent1, "obligation", "customer", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.onboarding.v1.OnboardingPlanCalculationService/SimulateOnboardingPlan"), "onboarding_plan", "audit", "updated_by"),
                ProbeSupport.value(sent1, "obligation", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.onboarding.v1.OnboardingPlanCalculationService/SimulateOnboardingPlan"), "onboarding_plan", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "obligation", "customer", "display_name"));
        var sent2 = channel.request("acme.risk.v1.StressTestValidationService/ValidateStressTest");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.onboarding.v1.OnboardingPlanCalculationService/SimulateOnboardingPlan"), "onboarding_plan", "card", "network"),
                ProbeSupport.value(sent2, "stress_test", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.onboarding.v1.OnboardingPlanCalculationService/SimulateOnboardingPlan"), "onboarding_plan", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "stress_test", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "customer", "display_name"),
                ProbeSupport.value(sent2, "stress_test", "counterparty", "display_name"));
        var sent3 = channel.request("acme.assets.v1.DisposalCalculationService/EstimateDisposal");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "sla", "policy_id"),
                ProbeSupport.value(sent3, "disposal", "sla", "policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.onboarding.v1.OnboardingPlanCalculationService/SimulateOnboardingPlan"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationCalculationService/CalculateObligation"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestValidationService/ValidateStressTest"), "stress_test", "counterparty", "email"),
                ProbeSupport.value(response, "obligation", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestValidationService/ValidateStressTest"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalCalculationService/EstimateDisposal"), "disposal", "sla", "paused_for"),
                ProbeSupport.value(response, "obligation", "sla", "paused_for"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalCalculationService/EstimateDisposal"), "disposal", "sla", "due_at"),
                ProbeSupport.value(response, "obligation", "sla", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation_id"),
                ProbeSupport.value(response, "obligation", "obligation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "custom_fields"),
                ProbeSupport.value(response, "obligation", "custom_fields"));
    }
}
