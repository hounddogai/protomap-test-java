package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.NormalizeAccessPolicyRequest;
import com.acme.authorization.v1.NormalizeAccessPolicyResponse;
import com.acme.banking.v1.FindStatementsRequest;
import com.acme.banking.v1.FindStatementsResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.StatementQueryServiceGrpc;
import com.acme.banking.v1.ValidateStandingOrderRequest;
import com.acme.banking.v1.ValidateStandingOrderResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.BrowsePoliciesRequest;
import com.acme.insurance.v1.BrowsePoliciesResponse;
import com.acme.insurance.v1.ExportPoliciesRequest;
import com.acme.insurance.v1.ExportPoliciesResponse;
import com.acme.insurance.v1.NormalizePremiumRequest;
import com.acme.insurance.v1.NormalizePremiumResponse;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.PremiumLineTax;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryBankSettlement;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.JournalEntryLine;
import com.acme.ledger.v1.JournalEntryLineTax;
import com.acme.ledger.v1.RevertJournalEntryRequest;
import com.acme.ledger.v1.RevertJournalEntryResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.ExplainCollateralDecisionRequest;
import com.acme.lending.v1.ExplainCollateralDecisionResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.ValidateLoanApplicationRequest;
import com.acme.lending.v1.ValidateLoanApplicationResponse;
import com.acme.risk.v1.CalculateStressTestRequest;
import com.acme.risk.v1.CalculateStressTestResponse;
import com.acme.risk.v1.CheckExposureEligibilityRequest;
import com.acme.risk.v1.CheckExposureEligibilityResponse;
import com.acme.risk.v1.CheckRiskScenarioEligibilityRequest;
import com.acme.risk.v1.CheckRiskScenarioEligibilityResponse;
import com.acme.risk.v1.CheckStressTestEligibilityRequest;
import com.acme.risk.v1.CheckStressTestEligibilityResponse;
import com.acme.risk.v1.ExplainRiskScenarioDecisionRequest;
import com.acme.risk.v1.ExplainRiskScenarioDecisionResponse;
import com.acme.risk.v1.ExplainStressTestDecisionRequest;
import com.acme.risk.v1.ExplainStressTestDecisionResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.LintStressTestsRequest;
import com.acme.risk.v1.LintStressTestsResponse;
import com.acme.risk.v1.NormalizeRiskScenarioRequest;
import com.acme.risk.v1.NormalizeRiskScenarioResponse;
import com.acme.risk.v1.NormalizeStressTestRequest;
import com.acme.risk.v1.NormalizeStressTestResponse;
import com.acme.risk.v1.PreviewStressTestRequest;
import com.acme.risk.v1.PreviewStressTestResponse;
import com.acme.risk.v1.RecalculateStressTestsRequest;
import com.acme.risk.v1.RecalculateStressTestsResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.ScoreStressTestRequest;
import com.acme.risk.v1.ScoreStressTestResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestBankSettlement;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import com.acme.risk.v1.ValidateRiskScenarioRequest;
import com.acme.risk.v1.ValidateRiskScenarioResponse;
import com.acme.risk.v1.ValidateStressTestRequest;
import com.acme.risk.v1.ValidateStressTestResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class StressTestValidationApplicationTest {
    @Test
    void checkStressTestEligibilityPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var listener = new StressTestValidationApplicationEvents(RiskScenarioValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof StressTestValidationApplication.PreviewStressTestStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new StressTestValidationApplication(
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                StandingOrderValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                PolicyQueryServiceGrpc.newBlockingStub(channel),
                PremiumValidationServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                CollateralValidationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioValidationServiceGrpc.newBlockingStub(channel),
                StressTestCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CheckStressTestEligibilityRequest.getDefaultInstance());
        var response = application.checkStressTestEligibility(request);
        var sent0 = channel.request("acme.insurance.v1.PolicyQueryService/ExportPolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "lines", "attributes"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "lines", "line_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "audit", "updated_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        var sent1 = channel.request("acme.risk.v1.StressTestCalculationService/RecalculateStressTests");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "bank", "account_holder"),
                ProbeSupport.value(sent1, "stress_test", "bank", "account_holder"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "audit", "updated_at"),
                ProbeSupport.value(sent1, "stress_test", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "stress_test", "counterparty", "display_name"));
        var sent2 = channel.request("acme.risk.v1.ExposureValidationService/CheckExposureEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "name"),
                ProbeSupport.value(sent2, "exposure", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "counterparty", "email"),
                ProbeSupport.value(sent2, "exposure", "contact", "email"));
        var sent3 = channel.request("acme.risk.v1.RiskScenarioValidationService/CheckRiskScenarioEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureValidationService/CheckExposureEligibility"), "exposure", "contact", "phone"),
                ProbeSupport.value(sent3, "risk_scenario", "requester", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "email"),
                ProbeSupport.value(sent3, "risk_scenario", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "preferred_channel"),
                ProbeSupport.value(sent3, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestCalculationService/RecalculateStressTests"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureValidationService/CheckExposureEligibility"), "exposure", "holder", "email"),
                ProbeSupport.value(response, "stress_test", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioValidationService/CheckRiskScenarioEligibility"), "risk_scenario", "audit", "updated_by"),
                ProbeSupport.value(response, "stress_test", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test_id"),
                ProbeSupport.value(response, "stress_test", "stress_test_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "notes"),
                ProbeSupport.value(response, "stress_test", "notes"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new StressTestValidationApplicationEvents(RiskScenarioValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ValidateRiskScenarioResponse>();
        listener.on(new StressTestValidationApplication.PreviewStressTestStage2Requested(ProbeSupport.sample(ValidateRiskScenarioRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
