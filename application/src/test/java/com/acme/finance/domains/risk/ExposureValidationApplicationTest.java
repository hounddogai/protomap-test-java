package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityRequest;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.CheckStandingOrderEligibilityRequest;
import com.acme.banking.v1.CheckStandingOrderEligibilityResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.QuoteClaimRequest;
import com.acme.insurance.v1.QuoteClaimResponse;
import com.acme.insurance.v1.ScoreUnderwritingRequest;
import com.acme.insurance.v1.ScoreUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.ValidatePolicyRequest;
import com.acme.insurance.v1.ValidatePolicyResponse;
import com.acme.insurance.v1.ValidatePremiumRequest;
import com.acme.insurance.v1.ValidatePremiumResponse;
import com.acme.risk.v1.CheckExposureEligibilityRequest;
import com.acme.risk.v1.CheckExposureEligibilityResponse;
import com.acme.risk.v1.EstimateExposureRequest;
import com.acme.risk.v1.EstimateExposureResponse;
import com.acme.risk.v1.EstimateStressTestRequest;
import com.acme.risk.v1.EstimateStressTestResponse;
import com.acme.risk.v1.ExplainExposureCalculationRequest;
import com.acme.risk.v1.ExplainExposureCalculationResponse;
import com.acme.risk.v1.ExplainExposureDecisionRequest;
import com.acme.risk.v1.ExplainExposureDecisionResponse;
import com.acme.risk.v1.ExplainStressTestCalculationRequest;
import com.acme.risk.v1.ExplainStressTestCalculationResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureBankFunding;
import com.acme.risk.v1.ExposureCalculationServiceGrpc;
import com.acme.risk.v1.ExposureCardFunding;
import com.acme.risk.v1.ExposureLimit;
import com.acme.risk.v1.GetExposureRulesRequest;
import com.acme.risk.v1.GetExposureRulesResponse;
import com.acme.risk.v1.LintExposuresRequest;
import com.acme.risk.v1.LintExposuresResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.NormalizeExposureRequest;
import com.acme.risk.v1.NormalizeExposureResponse;
import com.acme.risk.v1.PreviewExposureRequest;
import com.acme.risk.v1.PreviewExposureResponse;
import com.acme.risk.v1.PreviewRiskScenarioRequest;
import com.acme.risk.v1.PreviewRiskScenarioResponse;
import com.acme.risk.v1.RecalculateExposuresRequest;
import com.acme.risk.v1.RecalculateExposuresResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.ScoreExposureRequest;
import com.acme.risk.v1.ScoreExposureResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import com.acme.risk.v1.ValidateExposureRequest;
import com.acme.risk.v1.ValidateExposureResponse;
import com.acme.risk.v1.ValidateRiskRegisterRequest;
import com.acme.risk.v1.ValidateRiskRegisterResponse;
import com.acme.suppliers.v1.SupplierRef;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class ExposureValidationApplicationTest {
    @Test
    void previewExposurePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var application = new ExposureValidationApplication(
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                RetentionPolicyValidationServiceGrpc.newBlockingStub(channel),
                StandingOrderValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PolicyValidationServiceGrpc.newBlockingStub(channel),
                PremiumValidationServiceGrpc.newBlockingStub(channel),
                new ExposureValidationRequestMapperImpl(),
                repository,
                ExposureCalculationServiceGrpc.newBlockingStub(channel),
                RiskRegisterValidationServiceGrpc.newBlockingStub(channel),
                RiskScenarioValidationServiceGrpc.newBlockingStub(channel),
                StressTestCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(PreviewExposureRequest.getDefaultInstance());
        var response = application.previewExposure(request);
        var sent0 = channel.request("acme.risk.v1.ExposureCalculationService/RecalculateExposures");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "contact", "email"),
                ProbeSupport.value(sent0, "exposure", "holder", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "contact", "name"),
                ProbeSupport.value(sent0, "exposure", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "phone"),
                ProbeSupport.value(sent0, "exposure", "contact", "phone"));
        var sent1 = channel.request("acme.risk.v1.StressTestCalculationService/EstimateStressTest");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureCalculationService/RecalculateExposures"), "exposure", "holder", "email"),
                ProbeSupport.value(sent1, "stress_test", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureCalculationService/RecalculateExposures"), "exposure", "audit", "etag"),
                ProbeSupport.value(sent1, "stress_test", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "holder", "display_name"),
                ProbeSupport.value(sent1, "stress_test", "counterparty", "display_name"));
        var sent2 = channel.request("acme.risk.v1.ExposureCalculationService/EstimateExposure");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestCalculationService/EstimateStressTest"), "stress_test", "counterparty", "email"),
                ProbeSupport.value(sent2, "exposure", "contact", "email"));
        var sent3 = channel.request("acme.insurance.v1.ClaimCalculationService/QuoteClaim");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestCalculationService/EstimateStressTest"), "stress_test", "counterparty", "email"),
                ProbeSupport.value(sent3, "claim", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureCalculationService/RecalculateExposures"), "exposure", "audit", "created_by"),
                ProbeSupport.value(sent3, "claim", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "email"),
                ProbeSupport.value(sent3, "claim", "customer", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureCalculationService/EstimateExposure"), "exposure", "holder", "display_name"),
                ProbeSupport.value(sent3, "claim", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureCalculationService/RecalculateExposures"), "exposure", "audit", "updated_at"),
                ProbeSupport.value(response, "exposure", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.StressTestCalculationService/EstimateStressTest"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureCalculationService/EstimateExposure"), "exposure", "holder", "email"),
                ProbeSupport.value(response, "exposure", "holder", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureCalculationService/EstimateExposure"), "exposure", "contact"),
                ProbeSupport.value(response, "exposure", "contact"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/QuoteClaim"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimCalculationService/QuoteClaim"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure_id"),
                ProbeSupport.value(response, "exposure", "exposure_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "status"),
                ProbeSupport.value(response, "exposure", "status"));
    }
}
