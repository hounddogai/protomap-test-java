package com.acme.finance.domains.risk;

import com.acme.audit.v1.AnnotateRetentionPolicyVersionRequest;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.CheckAuditEventEligibilityRequest;
import com.acme.audit.v1.CheckAuditEventEligibilityResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyDiscountAction;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.RecalculateMandatesRequest;
import com.acme.banking.v1.RecalculateMandatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.corporateevents.v1.CorporateEvent;
import com.acme.corporateevents.v1.CorporateEventParticipant;
import com.acme.corporateevents.v1.RecalculateCorporateEventsRequest;
import com.acme.corporateevents.v1.RecalculateCorporateEventsResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagDiscountAction;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.NormalizeFeatureFlagRequest;
import com.acme.featureflags.v1.NormalizeFeatureFlagResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.CreateUserRequest;
import com.acme.identity.v1.CreateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.ExplainPolicyCalculationRequest;
import com.acme.insurance.v1.ExplainPolicyCalculationResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.ValidatePremiumRequest;
import com.acme.insurance.v1.ValidatePremiumResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CompareCollateralOptionsRequest;
import com.acme.lending.v1.CompareCollateralOptionsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.LoanCardFunding;
import com.acme.lending.v1.SimulateLoanRequest;
import com.acme.lending.v1.SimulateLoanResponse;
import com.acme.risk.v1.CalculateExposureRequest;
import com.acme.risk.v1.CalculateExposureResponse;
import com.acme.risk.v1.CompareExposureOptionsRequest;
import com.acme.risk.v1.CompareExposureOptionsResponse;
import com.acme.risk.v1.EstimateExposureRequest;
import com.acme.risk.v1.EstimateExposureResponse;
import com.acme.risk.v1.ExplainExposureCalculationRequest;
import com.acme.risk.v1.ExplainExposureCalculationResponse;
import com.acme.risk.v1.ExplainRiskRegisterDecisionRequest;
import com.acme.risk.v1.ExplainRiskRegisterDecisionResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.GetExposureRatesRequest;
import com.acme.risk.v1.GetExposureRatesResponse;
import com.acme.risk.v1.LintRiskRegistersRequest;
import com.acme.risk.v1.LintRiskRegistersResponse;
import com.acme.risk.v1.PreviewRiskRegisterRequest;
import com.acme.risk.v1.PreviewRiskRegisterResponse;
import com.acme.risk.v1.QuoteExposureRequest;
import com.acme.risk.v1.QuoteExposureResponse;
import com.acme.risk.v1.RecalculateExposuresRequest;
import com.acme.risk.v1.RecalculateExposuresResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.SimulateExposureRequest;
import com.acme.risk.v1.SimulateExposureResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.ValidateRiskRegisterRequest;
import com.acme.risk.v1.ValidateRiskRegisterResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.search.v1.ValidateSearchIndexRequest;
import com.acme.search.v1.ValidateSearchIndexResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
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

class ExposureCalculationApplicationTest {
    @Test
    void quoteExposurePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var listener = new ExposureCalculationApplicationEvents(LoanCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ExposureCalculationApplication.ExplainExposureCalculationStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ExposureCalculationApplication(
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                FeatureFlagValidationServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                PremiumValidationServiceGrpc.newBlockingStub(channel),
                CollateralCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterValidationServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SearchIndexValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(QuoteExposureRequest.getDefaultInstance());
        var response = application.quoteExposure(request);
        var sent0 = channel.request("acme.insurance.v1.PremiumValidationService/ValidatePremium");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "holder", "display_name"),
                ProbeSupport.value(sent0, "premium", "counterparty", "display_name"));
        var sent1 = channel.request("acme.audit.v1.AuditEventValidationService/CheckAuditEventEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "contact", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "contact", "name"),
                ProbeSupport.value(sent1, "details", "contact", "name"));
        var sent2 = channel.request("acme.lending.v1.CollateralCalculationService/CompareCollateralOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "country_code"),
                ProbeSupport.value(sent2, "collateral", "address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/ValidatePremium"), "premium", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/ValidatePremium"), "premium", "billing_address", "locality"),
                ProbeSupport.value(sent2, "collateral", "address", "locality"));
        var sent3 = channel.request("acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "limits", "amount"),
                ProbeSupport.value(sent3, "feature_flag", "discount", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "limits", "name"),
                ProbeSupport.value(sent3, "feature_flag", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "audit", "etag"),
                ProbeSupport.value(sent3, "feature_flag", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/ValidatePremium"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventValidationService/CheckAuditEventEligibility"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventValidationService/CheckAuditEventEligibility"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/CompareCollateralOptions"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure_id"),
                ProbeSupport.value(response, "exposure", "exposure_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "exposure", "balance"),
                ProbeSupport.value(response, "exposure", "balance"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ExposureCalculationApplicationEvents(LoanCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<SimulateLoanResponse>();
        listener.on(new ExposureCalculationApplication.ExplainExposureCalculationStage0Requested(ProbeSupport.sample(SimulateLoanRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
