package com.acme.finance.domains.banking;

import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.CompareMandateOptionsRequest;
import com.acme.banking.v1.CompareMandateOptionsResponse;
import com.acme.banking.v1.ExplainMandateDecisionRequest;
import com.acme.banking.v1.ExplainMandateDecisionResponse;
import com.acme.banking.v1.ExplainStandingOrderDecisionRequest;
import com.acme.banking.v1.ExplainStandingOrderDecisionResponse;
import com.acme.banking.v1.GetMandateRatesRequest;
import com.acme.banking.v1.GetMandateRatesResponse;
import com.acme.banking.v1.GetMandateRulesRequest;
import com.acme.banking.v1.GetMandateRulesResponse;
import com.acme.banking.v1.LintMandatesRequest;
import com.acme.banking.v1.LintMandatesResponse;
import com.acme.banking.v1.LintStandingOrdersRequest;
import com.acme.banking.v1.LintStandingOrdersResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.NormalizeMandateRequest;
import com.acme.banking.v1.NormalizeMandateResponse;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.banking.v1.ScoreMandateRequest;
import com.acme.banking.v1.ScoreMandateResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.banking.v1.Statement;
import com.acme.banking.v1.ValidateMandateRequest;
import com.acme.banking.v1.ValidateMandateResponse;
import com.acme.banking.v1.ValidateStandingOrderRequest;
import com.acme.banking.v1.ValidateStandingOrderResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.CompareCoverageOptionsRequest;
import com.acme.insurance.v1.CompareCoverageOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.NormalizeClaimRequest;
import com.acme.insurance.v1.NormalizeClaimResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.PreviewPremiumRequest;
import com.acme.insurance.v1.PreviewPremiumResponse;
import com.acme.insurance.v1.ScoreClaimRequest;
import com.acme.insurance.v1.ScoreClaimResponse;
import com.acme.insurance.v1.ValidateClaimRequest;
import com.acme.insurance.v1.ValidateClaimResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class MandateValidationApplicationTest {
    @Test
    void validateMandatePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BankingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankAccount(String tenant, BankAccount value) {}
            public void saveMandate(String tenant, Mandate value) {}
            public void saveStandingOrder(String tenant, StandingOrder value) {}
            public void saveStatement(String tenant, Statement value) {}
        };
        var listener = new MandateValidationApplicationEvents(ClaimValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof MandateValidationApplication.ExplainMandateDecisionStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new MandateValidationApplication(
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                StandingOrderValidationServiceGrpc.newBlockingStub(channel),
                events,
                ClaimValidationServiceGrpc.newBlockingStub(channel),
                CoverageCalculationServiceGrpc.newBlockingStub(channel),
                PremiumValidationServiceGrpc.newBlockingStub(channel),
                new MandateValidationRequestMapperImpl(),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(ValidateMandateRequest.getDefaultInstance());
        var response = application.validateMandate(request);
        var sent0 = channel.request("acme.banking.v1.StandingOrderValidationService/LintStandingOrders");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "name"),
                ProbeSupport.value(sent0, "standing_order", "participants", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate", "audit", "updated_by"),
                ProbeSupport.value(sent0, "standing_order", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate", "audit", "created_at"),
                ProbeSupport.value(sent0, "standing_order", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate", "counterparty", "display_name"),
                ProbeSupport.value(sent0, "standing_order", "organizer", "display_name"));
        var sent1 = channel.request("acme.insurance.v1.CoverageCalculationService/CompareCoverageOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate", "audit", "created_by"),
                ProbeSupport.value(sent1, "coverage", "audit", "created_by"));
        var sent2 = channel.request("acme.insurance.v1.PremiumValidationService/PreviewPremium");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageCalculationService/CompareCoverageOptions"), "coverage", "audit", "updated_at"),
                ProbeSupport.value(sent2, "premium", "audit", "updated_at"));
        var sent3 = channel.request("acme.insurance.v1.ClaimValidationService/ValidateClaim");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/PreviewPremium"), "premium", "counterparty", "email"),
                ProbeSupport.value(sent3, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/LintStandingOrders"), "standing_order", "location", "country_code"),
                ProbeSupport.value(response, "mandate", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/LintStandingOrders"), "standing_order", "location", "region"),
                ProbeSupport.value(response, "mandate", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageCalculationService/CompareCoverageOptions"), "coverage", "description"),
                ProbeSupport.value(response, "mandate", "lines", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageCalculationService/CompareCoverageOptions"), "coverage", "owner", "email"),
                ProbeSupport.value(response, "mandate", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/PreviewPremium"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimValidationService/ValidateClaim"), "claim", "audit", "updated_by"),
                ProbeSupport.value(response, "mandate", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimValidationService/ValidateClaim"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate_id"),
                ProbeSupport.value(response, "mandate", "mandate_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate", "notes"),
                ProbeSupport.value(response, "mandate", "notes"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new MandateValidationApplicationEvents(ClaimValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<NormalizeClaimResponse>();
        listener.on(new MandateValidationApplication.ExplainMandateDecisionStage0Requested(ProbeSupport.sample(NormalizeClaimRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
