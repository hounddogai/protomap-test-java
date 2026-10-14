package com.acme.finance.domains.banking;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CloneAccessPolicyRequest;
import com.acme.authorization.v1.CloneAccessPolicyResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.CalculateMandateRequest;
import com.acme.banking.v1.CalculateMandateResponse;
import com.acme.banking.v1.CompareMandateOptionsRequest;
import com.acme.banking.v1.CompareMandateOptionsResponse;
import com.acme.banking.v1.EstimateMandateRequest;
import com.acme.banking.v1.EstimateMandateResponse;
import com.acme.banking.v1.ExplainMandateCalculationRequest;
import com.acme.banking.v1.ExplainMandateCalculationResponse;
import com.acme.banking.v1.GetMandateRatesRequest;
import com.acme.banking.v1.GetMandateRatesResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.banking.v1.QuoteMandateRequest;
import com.acme.banking.v1.QuoteMandateResponse;
import com.acme.banking.v1.RecalculateMandatesRequest;
import com.acme.banking.v1.RecalculateMandatesResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.Statement;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.content.v1.BatchGetArticlesRequest;
import com.acme.content.v1.BatchGetArticlesResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.experiments.v1.Experiment;
import com.acme.experiments.v1.ExperimentCalculationServiceGrpc;
import com.acme.experiments.v1.ExperimentParticipant;
import com.acme.experiments.v1.ExplainExperimentCalculationRequest;
import com.acme.experiments.v1.ExplainExperimentCalculationResponse;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.ValidateFeatureFlagRequest;
import com.acme.featureflags.v1.ValidateFeatureFlagResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.fuel.v1.FuelCardServiceGrpc;
import com.acme.fuel.v1.ListFuelCardsRequest;
import com.acme.fuel.v1.ListFuelCardsResponse;
import com.acme.fuel.v1.WatchFuelCardsRequest;
import com.acme.fuel.v1.WatchFuelCardsResponse;
import com.acme.helpdesk.v1.ResolveTicketsRequest;
import com.acme.helpdesk.v1.ResolveTicketsResponse;
import com.acme.helpdesk.v1.TicketQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CheckCoverageEligibilityRequest;
import com.acme.insurance.v1.CheckCoverageEligibilityResponse;
import com.acme.insurance.v1.CheckPolicyEligibilityRequest;
import com.acme.insurance.v1.CheckPolicyEligibilityResponse;
import com.acme.insurance.v1.CountPoliciesRequest;
import com.acme.insurance.v1.CountPoliciesResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageDiscountAction;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ExplainCoverageDecisionRequest;
import com.acme.insurance.v1.ExplainCoverageDecisionResponse;
import com.acme.insurance.v1.ExplainUnderwritingCalculationRequest;
import com.acme.insurance.v1.ExplainUnderwritingCalculationResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.ResolvePoliciesRequest;
import com.acme.insurance.v1.ResolvePoliciesResponse;
import com.acme.insurance.v1.ScorePremiumRequest;
import com.acme.insurance.v1.ScorePremiumResponse;
import com.acme.insurance.v1.SimulateCoverageRequest;
import com.acme.insurance.v1.SimulateCoverageResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.media.v1.MediaAssetQueryServiceGrpc;
import com.acme.media.v1.SummarizeMediaAssetsRequest;
import com.acme.media.v1.SummarizeMediaAssetsResponse;
import com.acme.suppliers.v1.SupplierRef;
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
import org.springframework.stereotype.Service;

class MandateCalculationApplicationTest {
    @Test
    void compareMandateOptionsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BankingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankAccount(String tenant, BankAccount value) {}
            public void saveMandate(String tenant, Mandate value) {}
            public void saveStandingOrder(String tenant, StandingOrder value) {}
            public void saveStatement(String tenant, Statement value) {}
        };
        var application = new MandateCalculationApplication(
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ExperimentCalculationServiceGrpc.newBlockingStub(channel),
                FeatureFlagValidationServiceGrpc.newBlockingStub(channel),
                FuelCardServiceGrpc.newBlockingStub(channel),
                CoverageCalculationServiceGrpc.newBlockingStub(channel),
                CoverageValidationServiceGrpc.newBlockingStub(channel),
                PolicyQueryServiceGrpc.newBlockingStub(channel),
                PolicyValidationServiceGrpc.newBlockingStub(channel),
                MediaAssetQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(CompareMandateOptionsRequest.getDefaultInstance());
        var response = application.compareMandateOptions(request);
        var sent0 = channel.request("acme.insurance.v1.CoverageCalculationService/SimulateCoverage");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate", "billing_address", "lines"),
                ProbeSupport.value(sent0, "details", "destination", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate", "billing_address", "recipient"),
                ProbeSupport.value(sent0, "details", "destination", "recipient"));
        var sent1 = channel.request("acme.insurance.v1.CoverageValidationService/CheckCoverageEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate", "lines", "tax", "amount"),
                ProbeSupport.value(sent1, "coverage", "discount", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate", "lines", "description"),
                ProbeSupport.value(sent1, "coverage", "description"));
        var sent2 = channel.request("acme.insurance.v1.PremiumValidationService/ScorePremium");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "locality"),
                ProbeSupport.value(sent2, "premium", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate", "counterparty", "email"),
                ProbeSupport.value(sent2, "premium", "counterparty", "email"));
        var sent3 = channel.request("acme.insurance.v1.UnderwritingCalculationService/ExplainUnderwritingCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate", "counterparty", "email"),
                ProbeSupport.value(sent3, "underwriting", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageCalculationService/SimulateCoverage"), "breakdown", "total"),
                ProbeSupport.value(response, "breakdown", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/ScorePremium"), "premium", "issued_at"),
                ProbeSupport.value(response, "mandate", "issued_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumValidationService/ScorePremium"), "premium", "audit", "updated_by"),
                ProbeSupport.value(response, "mandate", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingCalculationService/ExplainUnderwritingCalculation"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingCalculationService/ExplainUnderwritingCalculation"), "underwriting", "assignee", "email"),
                ProbeSupport.value(response, "mandate", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate_id"),
                ProbeSupport.value(response, "mandate", "mandate_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mandate", "total"),
                ProbeSupport.value(response, "mandate", "total"));
    }
}
