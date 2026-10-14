package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineCardSettlement;
import com.acme.budgeting.v1.BudgetLineLine;
import com.acme.budgeting.v1.BudgetLineLineTax;
import com.acme.budgeting.v1.CalculateBudgetLineRequest;
import com.acme.budgeting.v1.CalculateBudgetLineResponse;
import com.acme.budgeting.v1.CheckBudgetEligibilityRequest;
import com.acme.budgeting.v1.CheckBudgetEligibilityResponse;
import com.acme.budgeting.v1.ExplainBudgetCalculationRequest;
import com.acme.budgeting.v1.ExplainBudgetCalculationResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.GetBudgetRatesRequest;
import com.acme.budgeting.v1.GetBudgetRatesResponse;
import com.acme.budgeting.v1.GetBudgetRulesRequest;
import com.acme.budgeting.v1.GetBudgetRulesResponse;
import com.acme.budgeting.v1.LintBudgetsRequest;
import com.acme.budgeting.v1.LintBudgetsResponse;
import com.acme.budgeting.v1.NormalizeBudgetRequest;
import com.acme.budgeting.v1.NormalizeBudgetResponse;
import com.acme.budgeting.v1.PreviewBudgetRequest;
import com.acme.budgeting.v1.PreviewBudgetResponse;
import com.acme.budgeting.v1.QuoteBudgetLineRequest;
import com.acme.budgeting.v1.QuoteBudgetLineResponse;
import com.acme.budgeting.v1.ScoreBudgetRequest;
import com.acme.budgeting.v1.ScoreBudgetResponse;
import com.acme.budgeting.v1.SimulateBudgetLineRequest;
import com.acme.budgeting.v1.SimulateBudgetLineResponse;
import com.acme.budgeting.v1.SimulateBudgetRequest;
import com.acme.budgeting.v1.SimulateBudgetResponse;
import com.acme.budgeting.v1.ValidateBudgetRequest;
import com.acme.budgeting.v1.ValidateBudgetResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.careplans.v1.BatchGetCarePlansRequest;
import com.acme.careplans.v1.BatchGetCarePlansResponse;
import com.acme.careplans.v1.CarePlanServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.content.v1.ListArticlesRequest;
import com.acme.content.v1.ListArticlesResponse;
import com.acme.featureflags.v1.ExplainFeatureFlagDecisionRequest;
import com.acme.featureflags.v1.ExplainFeatureFlagDecisionResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeRequest;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeResponse;
import com.acme.lending.v1.ExplainLoanCalculationRequest;
import com.acme.lending.v1.ExplainLoanCalculationResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.LoanLimit;
import com.acme.risk.v1.SimulateStressTestRequest;
import com.acme.risk.v1.SimulateStressTestResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import com.acme.risk.v1.StressTestCardSettlement;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.StressTestLineTax;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class BudgetValidationApplicationTest {
    @Test
    void checkBudgetEligibilityPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var application = new BudgetValidationApplication(
                FixedAssetServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BudgetCalculationServiceGrpc.newBlockingStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                CarePlanServiceGrpc.newBlockingStub(channel),
                ArticleServiceGrpc.newFutureStub(channel),
                FeatureFlagValidationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                LoanCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                StressTestCalculationServiceGrpc.newBlockingStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(CheckBudgetEligibilityRequest.getDefaultInstance());
        var response = application.checkBudgetEligibility(request);
        var sent0 = channel.request("acme.featureflags.v1.FeatureFlagValidationService/ExplainFeatureFlagDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget", "contact", "email"),
                ProbeSupport.value(sent0, "feature_flag", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget", "contact", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget", "audit", "created_by"),
                ProbeSupport.value(sent0, "feature_flag", "audit", "created_by"));
        var sent1 = channel.request("acme.budgeting.v1.BudgetLineCalculationService/CalculateBudgetLine");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagValidationService/ExplainFeatureFlagDecision"), "feature_flag", "discount", "amount"),
                ProbeSupport.value(sent1, "budget_line", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagValidationService/ExplainFeatureFlagDecision"), "feature_flag", "owner", "display_name"),
                ProbeSupport.value(sent1, "budget_line", "counterparty", "display_name"));
        var sent2 = channel.request("acme.budgeting.v1.BudgetCalculationService/GetBudgetRates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget", "budget_id"),
                ProbeSupport.value(sent2, "budget_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagValidationService/ExplainFeatureFlagDecision"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagValidationService/ExplainFeatureFlagDecision"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineCalculationService/CalculateBudgetLine"), "breakdown", "currency_code"),
                ProbeSupport.value(response, "budget", "currency_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetCalculationService/GetBudgetRates"), "budget", "holder", "number"),
                ProbeSupport.value(response, "budget", "holder", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_id"),
                ProbeSupport.value(response, "budget", "budget_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget", "opened_at"),
                ProbeSupport.value(response, "budget", "opened_at"));
    }
}
