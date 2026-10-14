package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.ListFixedAssetsRequest;
import com.acme.assets.v1.ListFixedAssetsResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetryAuditTrailExportRequest;
import com.acme.audit.v1.RetryAuditTrailExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineLine;
import com.acme.budgeting.v1.BudgetLineLineTax;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.CalculateForecastRequest;
import com.acme.budgeting.v1.CalculateForecastResponse;
import com.acme.budgeting.v1.CheckForecastEligibilityRequest;
import com.acme.budgeting.v1.CheckForecastEligibilityResponse;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionRequest;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionResponse;
import com.acme.budgeting.v1.ExplainForecastDecisionRequest;
import com.acme.budgeting.v1.ExplainForecastDecisionResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastCardSettlement;
import com.acme.budgeting.v1.GetForecastRatesRequest;
import com.acme.budgeting.v1.GetForecastRatesResponse;
import com.acme.budgeting.v1.NormalizeForecastRequest;
import com.acme.budgeting.v1.NormalizeForecastResponse;
import com.acme.budgeting.v1.PreviewBudgetRequest;
import com.acme.budgeting.v1.PreviewBudgetResponse;
import com.acme.budgeting.v1.PreviewForecastRequest;
import com.acme.budgeting.v1.PreviewForecastResponse;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.budgeting.v1.ScoreForecastRequest;
import com.acme.budgeting.v1.ScoreForecastResponse;
import com.acme.budgeting.v1.ValidateForecastRequest;
import com.acme.budgeting.v1.ValidateForecastResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryLine;
import com.acme.ledger.v1.JournalEntryLineTax;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.LintJournalEntriesRequest;
import com.acme.ledger.v1.LintJournalEntriesResponse;
import com.acme.ledger.v1.ListLedgerAccountsRequest;
import com.acme.ledger.v1.ListLedgerAccountsResponse;
import com.acme.ledger.v1.StreamFiscalPeriodExportRequest;
import com.acme.ledger.v1.StreamFiscalPeriodExportResponse;
import com.acme.lending.v1.CheckLoanApplicationEligibilityRequest;
import com.acme.lending.v1.CheckLoanApplicationEligibilityResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class ForecastValidationApplicationTest {
    @Test
    void explainForecastDecisionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var application = new ForecastValidationApplication(
                FixedAssetServiceGrpc.newStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BudgetLineValidationServiceGrpc.newBlockingStub(channel),
                BudgetValidationServiceGrpc.newBlockingStub(channel),
                ForecastCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                JournalEntryValidationServiceGrpc.newBlockingStub(channel),
                LedgerAccountServiceGrpc.newFutureStub(channel),
                LoanApplicationValidationServiceGrpc.newBlockingStub(channel),
                new ForecastValidationRequestMapperImpl(),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(ExplainForecastDecisionRequest.getDefaultInstance());
        var response = application.explainForecastDecision(request);
        var sent0 = channel.request("acme.ledger.v1.LedgerAccountService/ListLedgerAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "audit", "updated_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        var sent1 = channel.request("acme.ledger.v1.JournalEntryValidationService/LintJournalEntries");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "lines", "amount"),
                ProbeSupport.value(sent1, "journal_entry", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "lines", "tax", "jurisdiction"),
                ProbeSupport.value(sent1, "journal_entry", "lines", "tax", "jurisdiction"));
        var sent2 = channel.request("acme.budgeting.v1.BudgetValidationService/PreviewBudget");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryValidationService/LintJournalEntries"), "journal_entry", "card", "holder_name"),
                ProbeSupport.value(sent2, "budget", "card", "holder_name"));
        var sent3 = channel.request("acme.budgeting.v1.ForecastCalculationService/GetForecastRates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "forecast_id"),
                ProbeSupport.value(sent3, "forecast_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountService/ListLedgerAccounts"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountService/ListLedgerAccounts"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetValidationService/PreviewBudget"), "budget", "holder", "display_name"),
                ProbeSupport.value(response, "forecast", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastCalculationService/GetForecastRates"), "forecast", "counterparty", "budget_id"),
                ProbeSupport.value(response, "forecast", "counterparty", "budget_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast_id"),
                ProbeSupport.value(response, "forecast", "forecast_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "forecast", "billing_address"),
                ProbeSupport.value(response, "forecast", "billing_address"));
    }
}
