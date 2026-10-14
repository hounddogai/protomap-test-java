package com.acme.finance.domains.treasury;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.SyncRetentionPoliciesRequest;
import com.acme.audit.v1.SyncRetentionPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.lending.v1.CreateLoanApplicationRequest;
import com.acme.lending.v1.CreateLoanApplicationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationServiceGrpc;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.ValidateRepaymentRequest;
import com.acme.lending.v1.ValidateRepaymentResponse;
import com.acme.performance.v1.BrowseReviewsRequest;
import com.acme.performance.v1.BrowseReviewsResponse;
import com.acme.performance.v1.ReviewQueryServiceGrpc;
import com.acme.recommendations.v1.RecommendationSet;
import com.acme.recommendations.v1.RecommendationSetCardSettlement;
import com.acme.recommendations.v1.RecommendationSetServiceGrpc;
import com.acme.recommendations.v1.UpdateRecommendationSetRequest;
import com.acme.recommendations.v1.UpdateRecommendationSetResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionLimit;
import com.acme.treasury.v1.CheckCashPositionEligibilityRequest;
import com.acme.treasury.v1.CheckCashPositionEligibilityResponse;
import com.acme.treasury.v1.CompareCashPositionOptionsRequest;
import com.acme.treasury.v1.CompareCashPositionOptionsResponse;
import com.acme.treasury.v1.ExplainCashPositionDecisionRequest;
import com.acme.treasury.v1.ExplainCashPositionDecisionResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.GetCashPositionRatesRequest;
import com.acme.treasury.v1.GetCashPositionRatesResponse;
import com.acme.treasury.v1.GetCashPositionRulesRequest;
import com.acme.treasury.v1.GetCashPositionRulesResponse;
import com.acme.treasury.v1.LintCashPositionsRequest;
import com.acme.treasury.v1.LintCashPositionsResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.PreviewCashPositionRequest;
import com.acme.treasury.v1.PreviewCashPositionResponse;
import com.acme.treasury.v1.QuoteCashPositionRequest;
import com.acme.treasury.v1.QuoteCashPositionResponse;
import com.acme.treasury.v1.ScoreCashPositionRequest;
import com.acme.treasury.v1.ScoreCashPositionResponse;
import com.acme.treasury.v1.ValidateCashPositionRequest;
import com.acme.treasury.v1.ValidateCashPositionResponse;
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

class CashPositionValidationApplicationTest {
    @Test
    void validateCashPositionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new CashPositionValidationApplication(
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                LoanApplicationServiceGrpc.newBlockingStub(channel),
                RepaymentValidationServiceGrpc.newBlockingStub(channel),
                ReviewQueryServiceGrpc.newBlockingStub(channel),
                RecommendationSetServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                CashPositionCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ValidateCashPositionRequest.getDefaultInstance());
        var response = application.validateCashPosition(request);
        var sent0 = channel.request("acme.treasury.v1.CashPositionCalculationService/QuoteCashPosition");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "limits", "period"),
                ProbeSupport.value(sent0, "cash_position", "limits", "period"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "limits", "amount"),
                ProbeSupport.value(sent0, "cash_position", "limits", "amount"));
        var sent1 = channel.request("acme.treasury.v1.CashPositionCalculationService/GetCashPositionRates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "cash_position_id"),
                ProbeSupport.value(sent1, "cash_position_id"));
        var sent2 = channel.request("acme.lending.v1.RepaymentValidationService/ValidateRepayment");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/QuoteCashPosition"), "cash_position", "contact", "name"),
                ProbeSupport.value(sent2, "repayment", "participants", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/GetCashPositionRates"), "cash_position", "contact", "preferred_channel"),
                ProbeSupport.value(sent2, "repayment", "participants", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/GetCashPositionRates"), "cash_position", "contact", "email"),
                ProbeSupport.value(sent2, "repayment", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "contact", "name"),
                ProbeSupport.value(sent2, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/GetCashPositionRates"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position_id"),
                ProbeSupport.value(response, "cash_position", "cash_position_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "balance"),
                ProbeSupport.value(response, "cash_position", "balance"));
    }
}
