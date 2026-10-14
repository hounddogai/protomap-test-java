package com.acme.finance.domains.lending;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailAdminServiceGrpc;
import com.acme.audit.v1.AuditTrailBankSettlement;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.RepairAuditTrailRequest;
import com.acme.audit.v1.RepairAuditTrailResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.CountBankAccountsRequest;
import com.acme.banking.v1.CountBankAccountsResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.banking.v1.MandateCardSettlement;
import com.acme.banking.v1.RecalculateMandatesRequest;
import com.acme.banking.v1.RecalculateMandatesResponse;
import com.acme.collections.v1.FindPaymentPromisesRequest;
import com.acme.collections.v1.FindPaymentPromisesResponse;
import com.acme.collections.v1.PaymentPromiseQueryServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.ExplainUserDecisionRequest;
import com.acme.identity.v1.ExplainUserDecisionResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumBankSettlement;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.RecalculatePremiumsRequest;
import com.acme.insurance.v1.RecalculatePremiumsResponse;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.StreamLedgerAccountNoticesRequest;
import com.acme.ledger.v1.StreamLedgerAccountNoticesResponse;
import com.acme.lending.v1.CalculateLoanRequest;
import com.acme.lending.v1.CalculateLoanResponse;
import com.acme.lending.v1.CheckLoanApplicationEligibilityRequest;
import com.acme.lending.v1.CheckLoanApplicationEligibilityResponse;
import com.acme.lending.v1.CheckLoanEligibilityRequest;
import com.acme.lending.v1.CheckLoanEligibilityResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CompareLoanApplicationOptionsRequest;
import com.acme.lending.v1.CompareLoanApplicationOptionsResponse;
import com.acme.lending.v1.CompareLoanOptionsRequest;
import com.acme.lending.v1.CompareLoanOptionsResponse;
import com.acme.lending.v1.ExplainLoanCalculationRequest;
import com.acme.lending.v1.ExplainLoanCalculationResponse;
import com.acme.lending.v1.ExplainLoanDecisionRequest;
import com.acme.lending.v1.ExplainLoanDecisionResponse;
import com.acme.lending.v1.GetLoanRatesRequest;
import com.acme.lending.v1.GetLoanRatesResponse;
import com.acme.lending.v1.GetLoanRulesRequest;
import com.acme.lending.v1.GetLoanRulesResponse;
import com.acme.lending.v1.LintLoansRequest;
import com.acme.lending.v1.LintLoansResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.LoanBankFunding;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.LoanCardFunding;
import com.acme.lending.v1.LoanLimit;
import com.acme.lending.v1.NormalizeLoanRequest;
import com.acme.lending.v1.NormalizeLoanResponse;
import com.acme.lending.v1.PreviewLoanRequest;
import com.acme.lending.v1.PreviewLoanResponse;
import com.acme.lending.v1.QuoteLoanRequest;
import com.acme.lending.v1.QuoteLoanResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.ScoreLoanRequest;
import com.acme.lending.v1.ScoreLoanResponse;
import com.acme.lending.v1.SimulateLoanRequest;
import com.acme.lending.v1.SimulateLoanResponse;
import com.acme.lending.v1.ValidateLoanRequest;
import com.acme.lending.v1.ValidateLoanResponse;
import com.acme.shows.v1.CompareShowOptionsRequest;
import com.acme.shows.v1.CompareShowOptionsResponse;
import com.acme.shows.v1.Show;
import com.acme.shows.v1.ShowCalculationServiceGrpc;
import com.acme.workplaces.v1.Building;
import com.acme.workplaces.v1.BuildingCalculationServiceGrpc;
import com.acme.workplaces.v1.RecalculateBuildingsRequest;
import com.acme.workplaces.v1.RecalculateBuildingsResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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

class LoanValidationApplicationTest {
    @Test
    void validateLoanPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new LendingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollateral(String tenant, Collateral value) {}
            public void saveLoan(String tenant, Loan value) {}
            public void saveLoanApplication(String tenant, LoanApplication value) {}
            public void saveRepayment(String tenant, Repayment value) {}
        };
        var application = new LoanValidationApplication(
                AuditTrailAdminServiceGrpc.newBlockingStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                BankAccountQueryServiceGrpc.newBlockingStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                PaymentPromiseQueryServiceGrpc.newBlockingStub(channel),
                UserValidationServiceGrpc.newBlockingStub(channel),
                PremiumCalculationServiceGrpc.newBlockingStub(channel),
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                LoanApplicationValidationServiceGrpc.newBlockingStub(channel),
                LoanCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                ShowCalculationServiceGrpc.newBlockingStub(channel),
                BuildingCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ValidateLoanRequest.getDefaultInstance());
        var response = application.validateLoan(request);
        var sent0 = channel.request("acme.collections.v1.PaymentPromiseQueryService/FindPaymentPromises");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "labels"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "loan_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.lending.v1.LoanCalculationService/GetLoanRates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_id"),
                ProbeSupport.value(sent1, "loan_id"));
        var sent2 = channel.request("acme.ledger.v1.LedgerAccountNotificationService/StreamLedgerAccountNotices");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "contact", "name"),
                ProbeSupport.value(sent2, "subscriber", "name"));
        var sent3 = channel.request("acme.lending.v1.LoanCalculationService/ExplainLoanCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "limits", "amount"),
                ProbeSupport.value(sent3, "loan", "limits", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.PaymentPromiseQueryService/FindPaymentPromises"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanCalculationService/GetLoanRates"), "loan", "contact", "preferred_channel"),
                ProbeSupport.value(response, "loan", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan_id"),
                ProbeSupport.value(response, "loan", "loan_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "loan", "balance"),
                ProbeSupport.value(response, "loan", "balance"));
    }
}
