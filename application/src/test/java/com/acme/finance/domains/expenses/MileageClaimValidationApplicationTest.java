package com.acme.finance.domains.expenses;

import com.acme.careplans.v1.CarePlan;
import com.acme.careplans.v1.CarePlanBankSettlement;
import com.acme.careplans.v1.CarePlanServiceGrpc;
import com.acme.careplans.v1.CreateCarePlanRequest;
import com.acme.careplans.v1.CreateCarePlanResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlDiscountAction;
import com.acme.compliance.v1.EstimateControlRequest;
import com.acme.compliance.v1.EstimateControlResponse;
import com.acme.contracts.v1.ContractQueryServiceGrpc;
import com.acme.contracts.v1.ResolveContractsRequest;
import com.acme.contracts.v1.ResolveContractsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.CheckMileageClaimEligibilityRequest;
import com.acme.expenses.v1.CheckMileageClaimEligibilityResponse;
import com.acme.expenses.v1.CheckReceiptEligibilityRequest;
import com.acme.expenses.v1.CheckReceiptEligibilityResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportBankSettlement;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.expenses.v1.ExplainReceiptCalculationRequest;
import com.acme.expenses.v1.ExplainReceiptCalculationResponse;
import com.acme.expenses.v1.GetMileageClaimRulesRequest;
import com.acme.expenses.v1.GetMileageClaimRulesResponse;
import com.acme.expenses.v1.LintExpenseReportsRequest;
import com.acme.expenses.v1.LintExpenseReportsResponse;
import com.acme.expenses.v1.LintMileageClaimsRequest;
import com.acme.expenses.v1.LintMileageClaimsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.NormalizeMileageClaimRequest;
import com.acme.expenses.v1.NormalizeMileageClaimResponse;
import com.acme.expenses.v1.PreviewExpenseReportRequest;
import com.acme.expenses.v1.PreviewExpenseReportResponse;
import com.acme.expenses.v1.PreviewMileageClaimRequest;
import com.acme.expenses.v1.PreviewMileageClaimResponse;
import com.acme.expenses.v1.QuoteExpenseReportRequest;
import com.acme.expenses.v1.QuoteExpenseReportResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptCalculationServiceGrpc;
import com.acme.expenses.v1.ReceiptCardSettlement;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptLineTax;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ScoreExpenseReportRequest;
import com.acme.expenses.v1.ScoreExpenseReportResponse;
import com.acme.expenses.v1.ScoreMileageClaimRequest;
import com.acme.expenses.v1.ScoreMileageClaimResponse;
import com.acme.expenses.v1.ValidateExpenseReportRequest;
import com.acme.expenses.v1.ValidateExpenseReportResponse;
import com.acme.expenses.v1.ValidateMileageClaimRequest;
import com.acme.expenses.v1.ValidateMileageClaimResponse;
import com.acme.expenses.v1.ValidateReceiptRequest;
import com.acme.expenses.v1.ValidateReceiptResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountCardFunding;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.notifications.v1.CloneNotificationRequest;
import com.acme.notifications.v1.CloneNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.procurement.v1.ScoreRfqRequest;
import com.acme.procurement.v1.ScoreRfqResponse;
import com.google.protobuf.Any;
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

class MileageClaimValidationApplicationTest {
    @Test
    void scoreMileageClaimPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new MileageClaimValidationApplication(
                CarePlanServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                ContractQueryServiceGrpc.newFutureStub(channel),
                ExpenseReportCalculationServiceGrpc.newBlockingStub(channel),
                ExpenseReportValidationServiceGrpc.newBlockingStub(channel),
                ExpenseReportValidationServiceGrpc.newFutureStub(channel),
                ReceiptCalculationServiceGrpc.newBlockingStub(channel),
                ReceiptValidationServiceGrpc.newBlockingStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                NotificationServiceGrpc.newBlockingStub(channel),
                RfqValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(ScoreMileageClaimRequest.getDefaultInstance());
        var response = application.scoreMileageClaim(request);
        var sent0 = channel.request("acme.expenses.v1.ExpenseReportValidationService/ScoreExpenseReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "lines", "tax", "rate"),
                ProbeSupport.value(sent0, "expense_report", "lines", "tax", "rate"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "lines", "tax", "jurisdiction"),
                ProbeSupport.value(sent0, "expense_report", "lines", "tax", "jurisdiction"));
        var sent1 = channel.request("acme.expenses.v1.ReceiptValidationService/ValidateReceipt");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportValidationService/ScoreExpenseReport"), "expense_report", "billing_address", "postal_code"),
                ProbeSupport.value(sent1, "receipt", "billing_address", "postal_code"));
        var sent2 = channel.request("acme.ledger.v1.LedgerAccountValidationService/NormalizeLedgerAccount");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "card", "token"),
                ProbeSupport.value(sent2, "ledger_account", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "audit", "etag"),
                ProbeSupport.value(sent2, "ledger_account", "audit", "etag"));
        var sent3 = channel.request("acme.expenses.v1.ExpenseReportValidationService/ValidateExpenseReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/NormalizeLedgerAccount"), "ledger_account", "contact", "name"),
                ProbeSupport.value(sent3, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptValidationService/ValidateReceipt"), "receipt", "billing_address", "locality"),
                ProbeSupport.value(sent3, "expense_report", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "audit", "updated_at"),
                ProbeSupport.value(sent3, "expense_report", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptValidationService/ValidateReceipt"), "receipt", "audit", "etag"),
                ProbeSupport.value(sent3, "expense_report", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptValidationService/ValidateReceipt"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptValidationService/ValidateReceipt"), "receipt", "billing_address", "country_code"),
                ProbeSupport.value(response, "mileage_claim", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/NormalizeLedgerAccount"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportValidationService/ValidateExpenseReport"), "expense_report", "billing_address", "locality"),
                ProbeSupport.value(response, "mileage_claim", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim_id"),
                ProbeSupport.value(response, "mileage_claim", "mileage_claim_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "mileage_claim", "status"),
                ProbeSupport.value(response, "mileage_claim", "status"));
    }
}
