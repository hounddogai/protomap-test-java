package com.acme.finance.domains.expenses;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamAuditTrailExportRequest;
import com.acme.audit.v1.StreamAuditTrailExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateLineTax;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.banking.v1.ScoreMandateRequest;
import com.acme.banking.v1.ScoreMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.content.v1.SearchArticlesRequest;
import com.acme.content.v1.SearchArticlesResponse;
import com.acme.drivers.v1.DriverRef;
import com.acme.expenses.v1.CalculateExpensePolicyRequest;
import com.acme.expenses.v1.CalculateExpensePolicyResponse;
import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.CompareExpenseReportOptionsRequest;
import com.acme.expenses.v1.CompareExpenseReportOptionsResponse;
import com.acme.expenses.v1.EstimateExpensePolicyRequest;
import com.acme.expenses.v1.EstimateExpensePolicyResponse;
import com.acme.expenses.v1.EstimateExpenseReportRequest;
import com.acme.expenses.v1.EstimateExpenseReportResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExplainExpensePolicyCalculationRequest;
import com.acme.expenses.v1.ExplainExpensePolicyCalculationResponse;
import com.acme.expenses.v1.GetExpensePolicyRatesRequest;
import com.acme.expenses.v1.GetExpensePolicyRatesResponse;
import com.acme.expenses.v1.LintReceiptsRequest;
import com.acme.expenses.v1.LintReceiptsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.PreviewReceiptRequest;
import com.acme.expenses.v1.PreviewReceiptResponse;
import com.acme.expenses.v1.QuoteExpensePolicyRequest;
import com.acme.expenses.v1.QuoteExpensePolicyResponse;
import com.acme.expenses.v1.QuoteExpenseReportRequest;
import com.acme.expenses.v1.QuoteExpenseReportResponse;
import com.acme.expenses.v1.RecalculateExpensePoliciesRequest;
import com.acme.expenses.v1.RecalculateExpensePoliciesResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptBankSettlement;
import com.acme.expenses.v1.ReceiptLine;
import com.acme.expenses.v1.ReceiptLineTax;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.SimulateExpensePolicyRequest;
import com.acme.expenses.v1.SimulateExpensePolicyResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.ExplainClaimDecisionRequest;
import com.acme.insurance.v1.ExplainClaimDecisionResponse;
import com.acme.routing.v1.RecalculateRoutesRequest;
import com.acme.routing.v1.RecalculateRoutesResponse;
import com.acme.routing.v1.Route;
import com.acme.routing.v1.RouteCalculationServiceGrpc;
import com.acme.ticketing.v1.TicketServiceGrpc;
import com.acme.ticketing.v1.WatchTicketsRequest;
import com.acme.ticketing.v1.WatchTicketsResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ExpensePolicyCalculationApplicationTest {
    @Test
    void recalculateExpensePoliciesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var listener = new ExpensePolicyCalculationApplicationEvents(MandateValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ExpensePolicyCalculationApplication.RecalculateExpensePoliciesStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ExpensePolicyCalculationApplication(
                AuditEventServiceGrpc.newStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ArticleServiceGrpc.newBlockingStub(channel),
                events,
                ExpenseReportCalculationServiceGrpc.newBlockingStub(channel),
                ExpenseReportCalculationServiceGrpc.newFutureStub(channel),
                ReceiptValidationServiceGrpc.newBlockingStub(channel),
                ClaimValidationServiceGrpc.newBlockingStub(channel),
                repository,
                RouteCalculationServiceGrpc.newBlockingStub(channel),
                TicketServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RecalculateExpensePoliciesRequest.getDefaultInstance());
        var response = application.recalculateExpensePolicies(request);
        var sent0 = channel.request("acme.content.v1.ArticleService/SearchArticles");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "audit", "updated_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "owner", "user_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "parameters"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.banking.v1.MandateValidationService/ScoreMandate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "discount", "amount"),
                ProbeSupport.value(sent1, "mandate", "lines", "tax", "amount"));
        var sent2 = channel.request("acme.banking.v1.MandateValidationService/PreviewMandate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "discount", "amount"),
                ProbeSupport.value(sent2, "mandate", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "audit", "updated_at"),
                ProbeSupport.value(sent2, "mandate", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.content.v1.ArticleService/SearchArticles"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.content.v1.ArticleService/SearchArticles"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/ScoreMandate"), "mandate", "audit", "updated_by"),
                ProbeSupport.value(response, "expense_policy", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy_id"),
                ProbeSupport.value(response, "expense_policy", "expense_policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_policy", "description"),
                ProbeSupport.value(response, "expense_policy", "description"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ExpensePolicyCalculationApplicationEvents(MandateValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ScoreMandateResponse>();
        listener.on(new ExpensePolicyCalculationApplication.RecalculateExpensePoliciesStage1Requested(ProbeSupport.sample(ScoreMandateRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
