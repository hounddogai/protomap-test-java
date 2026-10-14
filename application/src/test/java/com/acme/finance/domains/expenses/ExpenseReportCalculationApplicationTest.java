package com.acme.finance.domains.expenses;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.DisposalComment;
import com.acme.assets.v1.ExplainDisposalCalculationRequest;
import com.acme.assets.v1.ExplainDisposalCalculationResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.NormalizeRetentionPolicyRequest;
import com.acme.audit.v1.NormalizeRetentionPolicyResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyDiscountAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.chat.v1.ChatRoom;
import com.acme.chat.v1.ChatRoomServiceGrpc;
import com.acme.chat.v1.CreateChatRoomRequest;
import com.acme.chat.v1.CreateChatRoomResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.CompareExpenseReportOptionsRequest;
import com.acme.expenses.v1.CompareExpenseReportOptionsResponse;
import com.acme.expenses.v1.EstimateExpenseReportRequest;
import com.acme.expenses.v1.EstimateExpenseReportResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExplainExpenseReportCalculationRequest;
import com.acme.expenses.v1.ExplainExpenseReportCalculationResponse;
import com.acme.expenses.v1.GetExpenseReportRatesRequest;
import com.acme.expenses.v1.GetExpenseReportRatesResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.QuoteExpenseReportRequest;
import com.acme.expenses.v1.QuoteExpenseReportResponse;
import com.acme.expenses.v1.RecalculateExpenseReportsRequest;
import com.acme.expenses.v1.RecalculateExpenseReportsResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.SimulateExpenseReportRequest;
import com.acme.expenses.v1.SimulateExpenseReportResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.labeling.v1.BatchGetLabelingTasksRequest;
import com.acme.labeling.v1.BatchGetLabelingTasksResponse;
import com.acme.labeling.v1.LabelingTaskServiceGrpc;
import com.acme.ledger.v1.CloneLedgerAccountRequest;
import com.acme.ledger.v1.CloneLedgerAccountResponse;
import com.acme.ledger.v1.CompareJournalEntryVersionsRequest;
import com.acme.ledger.v1.CompareJournalEntryVersionsResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryAttachment;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountCardFunding;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.lending.v1.CheckRepaymentEligibilityRequest;
import com.acme.lending.v1.CheckRepaymentEligibilityResponse;
import com.acme.lending.v1.ExplainRepaymentDecisionRequest;
import com.acme.lending.v1.ExplainRepaymentDecisionResponse;
import com.acme.lending.v1.PreviewRepaymentRequest;
import com.acme.lending.v1.PreviewRepaymentResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.RepaymentWeeklyRule;
import com.acme.lending.v1.ScoreRepaymentRequest;
import com.acme.lending.v1.ScoreRepaymentResponse;
import com.acme.notifications.v1.CountNotificationsRequest;
import com.acme.notifications.v1.CountNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.notifications.v1.ResolveNotificationsRequest;
import com.acme.notifications.v1.ResolveNotificationsResponse;
import com.acme.patients.v1.PatientRef;
import com.acme.prescriptions.v1.ComparePrescriptionOptionsRequest;
import com.acme.prescriptions.v1.ComparePrescriptionOptionsResponse;
import com.acme.prescriptions.v1.Prescription;
import com.acme.prescriptions.v1.PrescriptionCalculationServiceGrpc;
import com.acme.prescriptions.v1.PrescriptionCardSettlement;
import com.acme.procurement.v1.EstimateRfqRequest;
import com.acme.procurement.v1.EstimateRfqResponse;
import com.acme.procurement.v1.LintPurchaseOrdersRequest;
import com.acme.procurement.v1.LintPurchaseOrdersResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.search.v1.BatchGetSearchIndexesRequest;
import com.acme.search.v1.BatchGetSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.SuggestSearchIndexesRequest;
import com.acme.search.v1.SuggestSearchIndexesResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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

class ExpenseReportCalculationApplicationTest {
    @Test
    void explainExpenseReportCalculationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new ExpenseReportCalculationApplication(
                DisposalCalculationServiceGrpc.newBlockingStub(channel),
                FixedAssetServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                RetentionPolicyValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ChatRoomServiceGrpc.newBlockingStub(channel),
                LabelingTaskServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                LedgerAccountServiceGrpc.newBlockingStub(channel),
                RepaymentValidationServiceGrpc.newBlockingStub(channel),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                PrescriptionCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseOrderValidationServiceGrpc.newBlockingStub(channel),
                RfqCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel),
                SearchIndexServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExplainExpenseReportCalculationRequest.getDefaultInstance());
        var response = application.explainExpenseReportCalculation(request);
        var sent0 = channel.request("acme.notifications.v1.NotificationQueryService/CountNotifications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "due_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        var sent1 = channel.request("acme.procurement.v1.PurchaseOrderValidationService/LintPurchaseOrders");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "billing_address", "country_code"),
                ProbeSupport.value(sent1, "purchase_order", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "audit", "etag"),
                ProbeSupport.value(sent1, "purchase_order", "audit", "etag"));
        var sent2 = channel.request("acme.ledger.v1.JournalEntryHistoryService/CompareJournalEntryVersions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "attachments", "checksum"),
                ProbeSupport.value(sent2, "journal_entry", "attachments", "checksum"));
        var sent3 = channel.request("acme.lending.v1.RepaymentValidationService/PreviewRepayment");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryHistoryService/CompareJournalEntryVersions"), "journal_entry", "counterparty", "display_name"),
                ProbeSupport.value(sent3, "repayment", "organizer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "billing_address", "locality"),
                ProbeSupport.value(sent3, "repayment", "location", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "phone"),
                ProbeSupport.value(sent3, "repayment", "participants", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "billing_address", "recipient"),
                ProbeSupport.value(sent3, "repayment", "location", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationQueryService/CountNotifications"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderValidationService/LintPurchaseOrders"), "outcome", "reference_id"),
                ProbeSupport.value(response, "outcome", "reference_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentValidationService/PreviewRepayment"), "repayment", "audit", "etag"),
                ProbeSupport.value(response, "expense_report", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report_id"),
                ProbeSupport.value(response, "expense_report", "expense_report_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "expense_report", "metadata"),
                ProbeSupport.value(response, "expense_report", "metadata"));
    }
}
