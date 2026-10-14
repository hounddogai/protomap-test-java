package com.acme.finance.domains.expenses;

import com.acme.assets.v1.CompareAssetTransferOptionsRequest;
import com.acme.assets.v1.CompareAssetTransferOptionsResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.CheckAuditTrailEligibilityRequest;
import com.acme.audit.v1.CheckAuditTrailEligibilityResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.CalculateReceiptRequest;
import com.acme.expenses.v1.CalculateReceiptResponse;
import com.acme.expenses.v1.CompareExpenseReportOptionsRequest;
import com.acme.expenses.v1.CompareExpenseReportOptionsResponse;
import com.acme.expenses.v1.CompareReceiptOptionsRequest;
import com.acme.expenses.v1.CompareReceiptOptionsResponse;
import com.acme.expenses.v1.EstimateReceiptRequest;
import com.acme.expenses.v1.EstimateReceiptResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExpenseReportCardSettlement;
import com.acme.expenses.v1.ExplainReceiptCalculationRequest;
import com.acme.expenses.v1.ExplainReceiptCalculationResponse;
import com.acme.expenses.v1.GetReceiptRatesRequest;
import com.acme.expenses.v1.GetReceiptRatesResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimRef;
import com.acme.expenses.v1.QuoteExpenseReportRequest;
import com.acme.expenses.v1.QuoteExpenseReportResponse;
import com.acme.expenses.v1.RecalculateExpenseReportsRequest;
import com.acme.expenses.v1.RecalculateExpenseReportsResponse;
import com.acme.expenses.v1.RecalculateReceiptsRequest;
import com.acme.expenses.v1.RecalculateReceiptsResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.SimulateReceiptRequest;
import com.acme.expenses.v1.SimulateReceiptResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.PreviewCollateralRequest;
import com.acme.lending.v1.PreviewCollateralResponse;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
import com.acme.notifications.v1.CloneNotificationRequest;
import com.acme.notifications.v1.CloneNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.suppliers.v1.BrowseSuppliersRequest;
import com.acme.suppliers.v1.BrowseSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.webhooks.v1.SearchWebhookEndpointsRequest;
import com.acme.webhooks.v1.SearchWebhookEndpointsResponse;
import com.acme.webhooks.v1.WebhookEndpointServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ReceiptCalculationApplicationTest {
    @Test
    void recalculateReceiptsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var listener = new ReceiptCalculationApplicationEvents(AuditTrailValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ReceiptCalculationApplication.ExplainReceiptCalculationStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ReceiptCalculationApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                ExpenseReportCalculationServiceGrpc.newBlockingStub(channel),
                JournalEntryNotificationServiceGrpc.newBlockingStub(channel),
                CollateralCalculationServiceGrpc.newBlockingStub(channel),
                CollateralValidationServiceGrpc.newBlockingStub(channel),
                NotificationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionServiceGrpc.newStub(channel),
                repository,
                SupplierQueryServiceGrpc.newBlockingStub(channel),
                WebhookEndpointServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RecalculateReceiptsRequest.getDefaultInstance());
        var response = application.recalculateReceipts(request);
        var sent0 = channel.request("acme.notifications.v1.NotificationService/CloneNotification");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "counterparty", "number"),
                ProbeSupport.value(sent0, "notification", "customer", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "counterparty", "display_name"),
                ProbeSupport.value(sent0, "notification", "assignee", "display_name"));
        var sent1 = channel.request("acme.expenses.v1.ExpenseReportCalculationService/CompareExpenseReportOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "country_code"),
                ProbeSupport.value(sent1, "expense_report", "billing_address", "country_code"));
        var sent2 = channel.request("acme.lending.v1.CollateralValidationService/PreviewCollateral");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportCalculationService/CompareExpenseReportOptions"), "expense_report", "billing_address", "postal_code"),
                ProbeSupport.value(sent2, "collateral", "address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/CloneNotification"), "notification", "requester", "email"),
                ProbeSupport.value(sent2, "collateral", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "billing_address", "recipient"),
                ProbeSupport.value(sent2, "collateral", "address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/CloneNotification"), "notification", "audit", "updated_by"),
                ProbeSupport.value(sent2, "collateral", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/CloneNotification"), "notification", "audit", "etag"),
                ProbeSupport.value(response, "receipt", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/CloneNotification"), "notification", "audit", "created_by"),
                ProbeSupport.value(response, "receipt", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportCalculationService/CompareExpenseReportOptions"), "expense_report", "audit", "updated_by"),
                ProbeSupport.value(response, "receipt", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/PreviewCollateral"), "collateral", "address", "country_code"),
                ProbeSupport.value(response, "receipt", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/PreviewCollateral"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt_id"),
                ProbeSupport.value(response, "receipt", "receipt_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "receipt", "total"),
                ProbeSupport.value(response, "receipt", "total"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ReceiptCalculationApplicationEvents(AuditTrailValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CheckAuditTrailEligibilityResponse>();
        listener.on(new ReceiptCalculationApplication.ExplainReceiptCalculationStage1Requested(ProbeSupport.sample(CheckAuditTrailEligibilityRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
