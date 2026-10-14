package com.acme.finance.domains.billing;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.NormalizeAccessPolicyRequest;
import com.acme.authorization.v1.NormalizeAccessPolicyResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BrowseBillingAccountsRequest;
import com.acme.billing.v1.BrowseBillingAccountsResponse;
import com.acme.billing.v1.CheckInvoiceEligibilityRequest;
import com.acme.billing.v1.CheckInvoiceEligibilityResponse;
import com.acme.billing.v1.CountBillingAccountsRequest;
import com.acme.billing.v1.CountBillingAccountsResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteLine;
import com.acme.billing.v1.CreditNoteValidationServiceGrpc;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.ExportBillingAccountsRequest;
import com.acme.billing.v1.ExportBillingAccountsResponse;
import com.acme.billing.v1.FindBillingAccountsRequest;
import com.acme.billing.v1.FindBillingAccountsResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceBankSettlement;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.LookupBillingAccountRequest;
import com.acme.billing.v1.LookupBillingAccountResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PeekBillingAccountRequest;
import com.acme.billing.v1.PeekBillingAccountResponse;
import com.acme.billing.v1.PreviewInvoiceRequest;
import com.acme.billing.v1.PreviewInvoiceResponse;
import com.acme.billing.v1.ResolveBillingAccountsRequest;
import com.acme.billing.v1.ResolveBillingAccountsResponse;
import com.acme.billing.v1.ScoreCreditNoteRequest;
import com.acme.billing.v1.ScoreCreditNoteResponse;
import com.acme.billing.v1.SuggestBillingAccountsRequest;
import com.acme.billing.v1.SuggestBillingAccountsResponse;
import com.acme.billing.v1.ValidateCreditNoteRequest;
import com.acme.billing.v1.ValidateCreditNoteResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.CheckDocumentEligibilityRequest;
import com.acme.documents.v1.CheckDocumentEligibilityResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentBankSettlement;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.acme.notifications.v1.CheckNotificationEligibilityRequest;
import com.acme.notifications.v1.CheckNotificationEligibilityResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.ValidateNotificationRequest;
import com.acme.notifications.v1.ValidateNotificationResponse;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class BillingAccountQueryApplicationTest {
    @Test
    void lookupBillingAccountPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BillingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBillingAccount(String tenant, BillingAccount value) {}
            public void saveCreditNote(String tenant, CreditNote value) {}
            public void saveDunning(String tenant, Dunning value) {}
            public void saveInvoice(String tenant, Invoice value) {}
            public void savePaymentTerm(String tenant, PaymentTerm value) {}
        };
        var application = new BillingAccountQueryApplication(
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                CreditNoteValidationServiceGrpc.newBlockingStub(channel),
                InvoiceValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newFutureStub(channel),
                repository);
        var request = ProbeSupport.sample(LookupBillingAccountRequest.getDefaultInstance());
        var response = application.lookupBillingAccount(request);
        var sent0 = channel.request("acme.notifications.v1.NotificationValidationService/ValidateNotification");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "notification", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent0, "notification", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "locale"),
                ProbeSupport.value(sent0, "details", "locale"));
        var sent1 = channel.request("acme.ledger.v1.LedgerAccountNotificationService/SendLedgerAccountTestNotice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/ValidateNotification"), "notification", "requester", "phone"),
                ProbeSupport.value(sent1, "details", "recipient", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/ValidateNotification"), "notification", "requester", "name"),
                ProbeSupport.value(sent1, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/ValidateNotification"), "notification", "requester", "email"),
                ProbeSupport.value(sent1, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/ValidateNotification"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/ValidateNotification"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountNotificationService/SendLedgerAccountTestNotice"), "ledger_account", "contact", "phone"),
                ProbeSupport.value(response, "billing_account", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account_id"),
                ProbeSupport.value(response, "billing_account", "billing_account_id"));
    }

    @Test
    void exportBillingAccountsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BillingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBillingAccount(String tenant, BillingAccount value) {}
            public void saveCreditNote(String tenant, CreditNote value) {}
            public void saveDunning(String tenant, Dunning value) {}
            public void saveInvoice(String tenant, Invoice value) {}
            public void savePaymentTerm(String tenant, PaymentTerm value) {}
        };
        var application = new BillingAccountQueryApplication(
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                CreditNoteValidationServiceGrpc.newBlockingStub(channel),
                InvoiceValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newFutureStub(channel),
                repository);
        var request = ProbeSupport.sample(ExportBillingAccountsRequest.getDefaultInstance());
        var response = application.exportBillingAccounts(request);
        var sent0 = channel.request("acme.billing.v1.InvoiceValidationService/PreviewInvoice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "invoice", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "locale"),
                ProbeSupport.value(sent0, "details", "locale"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent0, "invoice", "issued_at"));
        var sent1 = channel.request("acme.documents.v1.DocumentValidationService/CheckDocumentEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/PreviewInvoice"), "invoice", "audit", "created_at"),
                ProbeSupport.value(sent1, "document", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/PreviewInvoice"), "invoice", "bank", "bic"),
                ProbeSupport.value(sent1, "document", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/PreviewInvoice"), "invoice", "counterparty", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/PreviewInvoice"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentValidationService/CheckDocumentEligibility"), "document", "audit", "created_by"),
                ProbeSupport.value(response, "billing_account", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentValidationService/CheckDocumentEligibility"), "document", "counterparty", "display_name"),
                ProbeSupport.value(response, "billing_account", "holder", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(response, "billing_account", "labels"));
        var endpoint = new BillingAccountQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportBillingAccountsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportBillingAccountsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportBillingAccounts(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
