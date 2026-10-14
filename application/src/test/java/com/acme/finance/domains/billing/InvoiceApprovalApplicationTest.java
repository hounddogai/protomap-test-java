package com.acme.finance.domains.billing;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.PreviewAccessPolicyRequest;
import com.acme.authorization.v1.PreviewAccessPolicyResponse;
import com.acme.billing.v1.ApproveInvoiceRequest;
import com.acme.billing.v1.ApproveInvoiceResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountApprovalServiceGrpc;
import com.acme.billing.v1.BillingAccountCardFunding;
import com.acme.billing.v1.BillingAccountLimit;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CheckBillingAccountEligibilityRequest;
import com.acme.billing.v1.CheckBillingAccountEligibilityResponse;
import com.acme.billing.v1.CommentInvoiceApprovalRequest;
import com.acme.billing.v1.CommentInvoiceApprovalResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.DelegateBillingAccountApprovalRequest;
import com.acme.billing.v1.DelegateBillingAccountApprovalResponse;
import com.acme.billing.v1.DelegateInvoiceApprovalRequest;
import com.acme.billing.v1.DelegateInvoiceApprovalResponse;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.EscalateInvoiceRequest;
import com.acme.billing.v1.EscalateInvoiceResponse;
import com.acme.billing.v1.GetInvoiceApprovalStatusRequest;
import com.acme.billing.v1.GetInvoiceApprovalStatusResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceCardSettlement;
import com.acme.billing.v1.InvoiceQueryServiceGrpc;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.RecalculateInvoicesRequest;
import com.acme.billing.v1.RecalculateInvoicesResponse;
import com.acme.billing.v1.RecallInvoiceRequest;
import com.acme.billing.v1.RecallInvoiceResponse;
import com.acme.billing.v1.RejectInvoiceRequest;
import com.acme.billing.v1.RejectInvoiceResponse;
import com.acme.billing.v1.ScoreBillingAccountRequest;
import com.acme.billing.v1.ScoreBillingAccountResponse;
import com.acme.billing.v1.SubmitInvoiceRequest;
import com.acme.billing.v1.SubmitInvoiceResponse;
import com.acme.billing.v1.SuggestInvoicesRequest;
import com.acme.billing.v1.SuggestInvoicesResponse;
import com.acme.budgeting.v1.EscalateVarianceRequest;
import com.acme.budgeting.v1.EscalateVarianceResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.FindUsersRequest;
import com.acme.identity.v1.FindUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.ApproveEInvoiceRequest;
import com.acme.invoicing.v1.ApproveEInvoiceResponse;
import com.acme.invoicing.v1.EInvoiceApprovalServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryBankSettlement;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SendPostingBatchTestNoticeRequest;
import com.acme.ledger.v1.SendPostingBatchTestNoticeResponse;
import com.acme.ledger.v1.ValidateJournalEntryRequest;
import com.acme.ledger.v1.ValidateJournalEntryResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesComponent;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.ValidateMetricSeriesRequest;
import com.acme.metrics.v1.ValidateMetricSeriesResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.ScoreNotificationRequest;
import com.acme.notifications.v1.ScoreNotificationResponse;
import com.acme.scheduling.v1.CreateShiftRequest;
import com.acme.scheduling.v1.CreateShiftResponse;
import com.acme.scheduling.v1.Shift;
import com.acme.scheduling.v1.ShiftParticipant;
import com.acme.scheduling.v1.ShiftServiceGrpc;
import com.acme.suppliers.v1.StartSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.StartSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingCardSettlement;
import com.acme.suppliers.v1.SupplierRatingWorkflowServiceGrpc;
import com.acme.venues.v1.SummarizeVenuesRequest;
import com.acme.venues.v1.SummarizeVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class InvoiceApprovalApplicationTest {
    @Test
    void recallInvoicePreservesPlannedFields() {
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
        var listener = new InvoiceApprovalApplicationEvents(BillingAccountApprovalServiceGrpc.newStub(channel), UserQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof InvoiceApprovalApplication.ApproveInvoiceStage2Requested command) {
                listener.on(command);
            }
            else if (event instanceof InvoiceApprovalApplication.EscalateInvoiceStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new InvoiceApprovalApplication(
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                BillingAccountApprovalServiceGrpc.newBlockingStub(channel),
                BillingAccountValidationServiceGrpc.newBlockingStub(channel),
                InvoiceCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceQueryServiceGrpc.newBlockingStub(channel),
                VarianceApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                events,
                EInvoiceApprovalServiceGrpc.newBlockingStub(channel),
                JournalEntryValidationServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                ShiftServiceGrpc.newBlockingStub(channel),
                SupplierRatingWorkflowServiceGrpc.newBlockingStub(channel),
                VenueQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RecallInvoiceRequest.getDefaultInstance());
        var response = application.recallInvoice(request);
        var sent0 = channel.request("acme.billing.v1.BillingAccountValidationService/ScoreBillingAccount");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice", "billing_address", "recipient"),
                ProbeSupport.value(sent0, "details", "destination", "recipient"));
        var sent1 = channel.request("acme.ledger.v1.PostingBatchNotificationService/SendPostingBatchTestNotice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice", "counterparty", "email"),
                ProbeSupport.value(sent1, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountValidationService/ScoreBillingAccount"), "billing_account", "contact", "name"),
                ProbeSupport.value(sent1, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountValidationService/ScoreBillingAccount"), "billing_account", "contact", "phone"),
                ProbeSupport.value(sent1, "details", "recipient", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountValidationService/ScoreBillingAccount"), "billing_account", "contact", "preferred_channel"),
                ProbeSupport.value(sent1, "details", "recipient", "preferred_channel"));
        var sent2 = channel.request("acme.billing.v1.BillingAccountApprovalService/DelegateBillingAccountApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountValidationService/ScoreBillingAccount"), "billing_account", "card", "last4"),
                ProbeSupport.value(sent2, "billing_account", "card", "last4"));
        var sent3 = channel.request("acme.authorization.v1.AccessPolicyValidationService/PreviewAccessPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchNotificationService/SendPostingBatchTestNotice"), "posting_batch", "audit", "updated_by"),
                ProbeSupport.value(sent3, "access_policy", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountValidationService/ScoreBillingAccount"), "billing_account", "contact", "email"),
                ProbeSupport.value(sent3, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "display_name"),
                ProbeSupport.value(sent3, "access_policy", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice", "audit", "etag"),
                ProbeSupport.value(sent3, "access_policy", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountValidationService/ScoreBillingAccount"), "billing_account", "holder", "number"),
                ProbeSupport.value(response, "invoice", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchNotificationService/SendPostingBatchTestNotice"), "posting_batch", "billing_address", "postal_code"),
                ProbeSupport.value(response, "invoice", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountApprovalService/DelegateBillingAccountApproval"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyValidationService/PreviewAccessPolicy"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_id"),
                ProbeSupport.value(response, "invoice", "invoice_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice", "notes"),
                ProbeSupport.value(response, "invoice", "notes"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new InvoiceApprovalApplicationEvents(BillingAccountApprovalServiceGrpc.newStub(channel), UserQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<DelegateBillingAccountApprovalResponse>();
        listener.on(new InvoiceApprovalApplication.ApproveInvoiceStage2Requested(ProbeSupport.sample(DelegateBillingAccountApprovalRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<FindUsersResponse>();
        listener.on(new InvoiceApprovalApplication.EscalateInvoiceStage2Requested(ProbeSupport.sample(FindUsersRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
