package com.acme.finance.domains.billing;

import com.acme.audit.v1.AnnotateAuditTrailVersionRequest;
import com.acme.audit.v1.AnnotateAuditTrailVersionResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.ListAuditTrailVersionsRequest;
import com.acme.audit.v1.ListAuditTrailVersionsResponse;
import com.acme.audit.v1.NotifyAuditTrailWatchersRequest;
import com.acme.audit.v1.NotifyAuditTrailWatchersResponse;
import com.acme.billing.v1.ApproveBillingAccountRequest;
import com.acme.billing.v1.ApproveBillingAccountResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountBankFunding;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CommentBillingAccountApprovalRequest;
import com.acme.billing.v1.CommentBillingAccountApprovalResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.CreditNoteCardSettlement;
import com.acme.billing.v1.CreditNoteLifecycleServiceGrpc;
import com.acme.billing.v1.DelegateBillingAccountApprovalRequest;
import com.acme.billing.v1.DelegateBillingAccountApprovalResponse;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningReportingServiceGrpc;
import com.acme.billing.v1.EscalateBillingAccountRequest;
import com.acme.billing.v1.EscalateBillingAccountResponse;
import com.acme.billing.v1.ExplainCreditNoteCalculationRequest;
import com.acme.billing.v1.ExplainCreditNoteCalculationResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.ListPendingBillingAccountsRequest;
import com.acme.billing.v1.ListPendingBillingAccountsResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PreviewBillingAccountRequest;
import com.acme.billing.v1.PreviewBillingAccountResponse;
import com.acme.billing.v1.RecallBillingAccountRequest;
import com.acme.billing.v1.RecallBillingAccountResponse;
import com.acme.billing.v1.RejectBillingAccountRequest;
import com.acme.billing.v1.RejectBillingAccountResponse;
import com.acme.billing.v1.ScheduleCreditNoteExpiryRequest;
import com.acme.billing.v1.ScheduleCreditNoteExpiryResponse;
import com.acme.billing.v1.ScoreBillingAccountRequest;
import com.acme.billing.v1.ScoreBillingAccountResponse;
import com.acme.billing.v1.StreamDunningMetricsRequest;
import com.acme.billing.v1.StreamDunningMetricsResponse;
import com.acme.billing.v1.SubmitBillingAccountRequest;
import com.acme.billing.v1.SubmitBillingAccountResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.customers.v1.BrowseCustomersRequest;
import com.acme.customers.v1.BrowseCustomersResponse;
import com.acme.customers.v1.CountCustomersRequest;
import com.acme.customers.v1.CountCustomersResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.SearchCustomersRequest;
import com.acme.customers.v1.SearchCustomersResponse;
import com.acme.documents.v1.BrowseDocumentsRequest;
import com.acme.documents.v1.BrowseDocumentsResponse;
import com.acme.documents.v1.CheckDocumentEligibilityRequest;
import com.acme.documents.v1.CheckDocumentEligibilityResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentCardSettlement;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.GetUserRequest;
import com.acme.identity.v1.GetUserResponse;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.personalization.v1.ListProfilesRequest;
import com.acme.personalization.v1.ListProfilesResponse;
import com.acme.personalization.v1.ProfileServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class BillingAccountApprovalApplicationTest {
    @Test
    void rejectBillingAccountPreservesPlannedFields() {
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
        var listener = new BillingAccountApprovalApplicationEvents(CustomerServiceGrpc.newStub(channel), DocumentQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof BillingAccountApprovalApplication.ApproveBillingAccountStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof BillingAccountApprovalApplication.ListPendingBillingAccountsStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new BillingAccountApprovalApplication(
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                AuditTrailNotificationServiceGrpc.newBlockingStub(channel),
                BillingAccountValidationServiceGrpc.newBlockingStub(channel),
                CreditNoteLifecycleServiceGrpc.newBlockingStub(channel),
                DunningReportingServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                DocumentValidationServiceGrpc.newFutureStub(channel),
                events,
                UserServiceGrpc.newBlockingStub(channel),
                ProfileServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(RejectBillingAccountRequest.getDefaultInstance());
        var response = application.rejectBillingAccount(request);
        var sent0 = channel.request("acme.billing.v1.BillingAccountValidationService/PreviewBillingAccount");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "contact", "email"),
                ProbeSupport.value(sent0, "billing_account", "contact", "email"));
        var sent1 = channel.request("acme.customers.v1.CustomerQueryService/BrowseCustomers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "labels"),
                ProbeSupport.value(sent1, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountValidationService/PreviewBillingAccount"), "billing_account", "contact", "phone"),
                ProbeSupport.value(response, "billing_account", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountValidationService/PreviewBillingAccount"), "billing_account", "contact", "preferred_channel"),
                ProbeSupport.value(response, "billing_account", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerQueryService/BrowseCustomers"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerQueryService/BrowseCustomers"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account_id"),
                ProbeSupport.value(response, "billing_account", "billing_account_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "billing_account", "available"),
                ProbeSupport.value(response, "billing_account", "available"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new BillingAccountApprovalApplicationEvents(CustomerServiceGrpc.newStub(channel), DocumentQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<SearchCustomersResponse>();
        listener.on(new BillingAccountApprovalApplication.ApproveBillingAccountStage1Requested(ProbeSupport.sample(SearchCustomersRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<BrowseDocumentsResponse>();
        listener.on(new BillingAccountApprovalApplication.ListPendingBillingAccountsStage1Requested(ProbeSupport.sample(BrowseDocumentsRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
