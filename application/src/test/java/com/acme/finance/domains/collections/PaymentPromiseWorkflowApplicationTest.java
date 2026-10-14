package com.acme.finance.domains.collections;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.collections.v1.AdvancePaymentPromiseWorkflowRequest;
import com.acme.collections.v1.AdvancePaymentPromiseWorkflowResponse;
import com.acme.collections.v1.BrowsePaymentPromisesRequest;
import com.acme.collections.v1.BrowsePaymentPromisesResponse;
import com.acme.collections.v1.CancelPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.CancelPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.CloneCollectionCaseRequest;
import com.acme.collections.v1.CloneCollectionCaseResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.CollectionCaseComment;
import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.ExportCollectionCasesRequest;
import com.acme.collections.v1.ExportCollectionCasesResponse;
import com.acme.collections.v1.InspectPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.InspectPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.ListPaymentPromiseWorkflowsRequest;
import com.acme.collections.v1.ListPaymentPromiseWorkflowsResponse;
import com.acme.collections.v1.NormalizeWriteOffRequest;
import com.acme.collections.v1.NormalizeWriteOffResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.PaymentPromiseQueryServiceGrpc;
import com.acme.collections.v1.RecallWriteOffRequest;
import com.acme.collections.v1.RecallWriteOffResponse;
import com.acme.collections.v1.RejectWriteOffRequest;
import com.acme.collections.v1.RejectWriteOffResponse;
import com.acme.collections.v1.ResolveCollectionCasesRequest;
import com.acme.collections.v1.ResolveCollectionCasesResponse;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowRequest;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowResponse;
import com.acme.collections.v1.RetryPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.RetryPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.SignalPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.SignalPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.StartPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.StartPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.SubmitCollectionCaseRequest;
import com.acme.collections.v1.SubmitCollectionCaseResponse;
import com.acme.collections.v1.SubmitWriteOffRequest;
import com.acme.collections.v1.SubmitWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.collections.v1.WriteOffBankSettlement;
import com.acme.collections.v1.WriteOffCardSettlement;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.forms.v1.CompareFormOptionsRequest;
import com.acme.forms.v1.CompareFormOptionsResponse;
import com.acme.forms.v1.Form;
import com.acme.forms.v1.FormBankSettlement;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CountCoverageTotalsRequest;
import com.acme.insurance.v1.CountCoverageTotalsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.DelegatePremiumApprovalRequest;
import com.acme.insurance.v1.DelegatePremiumApprovalResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.RetryCoverageWorkflowRequest;
import com.acme.insurance.v1.RetryCoverageWorkflowResponse;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.SendJournalEntryTestNoticeRequest;
import com.acme.ledger.v1.SendJournalEntryTestNoticeResponse;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesResponse;
import com.acme.suppliers.v1.BatchCreateSuppliersRequest;
import com.acme.suppliers.v1.BatchCreateSuppliersResponse;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class PaymentPromiseWorkflowApplicationTest {
    @Test
    void cancelPaymentPromiseWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new CollectionsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollectionCase(String tenant, CollectionCase value) {}
            public void savePaymentPromise(String tenant, PaymentPromise value) {}
            public void saveWriteOff(String tenant, WriteOff value) {}
        };
        var listener = new PaymentPromiseWorkflowApplicationEvents(JournalEntryNotificationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof PaymentPromiseWorkflowApplication.InspectPaymentPromiseWorkflowStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new PaymentPromiseWorkflowApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseApprovalServiceGrpc.newBlockingStub(channel),
                CollectionCaseQueryServiceGrpc.newBlockingStub(channel),
                CollectionCaseServiceGrpc.newBlockingStub(channel),
                PaymentPromiseQueryServiceGrpc.newBlockingStub(channel),
                WriteOffApprovalServiceGrpc.newBlockingStub(channel),
                WriteOffValidationServiceGrpc.newBlockingStub(channel),
                events,
                CoverageReportingServiceGrpc.newBlockingStub(channel),
                CoverageWorkflowServiceGrpc.newBlockingStub(channel),
                PremiumApprovalServiceGrpc.newBlockingStub(channel),
                JournalEntryNotificationServiceGrpc.newBlockingStub(channel),
                repository,
                SupplierServiceGrpc.newStub(channel));
        var request = ProbeSupport.sample(CancelPaymentPromiseWorkflowRequest.getDefaultInstance());
        var response = application.cancelPaymentPromiseWorkflow(request);
        var sent0 = channel.request("acme.collections.v1.CollectionCaseQueryService/ExportCollectionCases");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "location", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "audit", "updated_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "parameters"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.collections.v1.CollectionCaseApprovalService/SubmitCollectionCase");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "organizer", "email"),
                ProbeSupport.value(sent1, "collection_case", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent1, "collection_case", "requester", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "audit", "created_at"),
                ProbeSupport.value(sent1, "collection_case", "comments", "created_at"));
        var sent2 = channel.request("acme.insurance.v1.PremiumApprovalService/DelegatePremiumApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "location", "lines"),
                ProbeSupport.value(sent2, "premium", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent2, "details", "approver", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseApprovalService/SubmitCollectionCase"), "collection_case", "audit", "created_by"),
                ProbeSupport.value(sent2, "premium", "audit", "created_by"));
        var sent3 = channel.request("acme.collections.v1.PaymentPromiseQueryService/BrowsePaymentPromises");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "location", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseApprovalService/SubmitCollectionCase"), "collection_case", "requester", "preferred_channel"),
                ProbeSupport.value(response, "payment_promise", "participants", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumApprovalService/DelegatePremiumApproval"), "premium", "billing_address", "postal_code"),
                ProbeSupport.value(response, "payment_promise", "location", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.PaymentPromiseQueryService/BrowsePaymentPromises"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise_id"),
                ProbeSupport.value(response, "payment_promise", "payment_promise_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "title"),
                ProbeSupport.value(response, "payment_promise", "title"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new PaymentPromiseWorkflowApplicationEvents(JournalEntryNotificationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<SendJournalEntryTestNoticeResponse>();
        listener.on(new PaymentPromiseWorkflowApplication.InspectPaymentPromiseWorkflowStage0Requested(ProbeSupport.sample(SendJournalEntryTestNoticeRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
