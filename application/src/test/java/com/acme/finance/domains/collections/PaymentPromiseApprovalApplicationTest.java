package com.acme.finance.domains.collections;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.ListPendingMandatesRequest;
import com.acme.banking.v1.ListPendingMandatesResponse;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.collections.v1.ApprovePaymentPromiseRequest;
import com.acme.collections.v1.ApprovePaymentPromiseResponse;
import com.acme.collections.v1.BrowsePaymentPromisesRequest;
import com.acme.collections.v1.BrowsePaymentPromisesResponse;
import com.acme.collections.v1.CloneCollectionCaseRequest;
import com.acme.collections.v1.CloneCollectionCaseResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.CommentPaymentPromiseApprovalRequest;
import com.acme.collections.v1.CommentPaymentPromiseApprovalResponse;
import com.acme.collections.v1.CountPaymentPromisesRequest;
import com.acme.collections.v1.CountPaymentPromisesResponse;
import com.acme.collections.v1.EscalatePaymentPromiseRequest;
import com.acme.collections.v1.EscalatePaymentPromiseResponse;
import com.acme.collections.v1.FindPaymentPromisesRequest;
import com.acme.collections.v1.FindPaymentPromisesResponse;
import com.acme.collections.v1.GetPaymentPromiseApprovalStatusRequest;
import com.acme.collections.v1.GetPaymentPromiseApprovalStatusResponse;
import com.acme.collections.v1.ListPendingPaymentPromisesRequest;
import com.acme.collections.v1.ListPendingPaymentPromisesResponse;
import com.acme.collections.v1.NormalizeWriteOffRequest;
import com.acme.collections.v1.NormalizeWriteOffResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.PaymentPromiseQueryServiceGrpc;
import com.acme.collections.v1.PaymentPromiseRef;
import com.acme.collections.v1.RecallPaymentPromiseRequest;
import com.acme.collections.v1.RecallPaymentPromiseResponse;
import com.acme.collections.v1.RejectPaymentPromiseRequest;
import com.acme.collections.v1.RejectPaymentPromiseResponse;
import com.acme.collections.v1.ScoreWriteOffRequest;
import com.acme.collections.v1.ScoreWriteOffResponse;
import com.acme.collections.v1.SubmitCollectionCaseRequest;
import com.acme.collections.v1.SubmitCollectionCaseResponse;
import com.acme.collections.v1.SubmitPaymentPromiseRequest;
import com.acme.collections.v1.SubmitPaymentPromiseResponse;
import com.acme.collections.v1.SuggestPaymentPromisesRequest;
import com.acme.collections.v1.SuggestPaymentPromisesResponse;
import com.acme.collections.v1.SummarizeCollectionCasesRequest;
import com.acme.collections.v1.SummarizeCollectionCasesResponse;
import com.acme.collections.v1.WatchCollectionCasesRequest;
import com.acme.collections.v1.WatchCollectionCasesResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.AdvancePolicyWorkflowRequest;
import com.acme.insurance.v1.AdvancePolicyWorkflowResponse;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.StreamUnderwritingMetricsRequest;
import com.acme.insurance.v1.StreamUnderwritingMetricsResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class PaymentPromiseApprovalApplicationTest {
    @Test
    void approvePaymentPromisePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new CollectionsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollectionCase(String tenant, CollectionCase value) {}
            public void savePaymentPromise(String tenant, PaymentPromise value) {}
            public void saveWriteOff(String tenant, WriteOff value) {}
        };
        var listener = new PaymentPromiseApprovalApplicationEvents(MandateApprovalServiceGrpc.newStub(channel), WriteOffValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof PaymentPromiseApprovalApplication.ApprovePaymentPromiseStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof PaymentPromiseApprovalApplication.RejectPaymentPromiseStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new PaymentPromiseApprovalApplication(
                FixedAssetServiceGrpc.newStub(channel),
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseApprovalServiceGrpc.newBlockingStub(channel),
                CollectionCaseServiceGrpc.newBlockingStub(channel),
                PaymentPromiseQueryServiceGrpc.newBlockingStub(channel),
                PaymentPromiseQueryServiceGrpc.newFutureStub(channel),
                WriteOffValidationServiceGrpc.newBlockingStub(channel),
                events,
                PolicyWorkflowServiceGrpc.newBlockingStub(channel),
                UnderwritingReportingServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(ApprovePaymentPromiseRequest.getDefaultInstance());
        var response = application.approvePaymentPromise(request);
        var sent0 = channel.request("acme.banking.v1.MandateApprovalService/ListPendingMandates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "location", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "metadata"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.collections.v1.CollectionCaseService/CloneCollectionCase");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "display_name"),
                ProbeSupport.value(sent1, "collection_case", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "organizer", "email"),
                ProbeSupport.value(sent1, "collection_case", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "organizer", "user_id"),
                ProbeSupport.value(sent1, "collection_case", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "audit", "updated_at"),
                ProbeSupport.value(sent1, "collection_case", "audit", "updated_at"));
        var sent2 = channel.request("acme.collections.v1.CollectionCaseService/WatchCollectionCases");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "organizer", "email"),
                ProbeSupport.value(sent2, "subscriber", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "location", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseService/CloneCollectionCase"), "collection_case", "requester", "name"),
                ProbeSupport.value(sent2, "subscriber", "name"));
        var sent3 = channel.request("acme.insurance.v1.PolicyWorkflowService/AdvancePolicyWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseService/CloneCollectionCase"), "collection_case", "assignee", "display_name"),
                ProbeSupport.value(sent3, "policy", "holder", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "organizer", "email"),
                ProbeSupport.value(sent3, "policy", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "audit", "created_by"),
                ProbeSupport.value(sent3, "policy", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseService/CloneCollectionCase"), "collection_case", "requester", "preferred_channel"),
                ProbeSupport.value(sent3, "details", "notify", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateApprovalService/ListPendingMandates"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateApprovalService/ListPendingMandates"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseService/CloneCollectionCase"), "collection_case", "requester", "email"),
                ProbeSupport.value(response, "payment_promise", "organizer", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseService/CloneCollectionCase"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyWorkflowService/AdvancePolicyWorkflow"), "policy", "contact", "phone"),
                ProbeSupport.value(response, "payment_promise", "participants", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise_id"),
                ProbeSupport.value(response, "payment_promise", "payment_promise_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "payment_promise", "metadata"),
                ProbeSupport.value(response, "payment_promise", "metadata"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new PaymentPromiseApprovalApplicationEvents(MandateApprovalServiceGrpc.newStub(channel), WriteOffValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ListPendingMandatesResponse>();
        listener.on(new PaymentPromiseApprovalApplication.ApprovePaymentPromiseStage0Requested(ProbeSupport.sample(ListPendingMandatesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<NormalizeWriteOffResponse>();
        listener.on(new PaymentPromiseApprovalApplication.RejectPaymentPromiseStage0Requested(ProbeSupport.sample(NormalizeWriteOffRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
