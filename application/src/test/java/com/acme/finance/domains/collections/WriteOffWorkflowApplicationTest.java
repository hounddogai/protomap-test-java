package com.acme.finance.domains.collections;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RevertAuditEventRequest;
import com.acme.audit.v1.RevertAuditEventResponse;
import com.acme.audit.v1.SearchAuditEventsRequest;
import com.acme.audit.v1.SearchAuditEventsResponse;
import com.acme.audit.v1.StreamRetentionPolicyExportRequest;
import com.acme.audit.v1.StreamRetentionPolicyExportResponse;
import com.acme.collections.v1.AdvanceWriteOffWorkflowRequest;
import com.acme.collections.v1.AdvanceWriteOffWorkflowResponse;
import com.acme.collections.v1.CancelCollectionCaseWorkflowRequest;
import com.acme.collections.v1.CancelCollectionCaseWorkflowResponse;
import com.acme.collections.v1.CancelPaymentPromiseWorkflowRequest;
import com.acme.collections.v1.CancelPaymentPromiseWorkflowResponse;
import com.acme.collections.v1.CancelWriteOffWorkflowRequest;
import com.acme.collections.v1.CancelWriteOffWorkflowResponse;
import com.acme.collections.v1.CheckWriteOffEligibilityRequest;
import com.acme.collections.v1.CheckWriteOffEligibilityResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.CommentPaymentPromiseApprovalRequest;
import com.acme.collections.v1.CommentPaymentPromiseApprovalResponse;
import com.acme.collections.v1.InspectWriteOffWorkflowRequest;
import com.acme.collections.v1.InspectWriteOffWorkflowResponse;
import com.acme.collections.v1.ListWriteOffWorkflowsRequest;
import com.acme.collections.v1.ListWriteOffWorkflowsResponse;
import com.acme.collections.v1.PauseWriteOffWorkflowRequest;
import com.acme.collections.v1.PauseWriteOffWorkflowResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.PaymentPromiseRef;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.collections.v1.PreviewWriteOffRequest;
import com.acme.collections.v1.PreviewWriteOffResponse;
import com.acme.collections.v1.RejectPaymentPromiseRequest;
import com.acme.collections.v1.RejectPaymentPromiseResponse;
import com.acme.collections.v1.ResumeWriteOffWorkflowRequest;
import com.acme.collections.v1.ResumeWriteOffWorkflowResponse;
import com.acme.collections.v1.RetryWriteOffWorkflowRequest;
import com.acme.collections.v1.RetryWriteOffWorkflowResponse;
import com.acme.collections.v1.ScoreWriteOffRequest;
import com.acme.collections.v1.ScoreWriteOffResponse;
import com.acme.collections.v1.SignalWriteOffWorkflowRequest;
import com.acme.collections.v1.SignalWriteOffWorkflowResponse;
import com.acme.collections.v1.StartWriteOffWorkflowRequest;
import com.acme.collections.v1.StartWriteOffWorkflowResponse;
import com.acme.collections.v1.UpdateCollectionCaseRequest;
import com.acme.collections.v1.UpdateCollectionCaseResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffCardSettlement;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApproveClaimRequest;
import com.acme.insurance.v1.ApproveClaimResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.RecallPolicyRequest;
import com.acme.insurance.v1.RecallPolicyResponse;
import com.acme.insurance.v1.ShareCoverageReportRequest;
import com.acme.insurance.v1.ShareCoverageReportResponse;
import com.acme.insurance.v1.StreamUnderwritingMetricsRequest;
import com.acme.insurance.v1.StreamUnderwritingMetricsResponse;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class WriteOffWorkflowApplicationTest {
    @Test
    void resumeWriteOffWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new CollectionsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollectionCase(String tenant, CollectionCase value) {}
            public void savePaymentPromise(String tenant, PaymentPromise value) {}
            public void saveWriteOff(String tenant, WriteOff value) {}
        };
        var listener = new WriteOffWorkflowApplicationEvents(PaymentPromiseWorkflowServiceGrpc.newStub(channel), CoverageReportingServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof WriteOffWorkflowApplication.ResumeWriteOffWorkflowStage3Requested command) {
                listener.on(command);
            }
            else if (event instanceof WriteOffWorkflowApplication.InspectWriteOffWorkflowStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new WriteOffWorkflowApplication(
                AuditEventHistoryServiceGrpc.newBlockingStub(channel),
                AuditEventServiceGrpc.newBlockingStub(channel),
                RetentionPolicyExportServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseWorkflowServiceGrpc.newBlockingStub(channel),
                PaymentPromiseApprovalServiceGrpc.newBlockingStub(channel),
                WriteOffValidationServiceGrpc.newBlockingStub(channel),
                events,
                PolicyApprovalServiceGrpc.newBlockingStub(channel),
                UnderwritingReportingServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ResumeWriteOffWorkflowRequest.getDefaultInstance());
        var response = application.resumeWriteOffWorkflow(request);
        var sent0 = channel.request("acme.collections.v1.WriteOffValidationService/CheckWriteOffEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "audit", "updated_at"),
                ProbeSupport.value(sent0, "write_off", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "audit", "updated_by"),
                ProbeSupport.value(sent0, "write_off", "audit", "updated_by"));
        var sent1 = channel.request("acme.collections.v1.PaymentPromiseApprovalService/CommentPaymentPromiseApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "name"),
                ProbeSupport.value(sent1, "payment_promise", "participants", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent1, "payment_promise", "participants", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "payment_promise", "organizer", "display_name"));
        var sent2 = channel.request("acme.collections.v1.WriteOffValidationService/PreviewWriteOff");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.PaymentPromiseApprovalService/CommentPaymentPromiseApproval"), "payment_promise", "location", "region"),
                ProbeSupport.value(sent2, "write_off", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/CheckWriteOffEligibility"), "write_off", "billing_address", "recipient"),
                ProbeSupport.value(sent2, "write_off", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "billing_address", "locality"),
                ProbeSupport.value(sent2, "write_off", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/CheckWriteOffEligibility"), "write_off", "card", "network"),
                ProbeSupport.value(sent2, "write_off", "card", "network"));
        var sent3 = channel.request("acme.insurance.v1.CoverageReportingService/ShareCoverageReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.PaymentPromiseApprovalService/CommentPaymentPromiseApproval"), "payment_promise", "organizer", "email"),
                ProbeSupport.value(sent3, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent3, "details", "recipient", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "name"),
                ProbeSupport.value(sent3, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/CheckWriteOffEligibility"), "write_off", "audit", "updated_by"),
                ProbeSupport.value(response, "write_off", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/CheckWriteOffEligibility"), "write_off", "billing_address"),
                ProbeSupport.value(response, "write_off", "billing_address"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.PaymentPromiseApprovalService/CommentPaymentPromiseApproval"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off_id"),
                ProbeSupport.value(response, "write_off", "write_off_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "metadata"),
                ProbeSupport.value(response, "write_off", "metadata"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new WriteOffWorkflowApplicationEvents(PaymentPromiseWorkflowServiceGrpc.newStub(channel), CoverageReportingServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ShareCoverageReportResponse>();
        listener.on(new WriteOffWorkflowApplication.ResumeWriteOffWorkflowStage3Requested(ProbeSupport.sample(ShareCoverageReportRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<CancelPaymentPromiseWorkflowResponse>();
        listener.on(new WriteOffWorkflowApplication.InspectWriteOffWorkflowStage1Requested(ProbeSupport.sample(CancelPaymentPromiseWorkflowRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
