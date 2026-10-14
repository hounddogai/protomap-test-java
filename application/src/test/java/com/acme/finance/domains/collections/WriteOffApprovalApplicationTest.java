package com.acme.finance.domains.collections;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.RetryAuditEventExportRequest;
import com.acme.audit.v1.RetryAuditEventExportResponse;
import com.acme.audit.v1.SendAuditEventTestNoticeRequest;
import com.acme.audit.v1.SendAuditEventTestNoticeResponse;
import com.acme.audit.v1.SendAuditTrailTestNoticeRequest;
import com.acme.audit.v1.SendAuditTrailTestNoticeResponse;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.banking.v1.BankAccountWorkflowServiceGrpc;
import com.acme.banking.v1.EscalateStandingOrderRequest;
import com.acme.banking.v1.EscalateStandingOrderResponse;
import com.acme.banking.v1.ListBankAccountWorkflowsRequest;
import com.acme.banking.v1.ListBankAccountWorkflowsResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.collections.v1.ApprovePaymentPromiseRequest;
import com.acme.collections.v1.ApprovePaymentPromiseResponse;
import com.acme.collections.v1.ApproveWriteOffRequest;
import com.acme.collections.v1.ApproveWriteOffResponse;
import com.acme.collections.v1.BrowseCollectionCasesRequest;
import com.acme.collections.v1.BrowseCollectionCasesResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.CollectionCaseComment;
import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.CommentWriteOffApprovalRequest;
import com.acme.collections.v1.CommentWriteOffApprovalResponse;
import com.acme.collections.v1.EscalateCollectionCaseRequest;
import com.acme.collections.v1.EscalateCollectionCaseResponse;
import com.acme.collections.v1.EscalateWriteOffRequest;
import com.acme.collections.v1.EscalateWriteOffResponse;
import com.acme.collections.v1.GetCollectionCaseApprovalStatusRequest;
import com.acme.collections.v1.GetCollectionCaseApprovalStatusResponse;
import com.acme.collections.v1.ListPendingCollectionCasesRequest;
import com.acme.collections.v1.ListPendingCollectionCasesResponse;
import com.acme.collections.v1.ListPendingWriteOffsRequest;
import com.acme.collections.v1.ListPendingWriteOffsResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseRef;
import com.acme.collections.v1.RecallWriteOffRequest;
import com.acme.collections.v1.RecallWriteOffResponse;
import com.acme.collections.v1.RejectCollectionCaseRequest;
import com.acme.collections.v1.RejectCollectionCaseResponse;
import com.acme.collections.v1.RejectWriteOffRequest;
import com.acme.collections.v1.RejectWriteOffResponse;
import com.acme.collections.v1.StartCollectionCaseWorkflowRequest;
import com.acme.collections.v1.StartCollectionCaseWorkflowResponse;
import com.acme.collections.v1.SubmitCollectionCaseRequest;
import com.acme.collections.v1.SubmitCollectionCaseResponse;
import com.acme.collections.v1.SubmitWriteOffRequest;
import com.acme.collections.v1.SubmitWriteOffResponse;
import com.acme.collections.v1.ValidateWriteOffRequest;
import com.acme.collections.v1.ValidateWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffCardSettlement;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.RecallClaimRequest;
import com.acme.insurance.v1.RecallClaimResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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

class WriteOffApprovalApplicationTest {
    @Test
    void recallWriteOffPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new CollectionsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollectionCase(String tenant, CollectionCase value) {}
            public void savePaymentPromise(String tenant, PaymentPromise value) {}
            public void saveWriteOff(String tenant, WriteOff value) {}
        };
        var listener = new WriteOffApprovalApplicationEvents(StandingOrderApprovalServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof WriteOffApprovalApplication.ListPendingWriteOffsStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new WriteOffApprovalApplication(
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                BankAccountWorkflowServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseApprovalServiceGrpc.newBlockingStub(channel),
                CollectionCaseApprovalServiceGrpc.newFutureStub(channel),
                CollectionCaseQueryServiceGrpc.newBlockingStub(channel),
                CollectionCaseWorkflowServiceGrpc.newBlockingStub(channel),
                PaymentPromiseApprovalServiceGrpc.newBlockingStub(channel),
                WriteOffValidationServiceGrpc.newBlockingStub(channel),
                events,
                ClaimApprovalServiceGrpc.newBlockingStub(channel),
                new WriteOffApprovalRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(RecallWriteOffRequest.getDefaultInstance());
        var response = application.recallWriteOff(request);
        var sent0 = channel.request("acme.collections.v1.WriteOffValidationService/ValidateWriteOff");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "audit", "created_by"),
                ProbeSupport.value(sent0, "write_off", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "card", "token"),
                ProbeSupport.value(sent0, "write_off", "card", "token"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "billing_address", "postal_code"),
                ProbeSupport.value(sent0, "write_off", "billing_address", "postal_code"));
        var sent1 = channel.request("acme.collections.v1.CollectionCaseApprovalService/ListPendingCollectionCases");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/ValidateWriteOff"), "write_off", "audit", "created_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/ValidateWriteOff"), "write_off", "metadata"),
                ProbeSupport.value(sent1, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        var sent2 = channel.request("acme.audit.v1.AuditTrailNotificationService/SendAuditTrailTestNotice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent2, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent2, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent2, "reason"));
        var sent3 = channel.request("acme.collections.v1.PaymentPromiseApprovalService/ApprovePaymentPromise");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/ValidateWriteOff"), "write_off", "audit", "updated_at"),
                ProbeSupport.value(sent3, "payment_promise", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/ValidateWriteOff"), "write_off", "audit", "updated_at"),
                ProbeSupport.value(response, "write_off", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffValidationService/ValidateWriteOff"), "write_off", "total"),
                ProbeSupport.value(response, "write_off", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseApprovalService/ListPendingCollectionCases"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseApprovalService/ListPendingCollectionCases"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailNotificationService/SendAuditTrailTestNotice"), "audit_trail", "billing_address", "recipient"),
                ProbeSupport.value(response, "write_off", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.PaymentPromiseApprovalService/ApprovePaymentPromise"), "payment_promise", "location", "region"),
                ProbeSupport.value(response, "write_off", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.PaymentPromiseApprovalService/ApprovePaymentPromise"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off_id"),
                ProbeSupport.value(response, "write_off", "write_off_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "write_off", "issued_at"),
                ProbeSupport.value(response, "write_off", "issued_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new WriteOffApprovalApplicationEvents(StandingOrderApprovalServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<EscalateStandingOrderResponse>();
        listener.on(new WriteOffApprovalApplication.ListPendingWriteOffsStage1Requested(ProbeSupport.sample(EscalateStandingOrderRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
