package com.acme.finance.domains.assets;

import com.acme.assets.v1.ApproveDisposalRequest;
import com.acme.assets.v1.ApproveDisposalResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.CommentDisposalApprovalRequest;
import com.acme.assets.v1.CommentDisposalApprovalResponse;
import com.acme.assets.v1.DelegateDisposalApprovalRequest;
import com.acme.assets.v1.DelegateDisposalApprovalResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationApprovalServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalComment;
import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.DisposalSla;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.GetDisposalApprovalStatusRequest;
import com.acme.assets.v1.GetDisposalApprovalStatusResponse;
import com.acme.assets.v1.ListDisposalReportsRequest;
import com.acme.assets.v1.ListDisposalReportsResponse;
import com.acme.assets.v1.ListPendingDisposalsRequest;
import com.acme.assets.v1.ListPendingDisposalsResponse;
import com.acme.assets.v1.RecallDisposalRequest;
import com.acme.assets.v1.RecallDisposalResponse;
import com.acme.assets.v1.RejectDepreciationRequest;
import com.acme.assets.v1.RejectDepreciationResponse;
import com.acme.assets.v1.RejectDisposalRequest;
import com.acme.assets.v1.RejectDisposalResponse;
import com.acme.assets.v1.ScheduleDisposalReportRequest;
import com.acme.assets.v1.ScheduleDisposalReportResponse;
import com.acme.assets.v1.SubmitDisposalRequest;
import com.acme.assets.v1.SubmitDisposalResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.LintAuditEventsRequest;
import com.acme.audit.v1.LintAuditEventsResponse;
import com.acme.audit.v1.ReconcileAuditTrailsRequest;
import com.acme.audit.v1.ReconcileAuditTrailsResponse;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.PaymentPromiseRef;
import com.acme.collections.v1.RetryCollectionCaseWorkflowRequest;
import com.acme.collections.v1.RetryCollectionCaseWorkflowResponse;
import com.acme.collections.v1.SubmitWriteOffRequest;
import com.acme.collections.v1.SubmitWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.helpdesk.v1.CloneTicketRequest;
import com.acme.helpdesk.v1.CloneTicketResponse;
import com.acme.helpdesk.v1.Ticket;
import com.acme.helpdesk.v1.TicketServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.DelegateCoverageApprovalRequest;
import com.acme.insurance.v1.DelegateCoverageApprovalResponse;
import com.acme.insurance.v1.ImportUnderwritingsRequest;
import com.acme.insurance.v1.ImportUnderwritingsResponse;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.StreamRepaymentMetricsRequest;
import com.acme.lending.v1.StreamRepaymentMetricsResponse;
import com.google.protobuf.Any;
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

class DisposalApprovalApplicationTest {
    @Test
    void commentDisposalApprovalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new DisposalApprovalApplication(
                DepreciationApprovalServiceGrpc.newBlockingStub(channel),
                DisposalReportingServiceGrpc.newBlockingStub(channel),
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                AuditTrailSyncServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                CollectionCaseWorkflowServiceGrpc.newBlockingStub(channel),
                WriteOffApprovalServiceGrpc.newBlockingStub(channel),
                TicketServiceGrpc.newBlockingStub(channel),
                CoverageApprovalServiceGrpc.newBlockingStub(channel),
                UnderwritingImportServiceGrpc.newStub(channel),
                RepaymentReportingServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(CommentDisposalApprovalRequest.getDefaultInstance());
        var response = application.commentDisposalApproval(request);
        var sent0 = channel.request("acme.helpdesk.v1.TicketService/CloneTicket");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal", "audit", "created_by"),
                ProbeSupport.value(sent0, "ticket", "audit", "created_by"));
        var sent1 = channel.request("acme.audit.v1.AuditTrailSyncService/ResolveAuditTrailConflict");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.helpdesk.v1.TicketService/CloneTicket"), "ticket", "audit", "updated_at"),
                ProbeSupport.value(sent1, "audit_trail", "audit", "updated_at"));
        var sent2 = channel.request("acme.lending.v1.RepaymentReportingService/StreamRepaymentMetrics");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal", "requester", "email"),
                ProbeSupport.value(sent2, "subscriber", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.helpdesk.v1.TicketService/CloneTicket"), "ticket", "requester", "name"),
                ProbeSupport.value(sent2, "subscriber", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/ResolveAuditTrailConflict"), "audit_trail", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.helpdesk.v1.TicketService/CloneTicket"), "ticket", "requester", "preferred_channel"),
                ProbeSupport.value(sent2, "subscriber", "preferred_channel"));
        var sent3 = channel.request("acme.insurance.v1.CoverageApprovalService/DelegateCoverageApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.helpdesk.v1.TicketService/CloneTicket"), "ticket", "audit", "created_by"),
                ProbeSupport.value(sent3, "coverage", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.helpdesk.v1.TicketService/CloneTicket"), "ticket", "sla", "breached"),
                ProbeSupport.value(response, "disposal", "sla", "breached"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/ResolveAuditTrailConflict"), "audit_trail", "audit", "updated_by"),
                ProbeSupport.value(response, "disposal", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageApprovalService/DelegateCoverageApproval"), "coverage", "owner", "user_id"),
                ProbeSupport.value(response, "disposal", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageApprovalService/DelegateCoverageApproval"), "coverage", "audit", "created_at"),
                ProbeSupport.value(response, "disposal", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal_id"),
                ProbeSupport.value(response, "disposal", "disposal_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal", "custom_fields"),
                ProbeSupport.value(response, "disposal", "custom_fields"));
    }
}
