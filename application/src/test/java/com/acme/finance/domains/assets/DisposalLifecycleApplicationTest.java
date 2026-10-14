package com.acme.finance.domains.assets;

import com.acme.assets.v1.ActivateDisposalRequest;
import com.acme.assets.v1.ActivateDisposalResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.CountDisposalTotalsRequest;
import com.acme.assets.v1.CountDisposalTotalsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.DisposalSla;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.ExpireDisposalRequest;
import com.acme.assets.v1.ExpireDisposalResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.GenerateDisposalReportRequest;
import com.acme.assets.v1.GenerateDisposalReportResponse;
import com.acme.assets.v1.GetDisposalLifecycleRequest;
import com.acme.assets.v1.GetDisposalLifecycleResponse;
import com.acme.assets.v1.GetDisposalSyncStateRequest;
import com.acme.assets.v1.GetDisposalSyncStateResponse;
import com.acme.assets.v1.ListDisposalConflictsRequest;
import com.acme.assets.v1.ListDisposalConflictsResponse;
import com.acme.assets.v1.ListDisposalReportsRequest;
import com.acme.assets.v1.ListDisposalReportsResponse;
import com.acme.assets.v1.PushDisposalRequest;
import com.acme.assets.v1.PushDisposalResponse;
import com.acme.assets.v1.ReinstateDisposalRequest;
import com.acme.assets.v1.ReinstateDisposalResponse;
import com.acme.assets.v1.RenewDisposalRequest;
import com.acme.assets.v1.RenewDisposalResponse;
import com.acme.assets.v1.ScheduleDisposalExpiryRequest;
import com.acme.assets.v1.ScheduleDisposalExpiryResponse;
import com.acme.assets.v1.ScoreDepreciationRequest;
import com.acme.assets.v1.ScoreDepreciationResponse;
import com.acme.assets.v1.ScoreDisposalRequest;
import com.acme.assets.v1.ScoreDisposalResponse;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.assets.v1.SuspendDisposalRequest;
import com.acme.assets.v1.SuspendDisposalResponse;
import com.acme.assets.v1.TerminateDisposalRequest;
import com.acme.assets.v1.TerminateDisposalResponse;
import com.acme.assets.v1.ValidateFixedAssetRequest;
import com.acme.assets.v1.ValidateFixedAssetResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.PreviewAccessPolicyRequest;
import com.acme.authorization.v1.PreviewAccessPolicyResponse;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.RecallWriteOffRequest;
import com.acme.collections.v1.RecallWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.collections.v1.WriteOffCardSettlement;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CheckPostingBatchEligibilityRequest;
import com.acme.ledger.v1.CheckPostingBatchEligibilityResponse;
import com.acme.ledger.v1.ExportJournalEntriesRequest;
import com.acme.ledger.v1.ExportJournalEntriesResponse;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersRequest;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersResponse;
import com.acme.ledger.v1.NotifyPostingBatchWatchersRequest;
import com.acme.ledger.v1.NotifyPostingBatchWatchersResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.WatchLedgerAccountsRequest;
import com.acme.ledger.v1.WatchLedgerAccountsResponse;
import com.acme.scheduling.v1.CountShiftsRequest;
import com.acme.scheduling.v1.CountShiftsResponse;
import com.acme.scheduling.v1.ShiftQueryServiceGrpc;
import com.acme.shows.v1.CreateShowRequest;
import com.acme.shows.v1.CreateShowResponse;
import com.acme.shows.v1.Show;
import com.acme.shows.v1.ShowParticipant;
import com.acme.shows.v1.ShowServiceGrpc;
import com.acme.telematics.v1.CompareTelemetryReadingOptionsRequest;
import com.acme.telematics.v1.CompareTelemetryReadingOptionsResponse;
import com.acme.telematics.v1.TelemetryReading;
import com.acme.telematics.v1.TelemetryReadingCalculationServiceGrpc;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class DisposalLifecycleApplicationTest {
    @Test
    void terminateDisposalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var listener = new DisposalLifecycleApplicationEvents(DisposalSyncServiceGrpc.newStub(channel), PostingBatchValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof DisposalLifecycleApplication.ReinstateDisposalStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof DisposalLifecycleApplication.TerminateDisposalStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new DisposalLifecycleApplication(
                DepreciationValidationServiceGrpc.newFutureStub(channel),
                DisposalReportingServiceGrpc.newBlockingStub(channel),
                DisposalSyncServiceGrpc.newStub(channel),
                DisposalSyncServiceGrpc.newBlockingStub(channel),
                DisposalValidationServiceGrpc.newBlockingStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                BudgetServiceGrpc.newStub(channel),
                WriteOffApprovalServiceGrpc.newBlockingStub(channel),
                events,
                JournalEntryQueryServiceGrpc.newBlockingStub(channel),
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                LedgerAccountServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                new DisposalLifecycleRequestMapperImpl(),
                repository,
                Runnable::run,
                ShiftQueryServiceGrpc.newBlockingStub(channel),
                ShowServiceGrpc.newBlockingStub(channel),
                TelemetryReadingCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(TerminateDisposalRequest.getDefaultInstance());
        var response = application.terminateDisposal(request);
        var sent0 = channel.request("acme.assets.v1.DisposalSyncService/ListDisposalConflicts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "user_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.ledger.v1.LedgerAccountService/WatchLedgerAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "phone"),
                ProbeSupport.value(sent1, "subscriber", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent1, "subscriber", "name"));
        var sent2 = channel.request("acme.assets.v1.DepreciationValidationService/ScoreDepreciation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent2, "depreciation", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "user_id"),
                ProbeSupport.value(sent2, "depreciation", "organizer", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent2, "depreciation", "participants", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent2, "depreciation", "participants", "contact", "name"));
        var sent3 = channel.request("acme.assets.v1.DisposalSyncService/StreamDisposalChanges");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ScoreDepreciation"), "depreciation", "audit", "updated_by"),
                ProbeSupport.value(sent3, "change", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ScoreDepreciation"), "depreciation", "organizer", "email"),
                ProbeSupport.value(sent3, "change", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "email"),
                ProbeSupport.value(sent3, "change", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "display_name"),
                ProbeSupport.value(sent3, "change", "customer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalSyncService/ListDisposalConflicts"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ScoreDepreciation"), "depreciation", "organizer", "user_id"),
                ProbeSupport.value(response, "disposal", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal_id"),
                ProbeSupport.value(response, "disposal", "disposal_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new DisposalLifecycleApplicationEvents(DisposalSyncServiceGrpc.newStub(channel), PostingBatchValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CheckPostingBatchEligibilityResponse>();
        listener.on(new DisposalLifecycleApplication.ReinstateDisposalStage0Requested(ProbeSupport.sample(CheckPostingBatchEligibilityRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<ListDisposalConflictsResponse>();
        listener.on(new DisposalLifecycleApplication.TerminateDisposalStage0Requested(ProbeSupport.sample(ListDisposalConflictsRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
