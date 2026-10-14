package com.acme.finance.domains.assets;

import com.acme.assets.v1.ActivateDisposalRequest;
import com.acme.assets.v1.ActivateDisposalResponse;
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
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class DisposalLifecycleRequestMapperImpl implements DisposalLifecycleRequestMapper {
    @Override
    public NotifyLedgerAccountWatchersRequest renewDisposalStage3(RenewDisposalRequest request) {
        return NotifyLedgerAccountWatchersRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NotifyLedgerAccountWatchersRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
    }

    @Override
    public ListDisposalReportsRequest expireDisposalStage1(ExpireDisposalRequest request, PushDisposalResponse reply0) {
        return ListDisposalReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDisposalReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(reply0.getDisposal().getCustomFieldsMap())
                        .setOwnerId(reply0.getOutcome().getReferenceId())
                        .build())
                .build();
    }
}
