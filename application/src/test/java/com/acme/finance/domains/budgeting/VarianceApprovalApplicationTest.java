package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.AdvanceDepreciationWorkflowRequest;
import com.acme.assets.v1.AdvanceDepreciationWorkflowResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationWorkflowServiceGrpc;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.ListAuditEventsRequest;
import com.acme.audit.v1.ListAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.ScoreAuditTrailRequest;
import com.acme.audit.v1.ScoreAuditTrailResponse;
import com.acme.audit.v1.SearchAuditEventsRequest;
import com.acme.audit.v1.SearchAuditEventsResponse;
import com.acme.budgeting.v1.ApproveVarianceRequest;
import com.acme.budgeting.v1.ApproveVarianceResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetWorkflowServiceGrpc;
import com.acme.budgeting.v1.CommentVarianceApprovalRequest;
import com.acme.budgeting.v1.CommentVarianceApprovalResponse;
import com.acme.budgeting.v1.DelegateVarianceApprovalRequest;
import com.acme.budgeting.v1.DelegateVarianceApprovalResponse;
import com.acme.budgeting.v1.EscalateVarianceRequest;
import com.acme.budgeting.v1.EscalateVarianceResponse;
import com.acme.budgeting.v1.ExpireVarianceRequest;
import com.acme.budgeting.v1.ExpireVarianceResponse;
import com.acme.budgeting.v1.ExplainBudgetCalculationRequest;
import com.acme.budgeting.v1.ExplainBudgetCalculationResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.GenerateForecastReportRequest;
import com.acme.budgeting.v1.GenerateForecastReportResponse;
import com.acme.budgeting.v1.GetVarianceApprovalStatusRequest;
import com.acme.budgeting.v1.GetVarianceApprovalStatusResponse;
import com.acme.budgeting.v1.ListPendingVariancesRequest;
import com.acme.budgeting.v1.ListPendingVariancesResponse;
import com.acme.budgeting.v1.PreviewVarianceRequest;
import com.acme.budgeting.v1.PreviewVarianceResponse;
import com.acme.budgeting.v1.RecallVarianceRequest;
import com.acme.budgeting.v1.RecallVarianceResponse;
import com.acme.budgeting.v1.RenewVarianceRequest;
import com.acme.budgeting.v1.RenewVarianceResponse;
import com.acme.budgeting.v1.RetryBudgetWorkflowRequest;
import com.acme.budgeting.v1.RetryBudgetWorkflowResponse;
import com.acme.budgeting.v1.ShareForecastReportRequest;
import com.acme.budgeting.v1.ShareForecastReportResponse;
import com.acme.budgeting.v1.SubmitVarianceRequest;
import com.acme.budgeting.v1.SubmitVarianceResponse;
import com.acme.budgeting.v1.SyncVariancesRequest;
import com.acme.budgeting.v1.SyncVariancesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceChannelOrigin;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.budgeting.v1.VarianceLifecycleServiceGrpc;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.careplans.v1.CarePlanQueryServiceGrpc;
import com.acme.careplans.v1.ResolveCarePlansRequest;
import com.acme.careplans.v1.ResolveCarePlansResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.corporateevents.v1.CorporateEvent;
import com.acme.corporateevents.v1.CorporateEventCalculationServiceGrpc;
import com.acme.corporateevents.v1.CorporateEventParticipant;
import com.acme.corporateevents.v1.ExplainCorporateEventCalculationRequest;
import com.acme.corporateevents.v1.ExplainCorporateEventCalculationResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.giftcards.v1.GiftCardQueryServiceGrpc;
import com.acme.giftcards.v1.ResolveGiftCardsRequest;
import com.acme.giftcards.v1.ResolveGiftCardsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.metrics.v1.CloneMetricSeriesRequest;
import com.acme.metrics.v1.CloneMetricSeriesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.WatchSuppliersRequest;
import com.acme.suppliers.v1.WatchSuppliersResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class VarianceApprovalApplicationTest {
    @Test
    void commentVarianceApprovalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var application = new VarianceApprovalApplication(
                DepreciationWorkflowServiceGrpc.newBlockingStub(channel),
                AuditEventServiceGrpc.newBlockingStub(channel),
                AuditTrailValidationServiceGrpc.newBlockingStub(channel),
                BudgetCalculationServiceGrpc.newBlockingStub(channel),
                BudgetWorkflowServiceGrpc.newBlockingStub(channel),
                ForecastReportingServiceGrpc.newBlockingStub(channel),
                VarianceLifecycleServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newBlockingStub(channel),
                VarianceValidationServiceGrpc.newBlockingStub(channel),
                CarePlanQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CorporateEventCalculationServiceGrpc.newBlockingStub(channel),
                GiftCardQueryServiceGrpc.newBlockingStub(channel),
                new VarianceApprovalRequestMapperImpl(),
                repository,
                Runnable::run,
                SupplierServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CommentVarianceApprovalRequest.getDefaultInstance());
        var response = application.commentVarianceApproval(request);
        var sent0 = channel.request("acme.careplans.v1.CarePlanQueryService/ResolveCarePlans");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        var sent1 = channel.request("acme.budgeting.v1.VarianceLifecycleService/RenewVariance");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance_id"),
                ProbeSupport.value(sent1, "variance_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "requester", "preferred_channel"),
                ProbeSupport.value(sent1, "details", "notify_contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent1, "details", "new_owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "requester", "email"),
                ProbeSupport.value(sent1, "details", "notify_contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "display_name"),
                ProbeSupport.value(sent1, "details", "new_owner", "display_name"));
        var sent2 = channel.request("acme.audit.v1.AuditEventService/ListAuditEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceLifecycleService/RenewVariance"), "variance", "sla", "due_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceLifecycleService/RenewVariance"), "variance", "assignee", "user_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceLifecycleService/RenewVariance"), "result", "details"),
                ProbeSupport.value(sent2, "filter", "labels"));
        var sent3 = channel.request("acme.budgeting.v1.VarianceSyncService/SyncVariances");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceLifecycleService/RenewVariance"), "variance", "channel", "address"),
                ProbeSupport.value(sent3, "variance", "channel", "address"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.careplans.v1.CarePlanQueryService/ResolveCarePlans"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceLifecycleService/RenewVariance"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventService/ListAuditEvents"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceSyncService/SyncVariances"), "variance", "assignee", "email"),
                ProbeSupport.value(response, "variance", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceSyncService/SyncVariances"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance_id"),
                ProbeSupport.value(response, "variance", "variance_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "requester"),
                ProbeSupport.value(response, "variance", "requester"));
    }
}
