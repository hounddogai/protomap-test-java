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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class VarianceApprovalRequestMapperImpl implements VarianceApprovalRequestMapper {
    @Override
    public ExplainBudgetCalculationRequest recallVarianceStage0(RecallVarianceRequest request) {
        return ExplainBudgetCalculationRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getVariance().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
    }
}
