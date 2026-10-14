package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.FixedAssetWorkflowServiceGrpc;
import com.acme.assets.v1.ReconcileAssetTransfersRequest;
import com.acme.assets.v1.ReconcileAssetTransfersResponse;
import com.acme.assets.v1.SignalFixedAssetWorkflowRequest;
import com.acme.assets.v1.SignalFixedAssetWorkflowResponse;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.budgeting.v1.ActivateBudgetRequest;
import com.acme.budgeting.v1.ActivateBudgetResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.ExpireBudgetRequest;
import com.acme.budgeting.v1.ExpireBudgetResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.GetBudgetLifecycleRequest;
import com.acme.budgeting.v1.GetBudgetLifecycleResponse;
import com.acme.budgeting.v1.GetBudgetRatesRequest;
import com.acme.budgeting.v1.GetBudgetRatesResponse;
import com.acme.budgeting.v1.NormalizeBudgetLineRequest;
import com.acme.budgeting.v1.NormalizeBudgetLineResponse;
import com.acme.budgeting.v1.PullForecastsRequest;
import com.acme.budgeting.v1.PullForecastsResponse;
import com.acme.budgeting.v1.ReinstateBudgetRequest;
import com.acme.budgeting.v1.ReinstateBudgetResponse;
import com.acme.budgeting.v1.RenewBudgetRequest;
import com.acme.budgeting.v1.RenewBudgetResponse;
import com.acme.budgeting.v1.ScheduleBudgetExpiryRequest;
import com.acme.budgeting.v1.ScheduleBudgetExpiryResponse;
import com.acme.budgeting.v1.ScoreBudgetLineRequest;
import com.acme.budgeting.v1.ScoreBudgetLineResponse;
import com.acme.budgeting.v1.SuspendBudgetRequest;
import com.acme.budgeting.v1.SuspendBudgetResponse;
import com.acme.budgeting.v1.TerminateBudgetRequest;
import com.acme.budgeting.v1.TerminateBudgetResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.featureflags.v1.CloneFeatureFlagRequest;
import com.acme.featureflags.v1.CloneFeatureFlagResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
import com.acme.suppliers.v1.WatchSuppliersRequest;
import com.acme.suppliers.v1.WatchSuppliersResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class BudgetLifecycleApplicationTest {
    @Test
    void terminateBudgetPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var application = new BudgetLifecycleApplication(
                AssetTransferSyncServiceGrpc.newBlockingStub(channel),
                FixedAssetWorkflowServiceGrpc.newFutureStub(channel),
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                BudgetLineValidationServiceGrpc.newBlockingStub(channel),
                ForecastSyncServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                FeatureFlagServiceGrpc.newBlockingStub(channel),
                new BudgetLifecycleRequestMapperImpl(),
                repository,
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel),
                SupplierServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(TerminateBudgetRequest.getDefaultInstance());
        var response = application.terminateBudget(request);
        var sent0 = channel.request("acme.assets.v1.FixedAssetWorkflowService/SignalFixedAssetWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent0, "details", "notify", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent0, "details", "notify", "name"));
        var sent1 = channel.request("acme.suppliers.v1.SupplierRatingValidationService/ValidateSupplierRating");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetWorkflowService/SignalFixedAssetWorkflow"), "fixed_asset", "address", "lines"),
                ProbeSupport.value(sent1, "supplier_rating", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetWorkflowService/SignalFixedAssetWorkflow"), "fixed_asset", "address", "locality"),
                ProbeSupport.value(sent1, "supplier_rating", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetWorkflowService/SignalFixedAssetWorkflow"), "fixed_asset", "audit", "updated_by"),
                ProbeSupport.value(sent1, "supplier_rating", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetWorkflowService/SignalFixedAssetWorkflow"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/ValidateSupplierRating"), "supplier_rating", "audit", "created_by"),
                ProbeSupport.value(response, "budget", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/ValidateSupplierRating"), "supplier_rating", "audit", "updated_by"),
                ProbeSupport.value(response, "budget", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "budget_id"),
                ProbeSupport.value(response, "budget", "budget_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
