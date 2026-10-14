package com.acme.finance.domains.procurement;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CancelControlWorkflowRequest;
import com.acme.compliance.v1.CancelControlWorkflowResponse;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.RejectSanctionRequest;
import com.acme.compliance.v1.RejectSanctionResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionApprovalServiceGrpc;
import com.acme.customers.v1.CalculateCustomerRequest;
import com.acme.customers.v1.CalculateCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.PreviewPreferenceRequest;
import com.acme.customers.v1.PreviewPreferenceResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ListPendingPoliciesRequest;
import com.acme.insurance.v1.ListPendingPoliciesResponse;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.ScoreMetricSeriesRequest;
import com.acme.metrics.v1.ScoreMetricSeriesResponse;
import com.acme.procurement.v1.ActivateGoodsReceiptRequest;
import com.acme.procurement.v1.ActivateGoodsReceiptResponse;
import com.acme.procurement.v1.CheckGoodsReceiptEligibilityRequest;
import com.acme.procurement.v1.CheckGoodsReceiptEligibilityResponse;
import com.acme.procurement.v1.CompareRfqOptionsRequest;
import com.acme.procurement.v1.CompareRfqOptionsResponse;
import com.acme.procurement.v1.ExpireGoodsReceiptRequest;
import com.acme.procurement.v1.ExpireGoodsReceiptResponse;
import com.acme.procurement.v1.GetGoodsReceiptLifecycleRequest;
import com.acme.procurement.v1.GetGoodsReceiptLifecycleResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptLine;
import com.acme.procurement.v1.GoodsReceiptLineTax;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.procurement.v1.ListPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ListPurchaseRequisitionsResponse;
import com.acme.procurement.v1.ListRfqReportsRequest;
import com.acme.procurement.v1.ListRfqReportsResponse;
import com.acme.procurement.v1.LookupPurchaseRequisitionRequest;
import com.acme.procurement.v1.LookupPurchaseRequisitionResponse;
import com.acme.procurement.v1.PreviewPurchaseOrderRequest;
import com.acme.procurement.v1.PreviewPurchaseOrderResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionQueryServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.procurement.v1.ReinstateGoodsReceiptRequest;
import com.acme.procurement.v1.ReinstateGoodsReceiptResponse;
import com.acme.procurement.v1.RenewGoodsReceiptRequest;
import com.acme.procurement.v1.RenewGoodsReceiptResponse;
import com.acme.procurement.v1.ResolvePurchaseRequisitionsRequest;
import com.acme.procurement.v1.ResolvePurchaseRequisitionsResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.ScheduleGoodsReceiptExpiryRequest;
import com.acme.procurement.v1.ScheduleGoodsReceiptExpiryResponse;
import com.acme.procurement.v1.SearchPurchaseRequisitionsRequest;
import com.acme.procurement.v1.SearchPurchaseRequisitionsResponse;
import com.acme.procurement.v1.SuspendGoodsReceiptRequest;
import com.acme.procurement.v1.SuspendGoodsReceiptResponse;
import com.acme.procurement.v1.UpdatePurchaseRequisitionRequest;
import com.acme.procurement.v1.UpdatePurchaseRequisitionResponse;
import com.acme.shows.v1.QuoteShowRequest;
import com.acme.shows.v1.QuoteShowResponse;
import com.acme.shows.v1.Show;
import com.acme.shows.v1.ShowCalculationServiceGrpc;
import com.acme.subscriptions.v1.Subscription;
import com.acme.subscriptions.v1.SubscriptionServiceGrpc;
import com.acme.subscriptions.v1.UpdateSubscriptionRequest;
import com.acme.subscriptions.v1.UpdateSubscriptionResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class GoodsReceiptLifecycleApplicationTest {
    @Test
    void suspendGoodsReceiptPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var listener = new GoodsReceiptLifecycleApplicationEvents(SanctionApprovalServiceGrpc.newStub(channel), PurchaseRequisitionServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof GoodsReceiptLifecycleApplication.ReinstateGoodsReceiptStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof GoodsReceiptLifecycleApplication.ExpireGoodsReceiptStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new GoodsReceiptLifecycleApplication(
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                events,
                new GoodsReceiptLifecycleRequestMapperImpl(),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                GoodsReceiptValidationServiceGrpc.newBlockingStub(channel),
                PurchaseOrderValidationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionQueryServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionServiceGrpc.newBlockingStub(channel),
                RfqCalculationServiceGrpc.newBlockingStub(channel),
                RfqReportingServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                ShowCalculationServiceGrpc.newBlockingStub(channel),
                SubscriptionServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SuspendGoodsReceiptRequest.getDefaultInstance());
        var response = application.suspendGoodsReceipt(request);
        var sent0 = channel.request("acme.customers.v1.PreferenceValidationService/PreviewPreference");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent0, "preference", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "contact", "preferred_channel"));
        var sent1 = channel.request("acme.metrics.v1.MetricSeriesValidationService/ScoreMetricSeries");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "email"),
                ProbeSupport.value(sent1, "metric_series", "owner", "email"));
        var sent2 = channel.request("acme.procurement.v1.PurchaseOrderValidationService/PreviewPurchaseOrder");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesValidationService/ScoreMetricSeries"), "metric_series", "address", "country_code"),
                ProbeSupport.value(sent2, "purchase_order", "billing_address", "country_code"));
        var sent3 = channel.request("acme.subscriptions.v1.SubscriptionService/UpdateSubscription");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesValidationService/ScoreMetricSeries"), "metric_series", "owner", "email"),
                ProbeSupport.value(sent3, "subscription", "holder", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "phone"),
                ProbeSupport.value(sent3, "subscription", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "email"),
                ProbeSupport.value(sent3, "subscription", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesValidationService/ScoreMetricSeries"), "metric_series", "audit", "updated_by"),
                ProbeSupport.value(sent3, "subscription", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceValidationService/PreviewPreference"), "outcome", "reference_id"),
                ProbeSupport.value(response, "outcome", "reference_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesValidationService/ScoreMetricSeries"), "metric_series", "audit", "created_by"),
                ProbeSupport.value(response, "goods_receipt", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesValidationService/ScoreMetricSeries"), "metric_series", "audit", "updated_by"),
                ProbeSupport.value(response, "goods_receipt", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderValidationService/PreviewPurchaseOrder"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "goods_receipt_id"),
                ProbeSupport.value(response, "goods_receipt", "goods_receipt_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new GoodsReceiptLifecycleApplicationEvents(SanctionApprovalServiceGrpc.newStub(channel), PurchaseRequisitionServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<RejectSanctionResponse>();
        listener.on(new GoodsReceiptLifecycleApplication.ReinstateGoodsReceiptStage1Requested(ProbeSupport.sample(RejectSanctionRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<UpdatePurchaseRequisitionResponse>();
        listener.on(new GoodsReceiptLifecycleApplication.ExpireGoodsReceiptStage1Requested(ProbeSupport.sample(UpdatePurchaseRequisitionRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
