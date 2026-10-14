package com.acme.finance.domains.procurement;

import com.acme.accounting.v1.BatchCreateChartOfAccountsesRequest;
import com.acme.accounting.v1.BatchCreateChartOfAccountsesResponse;
import com.acme.accounting.v1.ChartOfAccountsServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetLifecycleServiceGrpc;
import com.acme.assets.v1.ScheduleFixedAssetExpiryRequest;
import com.acme.assets.v1.ScheduleFixedAssetExpiryResponse;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailAttachment;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.PreviewAuditTrailRequest;
import com.acme.audit.v1.PreviewAuditTrailResponse;
import com.acme.audit.v1.StreamAuditTrailNoticesRequest;
import com.acme.audit.v1.StreamAuditTrailNoticesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.EscalateBudgetRequest;
import com.acme.budgeting.v1.EscalateBudgetResponse;
import com.acme.collections.v1.PauseWriteOffWorkflowRequest;
import com.acme.collections.v1.PauseWriteOffWorkflowResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.EstimateConsentRecordRequest;
import com.acme.customers.v1.EstimateConsentRecordResponse;
import com.acme.customers.v1.FindHouseholdsRequest;
import com.acme.customers.v1.FindHouseholdsResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.RecalculateHouseholdsRequest;
import com.acme.customers.v1.RecalculateHouseholdsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.CollateralWorkflowServiceGrpc;
import com.acme.lending.v1.ListCollateralWorkflowsRequest;
import com.acme.lending.v1.ListCollateralWorkflowsResponse;
import com.acme.media.v1.MediaAssetServiceGrpc;
import com.acme.media.v1.WatchMediaAssetsRequest;
import com.acme.media.v1.WatchMediaAssetsResponse;
import com.acme.notifications.v1.FindNotificationsRequest;
import com.acme.notifications.v1.FindNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.onboarding.v1.ListOnboardingPlansRequest;
import com.acme.onboarding.v1.ListOnboardingPlansResponse;
import com.acme.onboarding.v1.OnboardingPlanServiceGrpc;
import com.acme.performers.v1.ClonePerformerRequest;
import com.acme.performers.v1.ClonePerformerResponse;
import com.acme.performers.v1.Performer;
import com.acme.performers.v1.PerformerServiceGrpc;
import com.acme.procurement.v1.AdvanceGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.AdvanceGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.CancelGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.CancelGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.GenerateRfqReportRequest;
import com.acme.procurement.v1.GenerateRfqReportResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.ListGoodsReceiptWorkflowsRequest;
import com.acme.procurement.v1.ListGoodsReceiptWorkflowsResponse;
import com.acme.procurement.v1.ListPendingGoodsReceiptsRequest;
import com.acme.procurement.v1.ListPendingGoodsReceiptsResponse;
import com.acme.procurement.v1.PauseGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.PauseGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.PreviewPurchaseRequisitionRequest;
import com.acme.procurement.v1.PreviewPurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderLifecycleServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionLine;
import com.acme.procurement.v1.PurchaseRequisitionLineTax;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.RecallRfqRequest;
import com.acme.procurement.v1.RecallRfqResponse;
import com.acme.procurement.v1.ResumeGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.ResumeGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqApprovalServiceGrpc;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryRequest;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryResponse;
import com.acme.procurement.v1.ScheduleRfqReportRequest;
import com.acme.procurement.v1.ScheduleRfqReportResponse;
import com.acme.procurement.v1.SignalGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.SignalGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.StartGoodsReceiptWorkflowRequest;
import com.acme.procurement.v1.StartGoodsReceiptWorkflowResponse;
import com.acme.procurement.v1.ValidatePurchaseOrderRequest;
import com.acme.procurement.v1.ValidatePurchaseOrderResponse;
import com.acme.scheduling.v1.ShiftQueryServiceGrpc;
import com.acme.scheduling.v1.SuggestShiftsRequest;
import com.acme.scheduling.v1.SuggestShiftsResponse;
import com.acme.sellers.v1.SuggestSellersRequest;
import com.acme.sellers.v1.SuggestSellersResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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

class GoodsReceiptWorkflowApplicationTest {
    @Test
    void resumeGoodsReceiptWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var application = new GoodsReceiptWorkflowApplication(
                ChartOfAccountsServiceGrpc.newStub(channel),
                DisposalSyncServiceGrpc.newStub(channel),
                FixedAssetLifecycleServiceGrpc.newBlockingStub(channel),
                AuditTrailNotificationServiceGrpc.newBlockingStub(channel),
                AuditTrailValidationServiceGrpc.newBlockingStub(channel),
                BudgetApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                WriteOffWorkflowServiceGrpc.newBlockingStub(channel),
                ConsentRecordCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdQueryServiceGrpc.newBlockingStub(channel),
                CollateralWorkflowServiceGrpc.newBlockingStub(channel),
                MediaAssetServiceGrpc.newBlockingStub(channel),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                OnboardingPlanServiceGrpc.newBlockingStub(channel),
                PerformerServiceGrpc.newBlockingStub(channel),
                PurchaseOrderLifecycleServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionValidationServiceGrpc.newBlockingStub(channel),
                RfqApprovalServiceGrpc.newBlockingStub(channel),
                RfqReportingServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                ShiftQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ResumeGoodsReceiptWorkflowRequest.getDefaultInstance());
        var response = application.resumeGoodsReceiptWorkflow(request);
        var sent0 = channel.request("acme.procurement.v1.RfqReportingService/ScheduleRfqReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "parameters"),
                ProbeSupport.value(sent0, "details", "filters"));
        var sent1 = channel.request("acme.procurement.v1.RfqApprovalService/RecallRfq");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "name"),
                ProbeSupport.value(sent1, "rfq", "requester", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent1, "rfq", "requester", "phone"));
        var sent2 = channel.request("acme.audit.v1.AuditTrailNotificationService/StreamAuditTrailNotices");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent2, "subscriber", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqApprovalService/RecallRfq"), "rfq", "requester", "name"),
                ProbeSupport.value(sent2, "subscriber", "name"));
        var sent3 = channel.request("acme.customers.v1.HouseholdCalculationService/RecalculateHouseholds");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "goods_receipt", "billing_address", "recipient"),
                ProbeSupport.value(sent3, "household", "children", "children", "location", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqApprovalService/RecallRfq"), "rfq", "requester", "email"),
                ProbeSupport.value(sent3, "household", "children", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "goods_receipt", "billing_address", "country_code"),
                ProbeSupport.value(sent3, "household", "children", "children", "location", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqReportingService/ScheduleRfqReport"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqApprovalService/RecallRfq"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdCalculationService/RecalculateHouseholds"), "household", "audit", "created_by"),
                ProbeSupport.value(response, "goods_receipt", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "goods_receipt_id"),
                ProbeSupport.value(response, "goods_receipt", "goods_receipt_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "goods_receipt", "total"),
                ProbeSupport.value(response, "goods_receipt", "total"));
    }
}
