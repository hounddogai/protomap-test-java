package com.acme.finance.domains.procurement;

import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetApprovalServiceGrpc;
import com.acme.budgeting.v1.CommentBudgetApprovalRequest;
import com.acme.budgeting.v1.CommentBudgetApprovalResponse;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.StreamVarianceMetricsRequest;
import com.acme.budgeting.v1.StreamVarianceMetricsResponse;
import com.acme.budgeting.v1.VarianceReportingServiceGrpc;
import com.acme.careplans.v1.BrowseCarePlansRequest;
import com.acme.careplans.v1.BrowseCarePlansResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationWorkflowServiceGrpc;
import com.acme.compliance.v1.StartAttestationWorkflowRequest;
import com.acme.compliance.v1.StartAttestationWorkflowResponse;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.content.v1.SearchArticlesRequest;
import com.acme.content.v1.SearchArticlesResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.EstimatePreferenceRequest;
import com.acme.customers.v1.EstimatePreferenceResponse;
import com.acme.customers.v1.LintCustomerSegmentsRequest;
import com.acme.customers.v1.LintCustomerSegmentsResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.UpdateCustomerRequest;
import com.acme.customers.v1.UpdateCustomerResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.giftcards.v1.GiftCardServiceGrpc;
import com.acme.giftcards.v1.SearchGiftCardsRequest;
import com.acme.giftcards.v1.SearchGiftCardsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CancelClaimWorkflowRequest;
import com.acme.insurance.v1.CancelClaimWorkflowResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.ListPendingPoliciesRequest;
import com.acme.insurance.v1.ListPendingPoliciesResponse;
import com.acme.insurance.v1.MapUnderwritingImportColumnsRequest;
import com.acme.insurance.v1.MapUnderwritingImportColumnsResponse;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.metrics.v1.CreateMetricSeriesRequest;
import com.acme.metrics.v1.CreateMetricSeriesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.procurement.v1.ActivateRfqRequest;
import com.acme.procurement.v1.ActivateRfqResponse;
import com.acme.procurement.v1.DeleteRfqReportRequest;
import com.acme.procurement.v1.DeleteRfqReportResponse;
import com.acme.procurement.v1.ExpireRfqRequest;
import com.acme.procurement.v1.ExpireRfqResponse;
import com.acme.procurement.v1.ExplainGoodsReceiptCalculationRequest;
import com.acme.procurement.v1.ExplainGoodsReceiptCalculationResponse;
import com.acme.procurement.v1.GetRfqLifecycleRequest;
import com.acme.procurement.v1.GetRfqLifecycleResponse;
import com.acme.procurement.v1.GetRfqRulesRequest;
import com.acme.procurement.v1.GetRfqRulesResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptCalculationServiceGrpc;
import com.acme.procurement.v1.GoodsReceiptCardSettlement;
import com.acme.procurement.v1.GoodsReceiptLifecycleServiceGrpc;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderLifecycleServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.ReinstateRfqRequest;
import com.acme.procurement.v1.ReinstateRfqResponse;
import com.acme.procurement.v1.RenewRfqRequest;
import com.acme.procurement.v1.RenewRfqResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.procurement.v1.ScheduleGoodsReceiptExpiryRequest;
import com.acme.procurement.v1.ScheduleGoodsReceiptExpiryResponse;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryRequest;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryResponse;
import com.acme.procurement.v1.ScheduleRfqExpiryRequest;
import com.acme.procurement.v1.ScheduleRfqExpiryResponse;
import com.acme.procurement.v1.SuspendRfqRequest;
import com.acme.procurement.v1.SuspendRfqResponse;
import com.acme.procurement.v1.TerminateRfqRequest;
import com.acme.procurement.v1.TerminateRfqResponse;
import com.acme.risk.v1.ResumeRiskRegisterWorkflowRequest;
import com.acme.risk.v1.ResumeRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.suppliers.v1.SignalSupplierWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierWorkflowResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class RfqLifecycleApplicationTest {
    @Test
    void reinstateRfqPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var application = new RfqLifecycleApplication(
                BudgetApprovalServiceGrpc.newBlockingStub(channel),
                VarianceReportingServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationWorkflowServiceGrpc.newBlockingStub(channel),
                ArticleServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newFutureStub(channel),
                CustomerServiceGrpc.newBlockingStub(channel),
                PreferenceCalculationServiceGrpc.newBlockingStub(channel),
                GiftCardServiceGrpc.newBlockingStub(channel),
                ClaimWorkflowServiceGrpc.newBlockingStub(channel),
                PolicyApprovalServiceGrpc.newBlockingStub(channel),
                UnderwritingImportServiceGrpc.newBlockingStub(channel),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                GoodsReceiptCalculationServiceGrpc.newBlockingStub(channel),
                GoodsReceiptLifecycleServiceGrpc.newBlockingStub(channel),
                PurchaseOrderLifecycleServiceGrpc.newBlockingStub(channel),
                RfqReportingServiceGrpc.newBlockingStub(channel),
                RfqValidationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterWorkflowServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ReinstateRfqRequest.getDefaultInstance());
        var response = application.reinstateRfq(request);
        var sent0 = channel.request("acme.procurement.v1.GoodsReceiptCalculationService/ExplainGoodsReceiptCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent0, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "phone"),
                ProbeSupport.value(sent0, "details", "contact", "phone"));
        var sent1 = channel.request("acme.procurement.v1.GoodsReceiptLifecycleService/ScheduleGoodsReceiptExpiry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptCalculationService/ExplainGoodsReceiptCalculation"), "goods_receipt", "card", "last4"),
                ProbeSupport.value(sent1, "goods_receipt", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent1, "details", "notify", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent1, "details", "notify", "email"));
        var sent2 = channel.request("acme.insurance.v1.PolicyApprovalService/ListPendingPolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptLifecycleService/ScheduleGoodsReceiptExpiry"), "goods_receipt", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptCalculationService/ExplainGoodsReceiptCalculation"), "goods_receipt", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptLifecycleService/ScheduleGoodsReceiptExpiry"), "outcome", "reference_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent2, "filter", "labels"));
        var sent3 = channel.request("acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "user_id"),
                ProbeSupport.value(sent3, "customer_segment", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptCalculationService/ExplainGoodsReceiptCalculation"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptLifecycleService/ScheduleGoodsReceiptExpiry"), "goods_receipt", "audit", "updated_at"),
                ProbeSupport.value(response, "rfq", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptLifecycleService/ScheduleGoodsReceiptExpiry"), "goods_receipt", "audit", "etag"),
                ProbeSupport.value(response, "rfq", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyApprovalService/ListPendingPolicies"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments"), "customer_segment", "audit", "updated_by"),
                ProbeSupport.value(response, "rfq", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq_id"),
                ProbeSupport.value(response, "rfq", "rfq_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
