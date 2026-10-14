package com.acme.finance.domains.procurement;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.ListAuditTrailConflictsRequest;
import com.acme.audit.v1.ListAuditTrailConflictsResponse;
import com.acme.audit.v1.PushAuditTrailRequest;
import com.acme.audit.v1.PushAuditTrailResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.NormalizePolicyRequest;
import com.acme.insurance.v1.NormalizePolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.ValidateRepaymentRequest;
import com.acme.lending.v1.ValidateRepaymentResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.metrics.v1.NormalizeMetricSeriesRequest;
import com.acme.metrics.v1.NormalizeMetricSeriesResponse;
import com.acme.metrics.v1.ValidateMetricSeriesRequest;
import com.acme.metrics.v1.ValidateMetricSeriesResponse;
import com.acme.procurement.v1.BrowsePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BrowsePurchaseRequisitionsResponse;
import com.acme.procurement.v1.CheckRfqEligibilityRequest;
import com.acme.procurement.v1.CheckRfqEligibilityResponse;
import com.acme.procurement.v1.CountPurchaseRequisitionsRequest;
import com.acme.procurement.v1.CountPurchaseRequisitionsResponse;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionRequest;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionResponse;
import com.acme.procurement.v1.ExportPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ExportPurchaseRequisitionsResponse;
import com.acme.procurement.v1.FindPurchaseRequisitionsRequest;
import com.acme.procurement.v1.FindPurchaseRequisitionsResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.LookupPurchaseRequisitionRequest;
import com.acme.procurement.v1.LookupPurchaseRequisitionResponse;
import com.acme.procurement.v1.PeekPurchaseRequisitionRequest;
import com.acme.procurement.v1.PeekPurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderCardSettlement;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.ResolvePurchaseRequisitionsRequest;
import com.acme.procurement.v1.ResolvePurchaseRequisitionsResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.procurement.v1.SimulateRfqRequest;
import com.acme.procurement.v1.SimulateRfqResponse;
import com.acme.procurement.v1.SuggestPurchaseRequisitionsRequest;
import com.acme.procurement.v1.SuggestPurchaseRequisitionsResponse;
import com.acme.procurement.v1.SummarizePurchaseRequisitionsRequest;
import com.acme.procurement.v1.SummarizePurchaseRequisitionsResponse;
import com.acme.procurement.v1.ValidatePurchaseRequisitionRequest;
import com.acme.procurement.v1.ValidatePurchaseRequisitionResponse;
import com.acme.risk.v1.RecalculateStressTestsRequest;
import com.acme.risk.v1.RecalculateStressTestsResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class PurchaseRequisitionQueryApplicationTest {
    @Test
    void exportPurchaseRequisitionsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var application = new PurchaseRequisitionQueryApplication(
                AuditTrailSyncServiceGrpc.newBlockingStub(channel),
                PolicyValidationServiceGrpc.newBlockingStub(channel),
                RepaymentValidationServiceGrpc.newBlockingStub(channel),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                PurchaseOrderValidationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionValidationServiceGrpc.newBlockingStub(channel),
                RfqCalculationServiceGrpc.newBlockingStub(channel),
                RfqValidationServiceGrpc.newBlockingStub(channel),
                repository,
                StressTestCalculationServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(ExportPurchaseRequisitionsRequest.getDefaultInstance());
        var response = application.exportPurchaseRequisitions(request);
        var sent0 = channel.request("acme.lending.v1.RepaymentValidationService/ValidateRepayment");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "repayment", "location", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "repayment", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent0, "repayment", "organizer", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent0, "repayment", "audit", "created_at"));
        var sent1 = channel.request("acme.procurement.v1.RfqValidationService/CheckRfqEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentValidationService/ValidateRepayment"), "repayment", "audit", "updated_by"),
                ProbeSupport.value(sent1, "rfq", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentValidationService/ValidateRepayment"), "repayment", "audit", "etag"),
                ProbeSupport.value(sent1, "rfq", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentValidationService/ValidateRepayment"), "repayment", "organizer", "user_id"),
                ProbeSupport.value(sent1, "rfq", "assignee", "user_id"));
        var sent2 = channel.request("acme.insurance.v1.PolicyValidationService/NormalizePolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqValidationService/CheckRfqEligibility"), "rfq", "audit", "updated_by"),
                ProbeSupport.value(sent2, "policy", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqValidationService/CheckRfqEligibility"), "rfq", "requester", "preferred_channel"),
                ProbeSupport.value(sent2, "policy", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqValidationService/CheckRfqEligibility"), "rfq", "customer", "display_name"),
                ProbeSupport.value(sent2, "policy", "holder", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentValidationService/ValidateRepayment"), "repayment", "location", "region"),
                ProbeSupport.value(response, "purchase_requisition", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentValidationService/ValidateRepayment"), "repayment", "location", "lines"),
                ProbeSupport.value(response, "purchase_requisition", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqValidationService/CheckRfqEligibility"), "rfq", "audit", "etag"),
                ProbeSupport.value(response, "purchase_requisition", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyValidationService/NormalizePolicy"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        var endpoint = new PurchaseRequisitionQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportPurchaseRequisitionsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportPurchaseRequisitionsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportPurchaseRequisitions(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
