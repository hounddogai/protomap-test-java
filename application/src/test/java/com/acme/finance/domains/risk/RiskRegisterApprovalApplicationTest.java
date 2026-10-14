package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.SendAuditTrailTestNoticeRequest;
import com.acme.audit.v1.SendAuditTrailTestNoticeResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.configuration.v1.ConfigSet;
import com.acme.configuration.v1.ConfigSetCalculationServiceGrpc;
import com.acme.configuration.v1.EstimateConfigSetRequest;
import com.acme.configuration.v1.EstimateConfigSetResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.customers.v1.PreviewCustomerSegmentRequest;
import com.acme.customers.v1.PreviewCustomerSegmentResponse;
import com.acme.customers.v1.ValidateCustomerSegmentRequest;
import com.acme.customers.v1.ValidateCustomerSegmentResponse;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.WatchDocumentsRequest;
import com.acme.documents.v1.WatchDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.pipelines.v1.PipelineServiceGrpc;
import com.acme.pipelines.v1.SearchPipelinesRequest;
import com.acme.pipelines.v1.SearchPipelinesResponse;
import com.acme.risk.v1.ApproveRiskRegisterRequest;
import com.acme.risk.v1.ApproveRiskRegisterResponse;
import com.acme.risk.v1.BrowseRiskRegistersRequest;
import com.acme.risk.v1.BrowseRiskRegistersResponse;
import com.acme.risk.v1.CommentRiskRegisterApprovalRequest;
import com.acme.risk.v1.CommentRiskRegisterApprovalResponse;
import com.acme.risk.v1.DelegateRiskRegisterApprovalRequest;
import com.acme.risk.v1.DelegateRiskRegisterApprovalResponse;
import com.acme.risk.v1.EscalateRiskRegisterRequest;
import com.acme.risk.v1.EscalateRiskRegisterResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.FindRiskRegistersRequest;
import com.acme.risk.v1.FindRiskRegistersResponse;
import com.acme.risk.v1.GetRiskRegisterApprovalStatusRequest;
import com.acme.risk.v1.GetRiskRegisterApprovalStatusResponse;
import com.acme.risk.v1.LintStressTestsRequest;
import com.acme.risk.v1.LintStressTestsResponse;
import com.acme.risk.v1.ListPendingRiskRegistersRequest;
import com.acme.risk.v1.ListPendingRiskRegistersResponse;
import com.acme.risk.v1.RecallRiskRegisterRequest;
import com.acme.risk.v1.RecallRiskRegisterResponse;
import com.acme.risk.v1.RejectRiskRegisterRequest;
import com.acme.risk.v1.RejectRiskRegisterResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.ScheduleStressTestExpiryRequest;
import com.acme.risk.v1.ScheduleStressTestExpiryResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestLifecycleServiceGrpc;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.acme.risk.v1.SubmitRiskRegisterRequest;
import com.acme.risk.v1.SubmitRiskRegisterResponse;
import com.acme.suppliers.v1.NormalizeSupplierRiskRequest;
import com.acme.suppliers.v1.NormalizeSupplierRiskResponse;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class RiskRegisterApprovalApplicationTest {
    @Test
    void rejectRiskRegisterPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var application = new RiskRegisterApprovalApplication(
                AuditTrailNotificationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ConfigSetCalculationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                PipelineServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterQueryServiceGrpc.newBlockingStub(channel),
                StressTestLifecycleServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierRiskValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RejectRiskRegisterRequest.getDefaultInstance());
        var response = application.rejectRiskRegister(request);
        var sent0 = channel.request("acme.customers.v1.CustomerSegmentValidationService/PreviewCustomerSegment");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register", "audit", "updated_by"),
                ProbeSupport.value(sent0, "customer_segment", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent0, "customer_segment", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent0, "customer_segment", "owner", "user_id"));
        var sent1 = channel.request("acme.suppliers.v1.SupplierRiskValidationService/NormalizeSupplierRisk");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register", "manager", "email"),
                ProbeSupport.value(sent1, "supplier_risk", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentValidationService/PreviewCustomerSegment"), "customer_segment", "owner", "email"),
                ProbeSupport.value(response, "risk_register", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRiskValidationService/NormalizeSupplierRisk"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRiskValidationService/NormalizeSupplierRisk"), "supplier_risk", "assignee", "display_name"),
                ProbeSupport.value(response, "risk_register", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register_id"),
                ProbeSupport.value(response, "risk_register", "risk_register_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "risk_register", "attributes"),
                ProbeSupport.value(response, "risk_register", "attributes"));
    }
}
