package com.acme.finance.domains.risk;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.ScoreAuditEventRequest;
import com.acme.audit.v1.ScoreAuditEventResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.BrowseAccessPoliciesRequest;
import com.acme.authorization.v1.BrowseAccessPoliciesResponse;
import com.acme.banking.v1.EscalateStandingOrderRequest;
import com.acme.banking.v1.EscalateStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.ListCustomersRequest;
import com.acme.customers.v1.ListCustomersResponse;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.WatchDocumentsRequest;
import com.acme.documents.v1.WatchDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageDiscountAction;
import com.acme.insurance.v1.CoverageWorkflowServiceGrpc;
import com.acme.insurance.v1.PauseCoverageWorkflowRequest;
import com.acme.insurance.v1.PauseCoverageWorkflowResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationAttachment;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.RecallLoanApplicationRequest;
import com.acme.lending.v1.RecallLoanApplicationResponse;
import com.acme.lending.v1.ResumeLoanApplicationWorkflowRequest;
import com.acme.lending.v1.ResumeLoanApplicationWorkflowResponse;
import com.acme.risk.v1.ActivateStressTestRequest;
import com.acme.risk.v1.ActivateStressTestResponse;
import com.acme.risk.v1.ApproveRiskScenarioRequest;
import com.acme.risk.v1.ApproveRiskScenarioResponse;
import com.acme.risk.v1.ApproveStressTestRequest;
import com.acme.risk.v1.ApproveStressTestResponse;
import com.acme.risk.v1.CommentStressTestApprovalRequest;
import com.acme.risk.v1.CommentStressTestApprovalResponse;
import com.acme.risk.v1.DelegateExposureApprovalRequest;
import com.acme.risk.v1.DelegateExposureApprovalResponse;
import com.acme.risk.v1.DelegateStressTestApprovalRequest;
import com.acme.risk.v1.DelegateStressTestApprovalResponse;
import com.acme.risk.v1.EscalateStressTestRequest;
import com.acme.risk.v1.EscalateStressTestResponse;
import com.acme.risk.v1.ExplainExposureDecisionRequest;
import com.acme.risk.v1.ExplainExposureDecisionResponse;
import com.acme.risk.v1.ExportRiskRegistersRequest;
import com.acme.risk.v1.ExportRiskRegistersResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureApprovalServiceGrpc;
import com.acme.risk.v1.ExposureBankFunding;
import com.acme.risk.v1.ExposureCardFunding;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.GetStressTestApprovalStatusRequest;
import com.acme.risk.v1.GetStressTestApprovalStatusResponse;
import com.acme.risk.v1.LintExposuresRequest;
import com.acme.risk.v1.LintExposuresResponse;
import com.acme.risk.v1.NormalizeExposureRequest;
import com.acme.risk.v1.NormalizeExposureResponse;
import com.acme.risk.v1.NormalizeRiskScenarioRequest;
import com.acme.risk.v1.NormalizeRiskScenarioResponse;
import com.acme.risk.v1.PauseRiskRegisterWorkflowRequest;
import com.acme.risk.v1.PauseRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RecallStressTestRequest;
import com.acme.risk.v1.RecallStressTestResponse;
import com.acme.risk.v1.RejectStressTestRequest;
import com.acme.risk.v1.RejectStressTestResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.ScoreExposureRequest;
import com.acme.risk.v1.ScoreExposureResponse;
import com.acme.risk.v1.SignalRiskRegisterWorkflowRequest;
import com.acme.risk.v1.SignalRiskRegisterWorkflowResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestLifecycleServiceGrpc;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.SubmitStressTestRequest;
import com.acme.risk.v1.SubmitStressTestResponse;
import com.acme.shipping.v1.ExportShipmentsRequest;
import com.acme.shipping.v1.ExportShipmentsResponse;
import com.acme.shipping.v1.ShipmentQueryServiceGrpc;
import com.acme.suppliers.v1.FindSupplierContractsRequest;
import com.acme.suppliers.v1.FindSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class StressTestApprovalApplicationTest {
    @Test
    void submitStressTestPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new RiskRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExposure(String tenant, Exposure value) {}
            public void saveRiskRegister(String tenant, RiskRegister value) {}
            public void saveRiskScenario(String tenant, RiskScenario value) {}
            public void saveStressTest(String tenant, StressTest value) {}
        };
        var application = new StressTestApprovalApplication(
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                StandingOrderApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                CoverageWorkflowServiceGrpc.newBlockingStub(channel),
                LoanApplicationApprovalServiceGrpc.newBlockingStub(channel),
                LoanApplicationWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                ExposureApprovalServiceGrpc.newBlockingStub(channel),
                ExposureValidationServiceGrpc.newBlockingStub(channel),
                RiskRegisterQueryServiceGrpc.newBlockingStub(channel),
                RiskRegisterWorkflowServiceGrpc.newBlockingStub(channel),
                RiskScenarioApprovalServiceGrpc.newFutureStub(channel),
                RiskScenarioValidationServiceGrpc.newBlockingStub(channel),
                StressTestLifecycleServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                ShipmentQueryServiceGrpc.newBlockingStub(channel),
                SupplierContractQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SubmitStressTestRequest.getDefaultInstance());
        var response = application.submitStressTest(request);
        var sent0 = channel.request("acme.insurance.v1.CoverageWorkflowService/PauseCoverageWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "lines", "tax", "amount"),
                ProbeSupport.value(sent0, "coverage", "discount", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "lines", "description"),
                ProbeSupport.value(sent0, "coverage", "description"));
        var sent1 = channel.request("acme.risk.v1.RiskRegisterQueryService/ExportRiskRegisters");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageWorkflowService/PauseCoverageWorkflow"), "coverage", "audit", "created_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageWorkflowService/PauseCoverageWorkflow"), "coverage", "owner", "user_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        var sent2 = channel.request("acme.documents.v1.DocumentService/WatchDocuments");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "counterparty", "email"),
                ProbeSupport.value(sent2, "subscriber", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "audit", "updated_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageWorkflowService/PauseCoverageWorkflow"), "coverage", "audit", "updated_at"),
                ProbeSupport.value(sent2, "since"));
        var sent3 = channel.request("acme.risk.v1.RiskRegisterWorkflowService/SignalRiskRegisterWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent3, "risk_register", "children", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test", "billing_address", "country_code"),
                ProbeSupport.value(sent3, "risk_register", "children", "children", "location", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterWorkflowService/SignalRiskRegisterWorkflow"), "risk_register", "audit", "updated_at"),
                ProbeSupport.value(response, "stress_test", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "stress_test_id"),
                ProbeSupport.value(response, "stress_test", "stress_test_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
