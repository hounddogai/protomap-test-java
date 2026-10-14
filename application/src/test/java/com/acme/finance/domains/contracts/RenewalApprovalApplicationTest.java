package com.acme.finance.domains.contracts;

import com.acme.collections.v1.AdvanceWriteOffWorkflowRequest;
import com.acme.collections.v1.AdvanceWriteOffWorkflowResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ApproveContractClauseRequest;
import com.acme.contracts.v1.ApproveContractClauseResponse;
import com.acme.contracts.v1.ApproveRenewalRequest;
import com.acme.contracts.v1.ApproveRenewalResponse;
import com.acme.contracts.v1.CancelContractWorkflowRequest;
import com.acme.contracts.v1.CancelContractWorkflowResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseApprovalServiceGrpc;
import com.acme.contracts.v1.ContractClauseQueryServiceGrpc;
import com.acme.contracts.v1.ContractQueryServiceGrpc;
import com.acme.contracts.v1.CountContractClausesRequest;
import com.acme.contracts.v1.CountContractClausesResponse;
import com.acme.contracts.v1.DelegateRenewalApprovalRequest;
import com.acme.contracts.v1.DelegateRenewalApprovalResponse;
import com.acme.contracts.v1.GetRenewalApprovalStatusRequest;
import com.acme.contracts.v1.GetRenewalApprovalStatusResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.RecallRenewalRequest;
import com.acme.contracts.v1.RecallRenewalResponse;
import com.acme.contracts.v1.RejectRenewalRequest;
import com.acme.contracts.v1.RejectRenewalResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.ResolveContractsRequest;
import com.acme.contracts.v1.ResolveContractsResponse;
import com.acme.contracts.v1.SubmitRenewalRequest;
import com.acme.contracts.v1.SubmitRenewalResponse;
import com.acme.contracts.v1.ValidateObligationRequest;
import com.acme.contracts.v1.ValidateObligationResponse;
import com.acme.customers.v1.CompareHouseholdOptionsRequest;
import com.acme.customers.v1.CompareHouseholdOptionsResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.ResolveCustomersRequest;
import com.acme.customers.v1.ResolveCustomersResponse;
import com.acme.drivers.v1.Driver;
import com.acme.drivers.v1.DriverServiceGrpc;
import com.acme.drivers.v1.UpdateDriverRequest;
import com.acme.drivers.v1.UpdateDriverResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.notifications.v1.SearchNotificationsRequest;
import com.acme.notifications.v1.SearchNotificationsResponse;
import com.acme.risk.v1.EscalateRiskRegisterRequest;
import com.acme.risk.v1.EscalateRiskRegisterResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class RenewalApprovalApplicationTest {
    @Test
    void approveRenewalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ContractsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveContract(String tenant, Contract value) {}
            public void saveContractClause(String tenant, ContractClause value) {}
            public void saveObligation(String tenant, Obligation value) {}
            public void saveRenewal(String tenant, Renewal value) {}
        };
        var application = new RenewalApprovalApplication(
                ChannelRegistry.fixed(channel),
                WriteOffWorkflowServiceGrpc.newBlockingStub(channel),
                ContractClauseApprovalServiceGrpc.newBlockingStub(channel),
                ContractClauseQueryServiceGrpc.newBlockingStub(channel),
                ContractQueryServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                DriverServiceGrpc.newBlockingStub(channel),
                NotificationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterApprovalServiceGrpc.newBlockingStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(ApproveRenewalRequest.getDefaultInstance());
        var response = application.approveRenewal(request);
        var sent0 = channel.request("acme.customers.v1.HouseholdCalculationService/CompareHouseholdOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "location", "lines"),
                ProbeSupport.value(sent0, "household", "children", "children", "location", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "user_id"),
                ProbeSupport.value(sent0, "household", "children", "children", "manager", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "organizer", "display_name"),
                ProbeSupport.value(sent0, "household", "children", "children", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "audit", "updated_at"),
                ProbeSupport.value(sent0, "household", "children", "audit", "updated_at"));
        var sent1 = channel.request("acme.notifications.v1.NotificationService/SearchNotifications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "location", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "audit", "created_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.risk.v1.RiskRegisterApprovalService/EscalateRiskRegister");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "participants", "contact", "email"),
                ProbeSupport.value(sent2, "risk_register", "children", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "audit", "created_at"),
                ProbeSupport.value(sent2, "risk_register", "children", "children", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdCalculationService/CompareHouseholdOptions"), "household", "location", "locality"),
                ProbeSupport.value(sent2, "risk_register", "children", "children", "location", "locality"));
        var sent3 = channel.request("acme.contracts.v1.ContractClauseApprovalService/ApproveContractClause");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterApprovalService/EscalateRiskRegister"), "risk_register", "manager", "email"),
                ProbeSupport.value(sent3, "contract_clause", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterApprovalService/EscalateRiskRegister"), "risk_register", "manager", "display_name"),
                ProbeSupport.value(sent3, "details", "approver", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdCalculationService/CompareHouseholdOptions"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdCalculationService/CompareHouseholdOptions"), "household", "location", "lines"),
                ProbeSupport.value(response, "renewal", "location", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/SearchNotifications"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractClauseApprovalService/ApproveContractClause"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal_id"),
                ProbeSupport.value(response, "renewal", "renewal_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "renewal", "metadata"),
                ProbeSupport.value(response, "renewal", "metadata"));
    }
}
