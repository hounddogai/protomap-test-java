package com.acme.finance.domains.contracts;

import com.acme.assets.v1.ApproveFixedAssetRequest;
import com.acme.assets.v1.ApproveFixedAssetResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferApprovalServiceGrpc;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.RejectAssetTransferRequest;
import com.acme.assets.v1.RejectAssetTransferResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.SubmitStandingOrderRequest;
import com.acme.banking.v1.SubmitStandingOrderResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseWorkflowServiceGrpc;
import com.acme.compliance.v1.ListSanctionReportsRequest;
import com.acme.compliance.v1.ListSanctionReportsResponse;
import com.acme.compliance.v1.RecallSanctionRequest;
import com.acme.compliance.v1.RecallSanctionResponse;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionApprovalServiceGrpc;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.contracts.v1.ApproveContractRequest;
import com.acme.contracts.v1.ApproveContractResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseQueryServiceGrpc;
import com.acme.contracts.v1.ContractLine;
import com.acme.contracts.v1.ContractQueryServiceGrpc;
import com.acme.contracts.v1.DelegateContractApprovalRequest;
import com.acme.contracts.v1.DelegateContractApprovalResponse;
import com.acme.contracts.v1.EscalateContractRequest;
import com.acme.contracts.v1.EscalateContractResponse;
import com.acme.contracts.v1.ExportContractsRequest;
import com.acme.contracts.v1.ExportContractsResponse;
import com.acme.contracts.v1.GetContractApprovalStatusRequest;
import com.acme.contracts.v1.GetContractApprovalStatusResponse;
import com.acme.contracts.v1.GetRenewalRulesRequest;
import com.acme.contracts.v1.GetRenewalRulesResponse;
import com.acme.contracts.v1.LintObligationsRequest;
import com.acme.contracts.v1.LintObligationsResponse;
import com.acme.contracts.v1.ListPendingContractsRequest;
import com.acme.contracts.v1.ListPendingContractsResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationSla;
import com.acme.contracts.v1.RecallContractRequest;
import com.acme.contracts.v1.RecallContractResponse;
import com.acme.contracts.v1.RejectContractRequest;
import com.acme.contracts.v1.RejectContractResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalRef;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ResolveContractClausesRequest;
import com.acme.contracts.v1.ResolveContractClausesResponse;
import com.acme.contracts.v1.ScoreRenewalRequest;
import com.acme.contracts.v1.ScoreRenewalResponse;
import com.acme.contracts.v1.SubmitContractRequest;
import com.acme.contracts.v1.SubmitContractResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.NormalizePreferenceRequest;
import com.acme.customers.v1.NormalizePreferenceResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.identity.v1.ValidateUserRequest;
import com.acme.identity.v1.ValidateUserResponse;
import com.acme.media.v1.BatchGetMediaAssetsRequest;
import com.acme.media.v1.BatchGetMediaAssetsResponse;
import com.acme.media.v1.MediaAssetServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class ContractApprovalApplicationTest {
    @Test
    void delegateContractApprovalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ContractsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveContract(String tenant, Contract value) {}
            public void saveContractClause(String tenant, ContractClause value) {}
            public void saveObligation(String tenant, Obligation value) {}
            public void saveRenewal(String tenant, Renewal value) {}
        };
        var application = new ContractApprovalApplication(
                AssetTransferApprovalServiceGrpc.newBlockingStub(channel),
                FixedAssetApprovalServiceGrpc.newBlockingStub(channel),
                StandingOrderApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseWorkflowServiceGrpc.newBlockingStub(channel),
                SanctionApprovalServiceGrpc.newBlockingStub(channel),
                SanctionReportingServiceGrpc.newBlockingStub(channel),
                ContractClauseQueryServiceGrpc.newBlockingStub(channel),
                ContractQueryServiceGrpc.newBlockingStub(channel),
                RenewalValidationServiceGrpc.newBlockingStub(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                UserValidationServiceGrpc.newBlockingStub(channel),
                new ContractApprovalRequestMapperImpl(),
                MediaAssetServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(DelegateContractApprovalRequest.getDefaultInstance());
        var response = application.delegateContractApproval(request);
        var sent0 = channel.request("acme.assets.v1.AssetTransferApprovalService/RejectAssetTransfer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract", "billing_address", "country_code"),
                ProbeSupport.value(sent0, "asset_transfer", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract", "audit", "etag"),
                ProbeSupport.value(sent0, "asset_transfer", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent0, "details", "approver", "email"));
        var sent1 = channel.request("acme.contracts.v1.ContractQueryService/ExportContracts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.contracts.v1.RenewalValidationService/ScoreRenewal");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferApprovalService/RejectAssetTransfer"), "asset_transfer", "billing_address", "postal_code"),
                ProbeSupport.value(sent2, "details", "destination", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract", "billing_address", "lines"),
                ProbeSupport.value(sent2, "renewal", "location", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferApprovalService/RejectAssetTransfer"), "asset_transfer", "billing_address", "country_code"),
                ProbeSupport.value(sent2, "renewal", "location", "country_code"));
        var sent3 = channel.request("acme.compliance.v1.SanctionReportingService/ListSanctionReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.RenewalValidationService/ScoreRenewal"), "renewal", "location", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.RenewalValidationService/ScoreRenewal"), "renewal", "audit", "updated_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferApprovalService/RejectAssetTransfer"), "asset_transfer", "counterparty", "fixed_asset_id"),
                ProbeSupport.value(sent3, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferApprovalService/RejectAssetTransfer"), "asset_transfer", "billing_address", "postal_code"),
                ProbeSupport.value(response, "contract", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.RenewalValidationService/ScoreRenewal"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.RenewalValidationService/ScoreRenewal"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_id"),
                ProbeSupport.value(response, "contract", "contract_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract", "due_at"),
                ProbeSupport.value(response, "contract", "due_at"));
    }
}
