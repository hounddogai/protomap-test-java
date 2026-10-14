package com.acme.finance.domains.contracts;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.NormalizeAssetTransferRequest;
import com.acme.assets.v1.NormalizeAssetTransferResponse;
import com.acme.audit.v1.SummarizeAuditTrailsRequest;
import com.acme.audit.v1.SummarizeAuditTrailsResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.ExportBankAccountsRequest;
import com.acme.banking.v1.ExportBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.BrowseContractClausesRequest;
import com.acme.contracts.v1.BrowseContractClausesResponse;
import com.acme.contracts.v1.CheckObligationEligibilityRequest;
import com.acme.contracts.v1.CheckObligationEligibilityResponse;
import com.acme.contracts.v1.CheckRenewalEligibilityRequest;
import com.acme.contracts.v1.CheckRenewalEligibilityResponse;
import com.acme.contracts.v1.CompareObligationOptionsRequest;
import com.acme.contracts.v1.CompareObligationOptionsResponse;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractQueryServiceGrpc;
import com.acme.contracts.v1.CountContractClausesRequest;
import com.acme.contracts.v1.CountContractClausesResponse;
import com.acme.contracts.v1.ExportContractClausesRequest;
import com.acme.contracts.v1.ExportContractClausesResponse;
import com.acme.contracts.v1.FindContractClausesRequest;
import com.acme.contracts.v1.FindContractClausesResponse;
import com.acme.contracts.v1.FindContractsRequest;
import com.acme.contracts.v1.FindContractsResponse;
import com.acme.contracts.v1.LookupContractClauseRequest;
import com.acme.contracts.v1.LookupContractClauseResponse;
import com.acme.contracts.v1.NormalizeRenewalRequest;
import com.acme.contracts.v1.NormalizeRenewalResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationCalculationServiceGrpc;
import com.acme.contracts.v1.ObligationValidationServiceGrpc;
import com.acme.contracts.v1.PeekContractClauseRequest;
import com.acme.contracts.v1.PeekContractClauseResponse;
import com.acme.contracts.v1.PreviewRenewalRequest;
import com.acme.contracts.v1.PreviewRenewalResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalDailyRule;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ResolveContractClausesRequest;
import com.acme.contracts.v1.ResolveContractClausesResponse;
import com.acme.contracts.v1.ScoreRenewalRequest;
import com.acme.contracts.v1.ScoreRenewalResponse;
import com.acme.contracts.v1.SummarizeContractClausesRequest;
import com.acme.contracts.v1.SummarizeContractClausesResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.ResolvePoliciesRequest;
import com.acme.insurance.v1.ResolvePoliciesResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class ContractClauseQueryRequestMapperImpl implements ContractClauseQueryRequestMapper {
    @Override
    public NormalizeAssetTransferRequest findContractClausesStage0(FindContractClausesRequest request) {
        return NormalizeAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
    }
}
