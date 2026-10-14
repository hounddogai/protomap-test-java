package com.acme.finance.domains.contracts;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.CommentAttestationApprovalRequest;
import com.acme.compliance.v1.CommentAttestationApprovalResponse;
import com.acme.compliance.v1.DelegateAttestationApprovalRequest;
import com.acme.compliance.v1.DelegateAttestationApprovalResponse;
import com.acme.contracts.v1.ApproveContractClauseRequest;
import com.acme.contracts.v1.ApproveContractClauseResponse;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.BatchGetContractsRequest;
import com.acme.contracts.v1.BatchGetContractsResponse;
import com.acme.contracts.v1.CommentContractClauseApprovalRequest;
import com.acme.contracts.v1.CommentContractClauseApprovalResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractBankSettlement;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.contracts.v1.CreateContractRequest;
import com.acme.contracts.v1.CreateContractResponse;
import com.acme.contracts.v1.DelegateContractClauseApprovalRequest;
import com.acme.contracts.v1.DelegateContractClauseApprovalResponse;
import com.acme.contracts.v1.EscalateContractClauseRequest;
import com.acme.contracts.v1.EscalateContractClauseResponse;
import com.acme.contracts.v1.GetContractClauseApprovalStatusRequest;
import com.acme.contracts.v1.GetContractClauseApprovalStatusResponse;
import com.acme.contracts.v1.ListPendingContractClausesRequest;
import com.acme.contracts.v1.ListPendingContractClausesResponse;
import com.acme.contracts.v1.RejectContractClauseRequest;
import com.acme.contracts.v1.RejectContractClauseResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalRef;
import com.acme.contracts.v1.RenewalValidationServiceGrpc;
import com.acme.contracts.v1.ScoreRenewalRequest;
import com.acme.contracts.v1.ScoreRenewalResponse;
import com.acme.contracts.v1.SearchContractsRequest;
import com.acme.contracts.v1.SearchContractsResponse;
import com.acme.contracts.v1.SubmitContractClauseRequest;
import com.acme.contracts.v1.SubmitContractClauseResponse;
import com.acme.contracts.v1.WatchContractsRequest;
import com.acme.contracts.v1.WatchContractsResponse;
import com.acme.customers.v1.CalculateConsentRecordRequest;
import com.acme.customers.v1.CalculateConsentRecordResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentCondition;
import com.acme.customers.v1.CustomerSegmentDiscountAction;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerCalculationRequest;
import com.acme.customers.v1.ExplainCustomerCalculationResponse;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationRequest;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationResponse;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.customers.v1.NormalizeConsentRecordRequest;
import com.acme.customers.v1.NormalizeConsentRecordResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceDiscountAction;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.PreviewPreferenceRequest;
import com.acme.customers.v1.PreviewPreferenceResponse;
import com.acme.customers.v1.QuoteCustomerSegmentRequest;
import com.acme.customers.v1.QuoteCustomerSegmentResponse;
import com.acme.documents.v1.BatchGetDocumentsRequest;
import com.acme.documents.v1.BatchGetDocumentsResponse;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchLine;
import com.acme.ledger.v1.PostingBatchLineTax;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.PreviewPostingBatchRequest;
import com.acme.ledger.v1.PreviewPostingBatchResponse;
import com.acme.venues.v1.ScoreVenueRequest;
import com.acme.venues.v1.ScoreVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueComponent;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class ContractClauseApprovalRequestMapperImpl implements ContractClauseApprovalRequestMapper {
    @Override
    public ExplainCustomerSegmentCalculationRequest rejectContractClauseStage2(RejectContractClauseRequest request) {
        return ExplainCustomerSegmentCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setDiscount(CustomerSegmentDiscountAction.newBuilder()
                                .setAmount(request.getContractClause().getDiscount().getAmount())
                                .setPercent(request.getContractClause().getDiscount().getPercent())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContractClause().getOwner().getEmail())
                                .setUserId(request.getContractClause().getOwner().getUserId())
                                .build())
                        .build())
                .build();
    }

    @Override
    public CreateContractRequest delegateContractClauseApprovalStage0(DelegateContractClauseApprovalRequest request) {
        return CreateContractRequest.newBuilder()
                .setContext(request.getContext())
                .setContract(Contract.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContractClause().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getContractClause().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(RenewalRef.newBuilder()
                                .setDisplayName(request.getContractClause().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
    }
}
