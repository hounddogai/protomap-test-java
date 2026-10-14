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
import com.acme.contracts.v1.Obligation;
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
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
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
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ContractClauseApprovalApplicationTest {
    @Test
    void rejectContractClausePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ContractsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveContract(String tenant, Contract value) {}
            public void saveContractClause(String tenant, ContractClause value) {}
            public void saveObligation(String tenant, Obligation value) {}
            public void saveRenewal(String tenant, Renewal value) {}
        };
        var listener = new ContractClauseApprovalApplicationEvents(ConsentRecordCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ContractClauseApprovalApplication.ApproveContractClauseStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ContractClauseApprovalApplication(
                AttestationApprovalServiceGrpc.newBlockingStub(channel),
                ContractServiceGrpc.newStub(channel),
                ContractServiceGrpc.newBlockingStub(channel),
                RenewalValidationServiceGrpc.newBlockingStub(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                events,
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                new ContractClauseApprovalRequestMapperImpl(),
                repository,
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RejectContractClauseRequest.getDefaultInstance());
        var response = application.rejectContractClause(request);
        var sent0 = channel.request("acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause", "owner", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        var sent1 = channel.request("acme.venues.v1.VenueValidationService/ScoreVenue");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation"), "customer", "primary_address", "postal_code"),
                ProbeSupport.value(sent1, "venue", "address", "postal_code"));
        var sent2 = channel.request("acme.customers.v1.CustomerSegmentCalculationService/ExplainCustomerSegmentCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause", "owner", "user_id"),
                ProbeSupport.value(sent2, "customer_segment", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause", "discount", "percent"),
                ProbeSupport.value(sent2, "customer_segment", "discount", "percent"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause", "owner", "email"),
                ProbeSupport.value(sent2, "customer_segment", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause", "discount", "amount"),
                ProbeSupport.value(sent2, "customer_segment", "discount", "amount"));
        var sent3 = channel.request("acme.customers.v1.PreferenceValidationService/PreviewPreference");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause", "owner", "email"),
                ProbeSupport.value(sent3, "preference", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause", "discount", "amount"),
                ProbeSupport.value(sent3, "preference", "discount", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation"), "customer", "audit", "created_by"),
                ProbeSupport.value(sent3, "preference", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ScoreVenue"), "venue", "audit", "updated_at"),
                ProbeSupport.value(sent3, "preference", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation"), "customer", "audit", "created_by"),
                ProbeSupport.value(response, "contract_clause", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ScoreVenue"), "venue", "audit", "updated_by"),
                ProbeSupport.value(response, "contract_clause", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ScoreVenue"), "venue", "components", "description"),
                ProbeSupport.value(response, "contract_clause", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentCalculationService/ExplainCustomerSegmentCalculation"), "customer_segment", "owner", "display_name"),
                ProbeSupport.value(response, "contract_clause", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceValidationService/PreviewPreference"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceValidationService/PreviewPreference"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause_id"),
                ProbeSupport.value(response, "contract_clause", "contract_clause_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "contract_clause", "effective"),
                ProbeSupport.value(response, "contract_clause", "effective"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ContractClauseApprovalApplicationEvents(ConsentRecordCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CalculateConsentRecordResponse>();
        listener.on(new ContractClauseApprovalApplication.ApproveContractClauseStage1Requested(ProbeSupport.sample(CalculateConsentRecordRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
