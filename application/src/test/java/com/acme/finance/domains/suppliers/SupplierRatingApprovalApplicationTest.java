package com.acme.finance.domains.suppliers;

import com.acme.assets.v1.AssetTransferApprovalServiceGrpc;
import com.acme.assets.v1.SubmitAssetTransferRequest;
import com.acme.assets.v1.SubmitAssetTransferResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateAttachment;
import com.acme.banking.v1.RejectMandateRequest;
import com.acme.banking.v1.RejectMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.AttestationCardSettlement;
import com.acme.compliance.v1.CommentAttestationApprovalRequest;
import com.acme.compliance.v1.CommentAttestationApprovalResponse;
import com.acme.contracts.v1.CommentContractClauseApprovalRequest;
import com.acme.contracts.v1.CommentContractClauseApprovalResponse;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.ContractClauseApprovalServiceGrpc;
import com.acme.contracts.v1.EscalateContractClauseRequest;
import com.acme.contracts.v1.EscalateContractClauseResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.PreviewCustomerRequest;
import com.acme.customers.v1.PreviewCustomerResponse;
import com.acme.customers.v1.QuoteHouseholdRequest;
import com.acme.customers.v1.QuoteHouseholdResponse;
import com.acme.customers.v1.ScoreConsentRecordRequest;
import com.acme.customers.v1.ScoreConsentRecordResponse;
import com.acme.customers.v1.ValidateCustomerSegmentRequest;
import com.acme.customers.v1.ValidateCustomerSegmentResponse;
import com.acme.employees.v1.EmployeeQueryServiceGrpc;
import com.acme.employees.v1.SummarizeEmployeesRequest;
import com.acme.employees.v1.SummarizeEmployeesResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.AnnotatePostingBatchVersionRequest;
import com.acme.ledger.v1.AnnotatePostingBatchVersionResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchLine;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeFiscalPeriodUpdatesResponse;
import com.acme.suppliers.v1.AdvanceSupplierWorkflowRequest;
import com.acme.suppliers.v1.AdvanceSupplierWorkflowResponse;
import com.acme.suppliers.v1.ApproveSupplierRatingRequest;
import com.acme.suppliers.v1.ApproveSupplierRatingResponse;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.BrowseSuppliersRequest;
import com.acme.suppliers.v1.BrowseSuppliersResponse;
import com.acme.suppliers.v1.CancelSupplierWorkflowRequest;
import com.acme.suppliers.v1.CancelSupplierWorkflowResponse;
import com.acme.suppliers.v1.CommentSupplierRatingApprovalRequest;
import com.acme.suppliers.v1.CommentSupplierRatingApprovalResponse;
import com.acme.suppliers.v1.DelegateSupplierRatingApprovalRequest;
import com.acme.suppliers.v1.DelegateSupplierRatingApprovalResponse;
import com.acme.suppliers.v1.EscalateSupplierRatingRequest;
import com.acme.suppliers.v1.EscalateSupplierRatingResponse;
import com.acme.suppliers.v1.GetSupplierRatingApprovalStatusRequest;
import com.acme.suppliers.v1.GetSupplierRatingApprovalStatusResponse;
import com.acme.suppliers.v1.ListPendingSupplierRatingsRequest;
import com.acme.suppliers.v1.ListPendingSupplierRatingsResponse;
import com.acme.suppliers.v1.PreviewSupplierRatingRequest;
import com.acme.suppliers.v1.PreviewSupplierRatingResponse;
import com.acme.suppliers.v1.RecallSupplierContractRequest;
import com.acme.suppliers.v1.RecallSupplierContractResponse;
import com.acme.suppliers.v1.RecallSupplierRatingRequest;
import com.acme.suppliers.v1.RecallSupplierRatingResponse;
import com.acme.suppliers.v1.RejectSupplierRatingRequest;
import com.acme.suppliers.v1.RejectSupplierRatingResponse;
import com.acme.suppliers.v1.ResolveSupplierContractsRequest;
import com.acme.suppliers.v1.ResolveSupplierContractsResponse;
import com.acme.suppliers.v1.ResolveSuppliersRequest;
import com.acme.suppliers.v1.ResolveSuppliersResponse;
import com.acme.suppliers.v1.SubmitSupplierRatingRequest;
import com.acme.suppliers.v1.SubmitSupplierRatingResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierContractLine;
import com.acme.suppliers.v1.SupplierContractLineTax;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
import com.acme.suppliers.v1.ValidateSupplierRiskRequest;
import com.acme.suppliers.v1.ValidateSupplierRiskResponse;
import com.google.protobuf.Any;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class SupplierRatingApprovalApplicationTest {
    @Test
    void submitSupplierRatingPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new SuppliersRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveSupplier(String tenant, Supplier value) {}
            public void saveSupplierContract(String tenant, SupplierContract value) {}
            public void saveSupplierRating(String tenant, SupplierRating value) {}
            public void saveSupplierRisk(String tenant, SupplierRisk value) {}
        };
        var listener = new SupplierRatingApprovalApplicationEvents(AssetTransferApprovalServiceGrpc.newStub(channel), CustomerSegmentValidationServiceGrpc.newStub(channel), SupplierContractQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof SupplierRatingApprovalApplication.SubmitSupplierRatingStage3Requested command) {
                listener.on(command);
            }
            else if (event instanceof SupplierRatingApprovalApplication.EscalateSupplierRatingStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof SupplierRatingApprovalApplication.GetSupplierRatingApprovalStatusStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new SupplierRatingApprovalApplication(
                MandateApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationApprovalServiceGrpc.newBlockingStub(channel),
                ContractClauseApprovalServiceGrpc.newBlockingStub(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newFutureStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                EmployeeQueryServiceGrpc.newBlockingStub(channel),
                events,
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                PostingBatchHistoryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierContractApprovalServiceGrpc.newFutureStub(channel),
                SupplierContractQueryServiceGrpc.newBlockingStub(channel),
                SupplierQueryServiceGrpc.newBlockingStub(channel),
                SupplierRiskValidationServiceGrpc.newBlockingStub(channel),
                SupplierWorkflowServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SubmitSupplierRatingRequest.getDefaultInstance());
        var response = application.submitSupplierRating(request);
        var sent0 = channel.request("acme.ledger.v1.FiscalPeriodNotificationService/UnsubscribeFiscalPeriodUpdates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent0, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        var sent1 = channel.request("acme.ledger.v1.PostingBatchHistoryService/AnnotatePostingBatchVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating", "lines", "tax", "amount"),
                ProbeSupport.value(sent1, "posting_batch", "lines", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating", "lines", "attributes"),
                ProbeSupport.value(sent1, "posting_batch", "lines", "attributes"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating", "lines", "unit_price"),
                ProbeSupport.value(sent1, "posting_batch", "lines", "unit_price"));
        var sent2 = channel.request("acme.suppliers.v1.SupplierRiskValidationService/ValidateSupplierRisk");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating", "counterparty", "email"),
                ProbeSupport.value(sent2, "supplier_risk", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodNotificationService/UnsubscribeFiscalPeriodUpdates"), "fiscal_period", "audit", "updated_by"),
                ProbeSupport.value(sent2, "supplier_risk", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating", "audit", "created_at"),
                ProbeSupport.value(sent2, "supplier_risk", "audit", "created_at"));
        var sent3 = channel.request("acme.suppliers.v1.SupplierContractQueryService/ResolveSupplierContracts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodNotificationService/UnsubscribeFiscalPeriodUpdates"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRiskValidationService/ValidateSupplierRisk"), "supplier_risk", "requester", "email"),
                ProbeSupport.value(response, "supplier_rating", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierContractQueryService/ResolveSupplierContracts"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierContractQueryService/ResolveSupplierContracts"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating_id"),
                ProbeSupport.value(response, "supplier_rating", "supplier_rating_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating", "metadata"),
                ProbeSupport.value(response, "supplier_rating", "metadata"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new SupplierRatingApprovalApplicationEvents(AssetTransferApprovalServiceGrpc.newStub(channel), CustomerSegmentValidationServiceGrpc.newStub(channel), SupplierContractQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ResolveSupplierContractsResponse>();
        listener.on(new SupplierRatingApprovalApplication.SubmitSupplierRatingStage3Requested(ProbeSupport.sample(ResolveSupplierContractsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<ValidateCustomerSegmentResponse>();
        listener.on(new SupplierRatingApprovalApplication.EscalateSupplierRatingStage0Requested(ProbeSupport.sample(ValidateCustomerSegmentRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        var completion2 = new CompletableFuture<SubmitAssetTransferResponse>();
        listener.on(new SupplierRatingApprovalApplication.GetSupplierRatingApprovalStatusStage1Requested(ProbeSupport.sample(SubmitAssetTransferRequest.getDefaultInstance()), completion2, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion2.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion2.getNow(null));
    }
}
