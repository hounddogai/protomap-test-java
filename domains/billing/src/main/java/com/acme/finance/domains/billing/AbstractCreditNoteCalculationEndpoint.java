package com.acme.finance.domains.billing;

import com.acme.billing.v1.CalculateCreditNoteRequest;
import com.acme.billing.v1.CalculateCreditNoteResponse;
import com.acme.billing.v1.CompareCreditNoteOptionsRequest;
import com.acme.billing.v1.CompareCreditNoteOptionsResponse;
import com.acme.billing.v1.CreditNoteCalculationServiceGrpc;
import com.acme.billing.v1.EstimateCreditNoteRequest;
import com.acme.billing.v1.EstimateCreditNoteResponse;
import com.acme.billing.v1.ExplainCreditNoteCalculationRequest;
import com.acme.billing.v1.ExplainCreditNoteCalculationResponse;
import com.acme.billing.v1.GetCreditNoteRatesRequest;
import com.acme.billing.v1.GetCreditNoteRatesResponse;
import com.acme.billing.v1.QuoteCreditNoteRequest;
import com.acme.billing.v1.QuoteCreditNoteResponse;
import com.acme.billing.v1.RecalculateCreditNotesRequest;
import com.acme.billing.v1.RecalculateCreditNotesResponse;
import com.acme.billing.v1.SimulateCreditNoteRequest;
import com.acme.billing.v1.SimulateCreditNoteResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Computes the figures of a credit note, such as totals, taxes, and estimates.
 */
public abstract class AbstractCreditNoteCalculationEndpoint extends CreditNoteCalculationServiceGrpc.CreditNoteCalculationServiceImplBase {
    protected final CreditNoteCalculationApplication application;

    protected AbstractCreditNoteCalculationEndpoint(CreditNoteCalculationApplication application) {
        this.application = application;
    }

    /**
     * Calculates a credit note.
     */
    @Override
    public void calculateCreditNote(CalculateCreditNoteRequest request, StreamObserver<CalculateCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.calculateCreditNote(request));
    }

    /**
     * Estimates a credit note.
     */
    @Override
    public void estimateCreditNote(EstimateCreditNoteRequest request, StreamObserver<EstimateCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.estimateCreditNote(request));
    }

    /**
     * Quotes a credit note.
     */
    @Override
    public void quoteCreditNote(QuoteCreditNoteRequest request, StreamObserver<QuoteCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.quoteCreditNote(request));
    }

    /**
     * Compares a credit note.
     */
    @Override
    public void compareCreditNoteOptions(CompareCreditNoteOptionsRequest request, StreamObserver<CompareCreditNoteOptionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareCreditNoteOptions(request));
    }

    /**
     * Recalculates a credit note.
     */
    @Override
    public void recalculateCreditNotes(RecalculateCreditNotesRequest request, StreamObserver<RecalculateCreditNotesResponse> observer) {
        RpcSupport.unary(observer, () -> application.recalculateCreditNotes(request));
    }

    /**
     * Simulates a credit note.
     */
    @Override
    public void simulateCreditNote(SimulateCreditNoteRequest request, StreamObserver<SimulateCreditNoteResponse> observer) {
        RpcSupport.unary(observer, () -> application.simulateCreditNote(request));
    }

    /**
     * Explains a credit note.
     */
    @Override
    public void explainCreditNoteCalculation(ExplainCreditNoteCalculationRequest request, StreamObserver<ExplainCreditNoteCalculationResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainCreditNoteCalculation(request));
    }

    /**
     * Returns a credit note.
     */
    @Override
    public void getCreditNoteRates(GetCreditNoteRatesRequest request, StreamObserver<GetCreditNoteRatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getCreditNoteRates(request));
    }
}
