package net.corda.docs

import co.paralleluniverse.fibers.Suspendable
import net.corda.contracts.asset.Cash
import net.corda.core.contracts.Amount
import net.corda.core.contracts.Issued
import net.corda.core.contracts.StateAndRef
import net.corda.core.contracts.withoutIssuer
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.TransactionSignature
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.InitiatedBy
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.*
import net.corda.core.identity.Party
import net.corda.core.node.ServiceHub
import net.corda.core.node.services.vault.QueryCriteria
import net.corda.core.node.services.vault.builder
import net.corda.core.serialization.CordaSerializable
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.unwrap
import net.corda.schemas.CashSchemaV1
import java.util.*

@CordaSerializable
private data class FxRequest(val tradeId: String,
                             val amount: Amount<Issued<Currency>>,
                             val owner: Party,
                             val counterparty: Party,
                             val notary: Party? = null)

// DOCSTART 1
// This is equivalent to the Cash.generateSpend
// Which is brought here to make the filtering logic more visible in the example
private fun gatherOurInputs(serviceHub: ServiceHub,
                            lockId: UUID,
                            amountRequired: Amount<Issued<Currency>>,
                            notary: Party?): Pair<List<StateAndRef<Cash.State>>, Long> {
    // extract our identity for convenience
    val ourKeys = serviceHub.keyManagementService.keys
    val ourParties = ourKeys.map { serviceHub.identityService.partyFromKey(it) ?: throw IllegalStateException("Unable to resolve party from key") }
    val fungibleCriteria = QueryCriteria.FungibleAssetQueryCriteria(owner = ourParties)

    val notaryName = if (notary != null) notary.name else serviceHub.networkMapCache.getAnyNotary()!!.name
    val vaultCriteria: QueryCriteria = QueryCriteria.VaultQueryCriteria(notaryName = listOf(notaryName))

    val logicalExpression = builder { CashSchemaV1.PersistentCashState::currency.equal(amountRequired.token.product.currencyCode) }
    val cashCriteria = QueryCriteria.VaultCustomQueryCriteria(logicalExpression)

    val fullCriteria = fungibleCriteria.and(vaultCriteria).and(cashCriteria)

    val eligibleStates = serviceHub.vaultService.tryLockFungibleStatesForSpending<Cash.State, Currency>(lockId, fullCriteria, amountRequired.withoutIssuer(), Cash.State::class.java)

    check(eligibleStates.isNotEmpty()) { "Insufficient funds" }
    val amount = eligibleStates.fold(0L) { tot, x -> tot + x.state.data.amount.quantity }
    val change = amount - amountRequired.quantity

    return Pair(eligibleStates, change)
}
// DOCEND 1

private fun prepareOurInputsAndOutputs(serviceHub: ServiceHub, lockId: UUID, request: FxRequest): Pair<List<StateAndRef<Cash.State>>, List<Cash.State>> {
    // Create amount with correct issuer details
    val sellAmount = request.amount

    // DOCSTART 2
    // Gather our inputs. We would normally use VaultService.generateSpend
    // to carry out the build in a single step. To be more explicit
    // we will use query manually in the helper function below.
    // Putting this into a non-suspendable function also prevents issues when
    // the flow is suspended.
    val (inputs, residual) = gatherOurInputs(serviceHub, lockId, sellAmount, request.notary)

    // Build and an output state for the counterparty
    val transferedFundsOutput = Cash.State(sellAmount, request.counterparty)

    val outputs = if (residual > 0L) {
        // Build an output state for the residual change back to us
        val residualAmount = Amount(residual, sellAmount.token)
        val residualOutput = Cash.State(residualAmount, serviceHub.myInfo.legalIdentity)
        listOf(transferedFundsOutput, residualOutput)
    } else {
        listOf(transferedFundsOutput)
    }
    return Pair(inputs, outputs)
    // DOCEND 2
}

// A flow representing creating a transaction that
// carries out exchange of cash assets.
@InitiatingFlow
class ForeignExchangeFlow(val tradeId: String,
                          val baseCurrencyAmount: Amount<Issued<Currency>>,
                          val quoteCurrencyAmount: Amount<Issued<Currency>>,
                          val baseCurrencyBuyer: Party,
                          val baseCurrencySeller: Party) : FlowLogic<SecureHash>() {
    @Suspendable
    override fun call(): SecureHash {
        // Select correct sides of the Fx exchange to query for.
        // Specifically we own the assets we wish to sell.
        // Also prepare the other side query
        val (localRequest, remoteRequest) = if (baseCurrencySeller == serviceHub.myInfo.legalIdentity) {
            val local = FxRequest(tradeId, baseCurrencyAmount, baseCurrencySeller, baseCurrencyBuyer)
            val remote = FxRequest(tradeId, quoteCurrencyAmount, baseCurrencyBuyer, baseCurrencySeller)
            Pair(local, remote)
        } else if (baseCurrencyBuyer == serviceHub.myInfo.legalIdentity) {
            val local = FxRequest(tradeId, quoteCurrencyAmount, baseCurrencyBuyer, baseCurrencySeller)
            val remote = FxRequest(tradeId, baseCurrencyAmount, baseCurrencySeller, baseCurrencyBuyer)
            Pair(local, remote)
        } else throw IllegalArgumentException("Our identity must be one of the parties in the trade.")

        // Call the helper method to identify suitable inputs and make the outputs
        val (outInputStates, ourOutputStates) = prepareOurInputsAndOutputs(serviceHub, runId.uuid, localRequest)

        // identify the notary for our states
        val notary = outInputStates.first().state.notary
        // ensure request to other side is for a consistent notary
        val remoteRequestWithNotary = remoteRequest.copy(notary = notary)

        // Send the request to the counterparty to verify and call their version of prepareOurInputsAndOutputs
        // Then they can return their candidate states
        send(remoteRequestWithNotary.owner, remoteRequestWithNotary)
        val theirInputStates = subFlow(ReceiveStateAndRefFlow<Cash.State>(remoteRequestWithNotary.owner))
        val theirOutputStates = receive<List<Cash.State>>(remoteRequestWithNotary.owner).unwrap {
            require(theirInputStates.all { it.state.notary == notary }) {
                "notary of remote states must be same as for our states"
            }
            require(theirInputStates.all { it.state.data.amount.token == remoteRequestWithNotary.amount.token }) {
                "Inputs not of the correct currency"
            }
            require(it.all { it.amount.token == remoteRequestWithNotary.amount.token }) {
                "Outputs not of the correct currency"
            }
            require(theirInputStates.map { it.state.data.amount.quantity }.sum()
                    >= remoteRequestWithNotary.amount.quantity) {
                "the provided inputs don't provide sufficient funds"
            }
            require(it.filter { it.owner == serviceHub.myInfo.legalIdentity }.
                    map { it.amount.quantity }.sum() == remoteRequestWithNotary.amount.quantity) {
                "the provided outputs don't provide the request quantity"
            }
            it // return validated response
        }

        // having collated the data create the full transaction.
        val signedTransaction = buildTradeProposal(outInputStates, ourOutputStates, theirInputStates, theirOutputStates)

        // pass transaction details to the counterparty to revalidate and confirm with a signature
        // Allow otherParty to access our data to resolve the transaction.
        subFlow(SendTransactionFlow(remoteRequestWithNotary.owner, signedTransaction))
        val allPartySignedTx = receive<TransactionSignature>(remoteRequestWithNotary.owner).unwrap {
            val withNewSignature = signedTransaction + it
            // check all signatures are present except the notary
            withNewSignature.verifySignaturesExcept(withNewSignature.tx.notary!!.owningKey)

            // This verifies that the transaction is contract-valid, even though it is missing signatures.
            // In a full solution there would be states tracking the trade request which
            // would be included in the transaction and enforce the amounts and tradeId
            withNewSignature.tx.toLedgerTransaction(serviceHub).verify()

            withNewSignature // return the almost complete transaction
        }

        // Initiate the standard protocol to notarise and distribute to the involved parties.
        subFlow(FinalityFlow(allPartySignedTx, setOf(baseCurrencyBuyer, baseCurrencySeller)))

        return allPartySignedTx.id
    }

    // DOCSTART 3
    private fun buildTradeProposal(ourInputStates: List<StateAndRef<Cash.State>>,
                                   ourOutputState: List<Cash.State>,
                                   theirInputStates: List<StateAndRef<Cash.State>>,
                                   theirOutputState: List<Cash.State>): SignedTransaction {
        // This is the correct way to create a TransactionBuilder,
        // do not construct directly.
        // We also set the notary to match the input notary
        val builder = TransactionBuilder(ourInputStates.first().state.notary)

        // Add the move commands and key to indicate all the respective owners and need to sign
        val ourSigners = ourInputStates.map { it.state.data.owner.owningKey }.toSet()
        val theirSigners = theirInputStates.map { it.state.data.owner.owningKey }.toSet()
        builder.addCommand(Cash.Commands.Move(), (ourSigners + theirSigners).toList())

        // Build and add the inputs and outputs
        builder.withItems(*ourInputStates.toTypedArray())
        builder.withItems(*theirInputStates.toTypedArray())
        builder.withItems(*ourOutputState.toTypedArray())
        builder.withItems(*theirOutputState.toTypedArray())

        // We have already validated their response and trust our own data
        // so we can sign. Note the returned SignedTransaction is still not fully signed
        // and would not pass full verification yet.
        return serviceHub.signInitialTransaction(builder, ourSigners.single())
    }
    // DOCEND 3
}

@InitiatedBy(ForeignExchangeFlow::class)
class ForeignExchangeRemoteFlow(val source: Party) : FlowLogic<Unit>() {
    @Suspendable
    override fun call() {
        // Initial receive from remote party
        val request = receive<FxRequest>(source).unwrap {
            // We would need to check that this is a known trade ID here!
            // Also that the amounts and source are correct with the trade details.
            // In a production system there would be other Corda contracts tracking
            // the lifecycle of the Fx trades which would be included in the transaction

            // Check request is for us
            require(serviceHub.myInfo.legalIdentity == it.owner) {
                "Request does not include the correct counterparty"
            }
            require(source == it.counterparty) {
                "Request does not include the correct counterparty"
            }
            it // return validated request
        }

        // Gather our inputs. We would normally use VaultService.generateSpend
        // to carry out the build in a single step. To be more explicit
        // we will use query manually in the helper function below.
        // Putting this into a non-suspendable function also prevent issues when
        // the flow is suspended.
        val (ourInputState, ourOutputState) = prepareOurInputsAndOutputs(serviceHub, runId.uuid, request)

        // Send back our proposed states and await the full transaction to verify
        val ourKey = serviceHub.keyManagementService.filterMyKeys(ourInputState.flatMap { it.state.data.participants }.map { it.owningKey }).single()
        // SendStateAndRefFlow allows otherParty to access our transaction data to resolve the transaction.
        subFlow(SendStateAndRefFlow(source, ourInputState))
        send(source, ourOutputState)
        val proposedTrade = subFlow(ReceiveTransactionFlow(source, checkSufficientSignatures = false)).let {
            val wtx = it.tx
            // check all signatures are present except our own and the notary
            it.verifySignaturesExcept(ourKey, wtx.notary!!.owningKey)

            // This verifies that the transaction is contract-valid, even though it is missing signatures.
            // In a full solution there would be states tracking the trade request which
            // would be included in the transaction and enforce the amounts and tradeId
            wtx.toLedgerTransaction(serviceHub).verify()
            it // return the SignedTransaction
        }

        // assuming we have completed state and business level validation we can sign the trade
        val ourSignature = serviceHub.createSignature(proposedTrade, ourKey)

        // send the other side our signature.
        send(source, ourSignature)
        // N.B. The FinalityProtocol will be responsible for Notarising the SignedTransaction
        // and broadcasting the result to us.
    }
}
