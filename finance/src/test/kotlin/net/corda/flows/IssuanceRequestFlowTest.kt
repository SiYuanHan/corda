package net.corda.flows

import net.corda.contracts.asset.Cash
import net.corda.core.concurrent.CordaFuture
import net.corda.core.contracts.Amount
import net.corda.core.contracts.DOLLARS
import net.corda.core.contracts.currency
import net.corda.core.flows.FlowException
import net.corda.core.internal.concurrent.transpose
import net.corda.core.node.services.Vault
import net.corda.core.node.services.trackBy
import net.corda.core.node.services.vault.QueryCriteria
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.getOrThrow
import net.corda.testing.contracts.calculateRandomlySizedAmounts
import net.corda.testing.expect
import net.corda.testing.expectEvents
import net.corda.testing.node.MockNetwork
import net.corda.testing.node.MockNetwork.MockNode
import net.corda.testing.sequence
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.util.*
import kotlin.test.assertFailsWith

@RunWith(Parameterized::class)
class IssuanceRequestFlowTest(val anonymous: Boolean) {
    companion object {
        @Parameterized.Parameters(name = "anonymous={0}")
        @JvmStatic
        fun params() = listOf(false, true)
    }

    lateinit var mockNet: MockNetwork
    lateinit var notaryNode: MockNode
    lateinit var bankOfCordaNode: MockNode
    lateinit var bankClientNode: MockNode

    @Before
    fun start() {
        mockNet = MockNetwork(threadPerNode = true)
        val basketOfNodes = mockNet.createSomeNodes(2)
        bankOfCordaNode = basketOfNodes.partyNodes[0]
        bankClientNode = basketOfNodes.partyNodes[1]
        notaryNode = basketOfNodes.notaryNode
        bankOfCordaNode.registerInitiatedFlow(IssuanceRequestHandlerFlow::class.java)
    }

    @After
    fun cleanUp() {
        mockNet.stopNodes()
    }

    @Test
    fun `test issuer flow`() {
        val (vaultUpdatesBoc, vaultUpdatesBankClient) = bankOfCordaNode.database.transaction {
            // Register for vault updates
            val criteria = QueryCriteria.VaultQueryCriteria(status = Vault.StateStatus.ALL)
            val (_, vaultUpdatesBoc) = bankOfCordaNode.services.vaultQueryService.trackBy<Cash.State>(criteria)
            val (_, vaultUpdatesBankClient) = bankClientNode.services.vaultQueryService.trackBy<Cash.State>(criteria)

            val issuerResult = requestCashIssuance(1000000.DOLLARS, OpaqueBytes.of(123))
            issuerResult.get()

            Pair(vaultUpdatesBoc, vaultUpdatesBankClient)
        }

        // Check Bank of Corda Vault Updates
        vaultUpdatesBoc.expectEvents {
            sequence(
                    // ISSUE
                    expect { update ->
                        require(update.consumed.isEmpty()) { "Expected 0 consumed states, actual: $update" }
                        require(update.produced.size == 1) { "Expected 1 produced states, actual: $update" }
                        val issued = update.produced.single().state.data
                        require(issued.owner.owningKey in bankOfCordaNode.services.keyManagementService.keys)
                    },
                    // MOVE
                    expect { update ->
                        require(update.consumed.size == 1) { "Expected 1 consumed states, actual: $update" }
                        require(update.produced.isEmpty()) { "Expected 0 produced states, actual: $update" }
                    }
            )
        }

        // Check Bank Client Vault Updates
        vaultUpdatesBankClient.expectEvents {
            // MOVE
            expect { (consumed, produced) ->
                require(consumed.isEmpty()) { consumed.size }
                require(produced.size == 1) { produced.size }
                val paidState = produced.single().state.data
                require(paidState.owner.owningKey in bankClientNode.services.keyManagementService.keys)
            }
        }
    }

    @Test
    fun `test issuer flow rejects restricted`() {
        // try to issue an amount of a restricted currency
        assertFailsWith<FlowException> {
            requestCashIssuance(Amount(100000L, currency("BRL")), OpaqueBytes.of(123)).getOrThrow()
        }
    }

    @Test
    fun `test issue flow to self`() {
        val vaultUpdatesBoc = bankOfCordaNode.database.transaction {
            val criteria = QueryCriteria.VaultQueryCriteria(status = Vault.StateStatus.ALL)
            val (_, vaultUpdatesBoc) = bankOfCordaNode.services.vaultQueryService.trackBy<Cash.State>(criteria)

            // using default IssueTo Party Reference
            requestCashIssuance(1000000.DOLLARS, OpaqueBytes.of(123), issuanceRequester = bankOfCordaNode).getOrThrow()
            vaultUpdatesBoc
        }

        // Check Bank of Corda Vault Updates
        vaultUpdatesBoc.expectEvents {
            sequence(
                    // ISSUE
                    expect { update ->
                        require(update.consumed.isEmpty()) { "Expected 0 consumed states, actual: $update" }
                        require(update.produced.size == 1) { "Expected 1 produced states, actual: $update" }
                    }
            )
        }
    }

    @Test
    fun `test concurrent issuer flow`() {
        // this test exercises the Cashflow issue and move subflows to ensure consistent spending of issued states
        val amount = 10000.DOLLARS
        val amounts = calculateRandomlySizedAmounts(10000.DOLLARS, 10, 10, Random())
        val handles = amounts.map { pennies ->
            requestCashIssuance(Amount(pennies, amount.token), OpaqueBytes.of(123))
        }
        handles.transpose().getOrThrow()
    }

    private fun requestCashIssuance(amount: Amount<Currency>,
                                    ref: OpaqueBytes,
                                    issuanceRequester: MockNode = bankClientNode): CordaFuture<IssuanceResult> {
        return issuanceRequester.services.startFlow(IssuanceRequestFlow(
                amount,
                ref,
                bankOfCordaNode.info.legalIdentity,
                notaryNode.info.notaryIdentity,
                anonymous)
        ).resultFuture
    }
}