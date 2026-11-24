/*
 * Copyright 2015-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.system_tests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import uk.co.real_logic.artio.decoder.ResendRequestDecoder;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.co.real_logic.artio.system_tests.AbstractGatewayToGatewaySystemTest.TEST_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.awaitIndexerCaughtUp;

/**
 * With persistent sequence numbers, a gap that Artio has asked the counterparty to fill must still be filled after
 * the connection drops before the counterparty has replayed it.
 */
public class InboundGapAcrossReconnectSystemTest extends AbstractMessageBasedAcceptorSystemTest
{
    @Test
    @Timeout(TEST_TIMEOUT_IN_MS)
    public void shouldRequestResendAgainAfterReconnectWhenGapOpenedMidSession() throws IOException
    {
        setup(false, true);
        setupLibrary();

        try (FixConnection connection = FixConnection.initiate(port))
        {
            connection.logon(true);
            testSystem.awaitBlocking(() -> connection.readLogon(1));

            // 2 to 4 never arrive, the next message is 5
            connection.msgSeqNum(5).sendTestRequest("gap");
            testSystem.awaitBlocking(() -> connection.readResendRequest(2, 0));
        }
        // the connection dropped before 2 to 4 were replayed

        awaitReconnectBeginSeqNo(2, 6);
    }

    @Test
    @Timeout(TEST_TIMEOUT_IN_MS)
    public void shouldRequestResendAgainAfterReconnectWhenGapOpenedByLogon() throws IOException
    {
        setup(false, true);
        setupLibrary();

        try (FixConnection connection = FixConnection.initiate(port))
        {
            connection.logon(true);
            testSystem.awaitBlocking(() -> connection.readLogon(1));
            testSystem.awaitBlocking(() -> connection.exchangeTestRequestHeartbeat("first"));
            testSystem.awaitBlocking(connection::logoutAndAwaitReply);
        }

        awaitIndexerCaughtUp(testSystem, mediaDriver.mediaDriver().aeronDirectoryName(), engine, library);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            // 4 to 6 never arrive, the Logon is 7
            connection.msgSeqNum(7).logon(false);
            testSystem.awaitBlocking(() -> connection.readLogon());
            testSystem.awaitBlocking(() -> connection.readResendRequest(4, 0));
        }
        // the connection dropped before 4 to 6 were replayed

        awaitReconnectBeginSeqNo(4, 8);
    }

    private void awaitReconnectBeginSeqNo(final int expectedBeginSeqNo, final int logonMsgSeqNum) throws IOException
    {
        // the framer reads the received sequence number index on logon
        awaitIndexerCaughtUp(testSystem, mediaDriver.mediaDriver().aeronDirectoryName(), engine, library);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            connection.msgSeqNum(logonMsgSeqNum).logon(false);
            testSystem.awaitBlocking(() -> connection.readLogon());

            // A ResendRequest for the gap would come straight after the Logon reply, before anything that answers
            // this TestRequest.
            connection.sendTestRequest("probe");
            final ResendRequestDecoder resendRequest = testSystem.awaitBlocking(
                () -> connection.readMessage(new ResendRequestDecoder()));
            assertEquals(expectedBeginSeqNo, resendRequest.beginSeqNo(), resendRequest.toString());
        }
    }
}
