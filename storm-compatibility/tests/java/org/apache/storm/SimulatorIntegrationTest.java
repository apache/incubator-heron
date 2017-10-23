package org.apache.storm;

import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NotAliveException;
import org.junit.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class SimulatorIntegrationTest {

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private PrintStream saveOut;
    private PrintStream errOut;

    @Test
    public void runTopology() throws InterruptedException, AlreadyAliveException, NotAliveException, InvalidTopologyException {
        WordCountTopology topology = new WordCountTopology();
        topology.execute();
        Assert.assertTrue(outContent.toString().contains("Execute Entry"));
        Assert.assertTrue(outContent.toString().contains("Emitting a count of"));
    }

    @Before
    public void setUpStreams() {
        saveOut = System.out;
        errOut = System.err;
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
    }

    @After
    public void cleanUpStreams() {
        System.setOut(saveOut);
        System.setErr(errOut);
    }
}
