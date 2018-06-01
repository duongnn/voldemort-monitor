package voldemort.server.protocol.vold;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import voldemort.VoldemortException;
import voldemort.predicatedetection.PredicateDetection;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class PutRequestHandler extends ClientRequestHandler {

    ByteArray key;
    byte[] value;
    byte[] transforms;
    VectorClock clock;

    public PutRequestHandler(Store<ByteArray, byte[], byte[]> store, int protocolVersion) {
        super(store, protocolVersion);
    }
    
    public static boolean isCompleteRequest(DataInputStream inputStream,
                                            ByteBuffer buffer,
                                            int protocolVersion)
            throws IOException, VoldemortException {
        if(!ClientRequestHandler.skipByteArray(inputStream, buffer))
            return false;

        if(!ClientRequestHandler.skipByteArray(inputStream, buffer))
            return false;

        ClientRequestHandler.readSingleTransform(inputStream, protocolVersion);
        return true;
    }

    @Override
    public boolean parseRequest(DataInputStream inputStream) throws IOException {
        key = ClientRequestHandler.readKey(inputStream);
        int valueSize = inputStream.readInt();
        clock = VectorClock.createVectorClock(inputStream);
        int vectorClockSize = clock.sizeInBytes();
        value = new byte[valueSize - vectorClockSize];
        ByteUtils.read(inputStream, value);

        transforms = ClientRequestHandler.readSingleTransform(inputStream, protocolVersion);
        return false;
    }

    @Override
    public void processRequest() throws VoldemortException {
        store.put(key, new Versioned<byte[]>(value, clock), transforms);

        // Duong predicate detection
        // After a put request, values could be changed
        // We need to check for predicate detection

        if(PredicateDetection.isActivated()) {
//            // in order to reduce detection overhead, we only perform
//            // predicate detection when the updated variable is in the list of interest
//            if(PredicateDetection.LocalPredicateDetection.isVariableInCombinedVariableList(key)) {
//                PredicateDetection.LocalPredicateDetection.checkAndSend(store);
//            }

            PredicateDetection.LocalPredicateDetection.checkAndSend(key, clock, value);

        }

    }

    @Override
    public void writeResponse(DataOutputStream outputStream) throws IOException {
        outputStream.writeShort(0);

        // attach current HVC to message, then increase HVC timestamp
        PredicateDetection.HvcProcessing.writeServerHvcTimestamp(outputStream);


        // advance the interval by update startPoint to current HVC
        PredicateDetection.LocalPredicateDetection.advanceStartPointToCurrentHvc();

    }

    @Override
    public int getResponseSize() {
        return 2 + PredicateDetection.HvcProcessing.getHvcTimestampSizeInBytes();
    }

    @Override
    public String getDebugMessage() {
        return "Operation PUT " + ClientRequestHandler.getDebugMessageForKey(key) + " ValueHash"
               + (value == null ? "null" : value.hashCode()) + " ClockSize " + clock.sizeInBytes()
               + " ValueSize " + (value == null ? "null" : value.length);
    }

}
