package predicatedetectionlib.monitor;

import predicatedetectionlib.common.HVC;
import predicatedetectionlib.common.HvcInterval;
import predicatedetectionlib.common.LocalSnapshot;

import java.util.Vector;

/**
 * Global snapshot: which is an array of local snapshot. Each local snapshot
 * is from one server.
 * Global snapshot also includes an array of colors of servers in the global snapshot
 * RED means you happens before some candidate is global snapshot
 * GREEN means you does not happen before anyone in global snapshot
 * all GREEN means the globalSnapshot is consistent
 *
 * Created by duongnn on 6/21/17.
 */
public class GlobalSnapshot {
    private final int numberOfProcesses;
    private long detectionTime;
    private long responseTime;

    private long physicalResponseTime;

    private Vector<LocalSnapshot> candidateSnapshots;
    private Vector<CandidateColor> candidateColors;

    // originally, global snapshot consists of "dump" local snapshots
    public GlobalSnapshot(int numberOfProcesses){
        this.numberOfProcesses = numberOfProcesses;

        candidateColors = new Vector<>(numberOfProcesses);
        candidateSnapshots = new Vector<>(numberOfProcesses);

        for(int i = 0; i < numberOfProcesses; i++){
            // at first, global snapshot consists of dump candidates
            candidateSnapshots.addElement(LocalSnapshot.createPlaceHolderLocalSnapshot(numberOfProcesses, i));

            // also all processes are marked as red
            candidateColors.addElement(CandidateColor.RED);
        }
    }

    public LocalSnapshot getProcessCandidate(int procId){
        return candidateSnapshots.elementAt(procId);
    }

    // get the next red process
    // return -1 if all processes are green
    public int getNextRedProcess(){
        for(int i = 0; i < numberOfProcesses; i++){
            if(candidateColors.elementAt(i) == CandidateColor.RED) {

                // Duong debug
                // System.out.println(" getNextRedProcess: return process " + i);

                return i;
            }
        }

        // Duong debug
//        System.out.println(" getNextRedProcess: all processes are GREEN");

        return -1;
    }

    /**
     * replace candidate for a given process
     * @param nextCandidate new candidate
     * @param procId     process id
     */
    public void replaceCandidate(LocalSnapshot nextCandidate, int procId){
        candidateSnapshots.setElementAt(nextCandidate, procId);
    }

    // update color of global snapshot after process procId has a new candidate
    public void updateColorAfterReplaceCandidate(int procId){

        HvcInterval myInterval = candidateSnapshots.elementAt(procId).getHvcInterval();

        // set color of new candidate as green
        candidateColors.setElementAt(CandidateColor.GREEN, procId);

        for(int i = 0; i < numberOfProcesses; i++){
            if(i != procId){
                HvcInterval otherInterval = candidateSnapshots.elementAt(i).getHvcInterval();
                int compareResult = myInterval.happenBefore(otherInterval, 2 * Monitor.getEpsilon());

                if(compareResult == HVC.BEFORE){
                    candidateColors.setElementAt(CandidateColor.RED, procId);
                }else if (compareResult == HVC.AFTER){
                    candidateColors.setElementAt(CandidateColor.RED, i);
                }
            }
        }
    }

    public void outputToFile(){
        Monitor.outFile.println(toString());
    }

    public String toString(){
        StringBuilder result = new StringBuilder();

        String aString = String.format("GlobalSnapshot: %n")+
                String.format("    epsilon         = %,d %n", Monitor.getEpsilon()) +
                String.format("    numberOfProcess = %d %n", numberOfProcesses) +
                String.format("    detection time  = %,d %n", getDetectionTime()) +
                String.format("    response time   = %,d %n", getResponseTime());

        result.append(aString);
        for(int i = 0; i < numberOfProcesses; i++){
            result.append("    color [" + i + "] = " + candidateColors.elementAt(i) + "\n");
            result.append(candidateSnapshots.elementAt(i).toString(6));
        }

        return result.toString();

    }

    // after finding a consistent cut, you restart so that
    // you can find the next consistent cut
    // there are several way to do it
    //   set the smallest as red
    //   set everyone as red
    public void restart(){
        // find the snapshot with smallest startPoint and mark as red
        int smallestProc = findSmallestSnapshot();

        markProcessAsRed(smallestProc);
    }

    public int findSmallestSnapshot(){
        int smallestProc = 0;
        long smallestStartPoint = candidateSnapshots.elementAt(smallestProc).getHvcInterval().getStartPoint().getPrimaryElement().getTimeValue();

        for(int i = 0; i < numberOfProcesses; i++){
            long iStartPoint = candidateSnapshots.elementAt(i).getHvcInterval().getStartPoint().getPrimaryElement().getTimeValue();
            if (smallestStartPoint > iStartPoint){
                smallestProc = i;
                smallestStartPoint = iStartPoint;
            }
        }

        return smallestProc;
    }

    /**
     * estimate the time since the predicate appears to the time it is detected by the monitor
     *      for the HVC interval from each process, take the startPoint (since we wants to know when
     *      the local predicate is true
     *      among those startPoints, take the latest one, since it is considered as the moment when
     *      the global predicate is true
     *      subtract monitor current time and that latest startPoint will be the detectionTime
     *
     */
    public void computeResponseTime(){
        int i = 0;

//        // Note: since HVC could grow unbounded from physical time when there are a lot of messages
//        //       response time should be computed from the physical time that is also contained in
//        //       the local snapshot
//        long latestStartPoint = candidateSnapshots.elementAt(i).getHvcInterval().getStartPoint().getPrimaryElement();
//
//        for(i = 1; i < numberOfProcesses; i++){
//            long currentStartPoint = candidateSnapshots.elementAt(i).getHvcInterval().getStartPoint().getPrimaryElement();
//            if(latestStartPoint < currentStartPoint)
//                latestStartPoint = currentStartPoint;
//        }
//
//        responseTime = getDetectionTime() - latestStartPoint;

        // compute response time based on physical timestamp
        i = 0;
        long latestPhysicalStartTime = candidateSnapshots.elementAt(i).getHvcInterval().getPhysicalStartTime();
        for(i = 1; i < numberOfProcesses; i++){
            long currentPhysicalStartTime = candidateSnapshots.elementAt(i).getHvcInterval().getPhysicalStartTime();
            if(latestPhysicalStartTime < currentPhysicalStartTime)
                latestPhysicalStartTime = currentPhysicalStartTime;
        }

        physicalResponseTime = getDetectionTime() - latestPhysicalStartTime;


        // response time is computed from HVC
        // unit is milliseconds
        i = 0;
        long latestStartPoint = candidateSnapshots.elementAt(i).getHvcInterval().getStartPoint().getPrimaryElement().getTimeValueInMs();
        for(i = 1; i < numberOfProcesses; i++){
            long currentStartPoint = candidateSnapshots.elementAt(i).getHvcInterval().getStartPoint().getPrimaryElement().getTimeValueInMs();
            if(latestStartPoint < currentStartPoint)
                latestStartPoint = currentStartPoint;
        }

        responseTime = getDetectionTime() - latestStartPoint;


    }

    public long getResponseTime(){
        return responseTime;
    }

    public void setDetectionTime(long detectionTime){
        this.detectionTime = detectionTime;
    }

    public long getDetectionTime(){
        return detectionTime;
    }

    public long getPhysicalResponseTime(){
        return physicalResponseTime;
    }

    public int getNumberOfProcesses(){
        return numberOfProcesses;
    }

    public void markProcessAsRed(int pid){
        candidateColors.setElementAt(CandidateColor.RED, pid);
    }
}
