/*
 * Copyright 2008-2013 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package predicatedetectionlib.versioning;

import java.util.SortedSet;

import com.google.common.collect.Sets;

public class VectorClockUtils {

    /**
     * Compare two VectorClocks, the outcomes will be one of the following: <br>
     * -- Clock 1 is BEFORE clock 2, if there exists an nodeId such that
     * c1(nodeId) <= c2(nodeId) and there does not exist another nodeId such
     * that c1(nodeId) > c2(nodeId). <br>
     * -- Clock 1 is CONCURRENT to clock 2 if there exists an nodeId, nodeId2
     * such that c1(nodeId) < c2(nodeId) and c1(nodeId2) > c2(nodeId2)<br>
     * -- Clock 1 is AFTER clock 2 otherwise
     *
     * @param v1 The first MonitorVectorClock
     * @param v2 The second MonitorVectorClock
     */
    public static Occurred compare(MonitorVectorClock v1, MonitorVectorClock v2) {
        if(v1 == null || v2 == null)
            throw new IllegalArgumentException("Can't compare null vector clocks!");
        // We do two checks: v1 <= v2 and v2 <= v1 if both are true then
        boolean v1Bigger = false;
        boolean v2Bigger = false;

        SortedSet<Short> v1Nodes = v1.getVersionMap().navigableKeySet();
        SortedSet<Short> v2Nodes = v2.getVersionMap().navigableKeySet();
        // get clocks(nodeIds) that both v1 and v2 has
        SortedSet<Short> commonNodes = Sets.newTreeSet(v1Nodes);
        commonNodes.retainAll(v2Nodes);
        // if v1 has more nodes than common nodes
        // v1 has clocks that v2 does not
        if(v1Nodes.size() > commonNodes.size()) {
            v1Bigger = true;
        }
        // if v2 has more nodes than common nodes
        // v2 has clocks that v1 does not
        if(v2Nodes.size() > commonNodes.size()) {
            v2Bigger = true;
        }
        // compare the common parts
        for(Short nodeId: commonNodes) {
            // no need to compare more
            if(v1Bigger && v2Bigger) {
                break;
            }
            long v1Version = v1.getVersionMap().get(nodeId);
            long v2Version = v2.getVersionMap().get(nodeId);
            if(v1Version > v2Version) {
                v1Bigger = true;
            } else if(v1Version < v2Version) {
                v2Bigger = true;
            }
        }

        /*
         * This is the case where they are equal. Consciously return BEFORE, so
         * that the we would throw back an ObsoleteVersionException for online
         * writes with the same clock.
         */
        if(!v1Bigger && !v2Bigger)
            return Occurred.BEFORE;
        /* This is the case where v1 is a successor clock to v2 */
        else if(v1Bigger && !v2Bigger)
            return Occurred.AFTER;
        /* This is the case where v2 is a successor clock to v1 */
        else if(!v1Bigger && v2Bigger)
            return Occurred.BEFORE;
        /* This is the case where both clocks are parallel to one another */
        else
            return Occurred.CONCURRENTLY;
    }


}
