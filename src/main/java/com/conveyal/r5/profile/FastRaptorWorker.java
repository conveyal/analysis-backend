package com.conveyal.r5.profile;

import com.conveyal.r5.api.util.TransitModes;
import com.conveyal.r5.transit.PickDropType;
import com.conveyal.r5.transit.RouteInfo;
import com.conveyal.r5.transit.TransitLayer;
import com.conveyal.r5.transit.TripPattern;
import com.conveyal.r5.transit.TripSchedule;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.map.TIntIntMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.conveyal.r5.profile.FastRaptorWorker.FrequencyBoardingMode.HALF_HEADWAY;
import static com.conveyal.r5.profile.FastRaptorWorker.FrequencyBoardingMode.MONTE_CARLO;
import static com.conveyal.r5.profile.FastRaptorWorker.FrequencyBoardingMode.UPPER_BOUND;
import static com.google.common.base.Preconditions.checkState;

/**
 * FastRaptorWorker is faster than the old RaptorWorker and made to be more maintainable.
 * It is simpler, as it only focuses on the transit network; see the Propagater class for the methods that extend
 * the travel times from the final transit stop of a trip out to the individual targets.
 *
 * The algorithm used herein is described in
 *
 * Conway, Matthew Wigginton, Andrew Byrd, and Marco van der Linden. “Evidence-Based Transit and Land Use Sketch Planning
 *   Using Interactive Accessibility Methods on Combined Schedule and Headway-Based Networks.” Transportation Research
 *   Record 2653 (2017). doi:10.3141/2653-06.
 *
 * Delling, Daniel, Thomas Pajor, and Renato Werneck. “Round-Based Public Transit Routing,” January 1, 2012.
 *   http://research.microsoft.com/pubs/156567/raptor_alenex.pdf.
 *
 * There is basic support for saving paths, so we can report how to reach a destination rather than just how long it takes.
 *
 * This class originated as a rewrite of our RAPTOR code that would use "thin workers", allowing computation by a
 * generic function-execution service like AWS Lambda. The gains in efficiency were significant enough that this is now
 * the way we do all analysis work. This system also accounts for pure-frequency routes by using Monte Carlo methods
 * (generating randomized schedules).
 *
 * TODO rename to remove "fast" and revise above comments, there is only one worker now.
 *      Maybe just call it TransitRouter. But then there's also McRaptor.
 */
public class FastRaptorWorker {

    private static final Logger LOG = LoggerFactory.getLogger(FastRaptorWorker.class);

    /**
     * This value essentially serves as Infinity for ints - it's bigger than every other number.
     * It is the travel time to a transit stop or a target before that stop or target is ever reached.
     * Be careful when propagating travel times from stops to targets, adding anything to UNREACHED will cause overflow.
     */
    public static final int UNREACHED = Integer.MAX_VALUE;

    /**
     * Minimum time between alighting from one vehicle and boarding another, in seconds.
     * TODO make this configurable, and use loop-transfers from transfers.txt.
     */
    public static final int BOARD_SLACK_SECONDS = 60;

    public static final int SECONDS_PER_MINUTE = 60;

    /**
     * Step for departure times. Use caution when changing this as the functions request.getTimeWindowLengthMinutes
     * and request.getMonteCarloDrawsPerMinute below which assume this value is 1 minute.
     */
    private static final int DEPARTURE_STEP_SEC = 60;

    /**
     * Minimum wait for boarding to account for schedule variation.
     * FIXME clarify why this is separate from BOARD_SLACK. If it is not, merge the two constants into BOARD_SLACK_SEC.
     */
    private static final int MINIMUM_BOARD_WAIT_SEC = 60;

    /** The width of the departure time window in minutes. */
    public final int nMinutes;

    /**
     * The number of different randomized schedules to create at each departure minute for frequency-based routes.
     * When frequency routes (non-scheduled routes) are present, we perform multiple searches per departure minute
     * using different randomly-offset schedules (a Monte Carlo exploration of all possible schedules). This field
     * controls how many such randomly offset schedules are generated. A value of 0 is a special case that triggers
     * the HALF_HEADWAY boarding assumption, which will lead one iteration per minute to be returned.
     */
    public final int monteCarloDrawsPerMinute;

    /** Track the time spent in each part of the Raptor search. */
    public final RaptorTimer raptorTimer = new RaptorTimer();

    /** The transit layer to route on. */
    private final TransitLayer transit;

    /** Times to access each transit stop using the street network (seconds). */
    private final TIntIntMap accessStops;

    /** The routing parameters. */
    private final ProfileRequest request;

    /** The indexes of the trip patterns running on a given day with frequency-based trips of selected modes. */
    private final BitSet runningFrequencyPatterns = new BitSet();

    /** The indexes of the trip patterns running on a given day with scheduled trips of selected modes. */
    private final BitSet runningScheduledPatterns = new BitSet();

    /** Generates and stores departure time offsets for every frequency-based set of trips. */
    private final FrequencyRandomOffsets offsets;

    /** Services active on the date of the search */
    private final BitSet servicesActive;

    /**
     * The state resulting from the scheduled search at a particular departure minute.
     * This state is reused at each departure minute without re-initializing it (this is the range-raptor optimization).
     * The randomized schedules at each departure minute are applied on top of this scheduled state.
     */
    private final RaptorState[] scheduleState;

    /**
     * This should be either HALF_HEADWAY or MONTE_CARLO.
     * The other value UPPER_BOUND is only used within a sub-search of MONTE_CARLO.
     */
    private final FrequencyBoardingMode boardingMode;

    /** Set to true to save path details for all optimal paths. */
    public boolean retainPaths = false;

    /** If we're going to store paths to every destination (e.g. for static sites) then they'll be retained here. */
    public List<Path[]> pathsPerIteration;

    public FastRaptorWorker (TransitLayer transitLayer, ProfileRequest request, TIntIntMap accessStops) {
        this.transit = transitLayer;
        this.request = request;
        this.accessStops = accessStops;
        this.servicesActive  = transit.getActiveServicesForDate(request.date);
        // we add one to request.maxRides, first state is result of initial walk
        this.scheduleState = IntStream.range(0, request.maxRides + 1)
                .mapToObj((i) -> new RaptorState(
                            transit.getStopCount(),
                            request.maxTripDurationMinutes * SECONDS_PER_MINUTE))
                .toArray(RaptorState[]::new);

        for (int i = 1; i < this.scheduleState.length; i++) {
            this.scheduleState[i].previous = this.scheduleState[i - 1];
        }

        offsets = new FrequencyRandomOffsets(transitLayer);

        // compute number of minutes for scheduled search
        nMinutes = request.getTimeWindowLengthMinutes();

        // how many monte carlo draws per minute of scheduled search to get desired total iterations?
        monteCarloDrawsPerMinute = request.getMonteCarloDrawsPerMinute();

        // Zero Monte Carlo draws means use half-headway instead of Monte Carlo randomization.
        boardingMode = (monteCarloDrawsPerMinute == 0) ? HALF_HEADWAY : MONTE_CARLO;
    }

    /**
     * For each iteration (minute + MC draw combination), return the minimum travel time to each transit stop in seconds.
     * Return value dimension order is [searchIteration][transitStopIndex]
     * TODO Create proper types for return values?
     */
    public int[][] route () {
        raptorTimer.fullSearch.start();
        prefilterPatterns();
        // Initialize result storage.
        // Results are one arrival time at each stop, for every raptor iteration.
        final int nStops = transit.getStopCount();
        final int nIterations;
        if (boardingMode == HALF_HEADWAY) {
            nIterations = nMinutes;
            LOG.info("Performing {} scheduled iterations using half-headway for frequency routes.", nMinutes);
        } else {
            nIterations = monteCarloDrawsPerMinute * nMinutes;
            LOG.info(
                "Performing {} scheduled iterations each with {} Monte Carlo draws for a total of {} iterations.",
                nMinutes, monteCarloDrawsPerMinute, nIterations
            );
        }
        int[][] travelTimesToStopsPerIteration = new int[nIterations][];
        if (retainPaths) pathsPerIteration = new ArrayList<>();

        // This main outer loop iterates backward over all minutes in the departure times window.
        int currentIteration = 0;
        for (int departureTime = request.toTime - DEPARTURE_STEP_SEC, minute = nMinutes;
             departureTime >= request.fromTime;
             departureTime -= DEPARTURE_STEP_SEC, minute--
        ) {
            if (minute % 15 == 0) LOG.debug("  minute {}", minute);

            // Run the raptor search. For this particular departure time, we receive N arrays of arrival times at all
            // stops, one for each randomized schedule: resultsForMinute[randScheduleNumber][transitStop]
            int[][] resultsForDepartureMinute = runRaptorForDepartureMinute(departureTime);
            // Iterate over the results for different Monte Carlo draws at this departure minute.
            for (int[] arrivalTimesAtStops : resultsForDepartureMinute) {
                // Make a protective copy of the arrival times, transforming them into travel times (durations).
                checkState(arrivalTimesAtStops.length == nStops, "Result should contain one value per stop.");
                int[] travelTimesToStops = new int[nStops];
                for (int s = 0; s < nStops; s++) {
                    int arrivalTime = arrivalTimesAtStops[s];
                    travelTimesToStops[s] = (arrivalTime == UNREACHED) ? UNREACHED : arrivalTime - departureTime;
                }
                // Accumulate the duration-transformed Monte Carlo iterations for the current minute
                // into one big flattened array representing all iterations at all minutes.
                travelTimesToStopsPerIteration[currentIteration++] = travelTimesToStops;
            }
        }
        checkState(currentIteration == nIterations, "Unexpected number of iterations.");
        raptorTimer.fullSearch.stop();
        raptorTimer.log();
        // For debugging:
        // dumpAllTimesToFile(travelTimesToStopsPerIteration, 45);
        return travelTimesToStopsPerIteration;
    }

    /**
     * This method is intended for use in debugging. It will create a file containing the non-transfer travel times
     * to every stop for every iteration. These times will be subjected to the maximum duration specified, and all
     * times greater than that number (including UNREACHED) will be recorded as "OVER", to facilitate simple comparisons
     * on the command line with diff.
     */
    private void dumpAllTimesToFile(int[][] arrivalTimesAtStopsPerIteration, int maxDurationMinutes) {
        try (PrintWriter pw = new PrintWriter(new BufferedOutputStream(new FileOutputStream("dump.csv")))) {
            for (int i = 0; i < arrivalTimesAtStopsPerIteration.length; i++) {
                int[] arrivalTimesForIteration = arrivalTimesAtStopsPerIteration[i];
                for (int s = 0; s < arrivalTimesForIteration.length; s++) {
                    int time = arrivalTimesForIteration[s];
                    String timeStr = "OVER"; // Including UNREACHED
                    if (time < maxDurationMinutes * 60) {
                        timeStr = Integer.toString(time);
                    }
                    pw.printf("%d,%d,%s\n",i, s, timeStr);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Before routing, filter the set of patterns down to only the ones that are actually running on the search date.
     * We can also filter down to only those modes enabled in the search request, because all trips in a pattern are
     * defined to be on same route, and GTFS allows only one mode per route.
     */
    private void prefilterPatterns () {
        for (int patternIndex = 0; patternIndex < transit.tripPatterns.size(); patternIndex++) {
            TripPattern pattern = transit.tripPatterns.get(patternIndex);
            RouteInfo routeInfo = transit.routes.get(pattern.routeIndex);
            TransitModes mode = TransitLayer.getTransitModes(routeInfo.route_type);
            if (pattern.servicesActive.intersects(servicesActive) && request.transitModes.contains(mode)) {
                // At least one trip on this pattern is relevant, based on the profile request's date and modes.
                if (pattern.hasFrequencies) {
                    runningFrequencyPatterns.set(patternIndex);
                }
                // Schedule case is not an "else" clause because we support patterns with both frequency and schedule.
                if (pattern.hasSchedules) {
                    runningScheduledPatterns.set(patternIndex);
                }
            }
        }
    }

    /**
     * Set the departure time in the scheduled search to the given departure time,
     * and prepare for the scheduled search at the next-earlier minute
     */
    private void advanceScheduledSearchToPreviousMinute (int nextMinuteDepartureTime) {
        for (RaptorState state : this.scheduleState) {
            state.setDepartureTime(nextMinuteDepartureTime);
        }
        // add initial stops
        RaptorState initialState = scheduleState[0];
        accessStops.forEachEntry((stop, accessTime) -> {
            initialState.setTimeAtStop(stop, accessTime + nextMinuteDepartureTime, -1, -1, 0, 0, true);
            return true; // continue iteration
        });
    }

    /**
     * Perform a RAPTOR search at one specific departure time (at one specific minute). A full range-RAPTOR search
     * consists of many such searches at different departure times, working backward from the end of a time
     * window toward its beginning, and reusing state between searches as an optimization.
     *
     * @param departureTime When this search departs.
     * @return an array of length iterationsPerMinute, containing the arrival times at each stop for each
     * iteration (clock times as opposed to durations).
     */
    private int[][] runRaptorForDepartureMinute (int departureTime) {
        advanceScheduledSearchToPreviousMinute(departureTime);

        int iterationsPerMinute = (boardingMode == MONTE_CARLO) ? monteCarloDrawsPerMinute : 1;

        // Run a Raptor search for only the scheduled routes (not the frequency-based routes).
        // The initial round 0 holds the results of the street search: the travel times to transit stops from the origin
        // using the non-transit access mode(s).
        // We are using the Range-RAPTOR extension described in Delling, Daniel, Thomas Pajor, and Renato Werneck.
        // “Round-Based Public Transit Routing,” January 1, 2012. http://research.microsoft.com/pubs/156567/raptor_alenex.pdf.
        // In this optimization, we re-use the arrival times found by searches that depart later, because the arrival
        // time at each location at time t + n is an upper bound on the arrival time departing at time t.
        if (transit.hasSchedules) {
            raptorTimer.scheduledSearch.start();
            // We always process the full number of rounds requested, i.e. we don't break out of the loop early when
            // no improvement in travel times is achieved. That would complicate the code that handles the results. We
            // do limit the patterns explored in each round to those passing through stops updated in the previous round.
            for (int round = 1; round <= request.maxRides; round++) {
                // Reuse state from the same round at a later minute, merging in any improvements from the previous
                // round at this same departure minute. If there are no later minutes or previous rounds, all stops will
                // be unreached except those reached by the initial street access search. This copies optimal times and
                // paths from one round to the next; if at a given stop those values are not improved upon, several
                // rounds in a row will contain identical state. The path reconstruction process must account for this.
                scheduleState[round].minMergePrevious();

                raptorTimer.scheduledSearchTransit.start();
                doScheduledSearchForRound(scheduleState[round - 1], scheduleState[round]);
                raptorTimer.scheduledSearchTransit.stop();

                // If there are frequency routes, we will be randomizing the offsets of those routes.
                // First perform a frequency search using worst-case boarding time to provide a tighter upper bound on
                // total travel time. Each randomized schedule will improve on these travel times.
                // This is only helpful for Monte Carlo, not half-headway. Even in Monte Carlo mode,
                // perhaps we should only do it when iterationsPerMinute is high (2 or more?).
                if (transit.hasFrequencies && boardingMode == MONTE_CARLO) {
                    raptorTimer.scheduledSearchFrequencyBounds.start();
                    doFrequencySearchForRound(scheduleState[round - 1], scheduleState[round], UPPER_BOUND);
                    raptorTimer.scheduledSearchFrequencyBounds.stop();
                }

                raptorTimer.scheduledSearchTransfers.start();
                doTransfers(scheduleState[round]);
                raptorTimer.scheduledSearchTransfers.stop();
            }
            raptorTimer.scheduledSearch.stop();
        }

        // Now run frequency searches using randomized schedules for all frequency lines. We use the scheduled search
        // and the worst-case boarding wait for all frequency routes to establish an upper bound on arrival times for
        // the frequency searches, so we are copying the arrival times from the just completed search.
        // This is a key innovation, described in more detail in Conway, Byrd, and van der Linden 2017.
        if (transit.hasFrequencies) {
            raptorTimer.frequencySearch.start();
            int[][] result = new int[iterationsPerMinute][];
            // In Monte Carlo mode, each iteration is a fresh randomization of frequency route offsets.
            // In half-headway mode, only one iteration will happen and schedules will not be randomized.
            for (int iteration = 0; iteration < iterationsPerMinute; iteration++) {
                // For each new randomized schedule, we start with a fresh copy of the upper bound travel times.
                RaptorState[] frequencyState = Stream.of(scheduleState)
                        .map(RaptorState::copy)
                        .toArray(RaptorState[]::new);

                // Recreate the chain of pointers between rounds for the freshly copied states.
                for (int i = 1; i < frequencyState.length; i++) {
                    frequencyState[i].previous = frequencyState[i - 1];
                }

                if (boardingMode == MONTE_CARLO) {
                    // Take a new Monte Carlo draw if requested (i.e. if boarding assumption is not half-headway): for
                    // each frequency-based route, choose how long after service starts the first vehicle leaves (the
                    // route's "phase"). We run all Raptor rounds with one draw before proceeding to the next draw.
                    offsets.randomize();
                }

                for (int round = 1; round <= request.maxRides; round++) {
                    frequencyState[round].minMergePrevious();

                    // Scheduled search. This is improving on travel times from stops touched by frequency routes or
                    // combinations of frequency and scheduled routes.
                    raptorTimer.frequencySearchScheduled.start();
                    doScheduledSearchForRound(frequencyState[round - 1], frequencyState[round]);
                    raptorTimer.frequencySearchScheduled.stop();

                    // Frequency search: additionally use stops touched by scheduled search
                    // okay to destructively modify last round frequency state, it will not be used after this
                    raptorTimer.frequencySearchFrequency.start();
                    frequencyState[round - 1].bestStopsTouched.or(scheduleState[round - 1].bestStopsTouched);
                    frequencyState[round - 1].nonTransferStopsTouched.or(scheduleState[round - 1].nonTransferStopsTouched);
                    doFrequencySearchForRound(frequencyState[round - 1], frequencyState[round], boardingMode);
                    raptorTimer.frequencySearchFrequency.stop();

                    raptorTimer.frequencySearchTransfers.start();
                    doTransfers(frequencyState[round]);
                    raptorTimer.frequencySearchTransfers.stop();
                }
                // We are processing frequency routes, states are already a copy of the retained scheduled search state,
                // no need to make an additional protective copy.
                RaptorState finalRoundState = frequencyState[request.maxRides];
                result[iteration] = finalRoundState.bestNonTransferTimes;
                if (retainPaths) {
                    pathsPerIteration.add(pathToEachStop(finalRoundState));
                }
            }
            raptorTimer.frequencySearch.stop();
            return result;
        } else {
            // If there are no frequency trips, return the result of the scheduled search, but repeated as many times
            // as there are requested MC draws, so that the scheduled search accessibility avoids potential bugs
            // where assumptions are made about how many results will be returned from a search, e.g., in
            // https://github.com/conveyal/r5/issues/306
            // FIXME on large networks with no frequency routes this seems extremely inefficient.
            // It may be somewhat less inefficient than it seems if we make arrays of references all to the same object.
            // TODO check whether we're actually hitting this code with iterationsPerMinute > 1 on scheduled networks.
            //      maybe we should even require that iterationsPerMinute == 1 for non-freq searches in an assertion.
            int[][] result = new int[iterationsPerMinute][];
            RaptorState finalRoundState = scheduleState[request.maxRides];
            // DEBUG print out full path (all rounds) to one stop at one departure minute, when no frequency trips.
            // System.out.printf("Departure time %d %s\n", departureTime, new Path(finalRoundState, 3164));
            // This scheduleState is repeatedly modified as the outer loop progresses over departure minutes.
            // We have to be careful here that creating these paths does not modify the state, and makes
            // protective copies of any information we want to retain.
            Path[] paths = retainPaths ? pathToEachStop(finalRoundState) : null;
            for (int iteration = 0; iteration < iterationsPerMinute; iteration++) {
                result[iteration] = finalRoundState.bestNonTransferTimes;
                if (retainPaths) {
                    pathsPerIteration.add(paths);
                }
            }
            return result;
        }
    }

    /**
     * Create the optimal path to each stop in the transit network, based on the given RaptorState.
     */
    private static Path[] pathToEachStop (RaptorState state) {
        int nStops = state.bestNonTransferTimes.length;
        Path[] paths = new Path[nStops];
        for (int s = 0; s < nStops; s++) {
            if (state.bestNonTransferTimes[s] == UNREACHED) {
                paths[s] = null;
            } else {
                paths[s] = new Path(state, s);
            }
        }
        return paths;
    }

    /**
     * A sub-step in the process of performing a RAPTOR search at one specific departure time (at one specific minute).
     * This method handles only the routes that have exact schedules. There is another method that handles only the
     * other kind of routes: the frequency-based routes.
     */
    private void doScheduledSearchForRound(RaptorState inputState, RaptorState outputState) {
        BitSet patternsToExplore = patternsToExploreInNextRound(inputState, runningScheduledPatterns);
        for (int patternIndex = patternsToExplore.nextSetBit(0);
             patternIndex >= 0;
             patternIndex = patternsToExplore.nextSetBit(patternIndex + 1)
        ) {
            TripPattern pattern = transit.tripPatterns.get(patternIndex);
            int onTrip = -1;
            int waitTime = 0;
            int boardTime = 0;
            int boardStop = -1;
            TripSchedule schedule = null;

            for (int stopPositionInPattern = 0; stopPositionInPattern < pattern.stops.length; stopPositionInPattern++) {
                int stop = pattern.stops[stopPositionInPattern];

                // attempt to alight if we're on board and if drop off is allowed, done above the board search so
                // that we don't check for alighting when boarding
                if (onTrip > -1 && pattern.dropoffs[stopPositionInPattern] != PickDropType.NONE) {
                    int alightTime = schedule.arrivals[stopPositionInPattern];
                    int onVehicleTime = alightTime - boardTime;

                    if (waitTime + onVehicleTime + inputState.bestTimes[boardStop] > alightTime) {
                        LOG.error("Components of travel time are larger than travel time!");
                    }

                    outputState.setTimeAtStop(stop, alightTime, patternIndex, boardStop, waitTime, onVehicleTime, false);
                }

                // Don't attempt to board if this stop was not reached in the last round or if pick up is not allowed.
                if (inputState.bestStopsTouched.get(stop) &&
                    pattern.pickups[stopPositionInPattern] != PickDropType.NONE
                ) {
                    int earliestBoardTime = inputState.bestTimes[stop] + MINIMUM_BOARD_WAIT_SEC;
                    if (onTrip == -1) {
                        if (inputState.bestStopsTouched.get(stop)) { // FIXME due to enclosing conditional this is always true.
                            int candidateTripIndex = -1;
                            EARLIEST_TRIP:
                            for (TripSchedule candidateSchedule : pattern.tripSchedules) {
                                candidateTripIndex++;

                                if (!servicesActive.get(candidateSchedule.serviceCode) || candidateSchedule.headwaySeconds != null) {
                                    // frequency trip or not running
                                    continue;
                                }

                                if (earliestBoardTime < candidateSchedule.departures[stopPositionInPattern]) {
                                    // board this vehicle
                                    onTrip = candidateTripIndex;
                                    schedule = candidateSchedule;
                                    boardTime = candidateSchedule.departures[stopPositionInPattern];
                                    waitTime = boardTime - inputState.bestTimes[stop];
                                    boardStop = stop;
                                    break EARLIEST_TRIP;
                                }
                            }
                        }
                    } else {
                        // We are already on a trip, but check if this stop was reached early enough to back up
                        // to an earlier trip on the same pattern.
                        int bestTripIdx = onTrip;
                        while (--bestTripIdx >= 0) {
                            TripSchedule trip = pattern.tripSchedules.get(bestTripIdx);
                            if (trip.headwaySeconds != null || !servicesActive.get(trip.serviceCode)) {
                                // This is a frequency trip or it is not running on the day of the search.
                                continue;
                            }
                            if (trip.departures[stopPositionInPattern] > earliestBoardTime) {
                                onTrip = bestTripIdx;
                                schedule = trip;
                                boardTime = trip.departures[stopPositionInPattern];
                                waitTime = boardTime - inputState.bestTimes[stop];
                                boardStop = stop;
                            } else {
                                // The trip under consideration arrives too early,
                                // stop searching since trips are sorted by departure time within a pattern.
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * The different ways of determining the wait time before boarding a vehicle for frequency-based routes.
     * Would it be more efficient to pass in a function reference, after defining a new departure search interface?
     * e.g. public interface frequencyDepartCalculator { ... }
     */
    public enum FrequencyBoardingMode {
        /** The primary frequency search, using various randomized departure offsets for each route. */
        MONTE_CARLO,
        /** Compute a deterministic upper bound, which helps speed up subsequent frequency searches. */
        UPPER_BOUND,
        /** Assume a vehicle always comes after exactly half the headway, for comparison with Monte Carlo. */
        HALF_HEADWAY
    }

    /**
     * Do a frequency search. If computeDeterministicUpperBound is true, worst-case frequency boarding time will be used
     * so that the output of this function can be used in a range-RAPTOR search. Otherwise Monte Carlo schedules will be
     * used to improve upon the output of the range-RAPTOR bounds search.
     * FIXME computeDeterministicUpperBound mentioned abve doesn't seem to exist
     * @param frequencyBoardingMode see comments on enum values.
     */
    private void doFrequencySearchForRound(
            RaptorState inputState,
            RaptorState outputState,
            FrequencyBoardingMode frequencyBoardingMode
    ) {
        BitSet patternsToExplore = patternsToExploreInNextRound(inputState, runningFrequencyPatterns);
        for (int patternIndex = patternsToExplore.nextSetBit(0);
             patternIndex >= 0;
             patternIndex = patternsToExplore.nextSetBit(patternIndex + 1)
        ){
            TripPattern pattern = transit.tripPatterns.get(patternIndex);

            int tripScheduleIndex = -1; // first increment lands at 0
            for (TripSchedule schedule : pattern.tripSchedules) {
                tripScheduleIndex++;

                // scheduled trip or not running
                if (!servicesActive.get(schedule.serviceCode) || schedule.headwaySeconds == null) continue;

                for (int frequencyEntryIdx = 0; frequencyEntryIdx < schedule.headwaySeconds.length; frequencyEntryIdx++) {
                    int boardTime = -1;
                    int boardStopPositionInPattern = -1;
                    int waitTime = -1;

                    for (int stopPositionInPattern = 0; stopPositionInPattern < pattern.stops.length; stopPositionInPattern++) {
                        int stop = pattern.stops[stopPositionInPattern];

                        // attempt to alight if boarded and if drop off is allowed
                        if (boardTime > -1 && pattern.dropoffs[stopPositionInPattern] != PickDropType.NONE) {
                            // attempt to alight
                            int travelTime = schedule.arrivals[stopPositionInPattern] - schedule.departures[boardStopPositionInPattern];
                            int alightTime = boardTime + travelTime;
                            int boardStop = pattern.stops[boardStopPositionInPattern];
                            outputState.setTimeAtStop(stop, alightTime, patternIndex, boardStop, waitTime, travelTime, false);
                        }

                        // attempt to board if pick up is allowed
                        // (even if already boarded, since this is a frequency trip and we could move back)
                        if (inputState.bestStopsTouched.get(stop) && pattern.pickups[stopPositionInPattern] != PickDropType.NONE) {
                            int earliestBoardTime = inputState.bestTimes[stop] + MINIMUM_BOARD_WAIT_SEC;

                            // if we're computing the upper bound, we want the worst case. This is the only thing that is
                            // valid in a range RAPTOR search; using random schedule draws in range RAPTOR would be problematic
                            // because they need to be independent across minutes.

                            final int newBoardingDepartureTimeAtStop;

                            if (frequencyBoardingMode == MONTE_CARLO) {
                                int offset = offsets.offsets.get(patternIndex)[tripScheduleIndex][frequencyEntryIdx];
                                newBoardingDepartureTimeAtStop = getRandomFrequencyDepartureTime(
                                        schedule,
                                        stopPositionInPattern,
                                        offset,
                                        frequencyEntryIdx,
                                        earliestBoardTime
                                );
                            } else if (frequencyBoardingMode == UPPER_BOUND) {
                                newBoardingDepartureTimeAtStop = getWorstCaseFrequencyDepartureTime(
                                        schedule,
                                        stopPositionInPattern,
                                        frequencyEntryIdx,
                                        earliestBoardTime
                                );
                            } else if (frequencyBoardingMode == HALF_HEADWAY){
                                  newBoardingDepartureTimeAtStop = getAverageCaseFrequencyDepartureTime(
                                          schedule,
                                          stopPositionInPattern,
                                          frequencyEntryIdx,
                                          earliestBoardTime
                                  );
                            } else {
                                // TODO make half_headway the else clause
                                throw new AssertionError("Unknown departure search method.");
                            }
                            int remainOnBoardDepartureTimeAtStop = Integer.MAX_VALUE;

                            if (boardTime > -1) {
                                // We cannot re-use the calculation from above. We're using departure time at this stop
                                // here to account for any dwell time. TODO clarify what that means and how this works.
                                int travelTime = schedule.departures[stopPositionInPattern]
                                        - schedule.departures[boardStopPositionInPattern];
                                remainOnBoardDepartureTimeAtStop = boardTime + travelTime;
                            }
                            if (newBoardingDepartureTimeAtStop > -1
                                    && newBoardingDepartureTimeAtStop < remainOnBoardDepartureTimeAtStop) {
                                // board this trip
                                boardTime = newBoardingDepartureTimeAtStop;
                                waitTime = boardTime - inputState.bestTimes[stop];
                                boardStopPositionInPattern = stopPositionInPattern;
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * @param earliestTime the time at or after which to find a departure time.
     * @return the earliest departure time on a particular scheduled frequency entry, or -1 if the frequency entry is
     * not usable.
     */
    public int getRandomFrequencyDepartureTime (
            TripSchedule schedule,
            int stopPositionInPattern,
            int offset,
            int frequencyEntryIdx,
            int earliestTime
    ) {
        // earliest board time is start time plus travel time plus offset
        int earliestBoardTimeThisEntry = schedule.startTimes[frequencyEntryIdx] +
                schedule.departures[stopPositionInPattern] +
                offset;

        // compute the number of trips on this entry
        // We take the difference between the end time and the start time including the offset
        // to get the time between the first trip and the last possible trip. We int-divide by the
        // headway and add one to correct for the fencepost problem.
        int numberOfTripsThisEntry = (schedule.endTimes[frequencyEntryIdx] -
                (schedule.startTimes[frequencyEntryIdx] + offset)) /
                schedule.headwaySeconds[frequencyEntryIdx] + 1;

        // the earliest time we can leave this stop based on when we arrived
        // We subtract one because we find trips that have departure time > this time, not
        // >=
        int lowerBoundBoardTime = earliestTime - 1;
        int earliestFeasibleTripIndexThisEntry;
        if (lowerBoundBoardTime <= earliestBoardTimeThisEntry) {
            earliestFeasibleTripIndexThisEntry = 0;
        } else {
            // find earliest trip later than the lower bound on board time
            // We add one because int math floors the result.
            // This is why we subtracted one second above, so that if the earliest board time
            // is exactly the second when the trip arrives, we will find that trip rather than the
            // next trip when we add one.
            earliestFeasibleTripIndexThisEntry =
                    (lowerBoundBoardTime - earliestBoardTimeThisEntry) / schedule.headwaySeconds[frequencyEntryIdx] + 1;
        }

        if (earliestFeasibleTripIndexThisEntry < numberOfTripsThisEntry) {
            return earliestBoardTimeThisEntry + earliestFeasibleTripIndexThisEntry * schedule.headwaySeconds[frequencyEntryIdx];
        } else {
            return -1;
        }
    }

    public int getWorstCaseFrequencyDepartureTime (TripSchedule schedule, int stopPositionInPattern, int frequencyEntryIdx, int earliestTime) {
        int headway = schedule.headwaySeconds[frequencyEntryIdx];
        int travelTimeFromStartOfTrip = schedule.departures[stopPositionInPattern];
        // The last vehicle could leave the terminal as early as headwaySeconds before the end of the frequency entry.
        int earliestEndTimeOfFrequencyEntry = schedule.endTimes[frequencyEntryIdx] - headway + travelTimeFromStartOfTrip;

        if (earliestEndTimeOfFrequencyEntry < earliestTime) return -1;

        // board pessimistically assuming the entry is already running
        int latestBoardTimeAssumingEntryIsAlreadyRunning = earliestTime + headway;
        // figure out the latest departure time of this trip at this stop
        int latestBoardTimeOfFirstTrip = schedule.startTimes[frequencyEntryIdx] + headway + travelTimeFromStartOfTrip;
        // return the max of those two
        return Math.max(latestBoardTimeAssumingEntryIsAlreadyRunning, latestBoardTimeOfFirstTrip);
    }

    /**
     * For half-headway (non-monte-carlo) evaluation of frequency-based routes. The caller should be looping through
     * all frequency entries (e.g. to allow a passenger to wait at a stop for a subsequent frequency entry to start).
     *
     * The departure time is assumed to be half-headway after the later (maximum) of the passenger's earliest possible
     * boarding time at the stop or the frequency entry's earliest possible arrival at the stop.
     *
     * TODO account for possible dwell time?
     *
     * @param earliestTime the time at or after which to find a departure time (i.e. when a passenger is
     *                    ready to board).
     *
     * @return clock time at which a passenger boards this frequency entry at this stop
     */
    public static int getAverageCaseFrequencyDepartureTime (
            TripSchedule schedule,
            int stopPositionInPattern,
            int frequencyEntryIdx,
            int earliestTime
    ) {
        int travelTimeFromStartOfTrip = schedule.departures[stopPositionInPattern];

        // Ensure the schedule has not ceased at this stop. Note that this approach assumes no trip for this frequency
        // entry leaves the first stop of the pattern after end_time, which is different from the assumption in the
        // approaches above. See discussion in issue #122
        int frequencyEndsAtThisStop = schedule.endTimes[frequencyEntryIdx] + travelTimeFromStartOfTrip;
        if (frequencyEndsAtThisStop < earliestTime) {
            return -1;
        }

        int frequencyStartsAtThisStop = schedule.startTimes[frequencyEntryIdx] + travelTimeFromStartOfTrip;

        int headway = schedule.headwaySeconds[frequencyEntryIdx];
        int halfHeadway = headway / 2;

        return halfHeadway + (Math.max(earliestTime, frequencyStartsAtThisStop));

    }

    /**
     * After processing transit for a particular round, transfer from any stops that were reached by transit and update
     * any stops where transferring is an improvement on the existing arrivals directly from transit.
     * When applying transfers from a stop, we do not apply a "transfer" from the stop to itself. In a sense these
     * trivial transfers are applied during transit routing: when the transfer arrival times are updated, the
     * post-transfer times are also updated.
     * The patterns to be explored in the next round are then determined by which stops were updated by either transit
     * or transfer arrivals.
     */
    private void doTransfers (RaptorState state) {
        // Cast and multiplication factored out of the tight loop below to ensure they are not repeatedly evaluated.
        int walkSpeedMillimetersPerSecond = (int) (request.walkSpeed * 1000);
        int maxWalkMillimeters = walkSpeedMillimetersPerSecond * (request.maxWalkTime * SECONDS_PER_MINUTE);

        for (int stop = state.nonTransferStopsTouched.nextSetBit(0);
             stop > -1;
             stop = state.nonTransferStopsTouched.nextSetBit(stop + 1)
        ){
            TIntList transfersFromStop = transit.transfersForStop.get(stop);
            if (transfersFromStop != null) {
                for (int i = 0; i < transfersFromStop.size(); i += 2) {
                    int targetStop = transfersFromStop.get(i);
                    int distanceToTargetStopMillimeters = transfersFromStop.get(i + 1);
                    if (distanceToTargetStopMillimeters < maxWalkMillimeters) {
                        int walkTimeToTargetStopSeconds = distanceToTargetStopMillimeters / walkSpeedMillimetersPerSecond;
                        checkState(walkTimeToTargetStopSeconds >= 0, "Transfer walk time must be positive.");
                        int timeAtTargetStop = state.bestNonTransferTimes[stop] + walkTimeToTargetStopSeconds;
                        state.setTimeAtStop(targetStop, timeAtTargetStop, -1, stop, 0, 0, true);
                    }
                }
            }
        }
    }

    /**
     * Find all patterns that could lead to improvements in the next round after the given state's raptor round.
     * Specifically, the patterns passing through all stops that were updated in the given state's round.
     * The pattern indexes returned are limited to those in the supplied set.
     */
    private BitSet patternsToExploreInNextRound (RaptorState state, BitSet runningPatterns) {
        BitSet patternsToExplore = new BitSet();
        for (int stop = state.bestStopsTouched.nextSetBit(0);
             stop >= 0;
             stop = state.bestStopsTouched.nextSetBit(stop + 1)
        ) {
            TIntIterator patternsAtStop = transit.patternsForStop.get(stop).iterator();
            while (patternsAtStop.hasNext()) {
                int pattern = patternsAtStop.next();
                if (runningPatterns.get(pattern)) {
                    patternsToExplore.set(pattern);
                }
            }
        }
        return patternsToExplore;
    }
}