
import dagreD3 from "dagre-d3"
import {flow, fromPairs, groupBy, map, sum, toPairs} from "lodash/fp";


const sample_pipeline_def_1 = {
    "taskGroups": [
        {
            "key_group": "aggregate_all",
            "keys": [
                "aggregate_all"
            ]
        },
        {
            "key_group": "preparation_task",
            "keys": [
                "preparation_task"
            ]
        },
        {
            "key_group": "work_chunk",
            "keys": [
                "work_chunk.3",
                "work_chunk.4",
                "work_chunk.2",
                "work_chunk.1"
            ]
        }
    ],
    "deps": [
        [
            "aggregate_all",
            [
                "work_chunk.*"
            ],
            "work_chunk"
        ],
        [
            "work_chunk",
            [
                "work_files"
            ],
            "preparation_task"
        ]
    ]
}

export const sampleG = () => sample_pipeline_def_1

export const statesToDisplayStateMap = {
    "waiting-for-deps":    "waiting",
    "prepared":            "waiting",
    "upload-failed":       "waiting",
    "download-failed":     "waiting",
    "scheduled":           "waiting",
    "killed":              "waiting",

    "launched":            "running",
    "completed-unsigned":  "running",
    "step-started":        "running",
    "download-started":    "running",
    "upload-started":      "running",
    "queued-for-upload":   "running",
    "upload-completed":    "running",
    "queued-for-download": "running",
    "download-completed":  "running",
    "queued":              "running",
    "step-completed":      "running",

    "completed":           "completed",

    "failed":              "failed",
    "timed-out":           "failed",
    "crashed":             "failed",
    "ignored":             "ignored"
}

export const stateGroupsForDisplay = () => flow(
    toPairs,
    groupBy(([state, group]) => group),
    toPairs,
    map(([group, listOfPairs]) => [
        group,
        listOfPairs.map(p => p[1])
    ]),
    fromPairs
)(statesToDisplayStateMap)


const parseStateSummaryTSV = tsvAsString => {

    const [colNames, ...rows] =
        tsvAsString
        .split("\n")
        .map(row => row.split("\t"))

    return [
        colNames,
        rows.map(
            ([taskKey, ...counts]) =>
            [taskKey, counts.map(s => parseInt(s))]
        )
    ]
}

export const stateCountsSummaryPerTaskGroup = tsvAsString => {

    const [states, countsPerTaskGroup] = parseStateSummaryTSV(tsvAsString)

    const stateGroups = stateGroupsForDisplay()

    const initCounterDict = () => flow(
        toPairs,
        map(([displayState, v]) => [displayState, 0]),
        fromPairs
    )(stateGroups)

    return  countsPerTaskGroup.map(([taskGroupKey, counts]) => {

        const countDict = initCounterDict()
        states.forEach((state, idx) => {

            countDict[statesToDisplayStateMap[state]] =
                countDict[statesToDisplayStateMap[state]] + counts[idx]
        })

        return {taskGroupKey, counts: countDict}
    })
}

function transpose(matrix) {
  return matrix[0].map((col, i) => matrix.map(row => row[i]));
}

export const sumStateTotals = countsSummary => {

    const [waiting, running, completed, failed, ignored] = transpose(
        countsSummary.map(g => [g.counts.waiting, g.counts.running, g.counts.completed, g.counts.failed, g.counts.ignored])
    ).map(counts => flow(sum)(counts))

    return {waiting, running, completed, failed, ignored}
}