
import {
    parseStateSummaryTSV,
    stateCountsSummaryPerTaskGroup, sumStateTotals
} from "./parsePipelineDAG"
import parseJSON from 'date-fns/parseJSON'
import parseISO from 'date-fns/parseISO'
import {flow, map, sum, zip, zipAll} from "lodash/fp";

export const countsTableTSV = () =>
`waiting-for-deps\tprepared\tqueued-for-upload\tupload-started\tupload-completed\tqueued-for-download\tdownload-started\tdownload-completed\tupload-failed\tdownload-failed\tcompleted-unsigned\tqueued\tlaunched\tscheduled\tstep-started\tstep-completed\tcrashed\tfailed\ttimed-out\tcompleted\tignored
preparation_task\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t1\t2\t1
work_chunk\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t4\t0\t4
aggregate_all\t1\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t1\t0\t1`


test('parseStateSummaryTSV', () => {

    const summary = stateCountsSummaryPerTaskGroup(countsTableTSV())


    expect(summary).toEqual([
      {
        "taskGroupKey": "preparation_task",
        "counts": {
          "waiting": 0, "running": 0, "completed": 1, "failed": 0, "ignored": 2
        }
      },
      {
        "taskGroupKey": "work_chunk",
        "counts": {
          "waiting": 0, "running": 0, "completed": 4, "failed": 0, "ignored": 0
        }
      },
      {
        "taskGroupKey": "aggregate_all",
        "counts": {
          "waiting": 1, "running": 0, "completed": 1, "failed": 0, "ignored": 0
        }
      }
    ])

})

//test('parseJSON date', () => {
//    const d = parseISO("2021-03-04-17:24:41.357346")
//})


test('taskStateTotals', () => {

    const summary = stateCountsSummaryPerTaskGroup(countsTableTSV())

    const res = sumStateTotals(summary)

    expect(res).toEqual({
        "waiting": 1,
        "running": 0,
        "completed": 6,
        "failed": 0,
        "ignored": 2
    })
})