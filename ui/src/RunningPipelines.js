import React, {useReducer, useEffect} from "react";
import {select as d3_select} from 'd3-selection';
import {stateCountsSummaryPerTaskGroup, sumStateTotals} from "./parsePipelineDAG"
import TasksView from "./TasksView";
import {taskFilterReducer} from "./taskFilterReducer";
import {buildDAG} from "./buildDAG";
import {flow, sum} from "lodash/fp";

import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import {faChevronDown, faChevronRight, faProjectDiagram} from '@fortawesome/free-solid-svg-icons'
import {usePipelineWebsocketSubscription} from "./piplineSubscriptions";


const pipelineListReducer = (state, action) => {

    switch (action.name) {
        case 'init':
            return {
                initialized: false,
                selectedPipelineDir: null,
                list: []
            }
        case 'refreshPipelineList':

            const newPipelines = action.list.filter(
                p =>  ! state.list.find(existingP => p.dir === existingP.dir)
            )

            const l = [
                ...state.list,
                ...newPipelines
            ]

            return {
                initialized: true,
                selectedPipelineDir: state.selectedPipelineDir,
                list: l.map(p => {
                    const updatedorNewP = action.list.find(p0 => p0 === p.dir)

                    if(!updatedorNewP) {
                        return p
                    }

                    debugger
                    return {
                        ...p,
                        totals: updatedorNewP.totals
                    }
                })
            }

        case 'unSelect':
            return {
                ...state,
                selectedPipelineDir: null
            }
        case 'setRowsData':
            return {
                ...state,
                selectedPipelineDir: action.pipelineDir,
                list: state.list.map(
                    p => p.dir !== action.pipelineDir ? p :
                    {
                        ...p,
                        pipelineDir: action.pipelineDir,
                        rowsData: action.rowsData
                    }
                )
            }
        case 'refreshAllTasksShallowInfo':
            return {
                ...state,
                list: state.list.map(
                    p => p.dir !== action.pipelineDir ? p :
                    {
                        ...p,
                        snapshot_time: action.snapshot_time,
                        allTasksShallowInfo: action.allTasksShallowInfo
                    }
                )
            }
        case 'taskActionSubmitted':
            return {
                ...state,
                list: state.list.map(
                    p => p.dir !== action.message.pipeline_dir ? p :
                    {
                        ...p,
                        allTasksShallowInfo: p.allTasksShallowInfo.map(taskShallowInfo => {

                            if(taskShallowInfo.key !== action.message.task_key) {
                                return taskShallowInfo
                            }

                            if (action.message.is_cancel) {
                                return {
                                    ...taskShallowInfo,
                                    action: null
                                }
                            }

                            return {
                                ...taskShallowInfo,
                                action: action.message.action_name
                            }
                        })
                    }
                )
            }

        default:
            throw Error(`unknown action ${action.name}`)
    }
}


const RunningPipelines = ({customPipelineInstanceLink, uninitializedPipelineListMessage}) => {

    const [pipelineList, pipelineListDispatcher] = useReducer(
        pipelineListReducer,
        pipelineListReducer(null, {name: "init"})
    )

    const {
        isConnected,
        socket,
        socketId,
        observePipeline,
        observedPipelineId,
        unObservePipeline,
        observeTask,
        observedTaskKey,
        unObserveTask
    } = usePipelineWebsocketSubscription({})

    useEffect(() => {
        if(observedPipelineId === null) {
            pipelineListDispatcher({
                name: 'unSelect'
            })
        }
    }, [observedPipelineId])

    useEffect(() => {

        if(socket === null) {
            return
        }

        socket.on("serverSideException", o => console.log(o))

        socket.on("taskActionSubmitted", o => {
            pipelineListDispatcher({
                name: "taskActionSubmitted",
                message: o
            })
        })

        socket.on("latestPipelineDetailedState", o => {

            const cs = stateCountsSummaryPerTaskGroup(o.tsv)
            cs.forEach(o => {
                for (const [displayState, count] of Object.entries(o.counts)) {
                    d3_select(`.task_count_${displayState}_${o.taskGroupKey}`)
                    .text(`${count}`)
                    .attr("opacity", count === 0 ? 0.3 : 0.8)
                }
            })

            pipelineListDispatcher({
                name: 'setRowsData',
                rowsData: cs,
                pipelineDir: o.pipelineDir
            })

            if(o.partial_task_infos !== null) {
                pipelineListDispatcher({
                    name: 'refreshAllTasksShallowInfo',
                    allTasksShallowInfo: o.partial_task_infos,
                    pipelineDir: o.pipelineDir,
                    snapshot_time: o.snapshot_time
                })
            }
        })

        socket.on("running-pipelines", o => {
            pipelineListDispatcher({name: 'refreshPipelineList', list: o})
        })

    }, [socketId])

    //useEffect(() => {
    //    pipelineListDispatcher({name: 'refreshPipelineList', list: initialRunningPipelineList})
    //}, [])

    const DEBUG_SELECT_PIPELINE_1_AND_TASK_LIST = (list) => {
        selectPipeline(list[0].dir)
        taskFilterDispatcher({name: 'open'})
    }

    const [taskFilter, taskFilterDispatcher] = useReducer(taskFilterReducer, null)

    const selectTaskGroupInTasksView = (keyPrefix, displayState) =>
        () => taskFilterDispatcher({
                   name: 'selectTaskGroup',
                   displayState,
                   keyPrefix
        })

    const closeTasksList = () => {
        taskFilterDispatcher({name: 'close'})
    }

    const pipelineTableOverviewRow = pipeline => {

        const tableCellStyle = {cursor: "default"}

        const totalTasksForGroup = stateCounts =>
            <td style={tableCellStyle}>
                {
                    flow(sum)([
                        stateCounts.waiting,
                        stateCounts.running,
                        stateCounts.completed,
                        stateCounts.failed,
                        stateCounts.killed
                    ])
                }
            </td>

        const countCel = (i, clickHandler) => {

            const countDisplay = i === 0 ? "" : i

            return <td style={tableCellStyle}>
                {clickHandler ?
                    <a onClick={clickHandler}>{countDisplay}</a> :
                    countDisplay
                }
            </td>
        }

        const firstCell = () =>
            <span className="icon-text">
                {pipeline.dir}
                <span className="icon">
                    <FontAwesomeIcon icon={pipelineList.selectedPipelineDir === pipeline.dir ? faChevronDown : faChevronRight} />
                </span>
                {pipelineList.selectedPipelineDir === pipeline.dir && customPipelineInstanceLink ?
                    customPipelineInstanceLink({pipelineDir: pipelineList.selectedPipelineDir}): ''
                }
            </span>


        const expandPipelineRow = () => observePipeline(pipeline.dir)

        const totalsTD = (keyPrefix) =>
            <React.Fragment key={`${keyPrefix}-${pipeline.dir}`}>
                <td>totals</td>
                {countCel(pipeline.totals.waiting, expandPipelineRow)}
                {countCel(pipeline.totals.running, expandPipelineRow)}
                {countCel(pipeline.totals.completed, expandPipelineRow)}
                {countCel(pipeline.totals.failed, expandPipelineRow)}
                {countCel(pipeline.totals.killed, expandPipelineRow)}
                {totalTasksForGroup(pipeline.totals)}
            </React.Fragment>

        const totalsTableRow = (pipeline) =>
            <React.Fragment key={`f-${pipeline.dir}-tot`}>
                <tr>
                    <td
                        onClick={
                            () => pipelineList.selectedPipelineDir === null ?
                                observePipeline(pipeline.dir):
                                unObservePipeline(pipeline.dir)
                        }
                        style={tableCellStyle}>
                        {firstCell()}
                    </td>
                    {totalsTD("tot1")}
                </tr>
            </React.Fragment>

        if(pipelineList.selectedPipelineDir === pipeline.dir && pipeline.rowsData) {
            return <React.Fragment key={`p-${pipeline.dir}`}>
                {
                    pipeline.rowsData.map((taskGroup, idx) =>
                            <tr key={`k1-${taskGroup.taskGroupKey}-${idx}`}>
                                {idx === 0 &&
                                    <td key={"td1"}
                                        rowSpan={pipeline.rowsData.length+1}
                                        onClick={() => unObservePipeline(pipeline.dir)} style={tableCellStyle}>
                                        {firstCell()}
                                    </td>
                                }
                                <td>{taskGroup.taskGroupKey}</td>
                                {countCel(taskGroup.counts.waiting, selectTaskGroupInTasksView(taskGroup.taskGroupKey, "waiting"))}
                                {countCel(taskGroup.counts.running, selectTaskGroupInTasksView(taskGroup.taskGroupKey, "running"))}
                                {countCel(taskGroup.counts.completed, selectTaskGroupInTasksView(taskGroup.taskGroupKey, "completed"))}
                                {countCel(taskGroup.counts.failed, selectTaskGroupInTasksView(taskGroup.taskGroupKey, "failed"))}
                                {countCel(taskGroup.counts.killed, selectTaskGroupInTasksView(taskGroup.taskGroupKey, "killed"))}
                                {totalTasksForGroup(taskGroup.counts)}
                                {idx === 0 &&
                                    <td
                                        key={"td-last"} rowSpan={pipeline.rowsData.length+1}
                                    >DAG {customPipelineInstanceLink && customPipelineInstanceLink(pipeline)}
                                    </td>
                                }
                            </tr>
                    )
                }
                <tr key={`tot-${pipeline.dir}`}>
                    {totalsTD("tot1")}
                </tr>
            </React.Fragment>
        }

        return totalsTableRow(pipeline)
    }

    const pipelineTableOverview = () => <div>
        <table className="table is-bordered is-fullwidth is-hoverableZ">
            <thead>
                <tr>
                    <th>pipeline instance</th>
                    <th>tasks</th>
                    <th>waiting</th>
                    <th>running</th>
                    <th>completed</th>
                    <th>failed</th>
                    <th>killed</th>
                    <th>totals</th>
                    <th></th>
                </tr>
            </thead>
            <tbody>
            {
                pipelineList.list.map(pipelineTableOverviewRow)
            }
            </tbody>
        </table>
    </div>

    if(!socket) {
        return <div>
            <div className="is-size-3">...connecting</div>
            <progress className="progress is-medium is-dark" max="100">45%</progress>
        </div>
    }

    if(pipelineList.initialized) {
        if(pipelineList.list.length === 0) {
            return <div>
                <div className="is-size-3">There are no pipelines currently running</div>
            </div>
        }
    }
    else {

        if(uninitializedPipelineListMessage) {
            return uninitializedPipelineListMessage()
        }

        return <div>
            <div className="is-size-3">Waiting list of running pipelines</div>
        </div>
    }

    const selectedPipeline =
        taskFilter &&
        pipelineList.selectedPipelineDir &&
        pipelineList.list.find(p => p.dir === pipelineList.selectedPipelineDir)

    return <div className="container">
        <div className="columns">
            <div className="column">

                {
                    selectedPipeline && selectedPipeline.rowsData ?
                        <TasksView
                            pipelineList={pipelineList}
                            taskFilter={taskFilter}
                            selectedPipeline={selectedPipeline}
                            closeFunc={() => closeTasksList()}
                            observeTask={observeTask}
                            unObserveTask={unObserveTask}
                            observedTaskKey={observedTaskKey}
                            socket={socket}
                        /> :
                        pipelineTableOverview()
                }
            </div>
        </div>
    </div>
}

/*
*
    const div = useCallback(node => {
        if (node !== null) {
            setSvgRendered(true)
            if(countsSummary) {
                setCounts(countsSummary)
            }
        }
    }, [])


    const dagView = () =>
        <div className="card">
            <header className="card-header">
                <p className="card-header-title">
                    DAG View of pipeline
                </p>
                <button className="card-header-icon" aria-label="more options" onClick={() => setPipelineOverviewView(TABLE_PIPELINE_VIEW)}>
                      Back to table view
                </button>
            </header>
            <div className="content" ref={div}>
                <svg width={800} height={1100} style={{"background": "#efefef"}}><g/></svg>
            </div>
        </div>
*
* */

export default RunningPipelines
