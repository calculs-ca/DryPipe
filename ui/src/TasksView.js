import React, {useEffect, useReducer, useState} from "react";
import {statesToDisplayStateMap} from "./parsePipelineDAG";
import {taskFilterReducer} from "./taskFilterReducer";
import TaskView from "./TaskView";
import parseJSON from 'date-fns/parseJSON'
import formatDuration from 'date-fns/formatDuration'
import differenceInSeconds from 'date-fns/differenceInSeconds'
import intervalToDuration from 'date-fns/intervalToDuration'
import formatDistance from "date-fns/formatDistance";

const parseDatesInTask = task => {

    const history = task.history.map(row => {
        const [eventName, dateString] = row

        const datetime = parseJSON(dateString)

        return {
            eventName,
            datetime
        }
    })

    const now = new Date()

    const setTimeAgo = idx => {
        const o = history[idx]
        o.timeAgo = formatDistance(
            o.datetime,
            now,
            {addSuffix: true})
    }

    if(history.length >= 2) {
        for(let i = 1; i < history.length; i++) {
            const t1 = history[i-1]
            const t2 = history[i]

            try {
                t1.duration = intervalToDuration({
                    start: t1.datetime,
                    end: t2.datetime
                })

                const diffInSeconds = differenceInSeconds(
                  t2.datetime,
                  t1.datetime,
                )
                if(diffInSeconds < 1) {
                    t1.durationDisplay = "less than 1s"
                }
                else {
                    t1.durationDisplay = formatDuration(
                        t1.duration,
                        {format: ['hours', 'minutes', 'seconds']}
                    )
                }
            }
            catch (ex) {
                t1.durationDisplay = ""
                console.log(ex)
            }
        }

        try {
            setTimeAgo(0)
            setTimeAgo(history.length - 1)
        }
        catch (ex) {
            console.log(ex)
        }
    }

    return {
        ...task,
        history
    }
}

const TasksView = ({pipelineList, taskFilter: localTaskFilter, closeFunc, socket, observeTask, unObserveTask, observedTaskKey}) => {

    const selectedPipeline =
        pipelineList.selectedPipelineDir &&
        pipelineList.list.find(p => p.dir === pipelineList.selectedPipelineDir)

    const [taskFilter, taskFilterDispatcher] = useReducer(taskFilterReducer, localTaskFilter)

    const [selectedTask, setSelectedTask] = useState(null)

    const [taskRoomKey, setTaskRoomKey] = useState(null)

    const [taskPopupProgress, setTaskPopupProgress] = useState(0)

    const taskStates = selectedPipeline.allTasksShallowInfo || []

    useEffect(() => {
        socket.on("latestTaskDetails", o => {
            setSelectedTask(parseDatesInTask(o))
            setTaskPopupProgress(0)
        })
    },[])

    // Refresh selectedTask state with selectedPipeline.allTasksShallowInfo, when more recent:
    useEffect(() => {

        if(false && selectedTask !== null && selectedTask.snapshot_time < selectedPipeline.snapshot_time) {

            const t = selectedPipeline.allTasksShallowInfo.find(t => t.key === selectedTask.key)

            setSelectedTask({
                ...selectedTask,
                state: t.state,
                snapshot_time: selectedPipeline.snapshot_time
            })
        }

    }, [selectedPipeline.snapshot_time])


    const popupTaskDetailsView = task => {
        setTaskPopupProgress(1)
        observeTask(pipelineList.selectedPipelineDir, task.key)
    }


    const taskGroupSelect = () =>
        <div className="field">
            <div className="control has-icons-left">
                <div className="select is-primary">
                    <select value={taskFilter.keyPrefix || ''} onChange={e =>
                        taskFilterDispatcher({
                            name: 'selectOtherTaskGroup',
                            keyPrefix: e.target.value
                        })
                    }>
                        <option key={'null'}>show all</option>

                        {
                            selectedPipeline.rowsData.map(taskGroup =>
                                <option key={taskGroup.taskGroupKey}>{taskGroup.taskGroupKey}</option>
                            )
                        }
                    </select>
                </div>

                <span className="icon is-small is-left">
                  <i className="fas fa-globe"/>
                </span>
            </div>
        </div>

    const ToggleIsSelected = ({displayState}) => {

        const classes = () => {
            if(displayState === taskFilter.displayState) {
                return "button is-selected is-active is-focused"
            }
            return "button"
        }

        return <button
            className={classes()}
            onClick={() => taskFilterDispatcher({name: 'selectOtherDisplayState', displayState})}
        >{displayState}</button>
    }

    const displayStateToggles = () => <>
        <div className="buttons has-addons">
            <button className={`button ${taskFilter === null ? 'is-selected is-active is-focused': ''}`}
                onClick={() => taskFilterDispatcher({name: 'selectOtherDisplayState', displayState: null})}
            >show all</button>
            <ToggleIsSelected displayState="waiting"/>
            <ToggleIsSelected displayState="running"/>
            <ToggleIsSelected displayState="completed"/>
            <ToggleIsSelected displayState="failed"/>
            <ToggleIsSelected displayState="ignored"/>
        </div>
    </>


    const filteredTaskStates = taskStates.filter(taskState => {

        const keyMatches = () => taskState.key.startsWith(taskFilter.keyPrefix) || taskFilter.keyPrefix === 'show all'

        const stateSelected = () => {

            if (taskFilter.displayState === null) {
                return true
            }
            const ds = statesToDisplayStateMap[taskState.state_name]
            return taskFilter.displayState === ds
        }

        if(taskFilter.keyPrefix === null) {
            return stateSelected()
        }
        else if(keyMatches()) {
            return stateSelected()
        }
    })


    const taskTable = () => {
        if(filteredTaskStates.length === 0) {
            return <span>no task in {taskFilter.displayState} state</span>
        }

        return <table className="table is-bordered is-fullwidth is-hoverable task-list">
            <thead>
                <tr>
                    <td>Key</td>
                    <td></td>
                    <td></td>
                </tr>
            </thead>
            <tbody>
            {
                filteredTaskStates.map(taskState =>
                    <tr key={taskState.key} onClick={() => popupTaskDetailsView(taskState)}>
                        <td onClick={e => e.preventDefault()}>{taskState.key}</td>
                        <td onClick={e => e.preventDefault()}>

                            {taskState.state_name}
                        </td>
                        <td onClick={e => e.preventDefault()}>
                            {taskState.action && <span className="tag is-warning">{taskState.action}</span>}
                        </td>
                    </tr>
                )
            }
            </tbody>
        </table>
    }

    const taskDetailDialog = () => {

        const close = () => {
            unObserveTask()
            setSelectedTask(null)
        }

        return <div className="modal is-active">
            <div className="modal-background"/>
            <div className="modal-card" style={{width:1100}}>
                <header className="modal-card-head">
                    <p className="modal-card-title">Task [ key={selectedTask.key} ]</p>
                    <button className="delete" aria-label="close" onClick={close}/>
                </header>
                <section className="modal-card-body">
                    <div className="columns">
                        <div className="column">
                            <TaskView task={selectedTask} socket={socket} selectedPipeline={selectedPipeline}/>
                        </div>
                    </div>
                </section>
                <footer className="modal-card-foot">
                    <button className="button is-default" onClick={close}>close</button>
                </footer>
            </div>
            <button
                className="modal-close is-large"
                aria-label="close"
                onClick={close}
            />
        </div>
    }


    return <>
        {selectedTask && taskDetailDialog()}
        <div className="columns">
            <div className="column">
                <div className="columns">
                    <div className="column">
                        <button className="button" onClick={closeFunc}>Back to pipeline list</button>
                    </div>
                    <div className="column">

                        {taskGroupSelect()}
                    </div>
                    <div className="column">
                        {displayStateToggles()}
                    </div>
                </div>
            </div>
        </div>
        <div className="columns">
            <div className="column">
                {taskTable()}
            </div>
        </div>
    </>
}



export default TasksView