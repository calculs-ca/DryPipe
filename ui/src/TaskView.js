import React, {useEffect, useReducer, useRef, useState} from "react";

const taskHistory = historyRows => {

    return <table className="table is-bordered is-fullwidth is-hoverable">
        <thead>
        <tr>
            <th colSpan={2}>History</th>
        </tr>
        </thead>
        <tbody>
        {
            historyRows.map((event, idx) => {
                return <tr key={`${idx}`}>
                    <td>{event.eventName}</td>
                    <td>{event.durationDisplay}
                        {event.timeAgo &&
                            <i style={{opacity: 0.5, marginLeft:4}}>{event.timeAgo}</i>
                        }
                    </td>
                </tr>
            })
        }
        </tbody>
    </table>
}

const psTable = psRows => {

    const [headers, row] = psRows

    return <table className="table is-bordered is-fullwidth is-hoverable">
        <thead>
        <tr>
            {
                headers.map(h => <th key={`h-${h}`}>{h}</th>)
            }
        </tr>
        </thead>
        <tbody>
            {
                row.map(h => <td key={`r-${h}`}>{h}</td>)
            }
        </tbody>
    </table>
}

const useTailLikeScrollToBottom = text => {

    const textAreaRef = useRef()

    useEffect(() => {
        if (textAreaRef.current && document.activeElement !== textAreaRef.current) {
            textAreaRef.current.scrollTop = textAreaRef.current.scrollHeight
        }
    }, [text])

    return textAreaRef
}

const TaskView = ({task, socket, selectedPipeline}) => {

    const [actionEmitted, setActionEmitted] = useState(null)

    const outLogRef = useTailLikeScrollToBottom(task.out)

    const errLogRef = useTailLikeScrollToBottom(task.err)

    useEffect(() => {
        setActionEmitted(null)
    }, [task.action])


    const pauseTask = () => {
        setActionEmitted("pause")
        socket.emit('submitTaskAction', {
            task_key: task.key,
            pipeline_dir: selectedPipeline.dir,
            action_name: 'pause',
            is_cancel: isPaused
        })
    }

    const restartTask = stepNumber => {
        setActionEmitted("restart")
        socket.emit('submitTaskAction', {
            task_key: task.key,
            pipeline_dir: selectedPipeline.dir,
            action_name: 'restart',
            step: stepNumber
        })
    }

    const killTask = () => {
        setActionEmitted("killTask")
        socket.emit('submitTaskAction', {
            task_key: task.key,
            pipeline_dir: selectedPipeline.dir,
            action_name: 'kill'
        })
    }


    const isPaused = task.action === "pause"
    const pauseEmitted = actionEmitted === "pause"

    const canPause = task.state !== "completed" && task.state !== "completed-unsigned"

    const pauseButton = () =>
        <button
            className={`button`}
            disabled={pauseEmitted || (task.action && task.action !== "pause") || (!canPause)}
            onClick={pauseTask}
        >
            Pause{isPaused && "d (click to unpause)"} {pauseEmitted && "..."}
        </button>

    const canRestart = () => {

        if(actionEmitted || isPaused) {
            return false
        }

        switch(task.state) {
            case "timed-out":
            case "failed":
            case "completed":
            case "killed":
                return true
            default:
                return false
        }
    }

    const isRestarting = task.action === "restart"

    const restartButton = () =>
        <button
            className={`button`}
            disabled={isRestarting || (!canRestart())}
            onClick={() => restartTask(0)}
        >
            Restart {pauseEmitted && "..."}
        </button>

    const killButton = () =>
        <button
            className={`button`}
            disabled={task.state === "killed"}
            onClick={killTask}
        >
            Kill
        </button>

    return <>
        <div className="columns">
            <div className="column">
                <label>State: {task.state}</label>
                {task.action && (!isPaused) &&
                    <progress className="progress is-small is-info" max="100">15%</progress>
                }
            </div>
            <div className="column">
                <label>Step: {task.step}</label>
            </div>
        </div>
        <div className="columns">
            <div className="column">
                {pauseButton()}
                {restartButton()}
                {killButton()}
            </div>
        </div>
        {
            task.missing_deps && task.missing_deps.length > 0 && <div className="columns">
                <div className="column">
                    <label>Missing upstream deps</label>
                    <ul>
                        {task.missing_deps.map(dep => <li>{dep}</li>)}
                    </ul>
                </div>
            </div>
        }
        <div className="columns">
            <div className="column">
                {task.control_err && <label className="has-text-danger">{task.control_err}</label>}
            </div>
        </div>
        <div className="field">
            <label className="label">out.log</label>
            <div className="control">
                <textarea
                    ref={outLogRef}
                    style={{caretColor: "transparent"}}
                    spellCheck="false"
                    className="textarea is-family-monospace has-text-success has-background-dark"
                    onChange={() => {}}
                    value={task.out || ""}
                />
            </div>
        </div>
        <div className="field">
            <label className="label">err.log</label>
            <div className="control">
                <textarea
                    ref={errLogRef}
                    style={{caretColor: "transparent"}}
                    spellCheck="false"
                    className="textarea is-family-monospace  has-text-danger has-background-dark"
                    onChange={() => {}}
                    value={task.err || ""}
                />
            </div>
        </div>
        <div className="field">
            <label className="label">drypipe.log</label>
            <div className="control">
                <textarea
                    ref={errLogRef}
                    style={{caretColor: "transparent"}}
                    spellCheck="false"
                    className="textarea is-family-monospace  has-text-info has-background-dark"
                    onChange={() => {}}
                    value={task.drypipe_log || ""}
                />
            </div>
        </div>
        <div>
            {psTable(task.ps)}
        </div>
        <div>
            {taskHistory(task.history)}
        </div>
    </>
}


export default TaskView