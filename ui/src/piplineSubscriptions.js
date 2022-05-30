import React, {useReducer, useEffect, useState} from "react";

import { io } from "socket.io-client"

export const usePipelineWebsocketSubscription = ({onSocketCreate}) => {

    const [socket, setSocket] = useState(null)

    const [socketId, setSocketId] = useState(null)

    const [subscription, setSubscription] = useState(null)

    const [observedPipelineId, setObservedPipelineId] = useState(null)

    const [observedTaskKey, setObservedTaskKey] = useState(null)

    useEffect(() => {

        const socket = io(location.protocol + '//' + document.domain + ':' + location.port + "/drypipe", {reconnectionDelay: 5000})

        if(onSocketCreate) {
            onSocketCreate(socket)
        }

        setSocket(socket)

        socket.on('subscription_update_ack', ack_message => {

            setSubscription(ack_message.updated_subscription)

            if(ack_message.action_name === "unObservePipeline") {
                setObservedPipelineId(null)
            }
            else if(ack_message.action_name === "unObserveTask") {
                setObservedTaskKey(null)
            }
        })

        socket.on("latestPipelineDetailedState", o => {
            setObservedPipelineId(o.pipelineDir)
        })

        socket.on("latestTaskDetails", o => {
            setObservedTaskKey(o.key)
        })

        socket.on('connect', () => {
            console.log("connected")
            setSocketId(socket.id)
            socket.emit("update_subscription", {
                name: "init",
                sid: socket.id
            })
        })

        socket.on('disconnect', () => {
            console.log("disconnected")
        })

        return () => {

            socket.emit("update_subscription", {
                name: "close",
                sid: socket.id
            })

            socket.disconnect()
        }
    }, [])

    const ensureConnected = () => {
        if(socket === null) {
            throw Error("can't call this hook when not connected")
        }
    }

    const observePipeline = pid => {
        ensureConnected()
        socket.emit("update_subscription", {
            name: "observePipeline",
            sid: socket.id,
            pid
        })
    }

    const unObservePipeline = () => {
        ensureConnected()
        socket.emit("update_subscription", {
            name: "unObservePipeline",
            sid: socket.id
        })
    }

    const observeTask = (pid, task_key) => {
        ensureConnected()
        socket.emit("update_subscription", {
            name: "observeTask",
            sid: socket.id,
            task_key: `${pid}|${task_key}`
        })
    }

    const unObserveTask = () => {
        ensureConnected()
        socket.emit("update_subscription", {
            name: "unObserveTask",
            sid: socket.id
        })
    }


    return {
        isConnected: subscription !== null,
        socket,
        socketId,
        observePipeline,
        observedPipelineId,
        unObservePipeline,
        observeTask,
        observedTaskKey,
        unObserveTask
    }
}